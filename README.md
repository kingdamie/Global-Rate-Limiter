# Global Rate Limiter as a Service (GRLaaS)

## What This Document Is

This explains how I designed a system that stops our company's many
servers from accidentally sending too many requests to outside APIs (like
banks or LLM providers). Those outside APIs only allow us a certain number
of requests per minute, and if we go over, they start blocking us or
charging us extra.

The idea: instead of every server guessing on its own how much it's
allowed to send, all servers ask one shared "referee" first. The referee
keeps the real count and says yes or no.

A quick honest note: this document describes the full design I was
aiming for. The multi-tenant `/v1/check` endpoint (Section 2) is really
built and working. The atomic counting (Section 4), the Redis-down
fallback (Section 5), and the background logging (Section 6) are
designed here but not finished in code yet.

---

## 1. The Big Picture (How the Pieces Fit Together)

There are three main parts:

```
   Our microservices (many copies running at once)
                 │
                 │  "Can I send this request?"
                 ▼
        The Rate Limiter Service  (the referee)
                 │
                 ▼
        Redis  (a super-fast memory store —
                the referee's notepad)
                 │
                 │  (sent in the background, doesn't slow anything down)
                 ▼
        A queue (Kafka or RabbitMQ)
                 │
                 ▼
        A history database (ClickHouse or TimescaleDB)
                 │
                 ▼
        A dashboard people can look at later
```

**Why these three tools and not others?**

- **Redis** — This is where the referee keeps the actual "how many
  requests so far" numbers. It's used because it's a database that lives
  in computer memory (RAM) instead of on a disk, so it answers almost
  instantly. We need speed here because every single request has to check
  in with the referee, and that check must happen in under 3 milliseconds.

- **Kafka or RabbitMQ (a queue)** — Every time the referee makes a
  decision, we want to remember it for later (for billing and reports).
  But writing that history record shouldn't slow down the actual decision.
  So instead of writing it directly, the referee just drops a note in a
  queue and moves on immediately. Something else picks up the note later,
  whenever it has time.

- **ClickHouse or TimescaleDB** — This is where all those notes end up
  permanently stored, in a way that's easy to search through later — like
  "show me all requests from the last 30 days." Regular databases are
  slow at this kind of search across huge amounts of time-based data, so
  we use a database built specifically for that.

The rate limiter service itself doesn't remember anything on its own
between requests — all the real memory lives in Redis. This means we can
run as many copies of the rate limiter as we want, and they don't need to
talk to each other. They all just talk to the same Redis.

---

## 2. Handling Lots of Different Clients and Different APIs at Once

We're not just rate-limiting one thing — we've got many clients (like
"Client-A" or "Client-B"), and each of them calls many different outside
APIs (like "Provider-X" or "Provider-Y"). And each client-API pair can
have its own completely different limit. For example:

- Client-A calling Provider-X → allowed 100 requests per minute
- Client-B calling Provider-X → allowed 5,000 requests per minute
- Client-A calling Provider-Y → allowed 50 requests per minute

So the referee can't just keep one big number. It needs a separate little
notepad entry for every single (client, API) combination.

**How I keep them separate:** every time a request comes in, I glue the
client's ID and the API's ID together into one text string, like:

```
ratelimit:{client_id}:{api_resource_id}

example: ratelimit:client-a:provider-x
```

That glued-together string becomes the "name" of the notepad entry in
Redis. Since Client-A and Client-B get different names, their counts
never bump into each other, even though they might be hitting the same
outside API.

**Where do the actual limit numbers (100/min, 5,000/min, etc.) live?**
They're kept in a small separate settings area in Redis (basically a
lookup table), not hard-coded into the referee's code. Something like:

```
limit-settings:client-a:provider-x  →  100 per minute
limit-settings:client-b:provider-x  →  5,000 per minute
```

This matters because it means someone can change Client-B's limit from
5,000 to 10,000 whenever the business deal changes, without redeploying
any code — they just update that one entry, and the very next request
picks up the new number. The referee reads this settings entry once per
check (Redis is fast enough that this doesn't hurt our 3ms budget), so
changes take effect basically immediately for everybody, everywhere.

---

## 3. Which Counting Method Did I Pick, and Why

There are four common ways to count requests and decide "yes" or "no."
I compared them like this:

| Method | Memory per client-API pair | Lets you burst? | Matches how real APIs count | My decision |
|---|---|---|---|---|
| Token Bucket | ~2 small numbers (~16-24 bytes) | Yes | Sort of | Used only as a backup plan (see Section 5) |
| Leaky Bucket | ~2 small numbers (~16-24 bytes) | No | No | Not used |
| Sliding Window Log | 1 timestamp PER REQUEST — keeps growing (could be many KB per busy client per minute) | Yes, exactly | Perfectly | Not used |
| **Sliding Window Counter** | **~3 small numbers (~24-32 bytes)** | **Yes, safely** | **Very closely** | **My main choice** |

**Why I picked Sliding Window Counter:**

It only needs to remember two or three small numbers per client (like "80
requests last minute" and "30 requests so far this minute" — that's just
two integers, roughly 24-32 bytes total once you add a timestamp, no
matter how much traffic that client sends). When a new request comes in,
it does simple math to guess how many requests happened in the last 60
seconds, by blending those two numbers together based on the clock.
Basically: count all of the current minute, plus a fair chunk of the
previous minute, based on how much of that old minute is still inside our
60-second window.

Compare that to Sliding Window Log, which has to remember the exact
timestamp of every single request that's still inside the window. For a
client sending 5,000 requests a minute, that's 5,000 separate timestamps
(roughly 8 bytes each, so ~40KB) sitting in memory PER CLIENT-API PAIR,
at all times, forever, as long as traffic keeps up. Multiply that by
hundreds of clients and APIs and it adds up to a real memory problem.
Sliding Window Counter stays flat at ~32 bytes no matter how much traffic
there is — that's the whole reason I picked it.

This method is cheap on memory (unlike the Log method, which writes down
every single request and gets huge fast), and it's still very accurate —
much more accurate than just resetting a counter every minute, which has
a weak spot where someone could sneak in double the allowed amount right
at the minute boundary.

**Why I didn't pick the others:**

- **Leaky Bucket** forces every request into a slow, steady, single-file
  line, even if the client hasn't used up their limit yet. That fights
  against the goal of letting clients use their full allowed speed.
- **Sliding Window Log** is the most accurate of all four, because it
  writes down the exact time of every single request. But that means the
  more traffic we get, the more it has to remember — for thousands of
  clients, this gets huge and slow. Too expensive for what we need.
- **Token Bucket** is good and cheap, but on its own it can let a client
  sneak in a big burst right at a time-window boundary, so I didn't use it
  as the main method. However, it works great as a simple backup plan when
  the main system is down (explained in Section 5).

---

## 4. How We Avoid Two Servers Messing Up the Same Count at Once

**The problem:** Imagine two servers both check the counter at the exact
same moment. Both see "99 out of 100 used — still room!" Both go ahead.
Now we're at 101, over the limit. This is called a race condition — it
happens because there's a tiny gap between "checking the number" and
"updating the number," and two things can sneak into that gap at once.

**The fix I chose:** Instead of locking things (basically making
everyone else wait in line while one server updates the number, which
would slow everything down), I use something Redis supports called a Lua
script.

A Lua script lets us bundle "check the number, do the math, and update
the number" into one single instruction that Redis runs all at once,
start to finish, without letting any other request sneak in the middle.
Redis naturally processes one thing at a time anyway, so this gives us
safety without needing a slow locking system.

**Why not use "Compare-And-Swap" instead?** That's another common trick —
read a number, then try to save your new number back, but only if nobody
else changed it in the meantime; if they did, you try again. This works
okay when traffic is light, but if lots of servers are hammering the same
counter at once, they'd keep bumping into each other and retrying over
and over, which would slow things down unpredictably. The single
all-at-once Lua script avoids that problem entirely.

**One more small thing:** each server keeps a small pool of
already-open connections to Redis, instead of opening a brand new
connection every single time it wants to ask a question. Opening
connections takes time, and we don't have time to spare if we want to
stay under 3 milliseconds.

---

## 5. What Happens If Redis Goes Down

**The rule:** if the referee's notepad (Redis) becomes unreachable, we
must NOT just block everyone's requests. That would cause the exact
disaster we're trying to prevent — a small outage turning into a much
bigger one. Instead, the system should "fail open," meaning: let requests
through using a safer, simpler backup plan, instead of stopping
everything.

**How the backup plan works, step by step:**

1. Each server keeps checking if Redis is responding. If it stops
   responding for too long, that server switches into "backup mode" on
   its own.

2. In backup mode, the server stops asking Redis anything and instead
   uses its own local memory to track requests — using the simple Token
   Bucket method from Section 3. It doesn't need to share anything with
   other servers to do this.

3. Here's the trick to staying safe even without coordination: ahead of
   time, while Redis was still working fine, each server already learned
   roughly how many other servers exist. So its local backup limit is set
   conservatively — like "the real limit divided by the number of
   servers." That way, even though the servers can't see each other
   anymore, they each only use their own small slice, and together they
   still don't go over the real limit.

4. This does mean we might waste a little bit of unused quota during the
   outage (since the slices are conservative), but that's a fair trade
   for not breaking everything.

5. Once Redis is healthy again, the server quietly switches back to
   asking Redis like normal, and just forgets its temporary backup
   counters — no need to try to "make up" for anything.

Here's that flow simplified:

```
A request comes in
        │
        ▼
Is Redis responding okay?
        │
   ┌────┴────┐
  yes         no
   │           │
   ▼           ▼
Ask Redis   Use my own local
(normal      backup counter
 mode)       (backup mode)
   │           │
   └────┬──────┘
        ▼
  Send back allowed or denied
        │
        ▼
  Quietly log this event in
  the background either way
```

---

## 6. Recording History Without Slowing Anything Down

Every time the referee makes a decision (yes or no), it also sends a
small note describing what happened — who asked, what they asked for,
whether it was allowed, and how long it took. This note is sent to the
queue (Kafka/RabbitMQ) and the referee doesn't wait around to see what
happens to it. If that note fails to send for some reason, the person who
made the original request never notices — their answer already went out.
Later, something else reads all these notes from the queue and saves them
into the history database.

**What can the dashboard actually ask for?** Because the history database
(ClickHouse/TimescaleDB) organizes notes by time automatically, the
dashboard can ask questions like:

- "What was the average response speed for the last 10 days?"
- "How many requests got blocked (denied) per day, over the last 15
  days?"
- "Show me a day-by-day trend of traffic for the last 30 days for
  Client-B."

These all work the same way under the hood: the notes are already
grouped into time buckets (like "one bucket per hour" or "one bucket per
day"), so instead of scanning through millions of individual notes one by
one, the database just adds up the buckets that fall inside whatever
range someone asks for (10 days, 15 days, 30 days, whatever). That's the
whole reason we didn't just dump these notes into a regular database —
a regular database would have to check every single row every time,
which gets really slow once you have months of history piling up.

---

## 7. Why I Only Used These Tools and Nothing Extra

| Tool | Why I needed it |
|---|---|
| Redis | The only realistic choice for something this fast and shareable across all our servers |
| Kafka or RabbitMQ | Keeps the history-logging from slowing down the actual yes/no decision |
| ClickHouse or TimescaleDB | Built specifically for fast searching across time, which is exactly what the dashboard needs |

I intentionally did not add anything extra, like a separate locking
system or an additional caching layer — Redis already does the atomic
counting job on its own, so bringing in more tools would just add
complexity without solving a real problem.
