const express = require("express");
const Redis = require("ioredis");

const app = express();
app.use(express.json());

const DEFAULT_LIMIT = 60; // used only if a client+API pair has no configured limit

const redis = new Redis({
	host: process.env.REDIS_HOST || "localhost",
	port: process.env.REDIS_PORT || 6379,
	retryStrategy: () => 1000,
});

redis.on("connect", () => console.log("Connected to Redis"));
redis.on("error", (err) =>
	console.error("Redis connection problem:", err.message),
);

app.get("/health", (req, res) => {
	res.json({ status: "ok" });
});

app.post("/v1/check", async (req, res) => {
	const { client_id, api_resource_id } = req.body;

	if (!client_id || !api_resource_id) {
		return res
			.status(400)
			.json({ error: "client_id and api_resource_id are required" });
	}

  const key = `ratelimit:${client_id}:${api_resource_id}`;
  const limitKey = `limit-settings:${client_id}:${api_resource_id}`;

  const count = await redis.incr(key);
	if (count === 1) {
		await redis.expire(key, 60);
	}

  const storedLimit = await redis.get(limitKey);
  const limit = storedLimit ? parseInt(storedLimit, 10) : DEFAULT_LIMIT;

	res.json({
		allowed: count <= limit,
		remaining_budget: Math.max(0, limit - count),
		reset_time: Math.floor(Date.now() / 1000) + 60,
	});

});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
	console.log(`Rate limiter listening on port ${PORT}`);
});


