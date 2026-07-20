const Redis = require("ioredis");

const redis = new Redis({
	host: process.env.REDIS_HOST || "localhost",
	port: process.env.REDIS_PORT || 6379,
});

// Example limits, matching the spec's sample scenario.
const limits = [
	{ client_id: "client-a", api_resource_id: "provider-x", limit: 100 },
	{ client_id: "client-b", api_resource_id: "provider-x", limit: 5000 },
	{ client_id: "client-a", api_resource_id: "provider-y", limit: 50 },
];

async function seed() {
	for (const { client_id, api_resource_id, limit } of limits) {
		const key = `limit-settings:${client_id}:${api_resource_id}`;
		await redis.set(key, limit);
		console.log(`Set ${key} = ${limit}`);
	}
	await redis.quit();
}

seed();
