/**
 * Every option the Redis driver takes is reachable from `config/queue.ts`,
 * which builds the driver through `drivers.redis()` and nowhere else.
 */
import { afterEach, describe, expect, it } from "vitest";
import { drivers } from "../../src/adapters.js";
import {
	type RedisClient,
	RedisDriver,
} from "../../src/drivers/RedisDriver.js";
import type { JobRecord } from "../../src/QueueManager.js";
import { describeRedis, freshPrefix, useRedis } from "../__helpers__/redis.js";

const inert = (): RedisClient => ({
	eval: async () => null,
	rpush: async () => 0,
	zadd: async () => 0,
	llen: async () => 0,
	zcard: async () => 0,
	lrange: async () => [],
});

function makeJob(id: string): JobRecord {
	return {
		id,
		name: "work",
		payload: {},
		attempts: 0,
		maxAttempts: 3,
		status: "pending",
		createdAt: Date.now(),
	};
}

describe("bay > drivers.redis() forwards the driver's options", () => {
	it("hands each bound to the driver, which validates it", () => {
		expect(() =>
			drivers.redis({ connection: inert(), maxStalledCount: -1 })(),
		).toThrow(/maxStalledCount/);
		expect(() =>
			drivers.redis({ connection: inert(), maxFailedJobs: 1.5 })(),
		).toThrow(/maxFailedJobs/);
		expect(() =>
			drivers.redis({ connection: inert(), visibilityTimeoutMs: 0 })(),
		).toThrow(/visibilityTimeoutMs/);
	});

	describe("in production", () => {
		const original = process.env.NODE_ENV;
		afterEach(() => {
			process.env.NODE_ENV = original;
		});

		it("builds over a client without LMOVE: no opt-in to lose jobs is needed any more", () => {
			process.env.NODE_ENV = "production";
			expect(drivers.redis({ connection: inert() })()).toBeInstanceOf(
				RedisDriver,
			);
		});
	});
});

describeRedis("bay > drivers.redis() options take effect", () => {
	const redis = useRedis();

	it("trims the failed list to maxFailedJobs", async () => {
		const driver = drivers.redis({
			connection: await redis.client(),
			prefix: freshPrefix("adapter-failed"),
			maxFailedJobs: 1,
		})();
		for (const id of ["a", "b", "c"]) {
			await driver.push(makeJob(id));
			const job = await driver.pop();
			if (!job) throw new Error("expected a job");
			await driver.fail(job, "boom");
		}
		expect((await driver.failed()).map((job) => job.id)).toEqual(["c"]);
	});

	it("allows maxStalledCount reclaims before failing the job", async () => {
		const prefix = freshPrefix("adapter-stalled");
		const driver = drivers.redis({
			connection: await redis.client(),
			prefix,
			maxStalledCount: 3,
		})();
		await driver.push(makeJob("poison"));
		let delivered = 0;
		for (let round = 0; round < 6; round++) {
			if (await driver.pop()) delivered++;
			await redis.manager.connection("w0").del(`${prefix}lease:poison`);
			await driver.recoverStale?.();
		}
		expect(delivered).toBe(4);
	});
});
