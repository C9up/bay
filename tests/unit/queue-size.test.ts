/**
 * `size()` and `sizeOf(queue)` answer the same thing on every driver: the
 * default queue for the first, the named one for the second, delayed jobs
 * included in both.
 */
import { describe, expect, it } from "vitest";
import { MemoryDriver } from "../../src/drivers/MemoryDriver.js";
import { RedisDriver } from "../../src/drivers/RedisDriver.js";
import { type QueueDriver, QueueManager } from "../../src/QueueManager.js";
import { FakeQueue } from "../../src/testing/FakeQueue.js";
import { describeRedis, freshPrefix, useRedis } from "../__helpers__/redis.js";

async function counts(driver: QueueDriver) {
	const queue = new QueueManager(driver);
	await queue.dispatch("a", {});
	await queue.dispatch("b", {}, { delay: 60_000 });
	await queue.dispatch("c", {}, { queue: "emails" });
	await queue.dispatch("d", {}, { queue: "emails", delay: 60_000 });
	await queue.dispatch("e", {}, { queue: "emails" });
	return {
		size: await queue.size(),
		emails: await queue.sizeOf("emails"),
		default: await queue.sizeOf("default"),
		other: await queue.sizeOf("other"),
	};
}

const expected = { size: 2, emails: 3, default: 2, other: 0 };

describe("bay > queue size", () => {
	it("counts the default queue for size() and a named one for sizeOf() in memory", async () => {
		expect(await counts(new MemoryDriver())).toEqual(expected);
	});

	it("counts the same way in the fake", async () => {
		expect(await counts(new FakeQueue())).toEqual(expected);
	});

	it("says so when a driver cannot count a named queue", async () => {
		const memory = new MemoryDriver();
		const minimal: QueueDriver = {
			push: (job) => memory.push(job),
			pop: () => memory.pop(),
			fail: (job, error) => memory.fail(job, error),
			complete: (job) => memory.complete(job),
			retry: (job) => memory.retry(job),
			failed: () => memory.failed(),
			size: () => memory.size(),
		};
		const queue = new QueueManager(minimal);
		expect(await queue.sizeOf("default")).toBe(0);
		await expect(queue.sizeOf("emails")).rejects.toThrow(
			expect.objectContaining({
				code: "E_BAY_UNSUPPORTED_OPERATION",
				message: expect.stringMatching(/sizeOf/),
			}),
		);
	});
});

describeRedis("bay > queue size on Redis", () => {
	const redis = useRedis();

	it("counts exactly like the memory driver", async () => {
		const driver = new RedisDriver(await redis.client(), {
			prefix: freshPrefix("size"),
		});
		expect(await counts(driver)).toEqual(expected);
	});
});
