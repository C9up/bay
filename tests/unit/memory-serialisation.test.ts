/**
 * The memory driver stores what the Redis driver would: the record as JSON,
 * not the dispatcher's objects. A test written against memory then sees what
 * production sees.
 */
import { describe, expect, it } from "vitest";
import { MemoryDriver } from "../../src/drivers/MemoryDriver.js";
import { QueueManager } from "../../src/QueueManager.js";

describe("bay > MemoryDriver serialises like Redis", () => {
	it("does not let the dispatcher change a job after it was queued", async () => {
		const queue = new QueueManager(new MemoryDriver());
		const seen: unknown[] = [];
		queue.register("charge", {
			async handle(payload) {
				seen.push(payload);
			},
		});
		const payload = { amount: 10 };
		await queue.dispatch("charge", payload);
		payload.amount = 999_999;

		await queue.processOne();

		expect(seen).toEqual([{ amount: 10 }]);
	});

	it("hands a handler JSON: a Date arrives as its ISO string, undefined fields are gone", async () => {
		const queue = new QueueManager(new MemoryDriver());
		const seen: unknown[] = [];
		queue.register("x", {
			async handle(payload) {
				seen.push(payload);
			},
		});
		await queue.dispatch("x", { at: new Date(0), gone: undefined });

		await queue.processOne();

		expect(seen).toEqual([{ at: "1970-01-01T00:00:00.000Z" }]);
	});

	it("refuses at dispatch what Redis could not store", async () => {
		const queue = new QueueManager(new MemoryDriver());
		await expect(queue.dispatch("x", { id: 1n })).rejects.toThrow(/BigInt/);
		expect(await queue.size()).toBe(0);
	});

	it("keeps the failed list as copies too", async () => {
		const driver = new MemoryDriver();
		const queue = new QueueManager(driver);
		queue.register("x", {
			async handle() {
				throw new Error("boom");
			},
		});
		await queue.dispatch("x", { n: 1 }, { maxAttempts: 1 });
		await queue.processOne();

		const [first] = await driver.failed();
		if (first === undefined) throw new Error("expected a failed job");
		first.error = "rewritten";
		expect((await driver.failed())[0]?.error).toBe("boom");
	});
});
