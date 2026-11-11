/**
 * The memory driver stores what the Redis driver would: the record as JSON,
 * not the dispatcher's objects. A test written against memory then sees what
 * production sees.
 */
import { describe, expect, it } from "vitest";
import { MemoryDriver } from "../../src/drivers/MemoryDriver.js";
import { Job } from "../../src/Job.js";
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

	it("hands a payload's own `__proto__` key over as data, polluting nothing", async () => {
		// What a JSON body or another service's record can carry: an OWN key
		// named `__proto__`. Copied with a spread or `Object.assign` on the way
		// through, it would become a prototype instead of a field.
		const hostile: unknown = JSON.parse('{"__proto__":{"admin":true},"n":1}');
		const seen: unknown[] = [];
		class Inspect extends Job<unknown> {
			async execute(): Promise<void> {
				seen.push(this.payload);
			}
		}
		const queue = new QueueManager(new MemoryDriver());
		queue.register("handler", {
			async handle(payload) {
				seen.push(payload);
			},
		});
		await queue.dispatch("handler", hostile);
		await queue.dispatch(Inspect, hostile);

		await queue.processOne();
		await queue.processOne();

		expect(seen).toHaveLength(2);
		for (const payload of seen) {
			if (typeof payload !== "object" || payload === null) {
				throw new Error("expected an object payload");
			}
			expect(Object.hasOwn(payload, "__proto__")).toBe(true);
			expect(Object.getPrototypeOf(payload)).toBe(Object.prototype);
			expect(Reflect.get(payload, "admin")).toBeUndefined();
		}
		expect(Reflect.get({}, "admin")).toBeUndefined();
		expect(Object.hasOwn(Object.prototype, "admin")).toBe(false);
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
