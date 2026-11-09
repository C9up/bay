/**
 * Priorities and deduplicated jobs, upstream's shapes, alike on both drivers.
 */
import { describe, expect, it } from "vitest";
import { MemoryDriver } from "../../src/drivers/MemoryDriver.js";
import { RedisDriver } from "../../src/drivers/RedisDriver.js";
import { Job } from "../../src/Job.js";
import { type QueueDriver, QueueManager } from "../../src/QueueManager.js";
import { describeRedis, freshPrefix, useRedis } from "../__helpers__/redis.js";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Run everything due, recording payloads in the order they ran. */
async function drain(queue: QueueManager, seen: unknown[]): Promise<void> {
	queue.register("job", {
		async handle(payload) {
			seen.push(payload);
		},
	});
	while (await queue.processOne(["default", "emails"])) {}
}

function behaviours(makeDriver: () => Promise<QueueDriver>) {
	it("runs a lower priority number first, and keeps FIFO within one", async () => {
		const queue = new QueueManager(await makeDriver());
		await queue.dispatch("job", "low", { priority: 10 });
		await queue.dispatch("job", "normal-1");
		await queue.dispatch("job", "urgent", { priority: 1 });
		await queue.dispatch("job", "normal-2", { priority: 5 });

		const seen: unknown[] = [];
		await drain(queue, seen);

		expect(seen).toEqual(["urgent", "normal-1", "normal-2", "low"]);
		expect(await queue.size()).toBe(0);
	});

	it("keeps a job's priority through its delay and its retries", async () => {
		const driver = await makeDriver();
		const queue = new QueueManager(driver);
		const order: string[] = [];
		let failedOnce = false;
		queue.register("job", {
			async handle(payload) {
				if (payload === "urgent-flaky" && !failedOnce) {
					failedOnce = true;
					throw new Error("first try fails");
				}
				order.push(String(payload));
			},
		});
		await queue.dispatch("job", "urgent-flaky", { priority: 1 });
		await queue.dispatch("job", "urgent-later", { priority: 2, delay: 20 });
		await queue.dispatch("job", "normal");
		await queue.dispatch("job", "normal-2");
		expect(await queue.size()).toBe(4);

		// The urgent job fails and goes back; it still outranks the rest.
		await queue.processOne();
		await sleep(30);
		while (await queue.processOne()) {}

		expect(order).toEqual([
			"urgent-flaky",
			"urgent-later",
			"normal",
			"normal-2",
		]);
	});

	it("queues a deduplicated job once, and answers the id of the one queued", async () => {
		const queue = new QueueManager(await makeDriver());
		const first = await queue.dispatch("job", "a", {
			dedup: { id: "order-1" },
		});
		const second = await queue.dispatch("job", "b", {
			dedup: { id: "order-1" },
		});
		const other = await queue.dispatch("other", "c", {
			dedup: { id: "order-1" },
		});

		expect(second).toBe(first);
		expect(other).not.toBe(first);
		expect(await queue.size()).toBe(2);
	});

	it("releases the dedup id once the job is done, or finally failed", async () => {
		const queue = new QueueManager(await makeDriver());
		queue.register("ok", { async handle() {} });
		queue.register("ko", {
			async handle() {
				throw new Error("boom");
			},
		});
		const done = await queue.dispatch("ok", {}, { dedup: { id: "x" } });
		await queue.processOne();
		const again = await queue.dispatch("ok", {}, { dedup: { id: "x" } });
		expect(again).not.toBe(done);
		await queue.processOne();

		const failed = await queue.dispatch(
			"ko",
			{},
			{ dedup: { id: "y" }, maxAttempts: 1 },
		);
		await queue.processOne();
		const retried = await queue.dispatch("ko", {}, { dedup: { id: "y" } });
		expect(retried).not.toBe(failed);
	});

	it("keeps the dedup id while the job is retried", async () => {
		const queue = new QueueManager(await makeDriver());
		queue.register("ko", {
			async handle() {
				throw new Error("boom");
			},
		});
		const first = await queue.dispatch(
			"ko",
			{},
			{ dedup: { id: "z" }, maxAttempts: 3 },
		);
		await queue.processOne();
		expect(await queue.dispatch("ko", {}, { dedup: { id: "z" } })).toBe(first);
	});

	it("lets the id go after its ttl, and extend restarts the window", async () => {
		const queue = new QueueManager(await makeDriver());
		const first = await queue.dispatch("job", 1, {
			dedup: { id: "t", ttl: 60 },
		});
		await sleep(80);
		const second = await queue.dispatch("job", 2, {
			dedup: { id: "t", ttl: 60 },
		});
		expect(second).not.toBe(first);

		const held = await queue.dispatch("job", 3, {
			dedup: { id: "e", ttl: 80, extend: true },
		});
		await sleep(50);
		expect(
			await queue.dispatch("job", 4, {
				dedup: { id: "e", ttl: 80, extend: true },
			}),
		).toBe(held);
		await sleep(50);
		// 100 ms after the first, but only 50 after the extension.
		expect(
			await queue.dispatch("job", 5, {
				dedup: { id: "e", ttl: 80, extend: true },
			}),
		).toBe(held);
	});
}

describe("bay > priorities and dedup in memory", () => {
	behaviours(async () => new MemoryDriver());
});

describeRedis("bay > priorities and dedup on Redis", () => {
	const redis = useRedis();
	behaviours(
		async () =>
			new RedisDriver(await redis.client(), { prefix: freshPrefix("prio") }),
	);
});

describe("bay > priority and dedup options are checked", () => {
	it("refuses a priority outside 1..10", async () => {
		const queue = new QueueManager(new MemoryDriver());
		for (const bad of [0, 11, 1.5, Number.NaN]) {
			await expect(
				queue.dispatch("job", {}, { priority: bad }),
				String(bad),
			).rejects.toThrow(/priority must be a whole number from 1 to 10/);
		}
		class Odd extends Job {
			static override options = { priority: 42 };
			async execute(): Promise<void> {}
		}
		expect(() => queue.registerJob(Odd)).toThrow(/Odd.*priority/);
	});

	it("refuses a dedup it cannot honour", async () => {
		const queue = new QueueManager(new MemoryDriver());
		await expect(
			queue.dispatch("job", {}, { dedup: { id: "" } }),
		).rejects.toThrow(/dedup.id/);
		await expect(
			queue.dispatch("job", {}, { dedup: { id: "a", extend: true } }),
		).rejects.toThrow(/extend needs a ttl/);
		await expect(
			queue.dispatch("job", {}, { dedup: { id: "a", ttl: 0 } }),
		).rejects.toThrow(/dedup.ttl/);
		expect(await queue.size()).toBe(0);
	});
});
