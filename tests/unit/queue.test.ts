import { beforeEach, describe, expect, it } from "vitest";
import { MemoryDriver } from "../../src/drivers/MemoryDriver.js";
import { type JobHandler, QueueManager } from "../../src/QueueManager.js";

describe("queue > MemoryDriver", () => {
	let queue: QueueManager;

	beforeEach(() => {
		queue = new QueueManager(new MemoryDriver());
	});

	it("dispatches a job and processes it through a registered handler", async () => {
		const seen: unknown[] = [];
		queue.register("email.send", {
			async handle(payload) {
				seen.push(payload);
			},
		});

		await queue.dispatch("email.send", { to: "a@b.com" });
		expect(await queue.size()).toBe(1);

		const processed = await queue.processOne();
		expect(processed).toBe(true);
		expect(seen).toEqual([{ to: "a@b.com" }]);
		expect(await queue.size()).toBe(0);
	});

	it("returns false from processOne when the queue is empty", async () => {
		expect(await queue.processOne()).toBe(false);
	});

	it("retries a failing job up to maxAttempts then moves it to failed", async () => {
		let attempts = 0;
		queue.register("flaky", {
			async handle() {
				attempts++;
				throw new Error("boom");
			},
		});

		await queue.dispatch("flaky", null, { maxAttempts: 3 });
		await queue.processOne(); // attempt 1 → retry
		await queue.processOne(); // attempt 2 → retry
		await queue.processOne(); // attempt 3 → fail

		expect(attempts).toBe(3);
		const failed = await queue.failedJobs();
		expect(failed).toHaveLength(1);
		expect(failed[0].error).toBe("boom");
		expect(failed[0].status).toBe("failed");
	});

	it("fails immediately when no handler is registered for the job name", async () => {
		await queue.dispatch("unknown.job", { x: 1 });
		await queue.processOne();
		const failed = await queue.failedJobs();
		expect(failed).toHaveLength(1);
		expect(failed[0].error).toContain("No handler");
	});

	it("accepts class-based handlers", async () => {
		let runs = 0;
		class Handler implements JobHandler {
			async handle() {
				runs++;
			}
		}
		queue.register("class.handler", Handler);
		await queue.dispatch("class.handler", null);
		await queue.processOne();
		expect(runs).toBe(1);
	});

	it("preserves every job under a concurrent dispatch burst", async () => {
		// Fan-in test: dispatch N jobs in parallel and confirm exactly N
		// land on the queue, in the same FIFO order each handler observed.
		// Catches future regressions where an async-shared cursor / counter
		// drops or reorders dispatches under burst load.
		const N = 64;
		const seen: number[] = [];
		queue.register("burst", {
			async handle(payload) {
				seen.push((payload as { i: number }).i);
			},
		});
		await Promise.all(
			Array.from({ length: N }, (_, i) => queue.dispatch("burst", { i })),
		);
		expect(await queue.size()).toBe(N);

		while ((await queue.processOne()) === true) {
			/* drain */
		}
		expect(seen).toHaveLength(N);
		expect(new Set(seen).size).toBe(N);
		// MemoryDriver is FIFO; under no concurrent processing the order
		// must match dispatch order exactly.
		expect(seen).toEqual(Array.from({ length: N }, (_, i) => i));
	});

	it("MemoryDriver trims the failed-jobs buffer past maxFailedJobs", async () => {
		const driver = new MemoryDriver({ maxFailedJobs: 2 });
		const small = new QueueManager(driver);
		small.register("noop", {
			async handle() {
				throw new Error("boom");
			},
		});
		// maxAttempts: 1 → each dispatch fails on its first processOne()
		await small.dispatch("noop", null, { maxAttempts: 1 });
		await small.dispatch("noop", null, { maxAttempts: 1 });
		await small.dispatch("noop", null, { maxAttempts: 1 });
		await small.processOne();
		await small.processOne();
		await small.processOne();
		const failed = await small.failedJobs();
		// 3 jobs failed but only the last 2 are retained — MemoryDriver
		// line 29 (splice trim) is the path under test.
		expect(failed).toHaveLength(2);
	});
});
