/**
 * A job option the queue cannot honour is refused where it is written — at
 * dispatch, or when the class is registered — never discovered later as a job
 * that runs forever or silently vanishes.
 */
import { describe, expect, it, vi } from "vitest";
import { MemoryDriver } from "../../src/drivers/MemoryDriver.js";
import { Job } from "../../src/Job.js";
import { QueueManager } from "../../src/QueueManager.js";

const NOT_A_COUNT = [Number.POSITIVE_INFINITY, Number.NaN, 1.5, 0, -1, 2 ** 53];

describe("bay > maxAttempts / maxRetries", () => {
	it("refuses anything but a whole number >= 1 at dispatch", async () => {
		const queue = new QueueManager(new MemoryDriver());
		for (const bad of NOT_A_COUNT) {
			await expect(
				queue.dispatch("x", {}, { maxAttempts: bad }),
				String(bad),
			).rejects.toThrow(/maxAttempts must be a whole number >= 1/);
			await expect(
				queue.dispatch("x", {}, { maxRetries: bad }),
				String(bad),
			).rejects.toThrow(/maxRetries must be a whole number >= 1/);
		}
		expect(await queue.size()).toBe(0);
	});

	it("refuses it on a job class when the class is registered, and when it is dispatched", async () => {
		class Forever extends Job {
			static override options = { maxRetries: Number.POSITIVE_INFINITY };
			async execute(): Promise<void> {}
		}
		const queue = new QueueManager(new MemoryDriver());
		expect(() => queue.registerJob(Forever)).toThrow(
			/Forever.*maxRetries must be a whole number >= 1/,
		);
		await expect(queue.dispatch(Forever, undefined)).rejects.toThrow(
			/maxRetries must be a whole number >= 1/,
		);
		// Refused, not half-registered.
		expect(queue.registeredJobs().has("Forever")).toBe(false);
	});

	it("still takes an ordinary count", async () => {
		const queue = new QueueManager(new MemoryDriver());
		let runs = 0;
		queue.register("x", {
			async handle() {
				runs++;
				throw new Error("boom");
			},
		});
		await queue.dispatch("x", {}, { maxAttempts: 2 });
		while (await queue.processOne()) {}
		expect(runs).toBe(2);
		expect(await queue.failedJobs()).toHaveLength(1);
	});
});

describe("bay > timeout", () => {
	it("refuses a timeout setTimeout cannot hold, at dispatch and at registration", async () => {
		class Month extends Job {
			static override options = { timeout: "30d" };
			async execute(): Promise<void> {}
		}
		const queue = new QueueManager(new MemoryDriver());
		for (const bad of ["30d", 2_592_000_000, 1.5, "0.5ms"]) {
			await expect(
				queue.dispatch("x", {}, { timeout: bad }),
				String(bad),
			).rejects.toThrow(/timeout must be a whole number of milliseconds/);
		}
		expect(() => queue.registerJob(Month)).toThrow(/Month.*timeout/);
		expect(await queue.size()).toBe(0);
	});

	it("still takes the longest one it can hold, and reads 0 as none", async () => {
		const driver = new MemoryDriver();
		const queue = new QueueManager(driver);
		await queue.dispatch("x", {}, { timeout: "24d" });
		await queue.dispatch("y", {}, { timeout: 0 });
		const timeouts: (number | undefined)[] = [];
		for (;;) {
			const job = await driver.pop();
			if (!job) break;
			timeouts.push(job.timeout);
		}
		expect(timeouts).toEqual([24 * 86_400_000, undefined]);
	});

	it("does not fail a slow attempt at once when the timeout is long", async () => {
		// Both the handler's 20 ms and the 24 d timeout run on the fake clock:
		// a timeout clamped to 1 ms fires first when the clock moves.
		vi.useFakeTimers();
		try {
			const queue = new QueueManager(new MemoryDriver());
			queue.register("slow", {
				async handle() {
					await new Promise((resolve) => setTimeout(resolve, 20));
				},
			});
			await queue.dispatch("slow", {}, { timeout: "24d", maxAttempts: 1 });
			const processing = queue.processOne();
			await vi.advanceTimersByTimeAsync(20);
			await processing;
			expect(await queue.failedJobs()).toEqual([]);
		} finally {
			vi.useRealTimers();
		}
	});
});
