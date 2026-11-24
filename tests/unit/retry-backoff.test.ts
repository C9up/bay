/**
 * A failed attempt waits before the next one, by the strategy the job asks
 * for — upstream's `retry.backoff` with its exponential, linear, fixed and
 * custom strategies.
 */
import { describe, expect, it } from "vitest";
import {
	BackoffStrategy,
	customBackoff,
	exponentialBackoff,
	fixedBackoff,
	linearBackoff,
} from "../../src/backoff.js";
import { MemoryDriver } from "../../src/drivers/MemoryDriver.js";
import { RedisDriver } from "../../src/drivers/RedisDriver.js";
import { Job } from "../../src/Job.js";
import { type QueueDriver, QueueManager } from "../../src/QueueManager.js";
import { describeRedis, freshPrefix } from "../__helpers__/redis.js";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe("bay > backoff strategies", () => {
	it("grows exponentially up to the cap", () => {
		const strategy = exponentialBackoff({
			baseDelay: 100,
			maxDelay: 1000,
			jitter: false,
		})();
		expect([1, 2, 3, 4, 5].map((n) => strategy.calculateDelay(n))).toEqual([
			100, 200, 400, 800, 1000,
		]);
	});

	it("grows linearly, or stays fixed", () => {
		const linear = linearBackoff({ baseDelay: "1s", maxDelay: "3s" })();
		expect([1, 2, 3, 4].map((n) => linear.calculateDelay(n))).toEqual([
			1000, 2000, 3000, 3000,
		]);
		const fixed = fixedBackoff("250ms")();
		expect([1, 5].map((n) => fixed.calculateDelay(n))).toEqual([250, 250]);
	});

	it("keeps jitter within a quarter either side", () => {
		const strategy = customBackoff({
			strategy: "fixed",
			baseDelay: 1000,
			jitter: true,
		})();
		for (let i = 0; i < 50; i++) {
			const delay = strategy.calculateDelay(1);
			expect(delay).toBeGreaterThanOrEqual(750);
			expect(delay).toBeLessThanOrEqual(1250);
		}
	});

	it("refuses a configuration that cannot back off", () => {
		expect(() => fixedBackoff(0)()).toThrow(
			expect.objectContaining({
				code: "E_INVALID_BASE_DELAY",
				message:
					"Invalid base delay. Reason: Base delay must be a positive integer greater than zero",
			}),
		);
		expect(() => linearBackoff({ baseDelay: "5s", maxDelay: "1s" })()).toThrow(
			expect.objectContaining({
				code: "E_INVALID_MAX_DELAY",
				message:
					"Invalid max delay. Reason: Max delay should be greater than base delay",
			}),
		);
		expect(() =>
			exponentialBackoff({ baseDelay: 100, multiplier: 0.5 })(),
		).toThrow(
			expect.objectContaining({
				code: "E_INVALID_MULTIPLIER",
				message:
					"Invalid multiplier. Reason: Exponential strategy multiplier should be >= 1",
			}),
		);
		expect(() =>
			new BackoffStrategy({ strategy: "fixed", baseDelay: 10 }).calculateDelay(
				0,
			),
		).toThrow(
			expect.objectContaining({
				code: "E_BAY_INVALID_ATTEMPTS",
				message: expect.stringMatching(/attempt/),
			}),
		);
	});
});

async function retriesWithBackoff(driver: QueueDriver): Promise<number[]> {
	const runs: number[] = [];
	const started = Date.now();
	class Flaky extends Job {
		static override options = {
			maxRetries: 3,
			retry: { backoff: fixedBackoff(60) },
		};
		async execute(): Promise<void> {
			runs.push(Date.now() - started);
			throw new Error("downstream is down");
		}
	}
	const queue = new QueueManager(driver);
	await queue.dispatch(Flaky, undefined);

	expect(await queue.processOne()).toBe(true);
	// Not at the head of the line: the next attempt is not due yet.
	expect(await queue.processOne()).toBe(false);
	expect(await queue.size()).toBe(1);

	for (let i = 0; i < 20 && runs.length < 3; i++) {
		await sleep(20);
		await queue.processOne();
	}
	return runs;
}

describe("bay > a retry waits for its backoff", () => {
	it("in memory", async () => {
		const runs = await retriesWithBackoff(new MemoryDriver());
		expect(runs).toHaveLength(3);
		expect(runs[1] ?? 0).toBeGreaterThanOrEqual(55);
		expect((runs[2] ?? 0) - (runs[1] ?? 0)).toBeGreaterThanOrEqual(55);
	});

	it("carries the strategy on the record, so a worker that never saw the class honours it", async () => {
		const driver = new MemoryDriver();
		const dispatcher = new QueueManager(driver);
		await dispatcher.dispatch(
			"by-name",
			{},
			{
				retry: {
					backoff: exponentialBackoff({ baseDelay: 5000, jitter: false }),
				},
			},
		);
		const worker = new QueueManager(driver);
		worker.register("by-name", {
			async handle() {
				throw new Error("boom");
			},
		});
		await worker.processOne();

		// Parked for the five seconds the dispatcher asked for.
		expect(await worker.processOne()).toBe(false);
		expect(await driver.size()).toBe(1);
	});

	it("still retries at once without a backoff", async () => {
		const queue = new QueueManager(new MemoryDriver());
		let runs = 0;
		queue.register("x", {
			async handle() {
				runs++;
				throw new Error("boom");
			},
		});
		await queue.dispatch("x", {}, { maxAttempts: 2 });
		await queue.processOne();
		await queue.processOne();
		expect(runs).toBe(2);
	});
});

describeRedis("bay > a retry waits for its backoff on Redis", (redis) => {
	it("through the delayed set", async () => {
		const driver = new RedisDriver(await redis.client(), {
			prefix: freshPrefix("backoff"),
		});
		const runs = await retriesWithBackoff(driver);
		expect(runs).toHaveLength(3);
		expect(runs[1] ?? 0).toBeGreaterThanOrEqual(55);
	});
});
