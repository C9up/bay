/**
 * What happens when a lease runs out — against a real Redis, whose expiry is
 * the thing under test.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { RedisDriver } from "../../src/drivers/RedisDriver.js";
import { type JobRecord, QueueManager } from "../../src/QueueManager.js";
import { describeRedis, freshPrefix, useRedis } from "../__helpers__/redis.js";
import { scripting } from "../__helpers__/scripting.js";

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

describeRedis("bay > stalled jobs", () => {
	const redis = useRedis();
	const connection = () => redis.manager.connection("w0");

	async function drivers(
		label: string,
		options: {
			visibilityTimeoutMs?: number;
			maxStalledCount?: number;
			maxFailedJobs?: number;
		} = {},
	): Promise<{ first: RedisDriver; second: RedisDriver; prefix: string }> {
		const prefix = freshPrefix(label);
		return {
			prefix,
			first: new RedisDriver(await redis.client(0), { prefix, ...options }),
			second: new RedisDriver(await redis.client(1), { prefix, ...options }),
		};
	}

	describe("two recovery passes over the same job", () => {
		it("deliver it once, and report it to exactly one of them", async () => {
			const { first, second, prefix } = await drivers("twice");
			await first.push(makeJob("a"));
			await first.pop();
			await connection().del(`${prefix}lease:a`);

			const counts = await Promise.all([
				first.recoverStale(),
				second.recoverStale(),
			]);

			expect(counts.reduce((total, n) => total + n, 0)).toBe(1);
			expect(await connection().llen(`${prefix}pending`)).toBe(1);
			expect(await connection().llen(`${prefix}processing`)).toBe(0);
		});
	});

	describe("a job that keeps killing its worker", () => {
		it("is filed as failed rather than reclaimed forever", async () => {
			const { first, prefix } = await drivers("poison");
			await first.push(makeJob("poison"));

			const delivered: string[] = [];
			for (let round = 0; round < 5; round++) {
				const job = await first.pop();
				if (job) delivered.push(job.id);
				// The process died; the lease goes with it.
				await connection().del(`${prefix}lease:poison`);
				await first.recoverStale();
			}

			// Upstream's default: reclaimed once, then failed. A crash never
			// reaches the failure path, so `attempts` cannot bound this.
			expect(delivered).toEqual(["poison", "poison"]);
			expect(await connection().llen(`${prefix}pending`)).toBe(0);
			const failed = await first.failed();
			expect(failed.map((job) => job.stalledCount)).toEqual([2]);
		});

		it("honours a larger maxStalledCount", async () => {
			const { first, prefix } = await drivers("poison3", {
				maxStalledCount: 3,
			});
			await first.push(makeJob("poison"));

			let delivered = 0;
			for (let round = 0; round < 6; round++) {
				if (await first.pop()) delivered++;
				await connection().del(`${prefix}lease:poison`);
				await first.recoverStale();
			}

			// One first delivery plus three reclaims.
			expect(delivered).toBe(4);
			expect(await first.failed()).toHaveLength(1);
		});
	});

	describe("a handler slower than its lease", () => {
		// The heartbeat is a `setInterval`, so only that is faked: the beat is
		// stepped by hand, and the lease is brought near its deadline with
		// PEXPIRE instead of being waited out. The client's own timers stay
		// real, so the round trips to the server still happen.
		beforeEach(() => {
			vi.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
		});
		afterEach(() => {
			vi.useRealTimers();
		});

		it("keeps the job instead of handing it to the next worker", async () => {
			const { first, second, prefix } = await drivers("slow", {
				visibilityTimeoutMs: 10_000,
			});
			const queue = new QueueManager(first);
			let runs = 0;
			let started: (() => void) | undefined;
			const running = new Promise<void>((resolve) => {
				started = resolve;
			});
			let release: (() => void) | undefined;
			const held = new Promise<void>((resolve) => {
				release = resolve;
			});
			queue.register("work", {
				async handle() {
					runs++;
					started?.();
					// Held past its lease — the shape of a report, an upload.
					await held;
				},
			});

			const id = await queue.dispatch("work", {});
			const processing = queue.processOne();
			await running;
			const lease = `${prefix}lease:${id}`;
			// Two seconds left: without a heartbeat it ends there.
			await connection().pexpire(lease, 2_000);

			await vi.advanceTimersByTimeAsync(first.renewIntervalMs);
			await vi.waitFor(async () =>
				expect(await connection().pttl(lease)).toBeGreaterThan(5_000),
			);
			const reclaimed = await second.recoverStale();
			release?.();
			await processing;

			expect(runs).toBe(1);
			expect(reclaimed).toBe(0);
			expect(await connection().llen(`${prefix}pending`)).toBe(0);
			expect(await connection().llen(`${prefix}processing`)).toBe(0);
		});

		it("stops renewing once the handler is done", async () => {
			const { first, prefix } = await drivers("done", {
				visibilityTimeoutMs: 200,
			});
			const queue = new QueueManager(first);
			queue.register("work", { async handle() {} });

			await queue.dispatch("work", {});
			await queue.processOne();

			// No heartbeat left behind, and stepping past several beats writes
			// no lease back.
			expect(vi.getTimerCount()).toBe(0);
			await vi.advanceTimersByTimeAsync(first.renewIntervalMs * 3);
			expect(await connection().keys(`${prefix}*`)).toEqual([]);
		});
	});

	describe("renewing a lease", () => {
		it("refuses when the lease has already gone", async () => {
			const { first, prefix } = await drivers("gone");
			await first.push(makeJob("a"));
			const job = await first.pop();
			if (!job) throw new Error("expected a job");
			await connection().del(`${prefix}lease:a`);

			expect(await first.renew(job)).toBe(false);
		});

		it("refuses when another worker now holds the job", async () => {
			const { first, second, prefix } = await drivers("stolen");
			await first.push(makeJob("a"));
			const job = await first.pop();
			if (!job) throw new Error("expected a job");

			await connection().del(`${prefix}lease:a`);
			await first.recoverStale();
			const taken = await second.pop();
			if (!taken) throw new Error("expected the second worker to take it");

			expect(await first.renew(job)).toBe(false);
			expect(await second.renew(taken)).toBe(true);
		});

		it("pushes the deadline back for the holder", async () => {
			const { first, prefix } = await drivers("extend", {
				visibilityTimeoutMs: 10_000,
			});
			await first.push(makeJob("a"));
			const job = await first.pop();
			if (!job) throw new Error("expected a job");
			await connection().pexpire(`${prefix}lease:a`, 50);

			expect(await first.renew(job)).toBe(true);
			expect(await connection().pttl(`${prefix}lease:a`)).toBeGreaterThan(
				5_000,
			);
		});
	});

	describe("the failed list has a ceiling", () => {
		async function failMany(driver: RedisDriver, count: number) {
			for (let index = 0; index < count; index++) {
				await driver.push(makeJob(`j${index}`));
				const job = await driver.pop();
				if (!job) throw new Error("expected a job");
				await driver.fail(job, "boom");
			}
		}

		it("keeps the most recent failures and drops the rest", async () => {
			const { first } = await drivers("cap", { maxFailedJobs: 5 });
			await failMany(first, 12);
			const failed = await first.failed();
			expect(failed.map((job) => job.id)).toEqual([
				"j7",
				"j8",
				"j9",
				"j10",
				"j11",
			]);
		});

		it("keeps everything when the ceiling is zero", async () => {
			const { first } = await drivers("nocap", { maxFailedJobs: 0 });
			await failMany(first, 12);
			expect(await first.failed()).toHaveLength(12);
		});
	});
});

describe("bay > recovery bounds are validated", () => {
	it("refuses a maxStalledCount or maxFailedJobs that is not a count", () => {
		const client = {
			...scripting(),
			rpush: async () => 0,
			zadd: async () => 0,
			lrange: async () => [],
		};
		for (const bad of [-1, 1.5, Number.NaN]) {
			expect(() => new RedisDriver(client, { maxStalledCount: bad })).toThrow(
				/maxStalledCount/,
			);
			expect(() => new RedisDriver(client, { maxFailedJobs: bad })).toThrow(
				/maxFailedJobs/,
			);
		}
	});
});
