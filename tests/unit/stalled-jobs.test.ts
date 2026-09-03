import { describe, expect, it } from "vitest";
import {
	type RedisClient,
	RedisDriver,
} from "../../src/drivers/RedisDriver.js";
import { type Job, QueueManager } from "../../src/QueueManager.js";

/**
 * A Redis that honours `PX`, which the other fakes in this suite do not need
 * to. Everything here is about what happens when a lease runs out, so the
 * expiry has to be real.
 */
function fakeRedis(): {
	client: RedisClient;
	lists: Map<string, string[]>;
	keys: Map<string, string>;
} {
	const lists = new Map<string, string[]>();
	const keys = new Map<string, string>();
	const timers = new Map<string, ReturnType<typeof setTimeout>>();
	const ensure = (k: string): string[] => {
		const existing = lists.get(k);
		if (existing) return existing;
		const fresh: string[] = [];
		lists.set(k, fresh);
		return fresh;
	};

	const client: RedisClient = {
		async rpush(key, ...values) {
			ensure(key).push(...values);
			return ensure(key).length;
		},
		async lpop(key) {
			return lists.get(key)?.shift() ?? null;
		},
		async lrem(key, _count, element) {
			const list = lists.get(key);
			if (!list) return 0;
			const index = list.indexOf(element);
			if (index < 0) return 0;
			list.splice(index, 1);
			return 1;
		},
		async llen(key) {
			return lists.get(key)?.length ?? 0;
		},
		async lrange(key, start, stop) {
			const list = lists.get(key);
			if (!list) return [];
			return list.slice(start, stop === -1 ? list.length : stop + 1);
		},
		async ltrim(key, start, stop) {
			const list = lists.get(key);
			if (!list) return "OK";
			const from = start < 0 ? Math.max(0, list.length + start) : start;
			const to = stop < 0 ? list.length + stop : stop;
			lists.set(key, list.slice(from, to + 1));
			return "OK";
		},
		async del(key) {
			return (lists.delete(key) ? 1 : 0) + (keys.delete(key) ? 1 : 0);
		},
		async set(key, value, ...args) {
			keys.set(key, value);
			const previous = timers.get(key);
			if (previous) clearTimeout(previous);
			const px = args.indexOf("PX");
			if (px < 0) return "OK";
			const timer = setTimeout(
				() => {
					keys.delete(key);
					timers.delete(key);
				},
				Number(args[px + 1]),
			);
			timer.unref();
			timers.set(key, timer);
			return "OK";
		},
		async get(key) {
			return keys.get(key) ?? null;
		},
		async lmove(source, destination) {
			const value = lists.get(source)?.shift();
			if (value === undefined) return null;
			ensure(destination).push(value);
			return value;
		},
	};
	return { client, lists, keys };
}

function makeJob(id: string): Job {
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

describe("bay > two recovery passes over the same job", () => {
	it("delivers it once, not once per pass", async () => {
		const { client, lists, keys } = fakeRedis();
		const driver = new RedisDriver(client);
		await driver.push(makeJob("a"));
		await driver.pop();
		keys.clear(); // the lease expired: the worker holding it is gone

		// Two workers reclaiming at the same moment, which is the ordinary state
		// of a queue with more than one worker: both read the same expired entry
		// before either has removed it.
		await Promise.all([driver.recoverStale(), driver.recoverStale()]);

		expect(lists.get("queue:pending")).toHaveLength(1);
		expect(lists.get("queue:processing") ?? []).toHaveLength(0);
	});

	it("reports the recovery to exactly one of them", async () => {
		const { client, keys } = fakeRedis();
		const driver = new RedisDriver(client);
		await driver.push(makeJob("a"));
		await driver.pop();
		keys.clear();

		const counts = await Promise.all([
			driver.recoverStale(),
			driver.recoverStale(),
		]);

		expect(counts.reduce((total, n) => total + n, 0)).toBe(1);
	});
});

describe("bay > a job that keeps killing its worker", () => {
	it("is filed as failed rather than reclaimed forever", async () => {
		const { client, lists, keys } = fakeRedis();
		const driver = new RedisDriver(client);
		await driver.push(makeJob("poison"));

		const delivered: string[] = [];
		for (let round = 0; round < 5; round++) {
			const job = await driver.pop();
			if (job) delivered.push(job.id);
			keys.clear(); // the process died; the lease goes with it
			await driver.recoverStale();
		}

		// The default is upstream's: reclaimed once, then failed. A crash never
		// reaches the failure path, so `attempts` cannot bound this — nothing
		// else would ever have stopped it.
		expect(delivered).toEqual(["poison", "poison"]);
		expect(lists.get("queue:pending") ?? []).toHaveLength(0);
		const failed = await driver.failed();
		expect(failed).toHaveLength(1);
		expect(failed[0]?.stalledCount).toBe(2);
	});

	it("honours a larger maxStalledCount", async () => {
		const { client, keys } = fakeRedis();
		const driver = new RedisDriver(client, { maxStalledCount: 3 });
		await driver.push(makeJob("poison"));

		let delivered = 0;
		for (let round = 0; round < 6; round++) {
			if (await driver.pop()) delivered++;
			keys.clear();
			await driver.recoverStale();
		}

		// One first delivery plus three reclaims.
		expect(delivered).toBe(4);
		expect(await driver.failed()).toHaveLength(1);
	});

	it("refuses a maxStalledCount that is not a count", () => {
		const { client } = fakeRedis();
		expect(() => new RedisDriver(client, { maxStalledCount: -1 })).toThrow(
			/maxStalledCount/,
		);
		expect(() => new RedisDriver(client, { maxStalledCount: 1.5 })).toThrow(
			/maxStalledCount/,
		);
	});
});

describe("bay > a handler slower than its lease", () => {
	it("keeps the job instead of handing it to the next worker", async () => {
		const { client, lists } = fakeRedis();
		const driver = new RedisDriver(client, { visibilityTimeoutMs: 200 });
		const queue = new QueueManager(driver);

		let runs = 0;
		queue.register("work", {
			async handle() {
				runs++;
				// Three times the lease — the shape of a report, an upload, any
				// call to something slow.
				await new Promise((resolve) => setTimeout(resolve, 600));
			},
		});

		await queue.dispatch("work", {});
		const processing = queue.processOne();
		// A recovery pass runs while the handler is still working.
		await new Promise((resolve) => setTimeout(resolve, 400));
		const reclaimed = await driver.recoverStale();
		await processing;

		expect(runs).toBe(1);
		expect(reclaimed).toBe(0);
		expect(lists.get("queue:pending") ?? []).toHaveLength(0);
		expect(lists.get("queue:processing") ?? []).toHaveLength(0);
	});

	it("stops renewing once the handler is done", async () => {
		const { client } = fakeRedis();
		const driver = new RedisDriver(client, { visibilityTimeoutMs: 200 });
		const queue = new QueueManager(driver);
		queue.register("work", {
			async handle() {},
		});

		await queue.dispatch("work", {});
		await queue.processOne();
		// Past several renewal intervals: a timer left running would keep
		// touching a job that no longer exists.
		await new Promise((resolve) => setTimeout(resolve, 350));

		expect(await driver.size()).toBe(0);
	});
});

describe("bay > renewing a lease", () => {
	it("refuses when the lease has already gone", async () => {
		const { client, keys } = fakeRedis();
		const driver = new RedisDriver(client);
		await driver.push(makeJob("a"));
		const job = await driver.pop();
		if (!job) throw new Error("expected a job");
		keys.clear();

		expect(await driver.renew(job)).toBe(false);
	});

	it("refuses when another worker now holds the job", async () => {
		const { client, keys } = fakeRedis();
		const first = new RedisDriver(client);
		const second = new RedisDriver(client);
		await first.push(makeJob("a"));
		const job = await first.pop();
		if (!job) throw new Error("expected a job");

		// The first worker stalled, the job was reclaimed, and the second worker
		// took it. A late heartbeat from the first must not extend a claim that
		// is no longer its own.
		keys.clear();
		await first.recoverStale();
		await second.pop();

		expect(await first.renew(job)).toBe(false);
		expect(await second.renew(job)).toBe(true);
	});
});

describe("bay > the failed list has a ceiling", () => {
	it("keeps the most recent failures and drops the rest", async () => {
		const { client, lists } = fakeRedis();
		const driver = new RedisDriver(client, { maxFailedJobs: 5 });
		for (let index = 0; index < 12; index++) {
			await driver.fail(makeJob(`j${index}`), "boom");
		}

		expect(lists.get("queue:failed")).toHaveLength(5);
		const failed = await driver.failed();
		expect(failed.map((job) => job.id)).toEqual([
			"j7",
			"j8",
			"j9",
			"j10",
			"j11",
		]);
	});

	it("keeps everything when the ceiling is zero", async () => {
		const { client, lists } = fakeRedis();
		const driver = new RedisDriver(client, { maxFailedJobs: 0 });
		for (let index = 0; index < 12; index++) {
			await driver.fail(makeJob(`j${index}`), "boom");
		}

		expect(lists.get("queue:failed")).toHaveLength(12);
	});

	it("leaves the list alone when the client has no LTRIM", async () => {
		const { client, lists } = fakeRedis();
		const withoutLtrim: RedisClient = { ...client };
		withoutLtrim.ltrim = undefined;
		const driver = new RedisDriver(withoutLtrim, { maxFailedJobs: 5 });
		for (let index = 0; index < 12; index++) {
			await driver.fail(makeJob(`j${index}`), "boom");
		}

		// Trimming is the client's command to run; a client that cannot is left
		// with the behaviour it had, not with a job silently dropped.
		expect(lists.get("queue:failed")).toHaveLength(12);
	});
});
