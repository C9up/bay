/**
 * Every Redis state transition is all-or-nothing, and only the lease holder
 * may make one.
 *
 * Against a real server: what is under test is that a move between two lists
 * happens in ONE server-side step, which a fake cannot show.
 */
import { describe, expect, it, vi } from "vitest";
import {
	type RedisClient,
	RedisDriver,
} from "../../src/drivers/RedisDriver.js";
import { type JobRecord, QueueManager } from "../../src/QueueManager.js";
import {
	describeRedis,
	entriesUnder,
	freshPrefix,
	intercept,
	useRedis,
	whereIs,
} from "../__helpers__/redis.js";

function makeJob(id: string, over: Partial<JobRecord> = {}): JobRecord {
	return {
		id,
		name: "work",
		payload: { id },
		attempts: 0,
		maxAttempts: 3,
		status: "pending",
		createdAt: Date.now(),
		...over,
	};
}

/** Commands that put a job somewhere, whatever the driver calls them. */
const WRITES = new Set(["rpush", "evalsha"]);

/**
 * A client whose next write after `arm()` fails — before reaching the server,
 * or after it ran there and the reply was lost.
 */
function flaky(
	base: RedisClient,
	mode: "before" | "after",
): { client: RedisClient; arm(): void } {
	let armed = false;
	return {
		arm: () => {
			armed = true;
		},
		client: intercept(base, async (command, run) => {
			if (!armed || !WRITES.has(command)) return run();
			armed = false;
			if (mode === "after") await run();
			throw new Error("ECONNRESET (simulated)");
		}),
	};
}

describeRedis("bay > a dropped connection never loses the job", () => {
	const redis = useRedis();

	for (const mode of ["before", "after"] as const) {
		it(`keeps a retried job reachable (reply lost ${mode} the server ran it)`, async () => {
			const prefix = freshPrefix("retry");
			const { client, arm } = flaky(await redis.client(), mode);
			const driver = new RedisDriver(client, { prefix });
			const queue = new QueueManager(driver);
			queue.register("work", {
				async handle() {
					arm();
					throw new Error("handler failed");
				},
			});
			const id = await queue.dispatch("work", {});

			await expect(queue.processOne()).rejects.toThrow(/ECONNRESET/);

			expect(await whereIs(redis.manager, prefix, id)).not.toEqual([]);
		});

		it(`keeps a finally failed job reachable (reply lost ${mode})`, async () => {
			const prefix = freshPrefix("fail");
			const { client, arm } = flaky(await redis.client(), mode);
			const driver = new RedisDriver(client, { prefix });
			const queue = new QueueManager(driver);
			queue.register("work", {
				async handle() {
					arm();
					throw new Error("handler failed");
				},
			});
			const id = await queue.dispatch("work", {}, { maxAttempts: 1 });

			await expect(queue.processOne()).rejects.toThrow(/ECONNRESET/);

			expect(await whereIs(redis.manager, prefix, id)).not.toEqual([]);
		});

		it(`keeps a delayed job reachable while it is promoted (reply lost ${mode})`, async () => {
			const prefix = freshPrefix("delayed");
			const { client, arm } = flaky(await redis.client(), mode);
			const driver = new RedisDriver(client, { prefix });
			const queue = new QueueManager(driver);
			queue.register("work", { async handle() {} });
			const id = await queue.dispatch("work", {}, { delay: 20 });
			await new Promise((resolve) => setTimeout(resolve, 40));

			arm();
			await expect(queue.processOne()).rejects.toThrow(/ECONNRESET/);

			expect(await whereIs(redis.manager, prefix, id)).not.toEqual([]);
		});
	}

	it("and a job whose retry never reached the server is recovered once its lease lapses", async () => {
		const prefix = freshPrefix("recover");
		const { client, arm } = flaky(await redis.client(), "before");
		const driver = new RedisDriver(client, { prefix });
		const queue = new QueueManager(driver);
		queue.register("work", {
			async handle() {
				arm();
				throw new Error("handler failed");
			},
		});
		const id = await queue.dispatch("work", {});
		await expect(queue.processOne()).rejects.toThrow(/ECONNRESET/);

		// The worker's lease running out, without waiting for it.
		await redis.manager.connection("w0").del(`${prefix}lease:${id}`);

		expect(await driver.recoverStale()).toBe(1);
		expect(await whereIs(redis.manager, prefix, id)).toEqual(["pending"]);
	});
});

describeRedis("bay > a worker whose lease lapsed cannot settle the job", () => {
	const redis = useRedis();

	for (const settle of ["complete", "fail", "retry"] as const) {
		it(`${settle}() leaves the new owner's entry and lease alone`, async () => {
			const prefix = freshPrefix(`late-${settle}`);
			// Two stalls in this story, so the bound must allow both.
			const options = { prefix, maxStalledCount: 2 };
			const a = new RedisDriver(await redis.client(0), options);
			const b = new RedisDriver(await redis.client(1), options);
			const connection = redis.manager.connection("w0");

			await a.push(makeJob("j1"));
			const jobA = await a.pop();
			if (!jobA) throw new Error("expected a job");

			// A's lease lapses; B reclaims the job and takes it.
			await connection.del(`${prefix}lease:j1`);
			expect(await b.recoverStale()).toBe(1);
			const jobB = await b.pop();
			expect(jobB?.id).toBe("j1");
			const before = await entriesUnder(redis.manager, prefix);

			jobA.attempts += 1;
			if (settle === "complete") await a.complete(jobA);
			else if (settle === "fail") await a.fail(jobA, "late");
			else await a.retry(jobA);

			// Nothing A did reached B's claim, and nothing was re-queued.
			expect(await entriesUnder(redis.manager, prefix)).toEqual(before);

			// B crashes: its job is still recoverable.
			await connection.del(`${prefix}lease:j1`);
			expect(await b.recoverStale()).toBe(1);
			expect(await whereIs(redis.manager, prefix, "j1")).toEqual(["pending"]);
		});
	}
});

describeRedis("bay > taking and renewing a lease is one step", () => {
	const redis = useRedis();

	it("a recovery pass running while a job is taken does not hand it to a second worker", async () => {
		const prefix = freshPrefix("acquire");
		let release: () => void = () => {};
		const held = new Promise<void>((resolve) => {
			release = resolve;
		});
		let paused: () => void = () => {};
		const pausedAt = new Promise<void>((resolve) => {
			paused = resolve;
		});
		// Hold any separate lease write, which is where the window was.
		const slowA = intercept(await redis.client(0), async (command, run) => {
			if (command === "set") {
				paused();
				await held;
			}
			return run();
		});
		const a = new RedisDriver(slowA, { prefix });
		const b = new RedisDriver(await redis.client(1), { prefix });
		await a.push(makeJob("j1"));

		const popping = a.pop();
		await Promise.race([pausedAt, popping]);
		const recovered = await b.recoverStale();
		const second = await b.pop();
		release();
		const first = await popping;

		expect(first?.id).toBe("j1");
		expect(recovered).toBe(0);
		expect(second).toBeNull();
	});

	it("a late renewal from a worker that lost the job does not overwrite the new owner's lease", async () => {
		const prefix = freshPrefix("renew");
		let release: () => void = () => {};
		const held = new Promise<void>((resolve) => {
			release = resolve;
		});
		let paused: () => void = () => {};
		const pausedAt = new Promise<void>((resolve) => {
			paused = resolve;
		});
		let holdSets = false;
		const slowA = intercept(await redis.client(0), async (command, run) => {
			if (holdSets && command === "set") {
				paused();
				await held;
			}
			return run();
		});
		const a = new RedisDriver(slowA, { prefix });
		const b = new RedisDriver(await redis.client(1), { prefix });
		const connection = redis.manager.connection("w0");
		await a.push(makeJob("j1"));
		const jobA = await a.pop();
		if (!jobA) throw new Error("expected a job");

		holdSets = true;
		const renewing = a.renew(jobA);
		await Promise.race([pausedAt, renewing]);
		// Meanwhile A's lease lapses and B takes the job.
		await connection.del(`${prefix}lease:j1`);
		expect(await b.recoverStale()).toBe(1);
		const jobB = await b.pop();
		if (!jobB) throw new Error("expected B to take the job");
		const leaseOfB = await connection.get(`${prefix}lease:j1`);
		release();
		await renewing;

		expect(await connection.get(`${prefix}lease:j1`)).toBe(leaseOfB);
		expect(await a.renew(jobA)).toBe(false);
		expect(await b.renew(jobB)).toBe(true);
	});
});

describe("bay > lease renewal that is refused", () => {
	it("stops the heartbeat and says so", async () => {
		let renewals = 0;
		const stderr = vi
			.spyOn(process.stderr, "write")
			.mockImplementation(() => true);
		try {
			const queue = new QueueManager({
				renewIntervalMs: 10,
				async renew() {
					renewals += 1;
					return false;
				},
				async push() {},
				async pop() {
					return makeJob("j1");
				},
				async complete() {},
				async fail() {},
				async retry() {},
				async failed() {
					return [];
				},
				async size() {
					return 0;
				},
			});
			queue.register("work", {
				handle: () => new Promise((resolve) => setTimeout(resolve, 80)),
			});
			await queue.processOne();
			expect(renewals).toBe(1);
			expect(
				stderr.mock.calls.map(([chunk]) => String(chunk)).join(""),
			).toMatch(/j1.*lease/);
		} finally {
			stderr.mockRestore();
		}
	});
});
