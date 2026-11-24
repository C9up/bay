/**
 * `RedisDriver` against a REAL Redis, wired the way an app wires it: through
 * `quasarConnection()`.
 *
 * What matters here is that taking a job is one server-side step, so two
 * workers popping the same queue must never receive the same job — something
 * only a real server, with separate sockets per worker, can show.
 *
 * Gated on `REDIS_TEST_URL`; skipped without one (see `__helpers__/redis.ts`).
 */
import { beforeEach, expect, it } from "vitest";
import { RedisDriver } from "../../src/drivers/RedisDriver.js";
import type { JobRecord } from "../../src/QueueManager.js";
import { quasarConnection } from "../../src/quasar.js";
import { describeRedis, freshPrefix, WORKERS } from "../__helpers__/redis.js";

function makeJob(id: string): JobRecord {
	return {
		id,
		name: "send-mail",
		payload: { to: `${id}@example.com` },
		attempts: 0,
		maxAttempts: 3,
		status: "pending",
		createdAt: Date.now(),
	};
}

describeRedis("RedisDriver against a live Redis", (redis) => {
	const prefix = freshPrefix("integration");
	const { manager } = redis;

	function driver(worker: number): RedisDriver {
		return new RedisDriver(quasarConnection(`w${worker}`), {
			prefix,
			visibilityTimeoutMs: 30_000,
		});
	}

	beforeEach(async () => {
		const client = manager.connection("w0");
		const keys = await redis.keys(`${prefix}*`);
		if (keys.length > 0) await client.del(...keys);
	});

	it("hands a job to exactly ONE of many concurrent workers", async () => {
		await driver(0).push(makeJob("only-once"));

		// Every worker pops at once — the shape that breaks a non-atomic reserve.
		const popped = await Promise.all(
			Array.from({ length: WORKERS }, (_, i) => driver(i).pop()),
		);

		const got = popped.filter((job): job is JobRecord => job !== null);
		expect(got).toHaveLength(1);
		expect(got[0]?.id).toBe("only-once");
	});

	it("distributes N jobs across workers without duplication or loss", async () => {
		const producer = driver(0);
		const ids = Array.from({ length: 20 }, (_, i) => `job-${i}`);
		for (const id of ids) await producer.push(makeJob(id));

		const drained = await Promise.all(
			Array.from({ length: 8 }, async (_, i) => {
				const worker = driver(i);
				const mine: string[] = [];
				for (;;) {
					const job = await worker.pop();
					if (!job) break;
					mine.push(job.id);
				}
				return mine;
			}),
		);

		// Every job delivered exactly once: none duplicated, none lost.
		const all = drained.flat().sort();
		expect(all).toEqual([...ids].sort());
		expect(new Set(all).size).toBe(ids.length);
	});

	it("moves a popped job out of the queue and clears it on complete", async () => {
		const d = driver(0);
		await d.push(makeJob("lifecycle"));
		expect(await d.size()).toBe(1);

		const job = await d.pop();
		expect(job).not.toBeNull();
		// Reserved: out of pending, held in processing under a lease.
		expect(await d.size()).toBe(0);

		if (job) await d.complete(job);
		expect(await d.pop()).toBeNull();
	});

	it("puts a retried job back in the queue for another worker", async () => {
		const d = driver(0);
		await d.push(makeJob("retry-me"));
		const job = await d.pop();
		if (!job) throw new Error("expected a job");

		await d.retry(job);

		expect((await driver(1).pop())?.id).toBe("retry-me");
	});

	it("records a failed job instead of losing it", async () => {
		const d = driver(0);
		await d.push(makeJob("boom"));
		const job = await d.pop();
		if (!job) throw new Error("expected a job");

		await d.fail(job, "handler threw");

		const failed = await d.failed();
		expect(failed.map((j) => j.id)).toEqual(["boom"]);
		expect(failed[0]?.error).toBe("handler threw");
	});
});
