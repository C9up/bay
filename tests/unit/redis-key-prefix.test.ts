/**
 * The Redis driver on a quasar connection that sets `keyPrefix`, through the
 * public API: `drivers.redis()` and `QueueManager`.
 *
 * The client applies `keyPrefix` to the KEYS of a script and to nothing in its
 * ARGV. A key a script builds from an argument therefore lands outside the
 * prefix, where every other command misses it: the lease went unseen, so jobs
 * ran twice and were then filed as stalled; an empty "no dedup" key became the
 * bare prefix, so every dispatch after the first was dropped.
 *
 * Gated on `REDIS_TEST_URL` (see `__helpers__/redis.ts`).
 */
import { defineConfig, QuasarManager } from "@c9up/quasar";
import { afterAll, describe, expect, it } from "vitest";
import { RedisDriver } from "../../src/drivers/RedisDriver.js";
import { drivers, QueueManager } from "../../src/index.js";
import { live } from "../__helpers__/redis.js";

const url = process.env.REDIS_TEST_URL ?? "";
const run = `bay-kp-${process.pid}-${Date.now()}`;
const appPrefix = `${run}-app:`;

const manager = new QuasarManager(
	defineConfig({
		connection: "plain",
		connections: {
			plain: { url, keyPrefix: "" },
			app: { url, keyPrefix: appPrefix },
			appWorker: { url, keyPrefix: appPrefix },
		},
	}),
);

let sequence = 0;
function freshQueuePrefix(): string {
	sequence += 1;
	return `queue${sequence}:`;
}

function queueOn(prefix: string): QueueManager {
	return new QueueManager(
		drivers.redis({ connection: manager.connection("app"), prefix })(),
	);
}

/** Every key this run wrote, as the server names them. */
async function serverKeys(): Promise<string[]> {
	return (await manager.connection("plain").keys(`*${run}*`)).sort();
}

const suite = live ? describe : describe.skip;

suite("bay > Redis driver on a connection that sets keyPrefix", () => {
	afterAll(async () => {
		const plain = manager.connection("plain");
		const keys = await plain.keys(`${run}*`);
		if (keys.length > 0) await plain.del(...keys);
		await manager.quitAll();
	});

	it("queues every dispatch and runs each job once", async () => {
		const prefix = freshQueuePrefix();
		const queue = queueOn(prefix);
		const runs: string[] = [];
		queue.register("send-invoice", {
			handle: async () => {
				runs.push("send-invoice");
			},
		});
		queue.register("reset-password", {
			handle: async () => {
				runs.push("reset-password");
			},
		});
		const first = await queue.dispatch("send-invoice", { invoice: 1 });
		const second = await queue.dispatch("reset-password", { user: 2 });
		const third = await queue.dispatch("send-invoice", { invoice: 3 });
		expect(new Set([first, second, third]).size).toBe(3);
		expect(await queue.size()).toBe(3);
		// Nothing sits at the bare connection prefix.
		expect(await manager.connection("plain").exists(appPrefix)).toBe(0);

		const working = queue.work({ idleDelay: 20, stalledInterval: 60 });
		await new Promise((resolve) => setTimeout(resolve, 500));
		await queue.stop();
		await working;

		expect(runs.sort()).toEqual([
			"reset-password",
			"send-invoice",
			"send-invoice",
		]);
		expect(await queue.failedJobs()).toEqual([]);
		expect(await queue.size()).toBe(0);
		expect(await manager.connection("app").llen(`${prefix}processing`)).toBe(0);
	});

	it("keeps dedup working, under the connection prefix", async () => {
		const prefix = freshQueuePrefix();
		const queue = queueOn(prefix);
		const first = await queue.dispatch(
			"send-invoice",
			{ invoice: 1 },
			{ dedup: { id: "invoice-1" } },
		);
		const again = await queue.dispatch(
			"send-invoice",
			{ invoice: 1 },
			{ dedup: { id: "invoice-1" } },
		);
		const other = await queue.dispatch("send-invoice", { invoice: 2 });
		expect(again).toBe(first);
		expect(other).not.toBe(first);
		expect(await queue.size()).toBe(2);
		expect(
			await manager
				.connection("plain")
				.get(`${appPrefix}${prefix}dedup:send-invoice::invoice-1`),
		).toBe(`0:${first}`);
	});

	it("writes the lease and the lookup hash under the prefix, and settles the job", async () => {
		const prefix = freshQueuePrefix();
		const queue = queueOn(prefix);
		const id = await queue.dispatch("charge-card", { order: 42 });
		const driver = new RedisDriver(manager.connection("app"), { prefix });
		const job = await driver.pop();
		expect(job?.id).toBe(id);
		if (job === null) return;

		const plain = manager.connection("plain");
		expect(await plain.exists(`${appPrefix}${prefix}lease:${id}`)).toBe(1);
		expect(await plain.exists(`${prefix}lease:${id}`)).toBe(0);
		expect((await driver.getJob(id))?.status).toBe("active");
		expect(await driver.renew(job)).toBe(true);

		await driver.complete(job);
		expect(await plain.exists(`${appPrefix}${prefix}lease:${id}`)).toBe(0);
		expect(await manager.connection("app").llen(`${prefix}processing`)).toBe(0);
		// Every key the queue wrote carries the connection prefix.
		for (const key of await serverKeys()) {
			expect(key.startsWith(appPrefix)).toBe(true);
		}
	});

	it("does not hand a leased job to another worker's recovery pass", async () => {
		const prefix = freshQueuePrefix();
		const queue = queueOn(prefix);
		const id = await queue.dispatch("charge-card", { order: 43 });
		const workerA = new RedisDriver(manager.connection("app"), { prefix });
		const workerB = new RedisDriver(manager.connection("appWorker"), {
			prefix,
		});
		const job = await workerA.pop();
		expect(job?.id).toBe(id);
		if (job === null) return;

		expect(await workerB.recoverStale()).toEqual({ recovered: 0, failed: [] });
		expect(await workerB.pop()).toBeNull();

		await workerA.complete(job);
		expect(await workerA.failed()).toEqual([]);
	});
});
