/**
 * What upstream's job surface offers beside the payload: the attempt's
 * context, a fluent `Job.dispatch()` / `Job.dispatchMany()`, `failOnTimeout`,
 * and asking what became of a job (`getJob`) under a retention policy
 * (`removeOnComplete` / `removeOnFail`) — on both drivers.
 */

import { afterEach, describe, expect, it } from "vitest";
import { MemoryDriver } from "../../src/drivers/MemoryDriver.js";
import { RedisDriver } from "../../src/drivers/RedisDriver.js";
import { Job, type JobContext } from "../../src/Job.js";
import { type QueueDriver, QueueManager } from "../../src/QueueManager.js";
import { clearQueue, setQueue } from "../../src/services/main.js";
import { describeRedis, freshPrefix, useRedis } from "../__helpers__/redis.js";

async function drainAll(queue: QueueManager): Promise<number> {
	let ran = 0;
	while (await queue.processOne(["default", "emails"])) ran++;
	return ran;
}

afterEach(() => {
	clearQueue();
});

describe("bay > a job knows its context", () => {
	it("hands each attempt its job id, attempt number, queue, priority and acquisition time", async () => {
		const seen: JobContext[] = [];
		class Flaky extends Job<{ n: number }> {
			static override options = { queue: "emails", priority: 2, maxRetries: 2 };
			async execute(): Promise<void> {
				seen.push(this.context);
				if (this.context.attempt === 1) throw new Error("first try fails");
			}
		}
		const queue = new QueueManager(new MemoryDriver());
		const before = Date.now();
		const id = await queue.dispatch(Flaky, { n: 1 });

		expect(await drainAll(queue)).toBe(2);

		expect(seen.map((c) => c.attempt)).toEqual([1, 2]);
		for (const context of seen) {
			expect(context.jobId).toBe(id);
			expect(context.name).toBe("Flaky");
			expect(context.queue).toBe("emails");
			expect(context.priority).toBe(2);
			expect(context.stalledCount).toBe(0);
			expect(context.acquiredAt).toBeInstanceOf(Date);
			expect(context.acquiredAt.getTime()).toBeGreaterThanOrEqual(before);
			expect(Object.isFrozen(context)).toBe(true);
		}
	});

	it("hands a registered handler the context as its third argument", async () => {
		const seen: JobContext[] = [];
		const queue = new QueueManager(new MemoryDriver());
		queue.register("named", {
			async handle(_payload, _signal, context) {
				if (context !== undefined) seen.push(context);
			},
		});
		const id = await queue.dispatch("named", {});
		await drainAll(queue);
		expect(seen).toHaveLength(1);
		expect(seen[0]?.jobId).toBe(id);
		expect(seen[0]?.priority).toBe(5);
		expect(seen[0]?.queue).toBe("default");
	});
});

describe("bay > Job.dispatch() and Job.dispatchMany()", () => {
	class SendEmail extends Job<{ to: string }> {
		static override options = { queue: "emails" };
		static sent: string[] = [];
		async execute(): Promise<void> {
			SendEmail.sent.push(this.payload.to);
		}
	}

	it("queues through the bound queue, fluently, and answers the job id", async () => {
		const driver = new MemoryDriver();
		const queue = new QueueManager(driver);
		setQueue(queue);

		const { jobId } = await SendEmail.dispatch({ to: "a@x.test" })
			.toQueue("default")
			.priority(1)
			.in(0);

		const found = await queue.getJob(jobId);
		expect(found?.status).toBe("pending");
		expect(found?.data.queue).toBe("default");
		expect(found?.data.priority).toBe(1);
	});

	it("holds the job back with in()", async () => {
		const queue = new QueueManager(new MemoryDriver());
		setQueue(queue);
		const { jobId } = await SendEmail.dispatch({ to: "b@x.test" }).in("1h");
		expect((await queue.getJob(jobId))?.status).toBe("delayed");
	});

	it("dispatchMany queues every payload and answers every id", async () => {
		const queue = new QueueManager(new MemoryDriver());
		setQueue(queue);
		SendEmail.sent = [];

		const { jobIds } = await SendEmail.dispatchMany([
			{ to: "1@x.test" },
			{ to: "2@x.test" },
		]).toQueue("emails");

		expect(jobIds).toHaveLength(2);
		expect(await queue.sizeOf("emails")).toBe(2);
		await drainAll(queue);
		expect(SendEmail.sent).toEqual(["1@x.test", "2@x.test"]);
	});

	it("refuses to dispatch before a queue is bound, naming what to do", async () => {
		await expect(SendEmail.dispatch({ to: "c@x.test" }).run()).rejects.toThrow(
			/setQueue|BayProvider/,
		);
	});
});

describe("bay > failOnTimeout", () => {
	it("fails a timed-out job at once instead of retrying it", async () => {
		let runs = 0;
		class Slow extends Job {
			static override options = {
				maxRetries: 3,
				timeout: 10,
				failOnTimeout: true,
			};
			async execute(): Promise<void> {
				runs++;
				await new Promise((resolve) => setTimeout(resolve, 50));
			}
		}
		const queue = new QueueManager(new MemoryDriver());
		await queue.dispatch(Slow, {});
		await drainAll(queue);
		expect(runs).toBe(1);
		expect(await queue.failedJobs()).toHaveLength(1);
	});

	it("still retries a timed-out job by default", async () => {
		let runs = 0;
		class Slow extends Job {
			static override options = { maxRetries: 2, timeout: 10 };
			async execute(): Promise<void> {
				runs++;
				await new Promise((resolve) => setTimeout(resolve, 30));
			}
		}
		const queue = new QueueManager(new MemoryDriver());
		await queue.dispatch(Slow, {});
		await drainAll(queue);
		expect(runs).toBe(2);
	});
});

/** The lookup suite, run against any driver. */
function lookupSuite(makeDriver: () => QueueDriver | Promise<QueueDriver>) {
	it("answers pending, then active while it runs, then nothing once completed by default", async () => {
		const queue = new QueueManager(await makeDriver());
		const during: Array<string | undefined> = [];
		queue.register("watch", {
			handle: async (_payload, _signal, context) => {
				during.push((await queue.getJob(context?.jobId ?? ""))?.status);
			},
		});
		const id = await queue.dispatch("watch", { n: 1 });
		expect((await queue.getJob(id))?.status).toBe("pending");
		expect((await queue.getJob(id))?.data.payload).toEqual({ n: 1 });

		await queue.processOne();

		expect(during).toEqual(["active"]);
		// upstream's default: a completed job is removed at once.
		expect(await queue.getJob(id)).toBeNull();
	});

	it("keeps a completed job with removeOnComplete: false", async () => {
		const queue = new QueueManager(await makeDriver());
		queue.register("keep", { handle: async () => {} });
		const id = await queue.dispatch("keep", {}, { removeOnComplete: false });
		await queue.processOne();
		const found = await queue.getJob(id);
		expect(found?.status).toBe("completed");
		expect(typeof found?.finishedAt).toBe("number");
		expect(found?.data.attempts).toBe(1);
	});

	it("keeps failed jobs up to removeOnFail.count, newest first kept, with the error", async () => {
		const queue = new QueueManager(await makeDriver());
		queue.register("boom", {
			handle: async () => {
				throw new Error("it broke");
			},
		});
		const options = { maxRetries: 1, removeOnFail: { count: 2 } };
		const first = await queue.dispatch("boom", { i: 1 }, options);
		await queue.processOne();
		const second = await queue.dispatch("boom", { i: 2 }, options);
		await queue.processOne();
		const third = await queue.dispatch("boom", { i: 3 }, options);
		await queue.processOne();

		expect(await queue.getJob(first)).toBeNull();
		expect((await queue.getJob(second))?.status).toBe("failed");
		const last = await queue.getJob(third);
		expect(last?.status).toBe("failed");
		expect(last?.error).toBe("it broke");
	});

	it("prunes kept jobs older than removeOnComplete.age", async () => {
		const queue = new QueueManager(await makeDriver());
		queue.register("aged", { handle: async () => {} });
		const options = { removeOnComplete: { age: 30 } };
		const old = await queue.dispatch("aged", {}, options);
		await queue.processOne();
		await new Promise((resolve) => setTimeout(resolve, 60));
		const fresh = await queue.dispatch("aged", {}, options);
		await queue.processOne();

		expect(await queue.getJob(old)).toBeNull();
		expect((await queue.getJob(fresh))?.status).toBe("completed");
	});

	it("answers a retried job as pending again, with its attempt counted", async () => {
		const queue = new QueueManager(await makeDriver());
		let runs = 0;
		queue.register("again", {
			handle: async () => {
				runs++;
				if (runs === 1) throw new Error("once");
			},
		});
		const id = await queue.dispatch("again", {}, { maxRetries: 2 });
		await queue.processOne();
		const found = await queue.getJob(id);
		expect(found?.status).toBe("pending");
		expect(found?.data.attempts).toBe(1);
	});

	it("answers null for an id it never saw, and for another queue's job", async () => {
		const queue = new QueueManager(await makeDriver());
		const id = await queue.dispatch("x", {}, { queue: "emails" });
		expect(await queue.getJob("job_nope")).toBeNull();
		expect(await queue.getJob(id, "default")).toBeNull();
		expect((await queue.getJob(id, "emails"))?.status).toBe("pending");
	});
}

describe("bay > getJob on the memory driver", () => {
	lookupSuite(() => new MemoryDriver());
});

describeRedis("bay > getJob on the Redis driver", () => {
	const redis = useRedis();
	lookupSuite(
		async () =>
			new RedisDriver(await redis.client(), { prefix: freshPrefix("lookup") }),
	);
});
