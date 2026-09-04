/**
 * A job as a class, and everything its declaration buys: named queues, a delay,
 * a timeout, and a worker that runs more than one at a time.
 */

import { describe, expect, it, vi } from "vitest";
import {
	jobStub,
	resolveJobName,
	toPascalCase,
	toSnakeCase,
} from "../../src/console/makeJob.js";
import { parseQueues } from "../../src/console/queueWork.js";
import { MemoryDriver } from "../../src/drivers/MemoryDriver.js";
import { Job, type JobOptions, toMilliseconds } from "../../src/Job.js";
import { directoryOf } from "../../src/jobs.js";
import { QueueManager } from "../../src/QueueManager.js";

/** Run every job the manager can see right now, then stop. */
async function drainAll(
	queue: QueueManager,
	queues?: readonly string[],
): Promise<number> {
	let ran = 0;
	while (await queue.processOne(queues)) ran++;
	return ran;
}

describe("bay > a job is a class", () => {
	it("runs it, with the payload it was dispatched with", async () => {
		const seen: string[] = [];
		class SendEmail extends Job<{ to: string }> {
			async execute(): Promise<void> {
				seen.push(this.payload.to);
			}
		}
		const queue = new QueueManager(new MemoryDriver());

		await queue.dispatch(SendEmail, { to: "user@example.com" });
		expect(await drainAll(queue)).toBe(1);
		expect(seen).toEqual(["user@example.com"]);
	});

	it("registers the class under its own name, for a worker that never dispatched it", async () => {
		// The HTTP process queues, the worker process runs: the worker never
		// calls dispatch, so it has to be told the class some other way.
		const seen: unknown[] = [];
		class Ping extends Job<{ n: number }> {
			async execute(): Promise<void> {
				seen.push(this.payload.n);
			}
		}
		const driver = new MemoryDriver();
		const dispatcher = new QueueManager(driver);
		await dispatcher.dispatch(Ping, { n: 1 });

		const worker = new QueueManager(driver);
		worker.registerJob(Ping);
		expect(await drainAll(worker)).toBe(1);
		expect(seen).toEqual([1]);
	});

	it("calls failed() once, after the last attempt — not after each one", async () => {
		const attempts: number[] = [];
		const failures: string[] = [];
		class Flaky extends Job {
			async execute(): Promise<void> {
				attempts.push(attempts.length + 1);
				throw new Error("nope");
			}
			override async failed(error: Error): Promise<void> {
				failures.push(error.message);
			}
		}
		const queue = new QueueManager(new MemoryDriver());

		await queue.dispatch(Flaky, {}, { maxAttempts: 3 });
		await drainAll(queue);

		expect(attempts).toHaveLength(3);
		expect(failures).toEqual(["nope"]);
	});

	it("reports a throwing failed() instead of failing the job twice", async () => {
		const stderr = vi
			.spyOn(process.stderr, "write")
			.mockImplementation(() => true);
		class Loud extends Job {
			async execute(): Promise<void> {
				throw new Error("first");
			}
			override async failed(): Promise<void> {
				throw new Error("and the hook too");
			}
		}
		const driver = new MemoryDriver();
		const queue = new QueueManager(driver);

		await queue.dispatch(Loud, {}, { maxAttempts: 1 });
		await expect(drainAll(queue)).resolves.toBe(1);

		const failed = await driver.failed();
		expect(failed).toHaveLength(1);
		// The job failed for what the handler threw, not for what the hook did.
		expect(failed[0]?.error).toBe("first");
		expect(String(stderr.mock.calls.flat().join(""))).toContain(
			"and the hook too",
		);
		stderr.mockRestore();
	});

	it("takes the class's options, and lets the call site override them", async () => {
		class Declared extends Job {
			static override options: JobOptions = { queue: "emails", maxRetries: 5 };
			async execute(): Promise<void> {}
		}
		const driver = new MemoryDriver();
		const queue = new QueueManager(driver);

		await queue.dispatch(Declared, {});
		await queue.dispatch(Declared, {}, { queue: "critical", maxRetries: 1 });

		// Neither is on the default queue, so a default worker sees nothing.
		expect(await queue.processOne()).toBe(false);

		const first = await driver.pop(["emails"]);
		expect(first?.maxAttempts).toBe(5);
		const second = await driver.pop(["critical"]);
		expect(second?.maxAttempts).toBe(1);
	});
});

describe("bay > named queues", () => {
	it("serves only the queues the worker was told to serve", async () => {
		const ran: string[] = [];
		class Emails extends Job {
			static override options: JobOptions = { queue: "emails" };
			async execute(): Promise<void> {
				ran.push("emails");
			}
		}
		class Reports extends Job {
			static override options: JobOptions = { queue: "reports" };
			async execute(): Promise<void> {
				ran.push("reports");
			}
		}
		const queue = new QueueManager(new MemoryDriver());
		await queue.dispatch(Emails, {});
		await queue.dispatch(Reports, {});

		// This is what keeps a slow queue from starving a fast one: the report
		// worker and the mail worker are different processes.
		expect(await drainAll(queue, ["emails"])).toBe(1);
		expect(ran).toEqual(["emails"]);
		expect(await drainAll(queue, ["reports"])).toBe(1);
		expect(ran).toEqual(["emails", "reports"]);
	});

	it("drains them in the order it was given", async () => {
		const driver = new MemoryDriver();
		const queue = new QueueManager(driver);
		await queue.dispatch("low", {}, { queue: "low" });
		await queue.dispatch("high", {}, { queue: "critical" });

		const first = await driver.pop(["critical", "low"]);
		expect(first?.name).toBe("high");
	});

	it("puts a record with no queue on the default one", async () => {
		const driver = new MemoryDriver();
		// What a job queued by a version that had no named queues looks like.
		await driver.push({
			id: "job_old",
			name: "legacy",
			payload: {},
			attempts: 0,
			maxAttempts: 3,
			status: "pending",
			createdAt: Date.now(),
		});
		expect((await driver.pop())?.id).toBe("job_old");
	});
});

describe("bay > a delayed job", () => {
	it("is not handed out before its time", async () => {
		const driver = new MemoryDriver();
		const queue = new QueueManager(driver);

		await queue.dispatch("later", {}, { delay: 50 });
		expect(await driver.pop()).toBeNull();
		// Still queued, though — anything draining before shutdown must see it.
		expect(await driver.size()).toBe(1);

		await new Promise((resolve) => setTimeout(resolve, 60));
		expect((await driver.pop())?.name).toBe("later");
	});

	it("reads a duration the way a config file writes one", () => {
		expect(toMilliseconds(250, "delay")).toBe(250);
		expect(toMilliseconds("500ms", "delay")).toBe(500);
		expect(toMilliseconds("10s", "delay")).toBe(10_000);
		expect(toMilliseconds("1m", "delay")).toBe(60_000);
		expect(toMilliseconds("2h", "delay")).toBe(7_200_000);
		expect(toMilliseconds("1d", "delay")).toBe(86_400_000);
	});

	it("refuses a duration it cannot read rather than parking the job forever", () => {
		// `NaN` would become a `runAt` no clock ever reaches, with nothing to
		// read about it.
		expect(() => toMilliseconds("soon", "delay")).toThrow(/delay must be/);
		expect(() => toMilliseconds("10 weeks", "delay")).toThrow();
		expect(() => toMilliseconds(-1, "delay")).toThrow(/non-negative/);
	});
});

describe("bay > a job that takes too long", () => {
	it("fails the attempt instead of holding the worker", async () => {
		let finished = false;
		class Slow extends Job {
			async execute(): Promise<void> {
				await new Promise((resolve) => setTimeout(resolve, 500));
				finished = true;
			}
		}
		const driver = new MemoryDriver();
		const queue = new QueueManager(driver);

		await queue.dispatch(Slow, {}, { timeout: 20, maxAttempts: 1 });
		const started = Date.now();
		expect(await queue.processOne()).toBe(true);

		// The worker stopped waiting; the handler itself is not interruptible.
		expect(Date.now() - started).toBeLessThan(400);
		expect(finished).toBe(false);
		const failed = await driver.failed();
		expect(failed[0]?.error).toMatch(/exceeded its 20ms timeout/);
	});

	it("lets a job inside its timeout through untouched", async () => {
		class Quick extends Job {
			async execute(): Promise<void> {}
		}
		const driver = new MemoryDriver();
		const queue = new QueueManager(driver);
		await queue.dispatch(Quick, {}, { timeout: "1m" });
		expect(await queue.processOne()).toBe(true);
		expect(await driver.failed()).toEqual([]);
	});
});

describe("bay > a worker running several at once", () => {
	it("refuses a concurrency that is not a whole number >= 1", async () => {
		const queue = new QueueManager(new MemoryDriver());
		await expect(queue.work({ concurrency: 0 })).rejects.toThrow(
			/concurrency must be/,
		);
		await expect(queue.work({ concurrency: 1.5 })).rejects.toThrow(
			/concurrency must be/,
		);
	});

	it("overlaps them instead of running them one after another", async () => {
		let inFlight = 0;
		let peak = 0;
		class Wait extends Job {
			async execute(): Promise<void> {
				inFlight++;
				peak = Math.max(peak, inFlight);
				await new Promise((resolve) => setTimeout(resolve, 40));
				inFlight--;
			}
		}
		const queue = new QueueManager(new MemoryDriver());
		for (let i = 0; i < 4; i++) await queue.dispatch(Wait, {});

		const running = queue.work({ concurrency: 4, idleDelay: 10 });
		await new Promise((resolve) => setTimeout(resolve, 80));
		await queue.stop();
		await running.catch(() => {});

		// One at a time is the safe default and a poor one for anything that
		// waits: four jobs of 40ms would take 160ms in sequence.
		expect(peak).toBeGreaterThan(1);
	});
});

describe("bay > make:job", () => {
	it("derives the file name and the class name from what was asked for", () => {
		expect(toSnakeCase("SendWelcomeEmail")).toBe("send_welcome_email");
		expect(toSnakeCase("HTTPPing")).toBe("http_ping");
		expect(toPascalCase("send_welcome_email")).toBe("SendWelcomeEmail");

		const resolved = resolveJobName("emails/SendWelcomeEmail");
		expect(resolved.dir).toBe("emails");
		expect(resolved.fileName).toBe("send_welcome_email.ts");
		expect(resolved.className).toBe("SendWelcomeEmail");
	});

	it("refuses a name that would leave the jobs directory", () => {
		expect(() => resolveJobName("../../etc/passwd")).toThrow(
			/Invalid job name/,
		);
		expect(() => resolveJobName("a/../../b")).toThrow(/Invalid job name/);
		expect(() => resolveJobName("")).toThrow(/Invalid job name/);
		expect(() => resolveJobName("9lives")).toThrow(/Invalid job name/);
	});

	it("scaffolds a job that compiles against the class it extends", () => {
		const stub = jobStub("SendWelcomeEmail");
		expect(stub).toContain(
			"export default class SendWelcomeEmail extends Job<",
		);
		expect(stub).toContain("async execute()");
		expect(stub).toContain("async failed(error: Error)");
		expect(stub).toContain("import { Job } from '@c9up/bay'");
	});
});

describe("bay > queue:work", () => {
	it("reads --queue as a list, in order", () => {
		expect(parseQueues("emails,notifications")).toEqual([
			"emails",
			"notifications",
		]);
		expect(parseQueues(" critical , default ")).toEqual([
			"critical",
			"default",
		]);
		// A trailing comma must not name an empty queue nothing ever serves.
		expect(parseQueues("emails,")).toEqual(["emails"]);
		expect(parseQueues("")).toBeUndefined();
		expect(parseQueues(undefined)).toBeUndefined();
	});
});

describe("bay > where the job classes are", () => {
	it("reads a glob as the directory it starts with", () => {
		// The spelling upstream's config uses, so a config copied from there
		// finds the same files.
		expect(directoryOf("./app/jobs/**/*.{ts,js}")).toBe("app/jobs");
		expect(directoryOf("app/jobs")).toBe("app/jobs");
		expect(directoryOf("./app/jobs/")).toBe("app/jobs");
		expect(directoryOf("src/queue/*.ts")).toBe("src/queue");
	});
});
