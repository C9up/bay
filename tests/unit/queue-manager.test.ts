import { describe, expect, it, vi } from "vitest";
import {
	type Job,
	type JobHandler,
	type QueueDriver,
	QueueManager,
} from "../../src/QueueManager.js";

/** Narrow away null/undefined without a `!` non-null assertion (which lies to the compiler). */
function defined<T>(value: T | null | undefined): T {
	if (value == null) throw new Error("expected a defined value");
	return value;
}

class CapturingDriver implements QueueDriver {
	pending: Job[] = [];
	completed: Job[] = [];
	failedList: Job[] = [];
	retried: Job[] = [];
	failArgs: Array<{ job: Job; error: string }> = [];

	async push(job: Job): Promise<void> {
		this.pending.push(job);
	}
	async pop(): Promise<Job | null> {
		return this.pending.shift() ?? null;
	}
	async complete(job: Job): Promise<void> {
		this.completed.push(job);
	}
	async fail(job: Job, error: string): Promise<void> {
		this.failArgs.push({ job, error });
		this.failedList.push(job);
	}
	async retry(job: Job): Promise<void> {
		this.retried.push(job);
		this.pending.push(job);
	}
	async failed(): Promise<Job[]> {
		return [...this.failedList];
	}
	async size(): Promise<number> {
		return this.pending.length;
	}
}

describe("bay > QueueManager > dispatch", () => {
	it("creates a job with sane defaults and pushes it via the driver", async () => {
		const driver = new CapturingDriver();
		const q = new QueueManager(driver);
		const id = await q.dispatch("send-email", { to: "x@y" });
		expect(id).toMatch(/^job_/);
		expect(driver.pending).toHaveLength(1);
		const job = defined(driver.pending[0]);
		expect(job.name).toBe("send-email");
		expect(job.payload).toEqual({ to: "x@y" });
		expect(job.maxAttempts).toBe(3);
		expect(job.attempts).toBe(0);
		expect(job.status).toBe("pending");
	});

	it("respects an explicit maxAttempts override", async () => {
		const driver = new CapturingDriver();
		await new QueueManager(driver).dispatch("x", null, { maxAttempts: 5 });
		expect(driver.pending[0]?.maxAttempts).toBe(5);
	});

	it("rejects maxAttempts < 1 (would never run the handler even once)", async () => {
		const driver = new CapturingDriver();
		const q = new QueueManager(driver);
		await expect(q.dispatch("x", null, { maxAttempts: 0 })).rejects.toThrow(
			/maxAttempts must be >= 1/,
		);
	});
});

describe("bay > QueueManager > processOne", () => {
	it("returns false when the queue is empty", async () => {
		expect(await new QueueManager(new CapturingDriver()).processOne()).toBe(
			false,
		);
	});

	it("fails the job (no retry) when no handler is registered for its name", async () => {
		const driver = new CapturingDriver();
		const stderrSpy = vi
			.spyOn(process.stderr, "write")
			.mockImplementation((() => true) as never);
		try {
			const q = new QueueManager(driver);
			await q.dispatch("missing-handler", null);
			expect(await q.processOne()).toBe(true);
			expect(driver.failArgs).toHaveLength(1);
			expect(driver.failArgs[0]?.error).toMatch(/No handler registered/);
		} finally {
			stderrSpy.mockRestore();
		}
	});

	it("invokes the registered handler with the job payload and completes on success", async () => {
		const driver = new CapturingDriver();
		const q = new QueueManager(driver);
		const handler: JobHandler = {
			handle: vi.fn(async () => {}),
		};
		q.register("email", handler);
		await q.dispatch("email", { id: 42 });
		expect(await q.processOne()).toBe(true);
		expect(handler.handle).toHaveBeenCalledWith({ id: 42 });
		expect(driver.completed).toHaveLength(1);
		expect(driver.completed[0]?.status).toBe("completed");
	});

	it("instantiates handler classes per job (newable form)", async () => {
		const driver = new CapturingDriver();
		const q = new QueueManager(driver);
		const constructed: number[] = [];
		class HandlerClass implements JobHandler {
			constructor() {
				constructed.push(1);
			}
			async handle() {}
		}
		q.register("c", HandlerClass);
		await q.dispatch("c", null);
		await q.dispatch("c", null);
		await q.processOne();
		await q.processOne();
		expect(constructed.length).toBe(2);
	});

	it("retries on failure when attempts < maxAttempts", async () => {
		const driver = new CapturingDriver();
		const q = new QueueManager(driver);
		q.register("flaky", {
			async handle() {
				throw new Error("transient");
			},
		});
		await q.dispatch("flaky", null, { maxAttempts: 3 });
		await q.processOne();
		expect(driver.retried).toHaveLength(1);
		expect(driver.failedList).toHaveLength(0);
		expect(driver.retried[0]?.attempts).toBe(1);
		expect(driver.retried[0]?.status).toBe("pending");
	});

	it("fails permanently once attempts reaches maxAttempts", async () => {
		const driver = new CapturingDriver();
		const q = new QueueManager(driver);
		q.register("doomed", {
			async handle() {
				throw new Error("perma");
			},
		});
		await q.dispatch("doomed", null, { maxAttempts: 1 });
		await q.processOne();
		expect(driver.failArgs).toHaveLength(1);
		expect(driver.failArgs[0]?.error).toBe("perma");
		expect(driver.retried).toHaveLength(0);
	});

	it("captures non-Error throwables via String(err) when failing the job", async () => {
		const driver = new CapturingDriver();
		const q = new QueueManager(driver);
		q.register("weird", {
			// Async handler that rejects with a non-Error value. Same runtime
			// effect as `throw "string-error"` but avoids the biome
			// `useThrowOnlyError` lint without a suppress comment.
			handle(): Promise<void> {
				return Promise.reject("string-error");
			},
		});
		await q.dispatch("weird", null, { maxAttempts: 1 });
		await q.processOne();
		expect(driver.failArgs[0]?.error).toBe("string-error");
	});
});

describe("bay > QueueManager > work / stop", () => {
	it("rejects pollIntervalMs <= 0 (would busy-loop)", async () => {
		const q = new QueueManager(new CapturingDriver());
		await expect(q.work(0)).rejects.toThrow(/pollIntervalMs must be positive/);
		await expect(q.work(-1)).rejects.toThrow(/pollIntervalMs must be positive/);
	});

	it("refuses to start a second concurrent work() loop", async () => {
		const driver = new CapturingDriver();
		const q = new QueueManager(driver);
		// Kick off work() but stop it on the next tick so the test doesn't hang.
		const looping = q.work(10);
		await Promise.resolve();
		await expect(q.work(10)).rejects.toThrow(/already running/);
		await q.stop();
		await looping;
	});

	it("processes pending jobs when work() loop is running, then stops cleanly", async () => {
		const driver = new CapturingDriver();
		const q = new QueueManager(driver);
		q.register("ping", {
			async handle() {},
		});
		await q.dispatch("ping", null);
		await q.dispatch("ping", null);

		const looping = q.work(5);
		// Yield long enough for both jobs to drain.
		await new Promise((r) => setTimeout(r, 50));
		await q.stop();
		await looping;

		expect(driver.completed.length).toBeGreaterThanOrEqual(2);
	});

	it("logs and continues the loop when processOne throws", async () => {
		const driver = new CapturingDriver();
		// Force pop() to throw on the very first call.
		let popCalls = 0;
		driver.pop = async () => {
			popCalls++;
			if (popCalls === 1) throw new Error("driver-fault");
			return null;
		};
		const stderrSpy = vi
			.spyOn(process.stderr, "write")
			.mockImplementation((() => true) as never);
		try {
			const q = new QueueManager(driver);
			const looping = q.work(5);
			await new Promise((r) => setTimeout(r, 30));
			await q.stop();
			await looping;
			expect(stderrSpy).toHaveBeenCalledWith(
				expect.stringMatching(/processOne error.*driver-fault/),
			);
		} finally {
			stderrSpy.mockRestore();
		}
	});
});

describe("bay > QueueManager > drain / passthrough getters", () => {
	it("drain() resolves immediately when no work is in flight", async () => {
		await expect(
			new QueueManager(new CapturingDriver()).drain(),
		).resolves.toBeUndefined();
	});

	it("failedJobs() and size() proxy to the driver", async () => {
		const driver = new CapturingDriver();
		const q = new QueueManager(driver);
		await q.dispatch("x", null);
		expect(await q.size()).toBe(1);

		driver.failedList.push({
			id: "j",
			name: "x",
			payload: null,
			attempts: 1,
			maxAttempts: 1,
			status: "failed",
			createdAt: 0,
		});
		expect(await q.failedJobs()).toHaveLength(1);
	});
});
