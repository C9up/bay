import { describe, expect, it } from "vitest";
import type { Job } from "../../src/QueueManager.js";
import { FakeQueue } from "../../src/testing/FakeQueue.js";

function makeJob(overrides: Partial<Job> = {}): Job {
	return {
		id: overrides.id ?? `job_${Math.random().toString(36).slice(2)}`,
		name: overrides.name ?? "send-email",
		payload: overrides.payload ?? { to: "user@x.com" },
		attempts: overrides.attempts ?? 0,
		maxAttempts: overrides.maxAttempts ?? 3,
		status: overrides.status ?? "pending",
		error: overrides.error,
		createdAt: overrides.createdAt ?? Date.now(),
		processedAt: overrides.processedAt,
	};
}

describe("FakeQueue — QueueDriver surface", () => {
	it("push captures the job", async () => {
		const q = new FakeQueue();
		await q.push(makeJob({ name: "send-email" }));
		expect(q.getPushed()).toHaveLength(1);
		expect(q.getPushed()[0].name).toBe("send-email");
	});

	it("pop always returns null (fakes never auto-dispatch)", async () => {
		const q = new FakeQueue();
		await q.push(makeJob());
		expect(await q.pop()).toBeNull();
	});

	it("fail mutates the captured copy to status=failed", async () => {
		const q = new FakeQueue();
		const job = makeJob({ id: "job_1" });
		await q.push(job);
		await q.fail(job, "boom");
		const captured = q.getPushed()[0];
		expect(captured.status).toBe("failed");
		expect(captured.error).toBe("boom");
	});

	it("complete sets status=completed + processedAt", async () => {
		const q = new FakeQueue();
		const job = makeJob({ id: "job_2" });
		await q.push(job);
		await q.complete(job);
		const captured = q.getPushed()[0];
		expect(captured.status).toBe("completed");
		expect(typeof captured.processedAt).toBe("number");
	});

	it("retry increments attempts, resets to pending, clears error/processedAt", async () => {
		const q = new FakeQueue();
		const job = makeJob({
			id: "job_3",
			attempts: 1,
			status: "failed",
			error: "prior failure",
			processedAt: 12345,
		});
		await q.push(job);
		await q.retry(job);
		const captured = q.getPushed()[0];
		expect(captured.attempts).toBe(2);
		expect(captured.status).toBe("pending");
		// M2 review fix: a pending job carrying a stale `error` string
		// and `processedAt` violates the Job state-machine invariant.
		expect(captured.error).toBeUndefined();
		expect(captured.processedAt).toBeUndefined();
	});

	it("push rejects duplicate ids", async () => {
		const q = new FakeQueue();
		await q.push(makeJob({ id: "dup" }));
		await expect(q.push(makeJob({ id: "dup" }))).rejects.toThrow(
			/duplicate job id/,
		);
	});

	it("fail throws when job id is not in the queue", async () => {
		const q = new FakeQueue();
		const orphan = makeJob({ id: "never-pushed" });
		await expect(q.fail(orphan, "oops")).rejects.toThrow(/not in the queue/);
	});

	it("complete throws when job id is not in the queue", async () => {
		const q = new FakeQueue();
		const orphan = makeJob({ id: "never-pushed" });
		await expect(q.complete(orphan)).rejects.toThrow(/not in the queue/);
	});

	it("retry throws when job id is not in the queue", async () => {
		const q = new FakeQueue();
		const orphan = makeJob({ id: "never-pushed" });
		await expect(q.retry(orphan)).rejects.toThrow(/not in the queue/);
	});

	it("failed returns only failed jobs", async () => {
		const q = new FakeQueue();
		const a = makeJob({ id: "a" });
		const b = makeJob({ id: "b" });
		await q.push(a);
		await q.push(b);
		await q.fail(a, "oops");
		const failed = await q.failed();
		expect(failed).toHaveLength(1);
		expect(failed[0].id).toBe("a");
	});

	it("size counts only pending jobs", async () => {
		const q = new FakeQueue();
		const a = makeJob({ id: "a" });
		const b = makeJob({ id: "b" });
		await q.push(a);
		await q.push(b);
		expect(await q.size()).toBe(2);
		await q.complete(a);
		expect(await q.size()).toBe(1);
	});
});

describe("FakeQueue — testing helpers", () => {
	it("getPushed returns a defensive snapshot (mutations do not leak back)", async () => {
		const q = new FakeQueue();
		await q.push(makeJob({ name: "a" }));
		const snapshot = q.getPushed();
		snapshot[0].name = "mutated";
		expect(q.getPushed()[0].name).toBe("a");
	});

	it("reset clears the captured array", async () => {
		const q = new FakeQueue();
		await q.push(makeJob());
		await q.push(makeJob());
		expect(q.getPushed()).toHaveLength(2);
		q.reset();
		expect(q.getPushed()).toHaveLength(0);
	});
});

describe("FakeQueue — assertPushed", () => {
	it("passes when a job with the given name was pushed", async () => {
		const q = new FakeQueue();
		await q.push(makeJob({ name: "send-email" }));
		expect(() => q.assertPushed("send-email")).not.toThrow();
	});

	it("throws when no job matches the name", async () => {
		const q = new FakeQueue();
		await q.push(makeJob({ name: "send-email" }));
		expect(() => q.assertPushed("process-payment")).toThrow(
			/no captured job matches/,
		);
	});

	it("payloadMatches narrows further", async () => {
		const q = new FakeQueue();
		await q.push(makeJob({ name: "notify", payload: { userId: 1 } }));
		await q.push(makeJob({ name: "notify", payload: { userId: 2 } }));
		expect(() =>
			q.assertPushed("notify", {
				payloadMatches: (p) => (p as { userId: number }).userId === 2,
			}),
		).not.toThrow();
		expect(() =>
			q.assertPushed("notify", {
				payloadMatches: (p) => (p as { userId: number }).userId === 99,
			}),
		).toThrow(/no captured job matches/);
	});

	it("function predicate gives full job access", async () => {
		const q = new FakeQueue();
		await q.push(makeJob({ name: "retry-me", attempts: 3 }));
		expect(() =>
			q.assertPushed("retry-me", (j) => j.attempts >= 3),
		).not.toThrow();
		expect(() => q.assertPushed("retry-me", (j) => j.attempts > 10)).toThrow(
			/no captured job matches/,
		);
	});

	it("error message includes captured job summary", async () => {
		const q = new FakeQueue();
		await q.push(makeJob({ name: "send-email" }));
		let err: unknown;
		try {
			q.assertPushed("does-not-exist");
		} catch (e) {
			err = e;
		}
		expect(String(err)).toContain("Captured (1)");
		expect(String(err)).toContain("send-email");
	});

	it("error message flags an empty-object predicate as name-only", async () => {
		const q = new FakeQueue();
		await q.push(makeJob({ name: "send-email" }));
		let err: unknown;
		try {
			q.assertPushed("does-not-exist", {});
		} catch (e) {
			err = e;
		}
		// FakeQueue.ts describePredicate() empty-object branch — clarifies
		// to the reader that {} predicate collapses to name-only match.
		expect(String(err)).toContain("empty predicate (name-only)");
	});
});

describe("FakeQueue — assertNotPushed", () => {
	it("passes when no job matches the name", async () => {
		const q = new FakeQueue();
		await q.push(makeJob({ name: "a" }));
		expect(() => q.assertNotPushed("b")).not.toThrow();
	});

	it("throws when at least one job matches", async () => {
		const q = new FakeQueue();
		await q.push(makeJob({ name: "a" }));
		expect(() => q.assertNotPushed("a")).toThrow(
			/at least one captured job matches/,
		);
	});

	it("payloadMatches narrows the negative assertion", async () => {
		const q = new FakeQueue();
		await q.push(makeJob({ name: "notify", payload: { userId: 1 } }));
		expect(() =>
			q.assertNotPushed("notify", {
				payloadMatches: (p) => (p as { userId: number }).userId === 99,
			}),
		).not.toThrow();
		expect(() =>
			q.assertNotPushed("notify", {
				payloadMatches: (p) => (p as { userId: number }).userId === 1,
			}),
		).toThrow(/at least one captured job matches/);
	});
});
