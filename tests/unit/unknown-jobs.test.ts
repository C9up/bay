/**
 * A job no handler answers to YET waits for a worker that knows it — the
 * ordinary state of a rolling deploy — instead of being filed as failed on
 * first sight.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MemoryDriver } from "../../src/drivers/MemoryDriver.js";
import { Job } from "../../src/Job.js";
import { QueueManager } from "../../src/QueueManager.js";

const UNKNOWN_JOB_RETRY_DELAY = 30_000;

describe("bay > a job the worker does not know yet", () => {
	let stderr: ReturnType<typeof vi.spyOn>;
	beforeEach(() => {
		vi.useFakeTimers({ toFake: ["Date"] });
		stderr = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
	});
	afterEach(() => {
		stderr.mockRestore();
		vi.useRealTimers();
	});

	/** Let the unknown-job delay pass. */
	function later(): void {
		vi.setSystemTime(Date.now() + UNKNOWN_JOB_RETRY_DELAY + 1);
	}

	it("is put back with a delay rather than failed, and says so", async () => {
		const queue = new QueueManager(new MemoryDriver());
		await queue.dispatch("not-yet-deployed", {});

		expect(await queue.processOne()).toBe(true);

		expect(await queue.failedJobs()).toEqual([]);
		expect(await queue.size()).toBe(1);
		// Not due before the delay.
		expect(await queue.processOne()).toBe(false);
		expect(stderr).toHaveBeenCalledWith(
			expect.stringMatching(/not-yet-deployed.*1\/10/),
		);
	});

	it("runs once a worker that knows it picks it up", async () => {
		const queue = new QueueManager(new MemoryDriver());
		await queue.dispatch("new-job", { n: 1 });
		await queue.processOne();

		const seen: unknown[] = [];
		queue.register("new-job", {
			async handle(payload) {
				seen.push(payload);
			},
		});
		later();
		await queue.processOne();

		expect(seen).toEqual([{ n: 1 }]);
		expect(await queue.failedJobs()).toEqual([]);
	});

	it("is failed once the sightings run out, without spending its attempts", async () => {
		const queue = new QueueManager(new MemoryDriver(), {
			unknownJobRetries: 2,
		});
		await queue.dispatch("gone", {}, { maxAttempts: 1 });

		for (let sighting = 0; sighting < 3; sighting++) {
			expect(await queue.processOne()).toBe(true);
			later();
		}

		const failed = await queue.failedJobs();
		expect(failed.map((job) => [job.attempts, job.error])).toEqual([
			[0, 'Requested job "gone" is not registered'],
		]);
	});

	it("takes a record named after an Object.prototype member for an unknown job", async () => {
		// A shared store (Redis written by another service) can hold any name.
		// Looked up on a plain object, `constructor` answered `Object` and
		// `__proto__` answered `Object.prototype`: a handler that does not
		// exist, run and failed, spending the job's attempts.
		class Known extends Job<Record<string, never>> {
			async execute(): Promise<void> {}
		}
		const queue = new QueueManager(new MemoryDriver());
		queue.registerJob(Known);
		queue.register("handled", { async handle() {} });
		for (const name of ["constructor", "__proto__", "toString"]) {
			await queue.dispatch(name, {}, { maxAttempts: 1 });
			expect(await queue.processOne()).toBe(true);
			expect(stderr).toHaveBeenCalledWith(
				expect.stringContaining(`no handler registered for job '${name}' yet`),
			);
		}

		// Put back, attempts untouched, nothing failed.
		expect(await queue.failedJobs()).toEqual([]);
		expect(await queue.size()).toBe(3);
	});

	it("refuses an unknownJobRetries that is not a count", async () => {
		for (const bad of [-1, 1.5, Number.NaN]) {
			expect(
				() => new QueueManager(new MemoryDriver(), { unknownJobRetries: bad }),
			).toThrow(
				expect.objectContaining({
					code: "E_BAY_INVALID_WORKER_OPTIONS",
					message: expect.stringMatching(/unknownJobRetries/),
				}),
			);
			await expect(
				new QueueManager(new MemoryDriver()).work({ unknownJobRetries: bad }),
			).rejects.toThrow(/unknownJobRetries/);
		}
	});
});
