/**
 * A timed-out attempt is told it timed out, and keeps its worker slot until it
 * actually returns — so `concurrency` stays a bound on what really runs.
 */
import { describe, expect, it, vi } from "vitest";
import { MemoryDriver } from "../../src/drivers/MemoryDriver.js";
import { Job } from "../../src/Job.js";
import { QueueManager } from "../../src/QueueManager.js";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe("bay > a timed-out handler", () => {
	it("keeps its slot until it returns, so concurrency 1 never runs two at once", async () => {
		const queue = new QueueManager(new MemoryDriver());
		let live = 0;
		let peak = 0;
		queue.register("slow", {
			async handle() {
				live++;
				peak = Math.max(peak, live);
				try {
					await sleep(120);
				} finally {
					live--;
				}
			},
		});
		await queue.dispatch("slow", {}, { timeout: 10, maxAttempts: 4 });

		const working = queue.work({ concurrency: 1, idleDelay: 5 });
		await sleep(250);
		await queue.stop();
		await working;

		expect(peak).toBe(1);
	});

	it("aborts the signal a registered handler is given", async () => {
		const queue = new QueueManager(new MemoryDriver());
		let seen: AbortSignal | undefined;
		queue.register("slow", {
			async handle(_payload, signal) {
				seen = signal;
				await new Promise((resolve) =>
					signal?.addEventListener("abort", resolve, { once: true }),
				);
			},
		});
		await queue.dispatch("slow", {}, { timeout: 10, maxAttempts: 1 });

		await queue.processOne();

		expect(seen?.aborted).toBe(true);
		expect((await queue.failedJobs())[0]?.error).toMatch(/10ms timeout/);
	});

	it("aborts this.signal on a job class, and leaves it alone within the timeout", async () => {
		const signals: AbortSignal[] = [];
		class Slow extends Job<{ ms: number }> {
			static override options = { timeout: 20, maxRetries: 1 };
			async execute(): Promise<void> {
				signals.push(this.signal);
				await sleep(this.payload.ms);
			}
		}
		const queue = new QueueManager(new MemoryDriver());
		await queue.dispatch(Slow, { ms: 60 });
		await queue.dispatch(Slow, { ms: 1 });

		await queue.processOne();
		await queue.processOne();

		expect(signals.map((signal) => signal.aborted)).toEqual([true, false]);
	});

	it("returns from processOne only once the abandoned attempt has returned", async () => {
		const queue = new QueueManager(new MemoryDriver());
		let finished = false;
		queue.register("slow", {
			async handle() {
				await sleep(60);
				finished = true;
			},
		});
		await queue.dispatch("slow", {}, { timeout: 5, maxAttempts: 1 });

		await queue.processOne();

		expect(finished).toBe(true);
		expect(await queue.failedJobs()).toHaveLength(1);
	});
});

describe("bay > a driver write that fails after a timeout", () => {
	/** A memory driver whose `method` rejects the first time it is called. */
	function flakyDriver(method: "retry" | "fail"): MemoryDriver {
		const driver = new MemoryDriver();
		let thrown = false;
		const original = driver[method].bind(driver);
		Object.defineProperty(driver, method, {
			value: async (...args: Parameters<MemoryDriver["fail"]>) => {
				if (!thrown) {
					thrown = true;
					throw new Error("redis went away");
				}
				return original(...args);
			},
		});
		return driver;
	}

	for (const method of ["retry", "fail"] as const) {
		it(`holds the slot until the aborted handler returns when ${method}() rejects`, async () => {
			const queue = new QueueManager(flakyDriver(method));
			let release: (() => void) | undefined;
			let aborted = false;
			let failedHook = 0;
			class Stubborn extends Job<Record<string, never>> {
				static override options = {
					timeout: 5,
					maxRetries: method === "retry" ? 2 : 1,
				};
				async execute(): Promise<void> {
					// Ignores its signal, the case the abort wait exists for.
					this.signal.addEventListener("abort", () => {
						aborted = true;
					});
					await new Promise<void>((resolve) => {
						release = resolve;
					});
				}
				override async failed(): Promise<void> {
					failedHook++;
				}
			}
			await queue.dispatch(Stubborn, {});

			let settled = false;
			const processing = queue.processOne().then(
				() => {
					settled = true;
				},
				(err: unknown) => {
					settled = true;
					throw err;
				},
			);
			await vi.waitFor(() => expect(aborted).toBe(true));
			for (let i = 0; i < 10; i++) await new Promise((r) => setImmediate(r));
			// The handler is still running, so the slot is still taken.
			expect(settled).toBe(false);

			if (release === undefined) throw new Error("expected the handler");
			release();
			// The write's failure is still reported, after the wait.
			await expect(processing).rejects.toThrow(/redis went away/);
			expect(failedHook).toBe(method === "fail" ? 1 : 0);
		});
	}
});
