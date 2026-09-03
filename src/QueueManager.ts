/**
 * QueueManager — dispatch and process background jobs.
 *
 * Usage:
 *   queue.register('send-email', new SendEmailHandler())
 *   await queue.dispatch('send-email', { to: 'user@example.com' })
 *   queue.work()
 */

export interface Job {
	id: string;
	name: string;
	payload: unknown;
	attempts: number;
	maxAttempts: number;
	status: "pending" | "processing" | "completed" | "failed";
	error?: string;
	createdAt: number;
	processedAt?: number;
	/**
	 * How many times this job has been recovered from a stalled worker.
	 *
	 * Separate from `attempts`, which counts times a handler RAN. A worker that
	 * dies mid-job never reaches the failure path, so `attempts` cannot see it —
	 * upstream carries the same two counters side by side for the same reason
	 * (`JobData.stalledCount` in `@boringnode/queue`).
	 */
	stalledCount?: number;
}

export interface JobHandler {
	handle(payload: unknown): Promise<void>;
}

export interface QueueDriver {
	push(job: Job): Promise<void>;
	pop(): Promise<Job | null>;
	fail(job: Job, error: string): Promise<void>;
	complete(job: Job): Promise<void>;
	retry(job: Job): Promise<void>;
	failed(): Promise<Job[]>;
	size(): Promise<number>;
	/**
	 * Optional crash recovery: move jobs orphaned in the driver's 'processing'
	 * state (expired visibility lease) back to pending, returning the count
	 * recovered. In-memory drivers omit this — their jobs don't survive a crash.
	 */
	recoverStale?(): Promise<number>;
	/**
	 * Optional lease renewal: tell the driver this job is still being worked on,
	 * answering `false` when the claim is gone (already recovered, or now held
	 * by another worker). Drivers with no lease omit it.
	 */
	renew?(job: Job): Promise<boolean>;
	/**
	 * How often the worker should call `renew` while a handler runs. The driver
	 * sets the cadence because the driver owns the deadline. Absent means no
	 * renewal.
	 */
	readonly renewIntervalMs?: number;
}

export class QueueManager {
	#driver: QueueDriver;
	#handlers: Map<string, JobHandler | (new () => JobHandler)> = new Map();
	#running = false;
	/** The running loop, so `stop()` can wait for it to finish. */
	#loopPromise: Promise<void> | undefined;
	/** Cuts the sleep between polls short. */
	#wake: (() => void) | undefined;
	#inflightPromise: Promise<boolean> | null = null;

	constructor(driver: QueueDriver) {
		this.#driver = driver;
	}

	/** Register a job handler. */
	register(name: string, handler: JobHandler | (new () => JobHandler)): void {
		this.#handlers.set(name, handler);
	}

	/** Dispatch a job to the queue. */
	async dispatch(
		name: string,
		payload: unknown,
		options?: { maxAttempts?: number },
	): Promise<string> {
		if (options?.maxAttempts !== undefined && options.maxAttempts < 1) {
			throw new Error("maxAttempts must be >= 1");
		}
		const id = `job_${crypto.randomUUID()}`;
		const job: Job = {
			id,
			name,
			payload,
			attempts: 0,
			maxAttempts: options?.maxAttempts ?? 3,
			status: "pending",
			createdAt: Date.now(),
		};
		await this.#driver.push(job);
		return id;
	}

	/** Process the next job in the queue. */
	async processOne(): Promise<boolean> {
		const job = await this.#driver.pop();
		if (!job) return false;

		const handlerOrClass = this.#handlers.get(job.name);
		if (!handlerOrClass) {
			process.stderr.write(
				`QueueManager: no handler registered for job '${job.name}'\n`,
			);
			await this.#driver.fail(
				job,
				`No handler registered for job: ${job.name}`,
			);
			return true;
		}

		const handler =
			typeof handlerOrClass === "function"
				? new handlerOrClass()
				: handlerOrClass;
		job.attempts++;
		job.status = "processing";
		job.processedAt = Date.now();

		// The lease a driver takes at pop() has a deadline, and a handler slower
		// than that deadline was being recovered and re-delivered WHILE IT WAS
		// STILL RUNNING — a second worker picked the job up, and the first one's
		// completion then removed an entry the second one owned. Upstream calls
		// the same mechanism a heartbeat (`Adapter.renewJobs`); a driver without
		// a lease supplies no cadence and nothing is scheduled.
		const stopRenewing = this.#startRenewing(job);
		let handled = false;
		try {
			await handler.handle(job.payload);
			handled = true;
		} catch (err) {
			const errorMsg = err instanceof Error ? err.message : String(err);
			if (job.attempts < job.maxAttempts) {
				job.status = "pending";
				await this.#driver.retry(job);
			} else {
				job.status = "failed";
				job.error = errorMsg;
				await this.#driver.fail(job, errorMsg);
			}
		} finally {
			stopRenewing();
		}

		// Outside the catch, and deliberately. Marking the job done is a write to
		// the driver, and a write can fail on its own — a Redis blip, a closed
		// connection. Inside, that failure was read as the HANDLER having failed:
		// the job went round again and the handler ran a second time, and once
		// `attempts` ran out the job was filed as failed with the driver's error
		// on it. A job that succeeded, in the failed list. The completion is
		// allowed to throw now; the job keeps its lease, and the ordinary stall
		// recovery is what re-delivers it.
		if (handled) {
			job.status = "completed";
			await this.#driver.complete(job);
		}

		return true;
	}

	/**
	 * Keep the driver's claim on `job` alive for as long as the handler runs.
	 * Returns the function that stops it — always called, including when the
	 * handler throws, so a finished job never keeps extending a lease.
	 */
	#startRenewing(job: Job): () => void {
		const driver = this.#driver;
		const every = driver.renewIntervalMs;
		if (!driver.renew || every === undefined || every <= 0) {
			return () => {};
		}
		const timer = setInterval(() => {
			// A renewal that fails is not a reason to interrupt the handler: the
			// job may already have been recovered, and the handler finishing is
			// still the best outcome available.
			void driver.renew?.(job).catch(() => {});
		}, every);
		// Unreffed: the handler's own promise is what holds the process open.
		timer.unref();
		return () => {
			clearInterval(timer);
		};
	}

	/**
	 * Start processing jobs continuously. Reclaims crash-orphaned jobs at
	 * startup and every `recoverStaleMs` thereafter (no-op for in-memory drivers
	 * without recoverStale) — otherwise a job left in 'processing' by a crashed
	 * worker would sit there forever.
	 */
	async work(pollIntervalMs = 1000, recoverStaleMs = 30_000): Promise<void> {
		if (pollIntervalMs <= 0) {
			throw new Error("pollIntervalMs must be positive");
		}
		if (recoverStaleMs <= 0) {
			throw new Error("recoverStaleMs must be positive");
		}
		if (this.#running) {
			throw new Error("QueueManager is already running");
		}
		this.#running = true;
		const loop = this.#loop(pollIntervalMs, recoverStaleMs);
		this.#loopPromise = loop;
		try {
			await loop;
		} finally {
			this.#loopPromise = undefined;
		}
	}

	/**
	 * The polling loop itself.
	 *
	 * Between jobs it sleeps, and that sleep is CANCELLABLE: `stop()` wakes it
	 * rather than waiting out the interval. Without that, stopping returned
	 * while the loop was still pending — up to a full poll interval of a worker
	 * that was supposed to be gone, and a timer holding the process open.
	 */
	async #loop(pollIntervalMs: number, recoverStaleMs: number): Promise<void> {
		await this.#tryRecoverStale();
		let lastRecover = Date.now();
		while (this.#running) {
			try {
				this.#inflightPromise = this.processOne();
				const processed = await this.#inflightPromise;
				if (!processed) await this.#sleep(pollIntervalMs);
			} catch (err) {
				process.stderr.write(
					`QueueManager processOne error: ${err instanceof Error ? err.message : String(err)}\n`,
				);
				await this.#sleep(pollIntervalMs);
			} finally {
				this.#inflightPromise = null;
			}
			if (this.#running && Date.now() - lastRecover >= recoverStaleMs) {
				await this.#tryRecoverStale();
				lastRecover = Date.now();
			}
		}
	}

	/** Wait, unless `stop()` says otherwise first. */
	#sleep(ms: number): Promise<void> {
		return new Promise((resolve) => {
			const timer = setTimeout(() => {
				this.#wake = undefined;
				resolve();
			}, ms);
			this.#wake = () => {
				clearTimeout(timer);
				this.#wake = undefined;
				resolve();
			};
		});
	}

	/** recoverStale() wrapper that swallows driver errors — used by the work loop. */
	async #tryRecoverStale(): Promise<void> {
		try {
			await this.recoverStale();
		} catch (err) {
			process.stderr.write(
				`QueueManager recoverStale error: ${err instanceof Error ? err.message : String(err)}\n`,
			);
		}
	}

	/**
	 * Reclaim jobs orphaned by a crashed worker — moves entries stuck in the
	 * driver's 'processing' state (expired lease) back to pending and returns
	 * the count recovered. Returns 0 for in-memory drivers without recovery.
	 * Called automatically by work(); also safe to schedule manually.
	 */
	async recoverStale(): Promise<number> {
		return (await this.#driver.recoverStale?.()) ?? 0;
	}

	/** Await the currently in-flight processOne, if any. */
	async drain(): Promise<void> {
		if (this.#inflightPromise) {
			await this.#inflightPromise.catch(() => {});
		}
	}

	/**
	 * Stop the worker and wait for it to actually be gone.
	 *
	 * Awaits the LOOP, not just the job in flight: a stop that returns while
	 * the loop is still sleeping leaves a worker running past the teardown that
	 * asked it to stop.
	 */
	async stop(): Promise<void> {
		this.#running = false;
		this.#wake?.();
		await this.drain();
		if (this.#loopPromise) await this.#loopPromise.catch(() => {});
	}

	/** Get failed jobs. */
	async failedJobs(): Promise<Job[]> {
		return this.#driver.failed();
	}

	/** Get queue size. */
	async size(): Promise<number> {
		return this.#driver.size();
	}
}
