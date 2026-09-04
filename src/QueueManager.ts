import {
	DEFAULT_QUEUE,
	isJobClass,
	type JobClass,
	type JobOptions,
	toMilliseconds,
} from "./Job.js";

/**
 * QueueManager — dispatch and process background jobs.
 *
 * Usage:
 *   queue.register('send-email', new SendEmailHandler())
 *   await queue.dispatch('send-email', { to: 'user@example.com' })
 *   queue.work()
 */

export interface JobRecord {
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
	/**
	 * Named queue this job waits in. A worker is told which queues to serve, so
	 * a slow queue cannot starve a fast one sharing the same process.
	 *
	 * Optional on the wire: a job written by a version that had no queues
	 * parses without it and reads as `default`.
	 */
	queue?: string;
	/** Epoch ms before which no worker may take the job (`delay`). */
	runAt?: number;
	/** Milliseconds the handler gets before the attempt counts as failed. */
	timeout?: number;
}

/** The queue a record belongs to, for a record written before named queues. */
export function queueOf(job: JobRecord): string {
	return job.queue ?? DEFAULT_QUEUE;
}

export interface JobHandler {
	handle(payload: unknown): Promise<void>;
}

/**
 * One attempt, whichever way the job was declared.
 *
 * A registered handler and a job class do the same two things — run, and maybe
 * be told it finally failed — so `processOne` deals with this and not with two
 * shapes.
 */
interface JobRunner {
	run(payload: unknown): Promise<void> | void;
	onFailed?(error: Error): Promise<void> | void;
}

/**
 * Reject once `ms` has passed, without touching the work.
 *
 * Nothing in Node can interrupt a running promise, so a job that ignores its
 * timeout goes on burning CPU. What this buys is that the WORKER stops waiting
 * for it — otherwise one stuck job costs the whole worker, which never picks
 * anything up again.
 */
function withTimeout(
	work: Promise<void> | void,
	ms: number | undefined,
	name: string,
): Promise<void> {
	const settled = Promise.resolve(work);
	if (ms === undefined || ms <= 0) return settled;
	return new Promise<void>((resolve, reject) => {
		const timer = setTimeout(() => {
			reject(new Error(`Job '${name}' exceeded its ${ms}ms timeout`));
		}, ms);
		// Unreffed: a pending timeout must not be the reason the process stays up.
		timer.unref();
		settled.then(
			(value) => {
				clearTimeout(timer);
				resolve(value);
			},
			(err) => {
				clearTimeout(timer);
				reject(err);
			},
		);
	});
}

/**
 * What a worker is told before it starts, by upstream's names for it
 * (`WorkerConfig.idleDelay`, `WorkerConfig.stalledInterval`).
 */
export interface WorkerOptions {
	/** Milliseconds to wait after finding nothing to do. Default `2000`. */
	idleDelay?: number;
	/** Milliseconds between stalled-job sweeps. Default `30_000`. */
	stalledInterval?: number;
	/**
	 * How many jobs this worker runs at once. Default `1`.
	 *
	 * One job at a time is the safe default and a poor one for anything that
	 * waits on the network: a worker sending mail spends nearly all of its time
	 * idle with a queue behind it.
	 */
	concurrency?: number;
	/**
	 * Which named queues to serve, in order. Default: the `default` queue.
	 *
	 * Naming them is how a slow queue is kept from starving a fast one — run
	 * one worker for `emails` and another for `default`, rather than one worker
	 * taking whatever comes.
	 */
	queues?: readonly string[];
}

/**
 * What a single `dispatch` may override.
 *
 * Everything a job class declares, plus `maxAttempts` — the name this method
 * took before job classes existed, kept because it is what every existing call
 * site passes. `maxRetries` is the class's name for the same number.
 */
export interface DispatchOptions extends JobOptions {
	/** The older spelling of `maxRetries`. Wins when both are given. */
	maxAttempts?: number;
}

export interface QueueDriver {
	push(job: JobRecord): Promise<void>;
	/**
	 * Take the next job from one of `queues`, or from the default queue when
	 * the caller names none.
	 *
	 * A driver written before named queues takes no argument and keeps working:
	 * it serves the one queue it has, which is the default one.
	 */
	pop(queues?: readonly string[]): Promise<JobRecord | null>;
	fail(job: JobRecord, error: string): Promise<void>;
	complete(job: JobRecord): Promise<void>;
	retry(job: JobRecord): Promise<void>;
	failed(): Promise<JobRecord[]>;
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
	renew?(job: JobRecord): Promise<boolean>;
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
	#jobs: Map<string, JobClass> = new Map();
	#running = false;
	/** The running loop, so `stop()` can wait for it to finish. */
	#loopPromise: Promise<void> | undefined;
	/** Cuts the sleep between polls short. */
	#wake: (() => void) | undefined;
	/** Every attempt currently in flight, so `drain()` can wait for all of them. */
	#inflight: Set<Promise<boolean>> = new Set();

	/** Defaults for `work()`, from the config's `worker` block. */
	readonly #workerDefaults: WorkerOptions;

	constructor(driver: QueueDriver, workerDefaults?: WorkerOptions) {
		this.#driver = driver;
		this.#workerDefaults = workerDefaults ?? {};
	}

	/** Register a job handler under a name. */
	register(name: string, handler: JobHandler | (new () => JobHandler)): void {
		this.#handlers.set(name, handler);
	}

	/**
	 * Register a job class under its own name, so a worker in another process
	 * can find it from what the record carries.
	 *
	 * `dispatch(SomeJob, …)` does this on its own; call it directly when the
	 * worker never dispatches — which is the ordinary case, since a worker
	 * process runs jobs and an HTTP process queues them.
	 */
	registerJob(job: JobClass): void {
		this.#jobs.set(job.name, job);
	}

	/** Every job class this manager knows, by name. */
	registeredJobs(): ReadonlyMap<string, JobClass> {
		return this.#jobs;
	}

	/**
	 * Queue a job.
	 *
	 * Takes a job class — the payload is then typed by the class's own
	 * parameter, so a field the handler reads cannot be one the dispatcher
	 * never sent:
	 *
	 *     await queue.dispatch(SendEmail, { to: 'user@example.com' })
	 *
	 * A registered name still works, and is what a job whose name is computed
	 * at runtime needs:
	 *
	 *     await queue.dispatch('send-email', { to: '…' })
	 */
	async dispatch<Payload>(
		job: JobClass<Payload>,
		payload: Payload,
		options?: DispatchOptions,
	): Promise<string>;
	async dispatch(
		name: string,
		payload: unknown,
		options?: DispatchOptions,
	): Promise<string>;
	async dispatch(
		job: string | JobClass,
		payload: unknown,
		options: DispatchOptions = {},
	): Promise<string> {
		let name: string;
		let declared: JobOptions = {};
		if (isJobClass(job)) {
			name = job.name;
			declared = job.options ?? {};
			// So a worker that never dispatches still resolves it by name.
			this.#jobs.set(name, job);
		} else {
			name = job;
		}

		// The call site wins over the class, and the class over the defaults —
		// the same order `work()` reads its own options in.
		const maxAttempts =
			options.maxAttempts ?? options.maxRetries ?? declared.maxRetries ?? 3;
		if (maxAttempts < 1) {
			throw new Error("maxAttempts must be >= 1");
		}
		const delay = options.delay ?? declared.delay;
		const delayMs = delay === undefined ? 0 : toMilliseconds(delay, "delay");
		const timeout = options.timeout ?? declared.timeout;

		const id = `job_${crypto.randomUUID()}`;
		const record: JobRecord = {
			id,
			name,
			payload,
			attempts: 0,
			maxAttempts,
			status: "pending",
			createdAt: Date.now(),
			queue: options.queue ?? declared.queue ?? DEFAULT_QUEUE,
		};
		if (delayMs > 0) record.runAt = Date.now() + delayMs;
		if (timeout !== undefined) {
			record.timeout = toMilliseconds(timeout, "timeout");
		}
		await this.#driver.push(record);
		return id;
	}

	/**
	 * Process the next job, from `queues` when the caller names any.
	 *
	 * Returns whether there was one — the loop uses that to decide between
	 * asking again and sleeping.
	 */
	async processOne(queues?: readonly string[]): Promise<boolean> {
		const job = await this.#driver.pop(queues);
		if (!job) return false;

		const run = this.#resolveRunner(job.name);
		if (!run) {
			process.stderr.write(
				`QueueManager: no handler registered for job '${job.name}'\n`,
			);
			await this.#driver.fail(
				job,
				`No handler registered for job: ${job.name}`,
			);
			return true;
		}

		const handler = run;
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
			await withTimeout(handler.run(job.payload), job.timeout, job.name);
			handled = true;
		} catch (err) {
			const error = err instanceof Error ? err : new Error(String(err));
			const errorMsg = error.message;
			if (job.attempts < job.maxAttempts) {
				job.status = "pending";
				await this.#driver.retry(job);
			} else {
				job.status = "failed";
				job.error = errorMsg;
				await this.#driver.fail(job, errorMsg);
				// After the last attempt, not after each one. A throw here is
				// reported and swallowed: the job has already failed, and
				// failing to say so must not be read as a second failure.
				try {
					await handler.onFailed?.(error);
				} catch (hookErr) {
					process.stderr.write(
						`QueueManager: failed() hook of '${job.name}' threw: ${
							hookErr instanceof Error ? hookErr.message : String(hookErr)
						}\n`,
					);
				}
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
	 * The runner for `name`: a registered handler, or a job class.
	 *
	 * A handler registered under the name wins — an application that registers
	 * one deliberately is overriding whatever else answers to it.
	 */
	#resolveRunner(name: string): JobRunner | undefined {
		const handlerOrClass = this.#handlers.get(name);
		if (handlerOrClass !== undefined) {
			const handler =
				typeof handlerOrClass === "function"
					? new handlerOrClass()
					: handlerOrClass;
			return { run: (payload) => handler.handle(payload) };
		}
		const JobConstructor = this.#jobs.get(name);
		if (JobConstructor === undefined) return undefined;
		const instance = new JobConstructor();
		return {
			run: (payload) => {
				// `payload` is declared readonly on the class so a handler cannot
				// rewrite what it was sent; it is assigned once, here.
				Object.defineProperty(instance, "payload", {
					value: payload,
					configurable: true,
					enumerable: true,
				});
				return instance.execute();
			},
			onFailed: instance.failed?.bind(instance),
		};
	}

	/**
	 * Keep the driver's claim on `job` alive for as long as the handler runs.
	 * Returns the function that stops it — always called, including when the
	 * handler throws, so a finished job never keeps extending a lease.
	 */
	#startRenewing(job: JobRecord): () => void {
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
	 * startup and every `stalledInterval` thereafter (no-op for in-memory
	 * drivers without recoverStale) — otherwise a job left in 'processing' by a
	 * crashed worker would sit there forever.
	 *
	 * The options are upstream's `worker` block, by the names it gives them:
	 *
	 *   queue.work({ idleDelay: 2000, stalledInterval: 30_000 })
	 *
	 * `idleDelay` defaults to 2 s, which is upstream's default too — a worker
	 * that finds nothing waits before asking again, and asking every second was
	 * bay's own number rather than the framework's.
	 *
	 * The positional form is the one this method had before it took the
	 * framework's names, and still works: `work(idleDelay, stalledInterval)`.
	 */
	async work(
		options?: WorkerOptions | number,
		stalledIntervalArg = 30_000,
	): Promise<void> {
		const asOptions = typeof options === "number" ? undefined : options;
		// An argument beats the config's `worker` block, which beats the
		// framework's own defaults.
		const defaults = this.#workerDefaults;
		const idleDelay =
			typeof options === "number"
				? options
				: (options?.idleDelay ?? defaults.idleDelay ?? 2000);
		const stalledInterval =
			typeof options === "number"
				? stalledIntervalArg
				: (options?.stalledInterval ??
					defaults.stalledInterval ??
					stalledIntervalArg);

		if (idleDelay <= 0) {
			throw new Error("idleDelay must be positive");
		}
		if (stalledInterval <= 0) {
			throw new Error("stalledInterval must be positive");
		}
		const concurrency =
			asOptions?.concurrency ?? this.#workerDefaults.concurrency ?? 1;
		if (!Number.isInteger(concurrency) || concurrency < 1) {
			throw new Error("concurrency must be a whole number >= 1");
		}
		const queues = asOptions?.queues ?? this.#workerDefaults.queues;

		if (this.#running) {
			throw new Error("QueueManager is already running");
		}
		this.#running = true;
		const loop = this.#loop(idleDelay, stalledInterval, concurrency, queues);
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
	async #loop(
		idleDelay: number,
		stalledInterval: number,
		concurrency: number,
		queues: readonly string[] | undefined,
	): Promise<void> {
		await this.#tryRecoverStale();
		let lastRecover = Date.now();
		while (this.#running) {
			// One round of up to `concurrency` jobs. `allSettled`, not `all`: a
			// driver that throws for one job must not abandon the others
			// mid-flight, and each attempt already reports its own failure.
			const round = Array.from({ length: concurrency }, () =>
				this.processOne(queues),
			);
			for (const attempt of round) this.#inflight.add(attempt);
			const outcomes = await Promise.allSettled(round);
			for (const attempt of round) this.#inflight.delete(attempt);

			let processed = false;
			for (const outcome of outcomes) {
				if (outcome.status === "fulfilled") {
					processed = processed || outcome.value;
				} else {
					const err = outcome.reason;
					process.stderr.write(
						`QueueManager processOne error: ${err instanceof Error ? err.message : String(err)}\n`,
					);
				}
			}
			// Nothing anywhere means the queues are empty; anything at all means
			// there may be more behind it, so ask again without waiting.
			if (!processed) await this.#sleep(idleDelay);

			if (this.#running && Date.now() - lastRecover >= stalledInterval) {
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

	/** Await every in-flight attempt, if any. */
	async drain(): Promise<void> {
		if (this.#inflight.size === 0) return;
		await Promise.allSettled([...this.#inflight]);
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
	async failedJobs(): Promise<JobRecord[]> {
		return this.#driver.failed();
	}

	/** Get queue size. */
	async size(): Promise<number> {
		return this.#driver.size();
	}
}
