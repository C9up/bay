import { type ResolvedBackoff, readBackoff } from "./backoff.js";
import {
	E_BAY_DUPLICATE_JOB,
	E_BAY_INVALID_DEDUP,
	E_BAY_INVALID_JOB_FACTORY,
	E_BAY_INVALID_WORKER_OPTIONS,
	E_BAY_UNSUPPORTED_OPERATION,
	E_JOB_MAX_ATTEMPTS_REACHED,
	E_JOB_NOT_FOUND,
	E_JOB_TIMEOUT,
	E_WORKER_ALREADY_RUNNING,
} from "./errors.js";
import {
	checkJobOptions,
	DEFAULT_PRIORITY,
	DEFAULT_QUEUE,
	type Duration,
	isJobClass,
	Job,
	type JobClass,
	type JobContext,
	type JobOptions,
	jobNameOf,
	MAX_TIMEOUT,
	type ResolvedRetention,
	toAttemptCount,
	toMilliseconds,
	toPriority,
	toRetention,
	toTimeout,
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
	 * How many times a worker took this job without anything answering to
	 * its name. Kept apart from `attempts`, which counts handler runs: a job
	 * that waited out a rolling deploy has not spent any of them.
	 */
	unknownCount?: number;
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
	/** 1 to 10, lower runs first. Absent reads as 5. */
	priority?: number;
	/**
	 * The dedup id, prefixed with the job name so two job types never share
	 * one, and how long it is held.
	 */
	dedup?: { id: string; ttl?: number; extend?: boolean };
	/** Milliseconds the handler gets before the attempt counts as failed. */
	timeout?: number;
	/**
	 * How long to wait before each retry, as data: a worker that never saw
	 * the job class — or a job registered by name — still honours it.
	 */
	backoff?: ResolvedBackoff;
	/** A timed-out attempt fails the job at once (`failOnTimeout`). */
	failOnTimeout?: true;
	/**
	 * Kept after it completed, and how long (`removeOnComplete`). Absent: removed
	 * at once, upstream's default.
	 */
	removeOnComplete?: ResolvedRetention;
	/** Kept after it finally failed, and how long (`removeOnFail`). */
	removeOnFail?: ResolvedRetention;
}

/** Where a job is — upstream's `JobStatus`. */
export type JobStatus =
	| "pending"
	| "active"
	| "delayed"
	| "completed"
	| "failed";

/**
 * What `getJob` answers — upstream's adapter `JobRecord`: where the job is, the
 * record as last stored, and for a finished one when it finished and why it
 * failed.
 */
export interface JobLookup {
	status: JobStatus;
	data: JobRecord;
	/** Epoch ms, for a completed or failed job. */
	finishedAt?: number;
	error?: string;
}

/** The queue a record belongs to, for a record written before named queues. */
export function queueOf(job: JobRecord): string {
	return job.queue ?? DEFAULT_QUEUE;
}

export interface JobHandler {
	/**
	 * `signal` aborts when the attempt's `timeout` passes. Nothing in Node can
	 * stop a running promise from outside, so a handler that wants to stop on
	 * time listens to it — and the worker slot stays taken until it returns.
	 */
	handle(
		payload: unknown,
		signal: AbortSignal,
		context?: JobContext,
	): Promise<void>;
}

/**
 * One attempt, whichever way the job was declared.
 *
 * A registered handler and a job class do the same two things — run, and maybe
 * be told it finally failed — so `processOne` deals with this and not with two
 * shapes.
 */
interface JobRunner {
	run(
		payload: unknown,
		signal: AbortSignal,
		context: JobContext,
	): Promise<void> | void;
	onFailed?(error: Error): Promise<void> | void;
	/**
	 * Give the job its payload and context without running it, for a
	 * `failed()` hook called outside an attempt (a job that stalled too often).
	 */
	hydrate?(payload: unknown, context: JobContext): void;
}

/**
 * Reject once `ms` has passed, aborting `controller` so the work can stop.
 *
 * The work itself is not interrupted — nothing in Node can do that to a
 * promise. What the abort buys is a handler that listens to its signal and
 * returns; what the rejection buys is that the attempt is settled on time.
 */
function withTimeout(
	work: Promise<void>,
	ms: number | undefined,
	name: string,
	controller: AbortController,
): Promise<void> {
	if (ms === undefined || ms <= 0) return work;
	return new Promise<void>((resolve, reject) => {
		const timer = setTimeout(() => {
			const error = new E_JOB_TIMEOUT([name, ms]);
			controller.abort(error);
			reject(error);
		}, ms);
		// Unreffed: a pending timeout must not be the reason the process stays up.
		timer.unref();
		work.then(
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
 * A delay between two polls, in milliseconds.
 *
 * Both end up in `setTimeout` or in a `>=` comparison, and `<= 0` alone let NaN
 * through: as `idleDelay` it became a 1 ms busy loop against the backend, as
 * `stalledInterval` it made the recovery check always false, so crash recovery
 * ran once at startup and never again.
 */
function toPollDelay(value: Duration, label: string): number {
	const ms = toMilliseconds(value, label);
	if (!(ms > 0) || ms > MAX_TIMEOUT) {
		throw new E_BAY_INVALID_WORKER_OPTIONS([
			`${label} must be a delay from 1 to ${MAX_TIMEOUT} ms, got ${String(value)}`,
		]);
	}
	return ms;
}

/** Check a dedup request and key it by job name. */
function toDedup(
	name: string,
	dedup: { id: string; ttl?: Duration; extend?: boolean },
): { id: string; ttl?: number; extend?: boolean } {
	if (typeof dedup.id !== "string" || dedup.id.length === 0) {
		throw new E_BAY_INVALID_DEDUP(["dedup.id must be a non-empty string"]);
	}
	const ttl =
		dedup.ttl === undefined
			? undefined
			: toMilliseconds(dedup.ttl, "dedup.ttl");
	if (ttl !== undefined && (!Number.isInteger(ttl) || ttl < 1)) {
		throw new E_BAY_INVALID_DEDUP([
			`dedup.ttl must be a whole number of milliseconds from 1 up, got ${String(dedup.ttl)}`,
		]);
	}
	if (dedup.extend && ttl === undefined) {
		throw new E_BAY_INVALID_DEDUP([
			"dedup.extend needs a ttl: there is no window to restart",
		]);
	}
	return {
		id: `${name}::${dedup.id}`,
		ttl,
		extend: dedup.extend === true ? true : undefined,
	};
}

/** Upstream's wait before a job nothing answers to is looked at again. */
const UNKNOWN_JOB_RETRY_DELAY = 30_000;

function toUnknownJobRetries(value: number | undefined): number {
	if (value === undefined) return 10;
	if (!Number.isSafeInteger(value) || value < 0) {
		throw new E_BAY_INVALID_WORKER_OPTIONS([
			`unknownJobRetries must be a whole number >= 0, got ${value}`,
		]);
	}
	return value;
}

/**
 * The context of the attempt about to run — upstream's `JobContext`, frozen
 * so a handler cannot rewrite what the next reader sees.
 */
function contextOf(job: JobRecord): JobContext {
	return Object.freeze({
		jobId: job.id,
		name: job.name,
		attempt: job.attempts,
		queue: queueOf(job),
		priority: job.priority ?? DEFAULT_PRIORITY,
		acquiredAt: new Date(job.processedAt ?? Date.now()),
		stalledCount: job.stalledCount ?? 0,
	});
}

/** A queue list that serves something, each name non-empty. */
function checkQueues(
	queues: readonly string[] | undefined,
): readonly string[] | undefined {
	if (queues === undefined) return undefined;
	if (queues.length === 0) {
		throw new E_BAY_INVALID_WORKER_OPTIONS([
			"queues must name at least one queue; leave it out to serve the default one",
		]);
	}
	for (const name of queues) {
		if (typeof name !== "string" || name.trim().length === 0) {
			throw new E_BAY_INVALID_WORKER_OPTIONS([
				`queues must hold non-empty names, got ${JSON.stringify(queues)}`,
			]);
		}
	}
	return queues;
}

/**
 * What a worker is told before it starts, by upstream's names for it
 * (`WorkerConfig.idleDelay`, `WorkerConfig.stalledInterval`).
 */
export interface WorkerOptions {
	/** Milliseconds to wait after finding nothing to do. Default `2000`. */
	idleDelay?: Duration;
	/** Milliseconds between stalled-job sweeps. Default `30_000`. */
	stalledInterval?: Duration;
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
	/**
	 * How many times a job nothing answers to is put back — 30 s later — before
	 * it is failed. Default `10`, upstream's. During a rolling deploy the new
	 * HTTP processes dispatch jobs the old workers do not know yet; failing
	 * them on first sight dropped every one into the failed list.
	 */
	unknownJobRetries?: number;
}

/**
 * What a single `dispatch` may override.
 *
 * Everything a job class declares, plus `maxAttempts` — the name this method
 * took before job classes existed, kept because it is what every existing call
 * site passes. `maxRetries` is the class's name for the same number.
 */
export interface DispatchOptions extends Omit<JobOptions, "name"> {
	/**
	 * Queue the job only if no job of the same name holds this `id` —
	 * upstream's dedup. Without `ttl` the id is held until the job completes
	 * or finally fails; with it, for `ttl` at most, and `extend` restarts that
	 * window on each duplicate. `dispatch` answers the id of the job that
	 * holds it.
	 */
	dedup?: { id: string; ttl?: Duration; extend?: boolean };
	/** The older spelling of `maxRetries`. Wins when both are given. */
	maxAttempts?: number;
}

/**
 * What one stall-recovery pass did: upstream's `recoverStalledJobs` answer.
 */
export interface StaleRecovery {
	/** Jobs moved back to their queue. */
	recovered: number;
	/**
	 * Jobs that stalled more than the driver allows, already filed as failed,
	 * each with the error its `failed()` hook receives (`E_JOB_STALLED`).
	 */
	failed: Array<{ job: JobRecord; error: Error }>;
}

export interface QueueDriver {
	/**
	 * Store a job. A driver that honours `job.dedup` answers the id (a string)
	 * of the job already holding it, when one does, and stores nothing; any
	 * other answer means the job was stored. `unknown` so a driver written
	 * before dedup, answering nothing, still fits.
	 */
	push(job: JobRecord): Promise<unknown>;
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
	/** Jobs waiting on the default queue, delayed ones included. */
	size(): Promise<number>;
	/**
	 * Jobs waiting on `queue`, delayed ones included. Optional for a driver
	 * written before named queues; `QueueManager.sizeOf` then answers for the
	 * default queue only.
	 */
	sizeOf?(queue: string): Promise<number>;
	/**
	 * Where the job with this id is, or `null` once it is gone (completed or
	 * failed and not kept by its retention). Optional for a driver written
	 * before lookups; `QueueManager.getJob` then refuses.
	 */
	getJob?(jobId: string): Promise<JobLookup | null>;
	/**
	 * Optional crash recovery: move jobs orphaned in the driver's 'processing'
	 * state (expired visibility lease) back to pending. In-memory drivers omit
	 * this — their jobs don't survive a crash.
	 *
	 * A driver that bounds the stalls answers a {@link StaleRecovery}, so the
	 * manager can call the `failed()` hook of each job it filed as failed; a
	 * driver that never fails a job for stalling may answer the count alone.
	 */
	recoverStale?(): Promise<number | StaleRecovery>;
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

/** A class whose instances the factory is asked to build. */
export type Constructor<T = unknown> = new (...args: never[]) => T;

/**
 * Build a job (or handler) class into an instance — upstream's `jobFactory`.
 *
 * The host container by default, through the provider, so a job takes its
 * dependencies in its constructor the way a controller does. What it returns
 * is checked: a job class must yield an instance of itself, a handler class
 * something with `handle()`.
 */
export type JobFactory = (jobClass: Constructor) => unknown;

/** A registered handler: an instance, or a class built for each job. */
export type HandlerRegistration = JobHandler | Constructor<JobHandler>;

export interface QueueManagerOptions {
	/** How job and handler classes are built. Default: `new` with no arguments. */
	jobFactory?: JobFactory;
}

function isJobHandler(value: unknown): value is JobHandler {
	return (
		typeof value === "object" &&
		value !== null &&
		typeof Reflect.get(value, "handle") === "function"
	);
}

/** What answers to a job's name, before it is built. */
type Target =
	| { kind: "handler"; handler: HandlerRegistration }
	| { kind: "job"; job: JobClass };

export class QueueManager {
	#driver: QueueDriver;
	#handlers: Map<string, HandlerRegistration> = new Map();
	#jobs: Map<string, JobClass> = new Map();
	#running = false;
	/** The running loop, so `stop()` can wait for it to finish. */
	#loopPromise: Promise<void> | undefined;
	/** Cuts the sleep between polls short. */
	#wake: (() => void) | undefined;
	/** Every attempt currently in flight, so `drain()` can wait for all of them. */
	#inflight: Set<Promise<void>> = new Set();

	/** Defaults for `work()`, from the config's `worker` block. */
	readonly #workerDefaults: WorkerOptions;
	/** The worker block's, or the running `work()` call's when it names one. */
	#unknownJobRetries: number;

	readonly #jobFactory: JobFactory;

	constructor(
		driver: QueueDriver,
		workerDefaults?: WorkerOptions,
		options?: QueueManagerOptions,
	) {
		this.#driver = driver;
		this.#workerDefaults = workerDefaults ?? {};
		this.#unknownJobRetries = toUnknownJobRetries(
			this.#workerDefaults.unknownJobRetries,
		);
		this.#jobFactory = options?.jobFactory ?? ((cls) => new cls());
	}

	/** Register a job handler under a name. */
	register(name: string, handler: HandlerRegistration): void {
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
		checkJobOptions(job);
		this.#claimName(job);
	}

	/**
	 * Key `job` by its name, refusing a different class already under it.
	 *
	 * Overwriting was silent: two `SendEmail` classes in two directories, one
	 * replaced the other, and a record dispatched for one ran the other's code.
	 */
	#claimName(job: JobClass): string {
		const name = jobNameOf(job);
		const existing = this.#jobs.get(name);
		if (existing !== undefined && existing !== job) {
			throw new E_BAY_DUPLICATE_JOB([name]);
		}
		this.#jobs.set(name, job);
		return name;
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
		const record = this.#record(job, payload, options);
		// A driver honouring dedup answers the id that holds the place.
		const holder = await this.#driver.push(record);
		return typeof holder === "string" ? holder : record.id;
	}

	/**
	 * Queue one job per payload — upstream's `dispatchMany`. Every record is
	 * built (and checked) before the first is pushed, so a bad option queues
	 * none of them; the pushes themselves are one per job.
	 */
	async dispatchMany<Payload>(
		job: JobClass<Payload>,
		payloads: readonly Payload[],
		options?: DispatchOptions,
	): Promise<string[]>;
	async dispatchMany(
		name: string,
		payloads: readonly unknown[],
		options?: DispatchOptions,
	): Promise<string[]>;
	async dispatchMany(
		job: string | JobClass,
		payloads: readonly unknown[],
		options: DispatchOptions = {},
	): Promise<string[]> {
		const records = payloads.map((payload) =>
			this.#record(job, payload, options),
		);
		const ids: string[] = [];
		for (const record of records) {
			const holder = await this.#driver.push(record);
			ids.push(typeof holder === "string" ? holder : record.id);
		}
		return ids;
	}

	/**
	 * Where a dispatched job is — upstream's `getJob`: `pending`, `delayed`,
	 * `active`, `completed` or `failed`, with the record as last stored; `null`
	 * once it is gone. A completed or failed job is only kept when its
	 * `removeOnComplete` / `removeOnFail` says so: by default, as upstream, it
	 * is removed at once.
	 *
	 * `queue`, when given, must be the job's: a job id names one job whichever
	 * queue it is in, so the argument only narrows.
	 */
	async getJob(jobId: string, queue?: string): Promise<JobLookup | null> {
		if (!this.#driver.getJob) {
			throw new E_BAY_UNSUPPORTED_OPERATION(["getJob()", "look a job up"]);
		}
		const found = await this.#driver.getJob(jobId);
		if (found === null) return null;
		if (queue !== undefined && queueOf(found.data) !== queue) return null;
		return found;
	}

	/** Build and check the record `dispatch` pushes. */
	#record(
		job: string | JobClass,
		payload: unknown,
		options: DispatchOptions,
	): JobRecord {
		let name: string;
		let declared: JobOptions = {};
		if (isJobClass(job)) {
			checkJobOptions(job);
			declared = job.options ?? {};
			// So a worker that never dispatches still resolves it by name.
			name = this.#claimName(job);
		} else {
			name = job;
		}

		// The call site wins over the class, and the class over the defaults —
		// the same order `work()` reads its own options in. Each spelling is
		// checked under its own name, so the message names what was written.
		const maxAttempts =
			options.maxAttempts !== undefined
				? toAttemptCount(options.maxAttempts, "maxAttempts")
				: toAttemptCount(
						options.maxRetries ??
							options.retry?.maxRetries ??
							declared.maxRetries ??
							declared.retry?.maxRetries ??
							3,
						"maxRetries",
					);
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
		const timeoutMs = toTimeout(timeout, name);
		if (timeoutMs !== undefined) record.timeout = timeoutMs;
		const backoff = options.retry?.backoff ?? declared.retry?.backoff;
		if (backoff !== undefined) record.backoff = backoff().getConfig();
		const priority = options.priority ?? declared.priority;
		if (priority !== undefined) record.priority = toPriority(priority);
		if (options.dedup !== undefined) {
			record.dedup = toDedup(name, options.dedup);
		}
		if ((options.failOnTimeout ?? declared.failOnTimeout) === true) {
			record.failOnTimeout = true;
		}
		const onComplete = toRetention(
			options.removeOnComplete ?? declared.removeOnComplete,
			"removeOnComplete",
		);
		if (onComplete !== undefined) record.removeOnComplete = onComplete;
		const onFail = toRetention(
			options.removeOnFail ?? declared.removeOnFail,
			"removeOnFail",
		);
		if (onFail !== undefined) record.removeOnFail = onFail;
		return record;
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
		await this.#attempt(job);
		return true;
	}

	/** Run one attempt of a job already taken from the driver. */
	async #attempt(job: JobRecord): Promise<void> {
		const target = this.#lookup(job.name);
		if (!target) {
			const seen = job.unknownCount ?? 0;
			if (seen < this.#unknownJobRetries) {
				job.unknownCount = seen + 1;
				job.status = "pending";
				job.runAt = Date.now() + UNKNOWN_JOB_RETRY_DELAY;
				process.stderr.write(
					`QueueManager: no handler registered for job '${job.name}' yet — putting it back for ${UNKNOWN_JOB_RETRY_DELAY / 1000}s (${seen + 1}/${this.#unknownJobRetries})\n`,
				);
				await this.#driver.retry(job);
				return;
			}
			// Failed with the library's error, whose message is what the failed
			// record keeps.
			const notFound = new E_JOB_NOT_FOUND([job.name]);
			process.stderr.write(`QueueManager: ${notFound.message}\n`);
			await this.#driver.fail(job, notFound.message);
			return;
		}

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
		const controller = new AbortController();
		let execution: Promise<void> | undefined;
		let runner: JobRunner | undefined;
		let handled = false;
		/** A retry/fail write that rejected, reported once the slot is free. */
		let writeError: { error: unknown } | undefined;
		try {
			// Built inside the attempt: a factory that throws — a dependency the
			// container cannot resolve — is this attempt failing, not the
			// worker.
			runner = await this.#build(target);
			execution = Promise.resolve(
				runner.run(job.payload, controller.signal, contextOf(job)),
			);
			await withTimeout(execution, job.timeout, job.name, controller);
			handled = true;
		} catch (err) {
			const error = err instanceof Error ? err : new Error(String(err));
			const errorMsg = error.message;
			// `failOnTimeout`: a timed-out attempt is the last one.
			const timedOutForGood =
				job.failOnTimeout === true && error instanceof E_JOB_TIMEOUT;
			const final = job.attempts >= job.maxAttempts || timedOutForGood;
			if (!final) {
				job.status = "pending";
				// Not straight back to the head of the line: a job failing
				// because something it calls is down would spend every attempt
				// within milliseconds. A driver parks a future `runAt` with its
				// delayed jobs.
				const backoff = readBackoff(job.backoff);
				job.runAt = backoff
					? Date.now() + backoff.calculateDelay(job.attempts)
					: undefined;
				try {
					await this.#driver.retry(job);
				} catch (retryErr) {
					writeError = { error: retryErr };
				}
			} else {
				job.status = "failed";
				job.error = errorMsg;
				// A write that rejects (a Redis blip) is held, not thrown: thrown
				// here it skipped both the hook below and the wait for an aborted
				// handler, and the slot was freed while that handler still ran.
				try {
					await this.#driver.fail(job, errorMsg);
				} catch (failErr) {
					writeError = { error: failErr };
				}
				// After the last attempt, not after each one. A throw here is
				// reported and swallowed: the job has already failed, and
				// failing to say so must not be read as a second failure.
				// What the hook receives is the library's: the attempt's own error
				// when no retry was allowed or `failOnTimeout` ended it, and
				// E_JOB_MAX_ATTEMPTS_REACHED, caused by it, once retries ran out.
				// The failed record keeps the attempt's own message either way.
				const hookError =
					timedOutForGood || job.maxAttempts <= 1
						? error
						: new E_JOB_MAX_ATTEMPTS_REACHED([job.name], { cause: error });
				try {
					await runner?.onFailed?.(hookError);
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
			// A timed-out attempt is settled, but its handler may still be
			// running. Returning now would free this worker's slot while it does,
			// and the retry could start beside it: concurrency 1 was measured
			// running four attempts of one job at once. Upstream waits for it the
			// same way. In the `finally`, so nothing thrown above skips it.
			if (controller.signal.aborted && execution !== undefined) {
				await execution.catch(() => {});
			}
		}
		if (writeError !== undefined) throw writeError.error;

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
	}

	/**
	 * What answers to `name`: a registered handler, or a job class.
	 *
	 * A handler registered under the name wins — an application that registers
	 * one deliberately is overriding whatever else answers to it.
	 */
	#lookup(name: string): Target | undefined {
		const handler = this.#handlers.get(name);
		if (handler !== undefined) return { kind: "handler", handler };
		const job = this.#jobs.get(name);
		return job === undefined ? undefined : { kind: "job", job };
	}

	/** Build what answers to the job into the thing that runs one attempt. */
	async #build(target: Target): Promise<JobRunner> {
		if (target.kind === "handler") {
			const registered = target.handler;
			const handler =
				typeof registered === "function"
					? await this.#jobFactory(registered)
					: registered;
			if (!isJobHandler(handler)) {
				throw new E_BAY_INVALID_JOB_FACTORY([
					"The job factory built something without a handle() method for a registered handler class",
				]);
			}
			return {
				run: (payload, signal, context) =>
					handler.handle(payload, signal, context),
			};
		}
		const JobConstructor = target.job;
		const instance = await this.#jobFactory(JobConstructor);
		if (!(instance instanceof Job)) {
			throw new E_BAY_INVALID_JOB_FACTORY([
				`The job factory built ${JobConstructor.name} into something that is not an instance of it`,
			]);
		}
		// `payload`, `context` and `signal` are declared readonly on the class
		// so a handler cannot rewrite them; they are assigned once, here.
		const hydrate = (payload: unknown, context: JobContext): void => {
			Object.defineProperty(instance, "payload", {
				value: payload,
				configurable: true,
				enumerable: true,
			});
			Object.defineProperty(instance, "context", {
				value: context,
				configurable: true,
			});
		};
		return {
			run: (payload, signal, context) => {
				hydrate(payload, context);
				Object.defineProperty(instance, "signal", {
					value: signal,
					configurable: true,
				});
				return instance.execute();
			},
			onFailed: instance.failed?.bind(instance),
			hydrate,
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
			// Neither outcome interrupts the handler: finishing is still the best
			// thing it can do. A refusal means the claim is gone for good — the
			// job was recovered, maybe taken by another worker — so the heartbeat
			// stops rather than knocking on a lease it can never hold again, and
			// says so, because the job may now run twice.
			driver.renew?.(job).then(
				(held) => {
					if (held) return;
					clearInterval(timer);
					process.stderr.write(
						`QueueManager: job '${job.id}' lost its lease while '${job.name}' was running; another worker may take it\n`,
					);
				},
				// A transport error is not a refusal: the next beat may get through.
				() => {},
			);
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

		const idleDelayMs = toPollDelay(idleDelay, "idleDelay");
		const stalledIntervalMs = toPollDelay(stalledInterval, "stalledInterval");
		const concurrency =
			asOptions?.concurrency ?? this.#workerDefaults.concurrency ?? 1;
		if (!Number.isInteger(concurrency) || concurrency < 1) {
			throw new E_BAY_INVALID_WORKER_OPTIONS([
				"concurrency must be a whole number >= 1",
			]);
		}
		if (asOptions?.unknownJobRetries !== undefined) {
			this.#unknownJobRetries = toUnknownJobRetries(
				asOptions.unknownJobRetries,
			);
		}
		const queues = checkQueues(
			asOptions?.queues ?? this.#workerDefaults.queues,
		);

		if (this.#running) {
			throw new E_WORKER_ALREADY_RUNNING();
		}
		this.#running = true;
		const loop = this.#loop(
			idleDelayMs,
			stalledIntervalMs,
			concurrency,
			queues,
		);
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
		// A pool, not rounds: a slot is refilled the moment its job ends.
		// Waiting for a whole round left every other slot idle behind the
		// slowest job in it, and mixed workloads ran at a concurrency near 1.
		const pool = new Set<Promise<void>>();
		while (this.#running) {
			let empty = false;
			while (this.#running && pool.size < concurrency) {
				let job: JobRecord | null;
				try {
					job = await this.#driver.pop(queues);
				} catch (err) {
					this.#report(err);
					empty = true;
					break;
				}
				if (!job) {
					empty = true;
					break;
				}
				const attempt: Promise<void> = this.#attempt(job)
					.catch((err: unknown) => this.#report(err))
					.finally(() => {
						pool.delete(attempt);
						this.#inflight.delete(attempt);
					});
				pool.add(attempt);
				this.#inflight.add(attempt);
			}
			if (!this.#running) break;

			if (Date.now() - lastRecover >= stalledInterval) {
				await this.#tryRecoverStale();
				lastRecover = Date.now();
			}
			// Full: wait for a slot. Nothing to take: wait before asking again.
			if (pool.size >= concurrency) await Promise.race(pool);
			else if (empty) await this.#sleep(idleDelay);
		}
		// A pop answered while `stop()` was waiting on it: that job is taken, and
		// the loop is not over until it has run. Returning here let `stop()`
		// resolve with the handler half-way through, and the process exit under
		// it — lost in memory, re-delivered and run twice on Redis. Each attempt
		// is bounded by its job's own `timeout`, which aborts its handler.
		await Promise.allSettled(pool);
	}

	#report(err: unknown): void {
		process.stderr.write(
			`QueueManager processOne error: ${err instanceof Error ? err.message : String(err)}\n`,
		);
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
		const outcome = await this.#driver.recoverStale?.();
		if (outcome === undefined) return 0;
		if (typeof outcome === "number") return outcome;
		for (const { job, error } of outcome.failed) {
			await this.#failStalled(job, error);
		}
		return outcome.recovered;
	}

	/**
	 * Tell a job that stalled too often that it failed — upstream's
	 * `failStalled`. The driver has already filed it; what is left is its
	 * `failed()` hook, which receives `E_JOB_STALLED` like any final failure
	 * and is reported and swallowed when it throws. Nothing runs the handler.
	 * A job nothing answers to, or whose class cannot be built, has no hook to
	 * call.
	 */
	async #failStalled(job: JobRecord, error: Error): Promise<void> {
		const target = this.#lookup(job.name);
		if (target === undefined) return;
		let runner: JobRunner;
		try {
			runner = await this.#build(target);
		} catch (buildErr) {
			process.stderr.write(
				`QueueManager: cannot build '${job.name}' to report it stalled: ${
					buildErr instanceof Error ? buildErr.message : String(buildErr)
				}\n`,
			);
			return;
		}
		runner.hydrate?.(job.payload, contextOf(job));
		try {
			await runner.onFailed?.(error);
		} catch (hookErr) {
			process.stderr.write(
				`QueueManager: failed() hook of '${job.name}' threw: ${
					hookErr instanceof Error ? hookErr.message : String(hookErr)
				}\n`,
			);
		}
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
	 * asked it to stop. The loop first, then the drain: a pop still in flight
	 * when `stop()` is called can answer a job, and that job only joins the
	 * in-flight set after the call — draining first missed it.
	 */
	async stop(): Promise<void> {
		this.#running = false;
		this.#wake?.();
		if (this.#loopPromise) await this.#loopPromise.catch(() => {});
		await this.drain();
	}

	/** Get failed jobs. */
	async failedJobs(): Promise<JobRecord[]> {
		return this.#driver.failed();
	}

	/**
	 * Jobs waiting on the default queue, delayed ones included — upstream's
	 * `size()`. A named queue is {@link sizeOf}'s.
	 */
	async size(): Promise<number> {
		return this.#driver.size();
	}

	/** Jobs waiting on `queue`, delayed ones included — upstream's `sizeOf`. */
	async sizeOf(queue: string): Promise<number> {
		if (this.#driver.sizeOf) return this.#driver.sizeOf(queue);
		if (queue === DEFAULT_QUEUE) return this.#driver.size();
		throw new E_BAY_UNSUPPORTED_OPERATION([
			"sizeOf()",
			`count the '${queue}' queue`,
		]);
	}
}
