/**
 * A job as a class — what an application writes, and what `dispatch` takes.
 *
 * The queue accepted a registered name and a payload:
 *
 *     queue.register('send-email', new SendEmailHandler())
 *     await queue.dispatch('send-email', { to: '…' })
 *
 * Two places to keep in step, and nothing tying the payload to the handler that
 * reads it. A class carries its own name, its own options and its payload type:
 *
 *     export default class SendEmail extends Job<{ to: string }> {
 *       static options: JobOptions = { queue: 'emails', maxRetries: 5 }
 *
 *       async execute() {
 *         await mail.send(this.payload.to)
 *       }
 *
 *       async failed(error: Error) {
 *         // after the last retry, not after each one
 *       }
 *     }
 *
 *     await queue.dispatch(SendEmail, { to: 'user@example.com' })
 *
 * Registering by name still works, and is what a job whose name is computed at
 * runtime still needs.
 */

import type { BackoffFactory } from "./backoff.js";
import {
	E_BAY_INVALID_ATTEMPTS,
	E_BAY_INVALID_JOB_NAME,
	E_BAY_INVALID_JOB_OPTIONS,
	E_BAY_INVALID_PRIORITY,
	E_BAY_INVALID_RETENTION,
	E_INVALID_DURATION_EXPRESSION,
	E_INVALID_TIMEOUT,
} from "./errors.js";
import { JobBatchDispatcher, JobDispatcher } from "./JobDispatcher.js";

/** Milliseconds, or a duration the way a config file writes one. */
export type Duration = number | string;

/**
 * What a job class declares about how it should be run.
 *
 * Every field is optional, and the defaults are the manager's: the `default`
 * queue, three attempts, no delay and no timeout.
 */
export interface JobOptions {
	/**
	 * The name the job is queued and looked up under. Default: the class name.
	 *
	 * Set it when two classes share a class name — two `SendEmail` in
	 * different directories — or when a bundler renames classes: the HTTP
	 * process and the worker must agree on the name, and a minifier's `class t`
	 * is not something they can agree on.
	 */
	name?: string;
	/** Named queue this job waits in. Default `"default"`. */
	queue?: string;
	/**
	 * 1 to 10, lower runs first — upstream's scale. Default `5`. Within one
	 * priority jobs keep the order they were queued in.
	 */
	priority?: number;
	/**
	 * How many times the handler may run before the job is filed as failed.
	 * Default `3`. Counts runs, not retries: `1` means one attempt and no
	 * second chance.
	 */
	maxRetries?: number;
	/** Hold the job for this long before any worker may take it. */
	delay?: Duration;
	/**
	 * Upstream's retry block. `backoff` spaces the attempts out —
	 * `exponentialBackoff()`, `linearBackoff()`, `fixedBackoff()` — and
	 * `maxRetries` here is the same count as the top-level one, which wins.
	 */
	retry?: {
		maxRetries?: number;
		backoff?: BackoffFactory;
	};
	/**
	 * How long the handler gets. Past it the attempt is a failure and the job
	 * retries or fails like any other, and `this.signal` is aborted.
	 *
	 * The handler is not killed — nothing in Node can interrupt a running
	 * promise — so it has to listen to its signal to stop. Until it returns it
	 * keeps its worker slot, so `concurrency` stays a bound on what runs.
	 */
	timeout?: Duration;
	/**
	 * Fail the job at once when an attempt times out, instead of retrying it.
	 * Default `false`: a timeout is a failed attempt like any other.
	 */
	failOnTimeout?: boolean;
	/**
	 * What happens to the job once it completed — upstream's `JobRetention`.
	 * `true` (the default) removes it at once, `false` keeps it, `{ age, count }`
	 * keeps it and prunes the completed jobs of its whole QUEUE, whatever their
	 * class, by age and/or count. What it keeps is what `queue.getJob(id)` can
	 * still answer.
	 */
	removeOnComplete?: JobRetention;
	/**
	 * The same for a job that finally failed. It governs `getJob` only: the
	 * failed list (`failedJobs()`, bounded by `maxFailedJobs`) keeps its own.
	 */
	removeOnFail?: JobRetention;
}

/**
 * How long a finished job is kept — upstream's `JobRetention`: `true` removes
 * it at once, `false` keeps it, `{ age, count }` keeps the newest `count` and
 * those younger than `age`.
 */
export type JobRetention = boolean | { age?: Duration; count?: number };

/** A retention as a record carries it: only present when the job is kept. */
export interface ResolvedRetention {
	/** Milliseconds; `0` for no age bound. */
	maxAge: number;
	/** `0` for no count bound. */
	maxCount: number;
}

/**
 * Check a retention and resolve it for the record, or `undefined` for "remove
 * at once" — upstream's `resolveRetention`.
 */
export function toRetention(
	value: JobRetention | undefined,
	label: string,
): ResolvedRetention | undefined {
	if (value === undefined || value === true) return undefined;
	if (value === false) return { maxAge: 0, maxCount: 0 };
	if (typeof value !== "object" || value === null) {
		throw new E_BAY_INVALID_RETENTION([
			`${label} must be a boolean or { age?, count? }`,
		]);
	}
	const maxAge =
		value.age === undefined ? 0 : toMilliseconds(value.age, `${label}.age`);
	const maxCount = value.count ?? 0;
	if (!Number.isSafeInteger(maxCount) || maxCount < 0) {
		throw new E_BAY_INVALID_RETENTION([
			`${label}.count must be a whole number >= 0, got ${String(value.count)}`,
		]);
	}
	if (!Number.isSafeInteger(maxAge)) {
		throw new E_BAY_INVALID_RETENTION([
			`${label}.age must be a whole number of milliseconds`,
		]);
	}
	return { maxAge, maxCount };
}

/**
 * What a job knows about the attempt it is running — upstream's
 * `JobContext`, frozen. `scheduleId` is not carried: recurring jobs are the
 * scheduler's (the README's recorded deviation), not the queue's.
 */
export interface JobContext {
	/** The id `dispatch` answered. */
	readonly jobId: string;
	/** The name the job is queued under. */
	readonly name: string;
	/** 1 on the first run, 2 on the first retry. */
	readonly attempt: number;
	readonly queue: string;
	/** 1 to 10, lower runs first. */
	readonly priority: number;
	/** When this worker took the job. */
	readonly acquiredAt: Date;
	/** How many times the job was recovered from a worker that died. */
	readonly stalledCount: number;
}

/** The queue a job goes to when nothing names one. */
export const DEFAULT_QUEUE = "default";

/**
 * A background job.
 *
 * `execute()` takes no argument: the payload is on the instance, typed by the
 * class's own parameter, so a handler cannot read a field the dispatcher never
 * sent.
 */
export abstract class Job<Payload = unknown> {
	/** Overridden by a subclass to change queue, retries, delay or timeout. */
	static options: JobOptions = {};

	/** What `dispatch` was given, as the class declared it. */
	declare readonly payload: Payload;

	/**
	 * Aborted when the attempt's `timeout` passes. Hand it to the I/O the job
	 * does (`fetch(url, { signal: this.signal })`) or check `aborted` between
	 * steps: the worker slot stays taken until `execute()` returns.
	 */
	declare readonly signal: AbortSignal;

	/**
	 * The attempt this run is: job id, attempt number (1-based), queue,
	 * priority, when it was taken — upstream's `this.context`. A job that must
	 * not repeat a side effect reads `attempt`; a log line reads `jobId`.
	 */
	declare readonly context: JobContext;

	/**
	 * Queue this job through the bound queue (`services/main`), fluently —
	 * upstream's `Job.dispatch`:
	 *
	 *     const { jobId } = await SendEmail.dispatch({ to }).toQueue('emails')
	 *
	 * Awaiting it is what sends it; `.run()` does the same explicitly.
	 */
	static dispatch<Payload>(
		this: JobClass<Payload>,
		payload: Payload,
	): JobDispatcher<Payload> {
		// biome-ignore lint/complexity/noThisInStatic: `this` is the subclass being dispatched; the suggested `Job` would queue the abstract base.
		return new JobDispatcher(this, payload);
	}

	/**
	 * Queue one job per payload — upstream's `Job.dispatchMany`. Answers every
	 * id, in the order of the payloads.
	 */
	static dispatchMany<Payload>(
		this: JobClass<Payload>,
		payloads: readonly Payload[],
	): JobBatchDispatcher<Payload> {
		// biome-ignore lint/complexity/noThisInStatic: `this` is the subclass being dispatched; the suggested `Job` would queue the abstract base.
		return new JobBatchDispatcher(this, payloads);
	}

	/** Do the work. Throwing is what makes the attempt fail. */
	abstract execute(): Promise<void> | void;

	/**
	 * Called once the last attempt has failed — for the cleanup or the alert,
	 * not for the retry. A throw here is reported and swallowed: the job is
	 * already failed, and failing to say so must not fail it twice.
	 */
	failed?(error: Error): Promise<void> | void;
}

/** A job class, as `dispatch` receives it. */
export interface JobClass<Payload = unknown> {
	/**
	 * Any constructor: a job takes its dependencies there, and the job factory
	 * (the host container by default) supplies them.
	 */
	new (...args: never[]): Job<Payload>;
	readonly name: string;
	readonly options?: JobOptions;
}

/** The name a job class is queued and looked up under — upstream's rule. */
export function jobNameOf(job: JobClass): string {
	return job.options?.name || job.name;
}

/** Is this a job class rather than a name or a handler instance? */
export function isJobClass(value: unknown): value is JobClass {
	return (
		typeof value === "function" &&
		value.prototype instanceof Job &&
		typeof Reflect.get(value, "name") === "string"
	);
}

/**
 * A count of runs: a whole number from 1 up.
 *
 * `< 1` alone let `Infinity`, `NaN` and `1.5` through. Infinity serialises as
 * `null`, which a Redis worker cannot read back, and in memory it retried a
 * failing job forever; NaN made the first failure final.
 */
export function toAttemptCount(value: number, label: string): number {
	if (!Number.isSafeInteger(value) || value < 1) {
		throw new E_BAY_INVALID_ATTEMPTS([label, String(value)]);
	}
	return value;
}

/** The priority a job runs at when nothing names one. */
export const DEFAULT_PRIORITY = 5;

/** A priority on upstream's scale: a whole number from 1 (first) to 10. */
export function toPriority(value: number): number {
	if (!Number.isInteger(value) || value < 1 || value > 10) {
		throw new E_BAY_INVALID_PRIORITY([String(value)]);
	}
	return value;
}

/** The longest delay `setTimeout` holds; past it Node fires after 1 ms. */
export const MAX_TIMEOUT = 2_147_483_647;

/**
 * A job timeout in milliseconds, or `undefined` for none — upstream's
 * `parseTimeout`: `0` means no timeout, anything else must be a whole number of
 * milliseconds `setTimeout` can hold. A '30d' timeout used to reach
 * `setTimeout` as-is, which Node clamps to 1 ms: the most generous timeout
 * became the strictest, and every attempt failed at once.
 */
export function toTimeout(
	value: Duration | undefined,
	jobName?: string,
): number | undefined {
	if (value === undefined) return undefined;
	const ms = toMilliseconds(value, "timeout");
	if (ms === 0) return undefined;
	if (!Number.isInteger(ms) || ms < 1 || ms > MAX_TIMEOUT) {
		throw new E_INVALID_TIMEOUT([
			String(value),
			jobName === undefined ? "" : ` of job "${jobName}"`,
		]);
	}
	return ms;
}

/**
 * Check what a job class declares, so a class that cannot be run is refused at
 * registration — at boot — rather than on every dispatch.
 */
export function checkJobOptions(job: JobClass): void {
	const options = job.options ?? {};
	try {
		if (
			options.name !== undefined &&
			(typeof options.name !== "string" || options.name.trim().length === 0)
		) {
			throw new E_BAY_INVALID_JOB_NAME([
				"options.name must be a non-empty string",
			]);
		}
		if (options.priority !== undefined) toPriority(options.priority);
		const maxRetries = options.maxRetries ?? options.retry?.maxRetries;
		if (maxRetries !== undefined) toAttemptCount(maxRetries, "maxRetries");
		toTimeout(options.timeout);
		toRetention(options.removeOnComplete, "removeOnComplete");
		toRetention(options.removeOnFail, "removeOnFail");
		// Built once here so a bad strategy fails at boot, not on the retry.
		options.retry?.backoff?.();
	} catch (err) {
		throw new E_BAY_INVALID_JOB_OPTIONS(
			[job.name, err instanceof Error ? err.message : String(err)],
			{ cause: err },
		);
	}
}

/**
 * The library's message names the expression only; which option held it goes
 * in `help`, so the message stays the library's and the option is still named.
 */
function invalidDuration(
	expression: string,
	help: string,
): InstanceType<typeof E_INVALID_DURATION_EXPRESSION> {
	const error = new E_INVALID_DURATION_EXPRESSION([expression]);
	error.help = help;
	return error;
}

/**
 * Milliseconds from a number or a duration string.
 *
 * `'10s'`, `'1m'`, `'2h'`, `'500ms'`, `'1d'` — the spellings a config file
 * uses. A number is already milliseconds. Anything else throws, rather than
 * silently becoming `NaN` and then a job that never runs: a typo in `delay`
 * would otherwise park the job forever with nothing to read about it.
 */
export function toMilliseconds(value: Duration, label: string): number {
	if (typeof value === "number") {
		if (!Number.isFinite(value) || value < 0) {
			throw invalidDuration(
				String(value),
				`${label} must be a non-negative number of milliseconds.`,
			);
		}
		return value;
	}
	const match = /^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)$/.exec(value.trim());
	if (!match) {
		throw invalidDuration(
			value,
			`${label} must be a number of milliseconds or a duration like '10s', '1m', '2h'.`,
		);
	}
	const amount = Number(match[1]);
	const unit = match[2];
	const scale: Record<string, number> = {
		ms: 1,
		s: 1_000,
		m: 60_000,
		h: 3_600_000,
		d: 86_400_000,
	};
	return amount * (scale[unit ?? "ms"] ?? 1);
}
