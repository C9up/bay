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

/** Milliseconds, or a duration the way a config file writes one. */
export type Duration = number | string;

/**
 * What a job class declares about how it should be run.
 *
 * Every field is optional, and the defaults are the manager's: the `default`
 * queue, three attempts, no delay and no timeout.
 */
export interface JobOptions {
	/** Named queue this job waits in. Default `"default"`. */
	queue?: string;
	/**
	 * How many times the handler may run before the job is filed as failed.
	 * Default `3`. Counts runs, not retries: `1` means one attempt and no
	 * second chance.
	 */
	maxRetries?: number;
	/** Hold the job for this long before any worker may take it. */
	delay?: Duration;
	/**
	 * How long the handler gets. Past it the attempt is a failure and the job
	 * retries or fails like any other, and `this.signal` is aborted.
	 *
	 * The handler is not killed — nothing in Node can interrupt a running
	 * promise — so it has to listen to its signal to stop. Until it returns it
	 * keeps its worker slot, so `concurrency` stays a bound on what runs.
	 */
	timeout?: Duration;
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
		throw new Error(`${label} must be a whole number >= 1, got ${value}`);
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
export function toTimeout(value: Duration | undefined): number | undefined {
	if (value === undefined) return undefined;
	const ms = toMilliseconds(value, "timeout");
	if (ms === 0) return undefined;
	if (!Number.isInteger(ms) || ms > MAX_TIMEOUT) {
		throw new Error(
			`timeout must be a whole number of milliseconds from 1 to ${MAX_TIMEOUT} (about 24.8 days), or 0 for none — got ${String(value)}`,
		);
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
		if (options.maxRetries !== undefined) {
			toAttemptCount(options.maxRetries, "maxRetries");
		}
		toTimeout(options.timeout);
	} catch (err) {
		throw new Error(
			`[bay] job '${job.name}': ${err instanceof Error ? err.message : String(err)}`,
		);
	}
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
			throw new Error(`${label} must be a non-negative number of milliseconds`);
		}
		return value;
	}
	const match = /^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)$/.exec(value.trim());
	if (!match) {
		throw new Error(
			`${label} must be a number of milliseconds or a duration like '10s', '1m', '2h' — got '${value}'`,
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
