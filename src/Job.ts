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
	 * retries or fails like any other.
	 *
	 * The handler is not killed — nothing in Node can interrupt a running
	 * promise — so a job that ignores its timeout goes on burning CPU. What the
	 * timeout buys is that the WORKER stops waiting for it, which is what a
	 * stuck job otherwise costs: a worker that never picks anything up again.
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
	new (): Job<Payload>;
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
