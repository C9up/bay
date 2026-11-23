/**
 * Bay errors, one per code, so a caller can branch on `error.code` or on
 * `instanceof errors.E_…` rather than on a message.
 *
 * A failure the queue library bay follows also has keeps that library's code,
 * status and message (`E_JOB_TIMEOUT`, `E_CONFIGURATION_ERROR`, …). What bay
 * adds carries the `E_BAY_` namespace.
 */
import { createError, Exception } from "@c9up/flare";

/**
 * The queue is used before `BayProvider` booted or `setQueue()` bound one.
 *
 * Deviation, named: the message is the site's own. The library's fixed text
 * says to call `QueueManager.init()`, which bay does not have; each site names
 * the wiring that is missing instead.
 */
export const E_QUEUE_NOT_INITIALIZED = createError<[string]>(
	"%s",
	"E_QUEUE_NOT_INITIALIZED",
	500,
);

/**
 * `config.queue` is not what `defineConfig()` builds, names an adapter it does
 * not declare, or a driver option is out of range.
 */
export const E_CONFIGURATION_ERROR = createError<[string]>(
	"Configuration error. Reason: %s",
	"E_CONFIGURATION_ERROR",
	500,
);

/** An adapter factory threw while building its driver; the error is the `cause`. */
export const E_ADAPTER_INIT_ERROR = createError<[string, string]>(
	'Failed to initialize adapter "%s". Reason: %s',
	"E_ADAPTER_INIT_ERROR",
	500,
);

/** The container answered something other than a QueueManager. */
export const E_BAY_INVALID_BINDING = createError(
	"[bay] the container did not answer a QueueManager",
	"E_BAY_INVALID_BINDING",
);

/** An adapter factory did not build a queue driver. */
export const E_BAY_INVALID_ADAPTER = createError<[string]>(
	"[bay] the adapter '%s' did not build a queue driver — an adapter factory answers an object with push(), pop(), complete(), fail(), retry(), failed() and size().",
	"E_BAY_INVALID_ADAPTER",
);

/** A `driver` name in place of an `adapters` entry. */
export const E_BAY_UNSUPPORTED_DRIVER = createError<[string]>(
	"[bay] Unsupported driver '%s' — name it under `adapters` instead: adapters: { redis: drivers.redis({ connection: 'main' }) }.",
	"E_BAY_UNSUPPORTED_DRIVER",
);

/** `@c9up/quasar` is not installed, or its manager is not available yet. */
export const E_BAY_QUASAR_MISSING = createError<[string]>(
	"%s",
	"E_BAY_QUASAR_MISSING",
);

/** The Redis connection lacks a command the driver issues. */
export const E_BAY_INCOMPLETE_CONNECTION = createError<[string]>(
	"%s",
	"E_BAY_INCOMPLETE_CONNECTION",
);

/** A Redis script answered something the driver cannot read. */
export const E_BAY_UNEXPECTED_REPLY = createError<[string]>(
	"[bay] unexpected reply from the %s script",
	"E_BAY_UNEXPECTED_REPLY",
);

/** A `removeOnComplete` / `removeOnFail` option bay cannot read. */
export const E_BAY_INVALID_RETENTION = createError<[string]>(
	"%s",
	"E_BAY_INVALID_RETENTION",
);

/** A number of attempts that is not a whole number from 1 up. */
export const E_BAY_INVALID_ATTEMPTS = createError<[string, string]>(
	"%s must be a whole number >= 1, got %s",
	"E_BAY_INVALID_ATTEMPTS",
);

/** A priority outside 1 to 10. */
export const E_BAY_INVALID_PRIORITY = createError<[string]>(
	"priority must be a whole number from 1 to 10, got %s",
	"E_BAY_INVALID_PRIORITY",
);

/**
 * A timeout that is not a whole number of milliseconds a timer can hold. The
 * second argument is ` of job "<name>"` when the job is known, else empty.
 */
export const E_INVALID_TIMEOUT = createError<[string, string]>(
	'Invalid timeout "%s"%s: use 0 for no timeout, or a whole number of milliseconds from 1 to 2147483647 (about 24.8 days)',
	"E_INVALID_TIMEOUT",
	500,
);

/** A job name that is empty, or cannot become a class name. */
export const E_BAY_INVALID_JOB_NAME = createError<[string]>(
	"%s",
	"E_BAY_INVALID_JOB_NAME",
);

/** A job class whose static options do not validate. */
export const E_BAY_INVALID_JOB_OPTIONS = createError<[string, string]>(
	"[bay] job '%s': %s",
	"E_BAY_INVALID_JOB_OPTIONS",
);

/** A duration that is neither milliseconds nor a string such as `'10s'`. */
export const E_INVALID_DURATION_EXPRESSION = createError<[string]>(
	'Invalid duration expression: "%s"',
	"E_INVALID_DURATION_EXPRESSION",
	500,
);

/** A backoff `baseDelay` that is not above zero. */
export const E_INVALID_BASE_DELAY = createError<[string]>(
	"Invalid base delay. Reason: %s",
	"E_INVALID_BASE_DELAY",
	500,
);

/** A backoff `maxDelay` that is not above zero, or not above `baseDelay`. */
export const E_INVALID_MAX_DELAY = createError<[string]>(
	"Invalid max delay. Reason: %s",
	"E_INVALID_MAX_DELAY",
	500,
);

/** A backoff `multiplier` not above zero, or below 1 for the exponential strategy. */
export const E_INVALID_MULTIPLIER = createError<[string]>(
	"Invalid multiplier. Reason: %s",
	"E_INVALID_MULTIPLIER",
	500,
);

/** A worker option out of range. */
export const E_BAY_INVALID_WORKER_OPTIONS = createError<[string]>(
	"%s",
	"E_BAY_INVALID_WORKER_OPTIONS",
);

/** A `dedup` option bay cannot use. */
export const E_BAY_INVALID_DEDUP = createError<[string]>(
	"%s",
	"E_BAY_INVALID_DEDUP",
);

/** Two job classes registered under one name. */
export const E_BAY_DUPLICATE_JOB = createError<[string]>(
	"[bay] two job classes answer to '%s'. Give one of them a static options.name so each has its own.",
	"E_BAY_DUPLICATE_JOB",
);

/** The driver lacks the method an operation needs. */
export const E_BAY_UNSUPPORTED_OPERATION = createError<[string, string]>(
	"This queue driver has no %s, so it cannot %s",
	"E_BAY_UNSUPPORTED_OPERATION",
);

/** The job factory built something that is not the job asked for. */
export const E_BAY_INVALID_JOB_FACTORY = createError<[string]>(
	"%s",
	"E_BAY_INVALID_JOB_FACTORY",
);

/**
 * `work()` called on a QueueManager whose worker is already running.
 *
 * Deviation, named: the library allows one worker per process and says so;
 * bay allows one per QueueManager, so the message names the manager.
 */
export const E_WORKER_ALREADY_RUNNING = createError(
	"This QueueManager's worker is already running. Await stop() before calling work() again.",
	"E_WORKER_ALREADY_RUNNING",
	500,
);

/** A job whose payload cannot be stored as JSON. */
export const E_BAY_UNSERIALIZABLE_JOB = createError<[string, string]>(
	"[bay] cannot store job '%s': %s",
	"E_BAY_UNSERIALIZABLE_JOB",
);

/** The jobs directory cannot be read. */
export const E_BAY_JOBS_DIRECTORY_UNREADABLE = createError<[string, string]>(
	"[bay] cannot read the jobs directory '%s': %s",
	"E_BAY_JOBS_DIRECTORY_UNREADABLE",
);

/** Job files were scanned and none yielded a Job. */
export const E_BAY_NO_JOBS_FOUND = createError<[number, string]>(
	"[bay] %d job file(s) were scanned and none yielded a Job, so the worker has no handlers:\n  %s",
	"E_BAY_NO_JOBS_FOUND",
);

/** `FakeQueue` received a job id it already holds. */
export const E_BAY_DUPLICATE_JOB_ID = createError<[string]>(
	"FakeQueue: duplicate job id '%s' — each push must use a unique id (the real drivers enforce this implicitly via crypto.randomUUID()).",
	"E_BAY_DUPLICATE_JOB_ID",
);

/** `FakeQueue` was asked to act on a job it never captured. */
export const E_BAY_FAKE_JOB_NOT_FOUND = createError<[string, string]>(
	"FakeQueue.%s: job id '%s' is not in the queue. Did you forget to push it, or is it from a different FakeQueue instance?",
	"E_BAY_FAKE_JOB_NOT_FOUND",
);

/**
 * A job name no handler or job class answers to: the error a job that stays
 * unknown is failed with, once `unknownJobRetries` ran out.
 */
export const E_JOB_NOT_FOUND = createError<[string]>(
	'Requested job "%s" is not registered',
	"E_JOB_NOT_FOUND",
);

/**
 * What a job's `failed()` hook receives when its last allowed attempt failed;
 * the attempt's own error is the `cause`.
 */
export const E_JOB_MAX_ATTEMPTS_REACHED = createError<[string]>(
	'The job "%s" has reached the maximum number of retry attempts',
	"E_JOB_MAX_ATTEMPTS_REACHED",
);

/** A job reclaimed from a stalled worker more than `maxStalledCount` times. */
export const E_JOB_STALLED = createError<[string, number]>(
	'The job "%s" stalled more than the allowed %d time(s)',
	"E_JOB_STALLED",
);

/**
 * An attempt that ran out of its `timeout`: what `failOnTimeout` looks for.
 * Built like the library's (`new E_JOB_TIMEOUT([name, ms])`), and carries the
 * job's name and the timeout it exceeded as fields.
 */
export class E_JOB_TIMEOUT extends Exception {
	static override code = "E_JOB_TIMEOUT";
	static override message = 'The job "%s" has exceeded the timeout of %dms';

	/** Name of the job whose attempt timed out. */
	readonly job: string;
	/** The timeout, in milliseconds, the attempt exceeded. */
	readonly timeout: number;

	constructor(args: [job: string, timeout: number], options?: ErrorOptions) {
		const [job, timeout] = args;
		super(`The job "${job}" has exceeded the timeout of ${timeout}ms`, options);
		this.job = job;
		this.timeout = timeout;
	}
}
