/**
 * @c9up/bay — Background job queue for the Ream framework.
 *
 * A job is a class: it carries its own name, its options and the type of the
 * payload it reads, and `dispatch(SomeJob, payload)` takes it. Named queues,
 * `delay`, `timeout` and worker `concurrency` come from the same declaration.
 *
 * Dispatch/process/retry/fail, with pluggable drivers (Memory, Redis).
 */

export {
	type AdapterFactory,
	drivers,
	type QueueStoreFactory,
	stores,
} from "./adapters.js";

import "./augmentations.js";

export type {
	BayAppContext,
	BayConfigStore,
	BayContainer,
	BayProviderConfig,
} from "./BayProvider.js";
export {
	type BackoffConfig,
	type BackoffFactory,
	BackoffStrategy,
	customBackoff,
	exponentialBackoff,
	fixedBackoff,
	linearBackoff,
	type ResolvedBackoff,
} from "./backoff.js";
export { MemoryDriver } from "./drivers/MemoryDriver.js";
export type {
	RedisClient,
	RedisClientSource,
	RedisDriverOptions,
} from "./drivers/RedisDriver.js";
export { RedisDriver } from "./drivers/RedisDriver.js";
export {
	DEFAULT_QUEUE,
	type Duration,
	isJobClass,
	Job,
	type JobClass,
	type JobOptions,
} from "./Job.js";
export type {
	Constructor,
	DispatchOptions,
	HandlerRegistration,
	JobFactory,
	JobHandler,
	JobRecord,
	QueueDriver,
	QueueManagerOptions,
	WorkerOptions,
} from "./QueueManager.js";
export { QueueManager, queueOf } from "./QueueManager.js";

import type { BayProviderConfig } from "./BayProvider.js";

/**
 * Author-time config helper for `config/queue.ts` — AdonisJS `defineConfig`
 * parity. Identity at runtime; the generic preserves literal types for inference.
 */
export function defineConfig<T extends BayProviderConfig>(config: T): T {
	return config;
}
export { quasarConnection } from "./quasar.js";
