/**
 * @c9up/bay — Background job queue for the Ream framework.
 *
 * Dispatch/process/retry/fail pattern with pluggable drivers (Memory, Redis).
 *
 * @implements MISS-11
 */

export type { BayProviderConfig } from "./BayProvider.js";
export { MemoryDriver } from "./drivers/MemoryDriver.js";
export type { RedisClient } from "./drivers/RedisDriver.js";
export { RedisDriver } from "./drivers/RedisDriver.js";
export type { Job, JobHandler, QueueDriver } from "./QueueManager.js";
export { QueueManager } from "./QueueManager.js";

import type { BayProviderConfig } from "./BayProvider.js";

/**
 * Author-time config helper for `config/queue.ts` — AdonisJS `defineConfig`
 * parity. Identity at runtime; the generic preserves literal types for inference.
 */
export function defineConfig<T extends BayProviderConfig>(config: T): T {
	return config;
}
