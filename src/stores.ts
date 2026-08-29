/**
 * The queue store factories a config file names — `{ default, stores }`.
 *
 * The shape AdonisJS gives a package with several backends and one selected:
 * a `stores` namespace imported beside `defineConfig`, each entry that
 * namespace's result, and the selection read from the environment. Bay had a
 * single `driver: "memory"` and told everyone else to build the manager by
 * hand, which meant Redis could not be reached from a config file at all.
 *
 *   import { defineConfig, stores } from '@c9up/bay'
 *
 *   export default defineConfig({
 *     default: env.get('QUEUE_STORE'),
 *     stores: {
 *       memory: stores.memory(),
 *       redis:  stores.redis({ connection: 'main' }),
 *     },
 *   })
 *
 * Factories are lazy: only the store an application actually uses is built, so
 * naming a Redis queue in a config that runs in memory opens no connection.
 */

import { MemoryDriver } from "./drivers/MemoryDriver.js";
import type { RedisClientSource } from "./drivers/RedisDriver.js";
import { RedisDriver } from "./drivers/RedisDriver.js";
import type { QueueDriver } from "./QueueManager.js";
import { quasarConnection } from "./quasar.js";

/** A driver, built on first use. */
export type QueueStoreFactory = () => QueueDriver;

export const stores = {
	/** In memory. Jobs do not survive a restart — for tests and dev. */
	memory(): QueueStoreFactory {
		return () => new MemoryDriver();
	},

	/**
	 * Redis. `connection` takes an ioredis-shaped client, a function answering
	 * one, or the NAME of a `@c9up/quasar` connection — the last resolved at
	 * first use, without bay importing quasar, which stays an optional peer.
	 */
	redis(options: {
		connection: RedisClientSource | string;
		prefix?: string;
		visibilityTimeoutMs?: number;
	}): QueueStoreFactory {
		const source: RedisClientSource =
			typeof options.connection === "string"
				? quasarConnection(options.connection)
				: options.connection;
		return () =>
			new RedisDriver(source, {
				prefix: options.prefix,
				visibilityTimeoutMs: options.visibilityTimeoutMs,
			});
	},
};
