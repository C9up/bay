/**
 * The queue adapter factories a config file names — `{ default, adapters }`.
 *
 * The shape the framework gives a package with several backends and one
 * selected: a `drivers` namespace imported beside `defineConfig`, each entry
 * that namespace's result, and the selection read from the environment. Bay had
 * a single `driver: "memory"` and told everyone else to build the manager by
 * hand, which meant Redis could not be reached from a config file at all.
 *
 *   import { defineConfig, drivers } from '@c9up/bay'
 *
 *   export default defineConfig({
 *     default: env.get('QUEUE_DRIVER'),
 *     adapters: {
 *       memory: drivers.memory(),
 *       redis:  drivers.redis({ connection: 'main' }),
 *     },
 *   })
 *
 * The names are upstream's: `@adonisjs/queue` reads `default` + `adapters` out
 * of `config/queue.ts`, fills them from a `drivers` namespace, and takes the
 * selection from `QUEUE_DRIVER`. Bay said `stores` / `QUEUE_STORE`, which is
 * the vocabulary of a different package. Both still work — see `stores` below.
 *
 * Factories are lazy: only the adapter an application actually uses is built,
 * so naming a Redis queue in a config that runs in memory opens no connection.
 */

import { MemoryDriver } from "./drivers/MemoryDriver.js";
import type { RedisClientSource } from "./drivers/RedisDriver.js";
import { RedisDriver } from "./drivers/RedisDriver.js";
import type { QueueDriver } from "./QueueManager.js";
import { quasarConnection } from "./quasar.js";

/** A driver, built on first use. */
export type AdapterFactory = () => QueueDriver;

/**
 * The older name for {@link AdapterFactory}, kept for configs typed against it.
 */
export type QueueStoreFactory = AdapterFactory;

export const drivers = {
	/** In memory. Jobs do not survive a restart — for tests and dev. */
	memory(): AdapterFactory {
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
	}): AdapterFactory {
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

/**
 * The older name for {@link drivers}, kept for configs written against it. The
 * same object: `stores.redis(...)` and `drivers.redis(...)` are one call.
 */
export const stores = drivers;
