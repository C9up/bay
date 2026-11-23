/**
 * Resolving a Redis connection by name, from `@c9up/quasar`.
 *
 * The loading, the shape check and the messages are the same in every package
 * that offers a Redis-backed option, so they are vendored rather than written
 * again: `src/vendor/quasarConnection.ts`, generated from one source. What is
 * specific to this package — the commands it issues, and what it does with
 * them — stays here, because that is the part a reader needs.
 */

import type { RedisClient } from "./drivers/RedisDriver.js";
import { E_BAY_INCOMPLETE_CONNECTION, E_BAY_QUASAR_MISSING } from "./errors.js";
import {
	hasCommands,
	quasarConnection as loadQuasarConnection,
} from "./vendor/quasarConnection.js";

// The commands this driver actually issues — every member of `RedisClient`.
// Scripts run through EVALSHA, loaded with SCRIPT LOAD when missing.
const REQUIRED = ["evalsha", "script", "rpush", "zadd", "lrange"] as const;

/** The connection is handed over only once it carries every command above. */
function isRedisClient(value: unknown): value is RedisClient {
	return hasCommands(value, REQUIRED);
}

/**
 * A resolver — quasar is loaded on first use, not at config time.
 */
export function quasarConnection(name?: string): () => Promise<RedisClient> {
	return async () =>
		loadQuasarConnection({
			pkg: "bay",
			name,
			required: REQUIRED,
			isConnection: isRedisClient,
			what: "the queue driver",
			raise: (reason, message, cause) =>
				reason === "quasar-missing"
					? new E_BAY_QUASAR_MISSING([message], { cause })
					: new E_BAY_INCOMPLETE_CONNECTION([message], { cause }),
		});
}
