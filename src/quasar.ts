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
import { quasarConnection as loadQuasarConnection } from "./vendor/quasarConnection.js";

// The commands this driver actually issues — every non-optional member of
// `RedisClient`. `lmove` is deliberately absent: the driver declares it
// optional and falls back when it is not there.
const REQUIRED = [
	"rpush",
	"lpop",
	"lrem",
	"llen",
	"lrange",
	"del",
	"set",
	"get",
] as const;

/**
 * A resolver — quasar is loaded on first use, not at config time.
 */
export function quasarConnection(name?: string): () => Promise<RedisClient> {
	return async () =>
		loadQuasarConnection<RedisClient>({
			pkg: "bay",
			name,
			required: REQUIRED,
			what: "the queue driver",
			raise: (_reason, message, cause) => new Error(message, { cause }),
		});
}
