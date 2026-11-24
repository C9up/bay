/**
 * A real Redis for the driver suites, gated on `REDIS_TEST_URL`.
 *
 * Every state transition of the Redis driver is a server-side script, and a
 * hand-written fake cannot run one — a fake that re-implemented the scripts in
 * JavaScript would test the re-implementation. So these suites talk to a real
 * server. Without `REDIS_TEST_URL` they are skipped, locally only: on CI, or
 * with a URL nothing answers, the file fails instead.
 *
 * Probed through quasar (a devDependency here) rather than ioredis, which bay
 * deliberately does not depend on.
 */
import { QuasarManager } from "@c9up/quasar";
import { clearQuasar, setQuasar } from "@c9up/quasar/services/main";
import { afterAll, beforeAll, describe } from "vitest";
import type { RedisClient } from "../../src/drivers/RedisDriver.js";
import { quasarConnection } from "../../src/quasar.js";

const url = process.env.REDIS_TEST_URL ?? "";

async function serverAnswers(): Promise<boolean> {
	const probe = new QuasarManager({
		connection: "probe" as const,
		// Fail fast instead of letting ioredis retry a dead port forever.
		connections: {
			probe: { url, lazyConnect: true, maxRetriesPerRequest: 1 },
		},
	});
	try {
		await probe.connection().ping();
		return true;
	} catch {
		return false;
	} finally {
		await probe.disconnect("probe");
	}
}

export const live = url ? await serverAnswers() : false;
// Set means meant, and CI always sets it: a URL that answers nothing, or no
// URL at all on CI, fails the file instead of quietly skipping the suite.
if ((url !== "" || process.env.CI === "true") && !live) {
	throw new Error(
		url === ""
			? "REDIS_TEST_URL is not set on CI: refusing to skip the live-Redis suite."
			: `REDIS_TEST_URL is set (${url}) but no server answers: refusing to skip the live-Redis suite.`,
	);
}

/**
 * Every live suite runs twice: on a bare connection, and on one that sets
 * `keyPrefix`, the documented way two applications share one database. The
 * client prefixes the KEYS of a script and nothing in its ARGV, so a script
 * that builds a key from an argument writes it outside the connection's
 * prefix: only the prefixed run sees that.
 */
export const KEY_PREFIXES = ["", "bay-app:"] as const;

/** Separate sockets, so concurrent workers really race on the server. */
export const WORKERS = 12;

function connectionsFor(keyPrefix: string) {
	return Object.fromEntries(
		Array.from({ length: WORKERS }, (_, i) => [`w${i}`, { url, keyPrefix }]),
	);
}
type Connections = ReturnType<typeof connectionsFor>;

/** One suite's live Redis. */
export interface LiveRedis {
	manager: QuasarManager<Connections>;
	/** The `keyPrefix` every connection of this run sets. */
	keyPrefix: string;
	/** A connection as the driver sees it. */
	client(worker?: number): Promise<RedisClient>;
	/**
	 * The keys matching `pattern`, as a prefixed connection names them. KEYS
	 * takes a pattern, not a key, so the client does not prefix it, and answers
	 * full names: both are adjusted here.
	 */
	keys(pattern: string): Promise<string[]>;
}

/**
 * One manager per run of a suite, bound into quasar's service slot so
 * `quasarConnection('wN')` resolves the way an application's does.
 */
function useRedis(keyPrefix: string): LiveRedis {
	const manager = new QuasarManager({
		connection: "w0",
		connections: connectionsFor(keyPrefix),
	});
	beforeAll(() => {
		setQuasar(manager);
	});
	afterAll(async () => {
		for (const name of manager.activeConnectionNames) await manager.quit(name);
		clearQuasar(manager);
	});
	return {
		manager,
		keyPrefix,
		client: (worker = 0) => quasarConnection(`w${worker}`)(),
		keys: async (pattern) => {
			const found = await manager.connection("w0").keys(keyPrefix + pattern);
			return found.map((key) => key.slice(keyPrefix.length));
		},
	};
}

/**
 * A `describe` that only runs where a Redis answers, once per entry of
 * {@link KEY_PREFIXES}.
 */
export function describeRedis(
	name: string,
	body: (redis: LiveRedis) => void,
): void {
	for (const keyPrefix of KEY_PREFIXES) {
		const title =
			keyPrefix === "" ? name : `${name} (connection keyPrefix '${keyPrefix}')`;
		const register = live ? describe : describe.skip;
		register(title, () => body(useRedis(keyPrefix)));
	}
}

let sequence = 0;
/** A key prefix no other test (or run) shares. */
export function freshPrefix(label: string): string {
	sequence += 1;
	return `bay-test:${process.pid}:${label}:${sequence}:`;
}

/**
 * Wrap a client so a test can step into individual commands.
 *
 * `around` receives the command name and a function that runs it; it may run it
 * late, run it and then throw, or throw without running it — the three shapes a
 * dropped connection takes from the client's point of view.
 */
export function intercept(
	base: RedisClient,
	around: (command: string, run: () => Promise<unknown>) => Promise<unknown>,
): RedisClient {
	return new Proxy(base, {
		get(target, prop, receiver) {
			const value: unknown = Reflect.get(target, prop, receiver);
			if (typeof prop !== "string" || typeof value !== "function") return value;
			return (...args: unknown[]) =>
				around(prop, async () => Reflect.apply(value, target, args));
		},
	});
}

/** Every list and sorted-set entry under `prefix`, raw. */
export async function entriesUnder(
	redis: LiveRedis,
	prefix: string,
): Promise<Record<string, string[]>> {
	const connection = redis.manager.connection("w0");
	const keys = (await redis.keys(`${prefix}*`)).sort();
	const out: Record<string, string[]> = {};
	for (const key of keys) {
		const type = await connection.type(key);
		if (type === "list") out[key] = await connection.lrange(key, 0, -1);
		else if (type === "zset")
			out[key] = await connection.zrange(key, "0", "-1");
		else if (type === "string") out[key] = [(await connection.get(key)) ?? ""];
	}
	return out;
}

/** The keys under `prefix` holding an entry whose record carries `id`. */
export async function whereIs(
	redis: LiveRedis,
	prefix: string,
	id: string,
): Promise<string[]> {
	const all = await entriesUnder(redis, prefix);
	return Object.entries(all)
		.filter(([key, values]) =>
			values.some(
				(value) => !key.includes(":lease:") && value.includes(`"${id}"`),
			),
		)
		.map(([key]) => key.slice(prefix.length));
}
