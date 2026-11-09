/**
 * A real Redis for the driver suites, gated on `REDIS_TEST_URL`.
 *
 * Every state transition of the Redis driver is a server-side script, and a
 * hand-written fake cannot run one — a fake that re-implemented the scripts in
 * JavaScript would test the re-implementation. So these suites talk to a real
 * server, and are skipped (not failed) where none answers. CI starts one.
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

/** A `describe` that only runs where a Redis answers. */
export function describeRedis(name: string, body: () => void): void {
	if (live) describe(name, body);
	else describe.skip(name, body);
}

/** Separate sockets, so concurrent workers really race on the server. */
export const WORKERS = 12;
const connections = Object.fromEntries(
	Array.from({ length: WORKERS }, (_, i) => [`w${i}`, { url }]),
);

/**
 * One manager for a whole test file, bound into quasar's service slot so
 * `quasarConnection('wN')` resolves the way an application's does.
 */
export function useRedis(): {
	manager: QuasarManager<typeof connections>;
	/** A connection as the driver sees it. */
	client(worker?: number): Promise<RedisClient>;
} {
	const manager = new QuasarManager({ connection: "w0", connections });
	beforeAll(() => {
		setQuasar(manager);
	});
	afterAll(async () => {
		for (const name of manager.activeConnectionNames) await manager.quit(name);
		clearQuasar(manager);
	});
	return {
		manager,
		client: (worker = 0) => quasarConnection(`w${worker}`)(),
	};
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
	manager: QuasarManager<typeof connections>,
	prefix: string,
): Promise<Record<string, string[]>> {
	const connection = manager.connection("w0");
	const keys = (await connection.keys(`${prefix}*`)).sort();
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
	manager: QuasarManager<typeof connections>,
	prefix: string,
	id: string,
): Promise<string[]> {
	const all = await entriesUnder(manager, prefix);
	return Object.entries(all)
		.filter(([key, values]) =>
			values.some(
				(value) => !key.includes(":lease:") && value.includes(`"${id}"`),
			),
		)
		.map(([key]) => key.slice(prefix.length));
}
