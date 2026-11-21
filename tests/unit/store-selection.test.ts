import { describe, expect, it } from "vitest";
import { drivers, stores } from "../../src/adapters.js";
import BayProvider, {
	type BayAppContext,
	type BayProviderConfig,
} from "../../src/BayProvider.js";
import { MemoryDriver } from "../../src/drivers/MemoryDriver.js";
import { RedisDriver } from "../../src/drivers/RedisDriver.js";
import { QueueManager } from "../../src/QueueManager.js";
import { defined } from "../__helpers__/defined.js";
import { scripting } from "../__helpers__/scripting.js";

/**
 * `{ default, adapters }` — the shape a package takes when several backends are
 * declared and one is selected, so a deployment names its queue in the
 * environment instead of building the manager by hand. The tests below use the
 * older `stores` spelling on purpose: it is still accepted, and a config
 * written against it must keep selecting the backend it named.
 */
function managerFrom(config: unknown): QueueManager {
	const bindings = new Map<unknown, () => unknown>();
	const app: BayAppContext = {
		container: {
			singleton(token: unknown, factory: () => unknown) {
				bindings.set(token, factory);
			},
			async resolve(token: unknown): Promise<unknown> {
				return defined(bindings.get(token))();
			},
		},
		config: { get: () => config },
	};
	new BayProvider(app).register();
	const manager = bindings.get(QueueManager)?.();
	if (!(manager instanceof QueueManager)) {
		throw new Error("expected the provider to bind a QueueManager");
	}
	return manager;
}

describe("bay > store selection", () => {
	it("builds the store `default` names", async () => {
		const manager = managerFrom({
			default: "memory",
			stores: { memory: stores.memory() },
		});

		// A working queue is the observable proof: the driver is private, so
		// dispatch and count.
		await manager.dispatch("job", {});
		expect(await manager.size()).toBe(1);
	});

	it("builds only the store it selected", () => {
		let built = 0;
		managerFrom({
			default: "memory",
			stores: {
				memory: stores.memory(),
				never: () => {
					built += 1;
					return new MemoryDriver();
				},
			},
		});

		// A config may name a Redis queue it does not use in this environment;
		// building it would open a connection nobody asked for.
		expect(built).toBe(0);
	});

	it("refuses a `default` that names nothing, listing what exists", () => {
		expect(() =>
			managerFrom({ default: "redis", stores: { memory: stores.memory() } }),
		).toThrow(/not in `stores`.*Declared: memory/s);
	});

	it("refuses a `default` that only names something on Object.prototype", () => {
		// `QUEUE_DRIVER=constructor` read `Object` off the prototype chain and
		// booted a driver without methods; `hasOwnProperty` threw a TypeError
		// unrelated to the config. Each is an adapter the config never declared.
		for (const name of [
			"constructor",
			"toString",
			"hasOwnProperty",
			"__proto__",
		]) {
			expect(() =>
				managerFrom({ default: name, adapters: { memory: drivers.memory() } }),
			).toThrow(
				new RegExp(`names the adapter '${name}', which is not in \`adapters\``),
			);
		}
	});

	it("refuses an adapter factory that does not build a driver", () => {
		expect(() =>
			managerFrom({ default: "broken", adapters: { broken: () => ({}) } }),
		).toThrow(/adapter 'broken' did not build a queue driver/);
	});

	it("refuses `stores` with no `default`", () => {
		expect(() => managerFrom({ stores: { memory: stores.memory() } })).toThrow(
			/no `default`/,
		);
	});

	it("still honours the single `driver` form", () => {
		expect(managerFrom({ driver: "memory" })).toBeInstanceOf(QueueManager);
		expect(managerFrom(undefined)).toBeInstanceOf(QueueManager);
	});

	it("points a non-memory `driver` at the adapters form", () => {
		expect(() => managerFrom({ driver: "redis" })).toThrow(
			/name it under `adapters`/,
		);
	});

	it("hands each factory its options", () => {
		expect(stores.memory()()).toBeInstanceOf(MemoryDriver);
		expect(
			stores.redis({
				connection: {
					...scripting(),
					rpush: async () => 1,
					zadd: async () => 1,
					lrange: async () => [],
				},
			})(),
		).toBeInstanceOf(RedisDriver);
	});

	it("resolves a quasar connection by name only when the queue is used", async () => {
		const driver = stores.redis({ connection: "main" })();
		// Building it resolved nothing: a config may name a connection that does
		// not exist in the environment that never selects this store.
		expect(driver).toBeInstanceOf(RedisDriver);

		// The failure surfaces on first use — from quasar when it is installed
		// but unbooted, from bay's bridge when the package is absent. Either way
		// it reaches the caller instead of being swallowed.
		await expect(driver.size()).rejects.toThrow();
	});
});

describe("bay > the names upstream uses", () => {
	it("selects through `adapters` and `drivers`", async () => {
		const manager = managerFrom({
			default: "memory",
			adapters: { memory: drivers.memory() },
		});

		await manager.dispatch("job", {});
		expect(await manager.size()).toBe(1);
	});

	it("prefers `adapters` when a config carries both", () => {
		let fromStores = 0;
		managerFrom({
			default: "memory",
			adapters: { memory: () => new MemoryDriver() },
			stores: {
				memory: () => {
					fromStores++;
					return new MemoryDriver();
				},
			},
		});

		expect(fromStores).toBe(0);
	});

	it("names the key the config actually used when it complains", () => {
		expect(() =>
			managerFrom({ default: "redis", adapters: { memory: drivers.memory() } }),
		).toThrow(/not in `adapters`/);
		expect(() =>
			managerFrom({ default: "redis", stores: { memory: drivers.memory() } }),
		).toThrow(/not in `stores`/);
	});

	it("is one namespace under two names", () => {
		expect(stores).toBe(drivers);
	});
});

describe("bay > the worker block", () => {
	it("reaches the manager the provider builds", async () => {
		const manager = managerFrom({
			default: "memory",
			adapters: { memory: drivers.memory() },
			worker: { stalledInterval: 0 },
		});

		// A `stalledInterval` the manager refuses is the observable proof it read
		// the block: nothing else would make `work()` reject.
		await expect(manager.work()).rejects.toThrow(
			/stalledInterval must be a delay from 1/,
		);
	});

	it("lets an argument to work() beat the block", async () => {
		const manager = managerFrom({
			default: "memory",
			adapters: { memory: drivers.memory() },
			worker: { stalledInterval: 0 },
		});

		const running = manager.work({ idleDelay: 5, stalledInterval: 50 });
		await manager.stop();
		await running;

		expect(await manager.size()).toBe(0);
	});
});

describe("bay > the token the framework namespaces", () => {
	function bindingsFrom(
		config: BayProviderConfig,
	): Map<unknown, () => unknown> {
		const bindings = new Map<unknown, () => unknown>();
		// A `singleton` that builds once, which is the whole difference between
		// it and a plain binding — without it the two tokens would each get
		// their own manager here for a reason the provider is not responsible
		// for.
		const built = new Map<unknown, unknown>();
		const app: BayAppContext = {
			container: {
				singleton(token: unknown, factory: () => unknown) {
					bindings.set(token, () => {
						if (!built.has(token)) built.set(token, factory());
						return built.get(token);
					});
				},
				async resolve(token: unknown): Promise<unknown> {
					return defined(bindings.get(token))();
				},
			},
			config: { get: () => config },
		};
		new BayProvider(app).register();
		return bindings;
	}

	it("binds the queue under its package and under the bare name", async () => {
		const bindings = bindingsFrom({
			default: "memory",
			adapters: { memory: drivers.memory() },
		});

		// Upstream namespaces `lucid.db`, `auth.manager`, `queue.manager` by the
		// package that owns them; the bare token is what existing calls ask for.
		expect(bindings.has("bay.queue")).toBe(true);
		expect(bindings.has("queue")).toBe(true);
	});

	it("hands back the same manager through either name", async () => {
		const bindings = bindingsFrom({
			default: "memory",
			adapters: { memory: drivers.memory() },
		});

		const namespaced = await bindings.get("bay.queue")?.();
		const bare = await bindings.get("queue")?.();
		expect(namespaced).toBeInstanceOf(QueueManager);
		expect(namespaced).toBe(bare);
	});
});
