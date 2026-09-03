import { describe, expect, it } from "vitest";
import { drivers, stores } from "../../src/adapters.js";
import BayProvider, { type BayProviderConfig } from "../../src/BayProvider.js";
import { MemoryDriver } from "../../src/drivers/MemoryDriver.js";
import { RedisDriver } from "../../src/drivers/RedisDriver.js";
import { QueueManager } from "../../src/QueueManager.js";

/**
 * `{ default, adapters }` — the shape a package takes when several backends are
 * declared and one is selected, so a deployment names its queue in the
 * environment instead of building the manager by hand. The tests below use the
 * older `stores` spelling on purpose: it is still accepted, and a config
 * written against it must keep selecting the backend it named.
 */
function managerFrom(config: BayProviderConfig | undefined): QueueManager {
	const bindings = new Map<unknown, () => unknown>();
	const app = {
		container: {
			singleton(token: unknown, factory: () => unknown) {
				bindings.set(token, factory);
			},
			resolve: <T>(token: unknown): T => bindings.get(token)?.() as T,
		},
		config: { get: <T>() => config as T },
	};
	// biome-ignore lint/suspicious/noExplicitAny: the provider's app context is
	// structural; the stub above is the slice register() touches.
	new BayProvider(app as any).register();
	return bindings.get(QueueManager)?.() as QueueManager;
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
		expect(() =>
			managerFrom({ driver: "redis" as BayProviderConfig["driver"] }),
		).toThrow(/name it under `adapters`/);
	});

	it("hands each factory its options", () => {
		expect(stores.memory()()).toBeInstanceOf(MemoryDriver);
		expect(
			stores.redis({
				connection: {
					rpush: async () => 1,
					lpop: async () => null,
					lrem: async () => 1,
					lrange: async () => [],
					llen: async () => 0,
					set: async () => "OK",
					get: async () => null,
					del: async () => 1,
				} as never,
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
			/stalledInterval must be positive/,
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
