/**
 * The quasar bridge, the provider and the service accessor.
 *
 * quasar is an optional peer, so the bridge builds its specifier at runtime and
 * never imports it — and nothing exercised that path, which means each failure
 * (absent package, wrong shape, a connection missing a command) first appears
 * on a job push in production. Same for the accessor: read before boot, it has
 * one job, which is to say what to wire.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import BayProvider from "../../src/BayProvider.js";
import { defineConfig } from "../../src/index.js";
import { QueueManager } from "../../src/QueueManager.js";
import queue, { getQueue, setQueue } from "../../src/services/main.js";

const SPECIFIER = "@c9up/quasar/services/main";

/** Every command the redis driver actually issues. */
const commands = [
	"rpush",
	"lpop",
	"lrem",
	"llen",
	"lrange",
	"del",
	"set",
	"get",
];

const client = (omit?: string) =>
	Object.fromEntries(
		commands.filter((c) => c !== omit).map((c) => [c, () => {}]),
	);

/**
 * A mocked namespace throws on any export the factory did not declare, unlike
 * a real one — so both shapes the bridge probes are always declared.
 */
const mockQuasar = (shape: { connection?: unknown; default?: unknown }) => {
	vi.doMock(SPECIFIER, () => ({
		connection: shape.connection,
		default: shape.default,
	}));
};

const load = async () => (await import("../../src/quasar.js")).quasarConnection;

afterEach(() => {
	vi.doUnmock(SPECIFIER);
	vi.resetModules();
	setQueue(undefined as unknown as QueueManager);
});

describe("bay > the quasar bridge", () => {
	it("accepts a manager that answers only through a get trap", async () => {
		// Quasar's accessor is a Proxy over an empty null-prototype object with
		// only a `get` trap, so `"connection" in manager` is FALSE while reading
		// it returns the function. Probing with `in` rejected the real manager —
		// and every unit test here used a plain object, so only the live-Redis
		// suite noticed.
		const connection = client();
		mockQuasar({
			default: new Proxy(Object.create(null), {
				get: (_target, property) =>
					property === "connection" ? () => connection : undefined,
			}),
		});

		expect(await (await load())("jobs")()).toBe(connection);
	});

	it("hands back the named connection", async () => {
		const connection = client();
		const manager = { connection: vi.fn(() => connection) };
		mockQuasar({ default: manager });

		expect(await (await load())("jobs")()).toBe(connection);
		expect(manager.connection).toHaveBeenCalledWith("jobs");
	});

	it("takes the manager on the namespace as well as on the default export", async () => {
		const connection = client();
		mockQuasar({ connection: () => connection });

		expect(await (await load())()()).toBe(connection);
	});

	it("says the package is missing, and how to add it", async () => {
		vi.doMock(SPECIFIER, () => {
			throw new Error("Cannot find module");
		});

		await expect((await load())("jobs")()).rejects.toThrow(
			/@c9up\/quasar is not installed[\s\S]*pnpm add @c9up\/quasar/,
		);
	});

	it("names the queue that asked for it", async () => {
		vi.doMock(SPECIFIER, () => {
			throw new Error("Cannot find module");
		});
		const resolve = await load();

		await expect(resolve("jobs")()).rejects.toThrow(/quasar connection "jobs"/);
		await expect(resolve()()).rejects.toThrow(/quasar connection "default"/);
	});

	it("refuses a module that is not a connection manager", async () => {
		mockQuasar({ default: { somethingElse: () => {} } });

		await expect((await load())()()).rejects.toThrow(
			/did not expose a connection\(\) manager/,
		);
	});

	it("refuses a connection missing a command it will need", async () => {
		// Accepting it would fail on the first push, with a message naming
		// neither the connection nor the missing command.
		for (const missing of commands) {
			vi.resetModules();
			mockQuasar({ default: { connection: () => client(missing) } });

			// The message names the connection AND the missing command — the
			// point of checking before handing it over.
			await expect((await load())("jobs")(), missing).rejects.toThrow(
				new RegExp(`connection 'jobs' is missing ${missing}`),
			);
		}
	});

	it("accepts a connection without lmove", async () => {
		// The driver declares `lmove` optional and falls back when it is not
		// there, so demanding it would reject a connection that works.
		mockQuasar({ default: { connection: () => client() } });

		await expect((await load())("jobs")()).resolves.toBeDefined();
	});

	it("resolves nothing until the queue is used", async () => {
		expect(typeof (await load())("jobs")).toBe("function");
	});
});

describe("bay > the provider", () => {
	/** The slice of an app container the provider actually uses. */
	const app = (config: unknown) => {
		const bindings = new Map<unknown, () => unknown>();
		const built = new Map<unknown, unknown>();
		return {
			config: { get: <T>(): T | undefined => config as T | undefined },
			container: {
				singleton(token: unknown, factory: () => unknown) {
					bindings.set(token, factory);
				},
				async resolve<T>(token: unknown): Promise<T> {
					if (!built.has(token)) {
						const factory = bindings.get(token);
						if (!factory) throw new Error(`unbound: ${String(token)}`);
						built.set(token, await factory());
					}
					return built.get(token) as T;
				},
			},
			bindings,
		};
	};

	it("binds the manager under the class and both string tokens", async () => {
		const context = app(defineConfig({}));
		new BayProvider(context).register();

		// `bay.queue` is the namespaced form upstream uses for a satellite's
		// binding; the bare `queue` stays for everything already asking for it.
		expect([...context.bindings.keys()]).toEqual([
			QueueManager,
			"bay.queue",
			"queue",
		]);
		expect(await context.container.resolve("bay.queue")).toBeInstanceOf(
			QueueManager,
		);
		expect(await context.container.resolve("queue")).toBeInstanceOf(
			QueueManager,
		);
	});

	it("stops the worker at shutdown", async () => {
		const context = app(defineConfig({}));
		const provider = new BayProvider(context);
		provider.register();
		await provider.boot();
		const queue = getQueue();
		if (!queue) throw new Error("boot should have published a queue");

		// Started, then shut down. Left running, the worker survives a dev
		// reload or a SIGTERM and keeps pulling jobs the next process is also
		// pulling — the same job runs twice.
		const working = queue.work(10);
		await provider.shutdown();
		await working;

		// A second start proves the loop really exited: `work()` refuses to
		// run twice at once.
		const again = queue.work(10);
		await queue.stop();
		await again;
	});

	it("shuts down the queue IT booted, not whatever the singleton holds", async () => {
		// Two applications can share a process — parallel tests, a hot reload.
		// Stopping the other one's queue is a worker killed out from under a
		// running app.
		const first = new BayProvider(app(defineConfig({})));
		first.register();
		await first.boot();
		const mine = getQueue();

		const second = new BayProvider(app(defineConfig({})));
		second.register();
		await second.boot();
		expect(getQueue()).not.toBe(mine);

		const theirs = getQueue();
		await first.shutdown();

		// The singleton still belongs to the second application, untouched.
		expect(getQueue()).toBe(theirs);

		await second.shutdown();
		expect(getQueue()).toBeUndefined();
	});

	it("waits for the worker to actually stop", async () => {
		const context = app(defineConfig({}));
		const provider = new BayProvider(context);
		provider.register();
		await provider.boot();
		const queue = getQueue();
		if (!queue) throw new Error("boot should have published a queue");

		// A one-second poll: a stop that did not cut the sleep short would
		// return here with the worker still pending.
		const working = queue.work(1000);
		await new Promise((resolve) => setTimeout(resolve, 20));
		const started = Date.now();
		await provider.shutdown();

		expect(Date.now() - started).toBeLessThan(500);
		await working;
	});

	it("publishes the singleton at boot", async () => {
		const context = app(defineConfig({}));
		const provider = new BayProvider(context);
		provider.register();

		await provider.boot();

		expect(getQueue()).toBeInstanceOf(QueueManager);
		await expect(provider.shutdown()).resolves.toBeUndefined();
	});
});

describe("bay > the service accessor", () => {
	it("answers undefined to a loader's probes instead of throwing", () => {
		// A module loader reads `then` to decide whether the namespace is
		// thenable, and symbols for interop. Throwing there turns a plain
		// import into a crash far from any real use.
		expect((queue as unknown as { then?: unknown }).then).toBeUndefined();
		expect(Reflect.get(queue, Symbol.toPrimitive)).toBeUndefined();
	});

	it("says what to wire when it is read before boot", () => {
		expect(() => queue.dispatch).toThrow(
			/accessed before BayProvider.boot\(\)/,
		);
	});

	it("forwards to the bound manager, bound to it", async () => {
		const { MemoryDriver } = await import("../../src/drivers/MemoryDriver.js");
		const manager = new QueueManager(new MemoryDriver());
		setQueue(manager);

		// Unbound, the forwarded method would lose its private state.
		const { dispatch, size } = queue;
		await dispatch("welcome", { id: 1 });

		expect(await size()).toBe(1);
	});
});

describe("bay > defineConfig", () => {
	it("hands the config straight back", () => {
		const config = { driver: "memory" as const };

		expect(defineConfig(config)).toBe(config);
	});
});

/**
 * Discovery imports application modules, so it belongs in `start()`.
 *
 * In `boot()` it ran before preloads: a job reaching for a container service —
 * the ordinary way to write one — waited on a boot that was waiting on its own
 * import. Upstream runs preloads between the two phases for exactly this.
 */
describe("bay > when jobs are discovered", () => {
	const app = (locations: string[]) => {
		const bindings = new Map<unknown, () => unknown>();
		const built = new Map<unknown, unknown>();
		return {
			config: { get: <T>(): T | undefined => ({ locations }) as T },
			container: {
				singleton(token: unknown, factory: () => unknown) {
					bindings.set(token, factory);
				},
				async resolve<T>(token: unknown): Promise<T> {
					if (!built.has(token)) {
						const factory = bindings.get(token);
						if (!factory) throw new Error(`unbound: ${String(token)}`);
						built.set(token, await factory());
					}
					return built.get(token) as T;
				},
			},
		};
	};

	it("does not import anything during boot", async () => {
		// A directory of unloadable files: discovery would throw. Reaching boot
		// without a throw is the proof that it did not scan.
		const provider = new BayProvider(app(["./does-not-exist-either"]) as never);
		provider.register();

		await expect(provider.boot()).resolves.toBeUndefined();
		await provider.shutdown();
	});

	it("refuses to become ready before it has booted", async () => {
		const provider = new BayProvider(app(["./nowhere"]) as never);
		provider.register();

		await expect(provider.ready()).rejects.toThrow(/before boot\(\)/);
	});
});
