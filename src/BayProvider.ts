import type { AdapterFactory } from "./adapters.js";
import { MemoryDriver } from "./drivers/MemoryDriver.js";
import type { QueueDriver } from "./QueueManager.js";
import { QueueManager } from "./QueueManager.js";
import { clearQueue, getQueue, setQueue } from "./services/main.js";

/**
 * Slim, duck-typed host context — bay stays publishable without
 * importing `@c9up/ream`. Any framework that exposes a Container with
 * `singleton(token, factory)` + `resolve(token)` and a config store
 * with `get(key)` satisfies the contract.
 */
interface BayContainer {
	singleton(token: unknown, factory: () => unknown): void;
	resolve<T = unknown>(token: unknown): Promise<T>;
}
interface BayConfigStore {
	get<T = unknown>(key: string): T | undefined;
}
export interface BayAppContext {
	container: BayContainer;
	config: BayConfigStore;
}

export interface BayProviderConfig {
	/**
	 * Which named adapter to use — a key of {@link adapters}. Read from the
	 * environment in the generated config, so a deployment picks its queue
	 * backend without editing a file.
	 */
	default?: string;
	/**
	 * The queue adapters this application can use, by name. Each is a factory
	 * from `drivers.*`, built only when it is the one selected.
	 */
	adapters?: Record<string, AdapterFactory>;
	/**
	 * The name this key had before it matched upstream's. Read when `adapters`
	 * is absent, so a config written against the older spelling keeps selecting
	 * the backend it named — silently falling back to an in-process queue is the
	 * one outcome `buildDriver` exists to prevent.
	 */
	stores?: Record<string, AdapterFactory>;
	/**
	 * The single-adapter form, kept for configs written against it: only
	 * `"memory"` was ever accepted. Prefer `default` + `adapters`, which is how
	 * a pluggable backend is configured everywhere else and what lets the
	 * environment choose.
	 */
	driver?: "memory";
}

/**
 * BayProvider — registers a default in-memory `QueueManager` in the
 * host container so apps that don't need custom driver wiring can
 * `import queue from '@c9up/bay/services/main'` and dispatch
 * straight away. Job handlers are still registered manually via
 * `queue.register(name, handler)` — that's intrinsic to the queue
 * design (handlers are app-defined, not config-driven).
 *
 * Apps with non-trivial wiring (Redis driver, custom queue config)
 * can ignore this provider and bind their own `QueueManager` instance
 * in the container; the `services/main` proxy resolves whatever is
 * registered.
 *
 *   // reamrc.ts
 *   providers: [() => import('@c9up/bay/provider')]
 *
 *   // start/queue.ts
 *   import queue from '@c9up/bay/services/main'
 *
 *   queue.register('send-email', new SendEmailJob())
 *   await queue.dispatch('send-email', { to: 'user@example.com' })
 */
/**
 * The driver the config asks for.
 *
 * `default` + `stores` first — the form an environment variable can steer. The
 * `driver` key is the single-store form kept for configs written against it.
 * Naming a store that does not exist throws rather than falling back to memory:
 * an application that meant to queue in Redis and silently got an in-process
 * queue would only find out when a restart dropped every pending job.
 */
function buildDriver(config: BayProviderConfig | undefined): QueueDriver {
	// `adapters` first, `stores` when it is absent: the key was renamed to the
	// one upstream reads, and a config that still says `stores` must keep
	// selecting its backend rather than quietly landing on memory.
	const adapters = config?.adapters ?? config?.stores;
	const key = config?.adapters ? "adapters" : "stores";
	const name = config?.default;

	if (adapters && name !== undefined) {
		const selected = adapters[name];
		if (!selected) {
			const known = Object.keys(adapters);
			throw new Error(
				`[bay] config.queue names the adapter '${name}', which is not in \`${key}\`. ` +
					(known.length > 0
						? `Declared: ${known.join(", ")}.`
						: `\`${key}\` is empty — declare one with drivers.memory() or drivers.redis().`),
			);
		}
		return selected();
	}

	if (adapters && name === undefined) {
		throw new Error(
			`[bay] config.queue declares \`${key}\` but no \`default\` naming which one to use. ` +
				`Set default to one of: ${Object.keys(adapters).join(", ")}.`,
		);
	}

	const driverName = config?.driver ?? "memory";
	if (driverName !== "memory") {
		throw new Error(
			`[bay] Unsupported driver '${driverName}' — name it under \`adapters\` instead: ` +
				"adapters: { redis: drivers.redis({ connection: 'main' }) }.",
		);
	}
	return new MemoryDriver();
}

export default class BayProvider {
	constructor(protected app: BayAppContext) {}

	register(): void {
		this.app.container.singleton(QueueManager, () => {
			const config = this.app.config.get<BayProviderConfig>("queue");
			return new QueueManager(buildDriver(config));
		});
		this.app.container.singleton("queue", () =>
			this.app.container.resolve<QueueManager>(QueueManager),
		);
	}

	/** The queue THIS provider booted — not whatever the module singleton holds. */
	#queue: QueueManager | undefined;

	async boot(): Promise<void> {
		// Populate the `@c9up/bay/services/main` singleton so apps can
		// `import queue from '@c9up/bay/services/main'` from anywhere.
		this.#queue = await this.app.container.resolve<QueueManager>(QueueManager);
		setQueue(this.#queue);
	}

	/**
	 * Stop the worker the app started, and let the job in flight finish.
	 *
	 * A worker polls on a timer. Left running, it survives a dev reload, a test
	 * teardown and a SIGTERM — so the old process keeps pulling jobs the new
	 * one is also pulling, and the same job runs twice.
	 */
	async shutdown(): Promise<void> {
		if (!this.#queue) return;
		await this.#queue.stop();
		// Two applications can share a process — parallel tests, a hot reload.
		// The module singleton holds whichever booted last, so it is only ours
		// to clear while it still points at the queue this provider booted.
		if (getQueue() === this.#queue) clearQueue();
		this.#queue = undefined;
	}
}
