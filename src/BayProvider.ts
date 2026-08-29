import { MemoryDriver } from "./drivers/MemoryDriver.js";
import type { QueueDriver } from "./QueueManager.js";
import { QueueManager } from "./QueueManager.js";
import { setQueue } from "./services/main.js";
import type { QueueStoreFactory } from "./stores.js";

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
	 * Which named store to use — a key of {@link stores}. Read from the
	 * environment in the generated config, so a deployment picks its queue
	 * backend without editing a file.
	 */
	default?: string;
	/**
	 * The queue stores this application can use, by name. Each is a factory
	 * from `stores.*`, built only when it is the one selected.
	 */
	stores?: Record<string, QueueStoreFactory>;
	/**
	 * The single-store form, kept for configs written against it: only
	 * `"memory"` was ever accepted. Prefer `default` + `stores`, which is how a
	 * pluggable backend is configured everywhere else and what lets the
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
	const stores = config?.stores;
	const name = config?.default;

	if (stores && name !== undefined) {
		const selected = stores[name];
		if (!selected) {
			const known = Object.keys(stores);
			throw new Error(
				`[bay] config.queue names the store '${name}', which is not in \`stores\`. ` +
					(known.length > 0
						? `Declared: ${known.join(", ")}.`
						: "`stores` is empty — declare one with stores.memory() or stores.redis()."),
			);
		}
		return selected();
	}

	if (stores && name === undefined) {
		throw new Error(
			"[bay] config.queue declares `stores` but no `default` naming which one to use. " +
				`Set default to one of: ${Object.keys(stores).join(", ")}.`,
		);
	}

	const driverName = config?.driver ?? "memory";
	if (driverName !== "memory") {
		throw new Error(
			`[bay] Unsupported driver '${driverName}' — name it under \`stores\` instead: ` +
				"stores: { redis: stores.redis({ connection: 'main' }) }.",
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

	async boot(): Promise<void> {
		// Populate the `@c9up/bay/services/main` singleton so apps can
		// `import queue from '@c9up/bay/services/main'` from anywhere.
		setQueue(await this.app.container.resolve<QueueManager>(QueueManager));
	}

	async shutdown(): Promise<void> {}
}
