import { MemoryDriver } from "./drivers/MemoryDriver.js";
import { QueueManager } from "./QueueManager.js";
import { setQueue } from "./services/main.js";

/**
 * Slim, duck-typed host context — bay stays publishable without
 * importing `@c9up/ream`. Any framework that exposes a Container with
 * `singleton(token, factory)` + `resolve(token)` and a config store
 * with `get(key)` satisfies the contract.
 */
interface BayContainer {
	singleton(token: unknown, factory: () => unknown): void;
	resolve<T = unknown>(token: unknown): T;
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
	 * Driver to bind by default. Recognized strings: `"memory"`,
	 * `"redis"`. For any other case (custom driver, pre-built
	 * instance), wire `QueueManager` directly in your app's startup
	 * and skip the provider — the `services/main` singleton accepts
	 * `setQueue(myQueue)` from outside.
	 *
	 * Default `"memory"`.
	 */
	driver?: "memory" | "redis";
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
export default class BayProvider {
	constructor(protected app: BayAppContext) {}

	register(): void {
		this.app.container.singleton(QueueManager, () => {
			const config = this.app.config.get<BayProviderConfig>("queue");
			const driverName = config?.driver ?? "memory";
			if (driverName !== "memory") {
				throw new Error(
					`[bay] Unsupported driver '${driverName}' for default provider — ` +
						"wire QueueManager yourself in start/queue.ts for non-memory drivers.",
				);
			}
			return new QueueManager(new MemoryDriver());
		});
		this.app.container.singleton("queue", () =>
			this.app.container.resolve<QueueManager>(QueueManager),
		);
	}

	async boot(): Promise<void> {
		// Populate the `@c9up/bay/services/main` singleton so apps can
		// `import queue from '@c9up/bay/services/main'` from anywhere.
		setQueue(this.app.container.resolve<QueueManager>(QueueManager));
	}

	async shutdown(): Promise<void> {}
}
