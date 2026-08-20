/**
 * Default `QueueManager` singleton — mirror of Adonis's
 * `import queue from '@adonisjs/queue/services/main'` shape.
 *
 * Populated by either:
 *   - `BayProvider.boot()`, when the app uses `() => import('@c9up/bay/provider')`
 *   - The app itself, via `setQueue(myQueue)`, when it wires a
 *     custom-driver `QueueManager` outside the provider flow.
 *
 *   import queue from '@c9up/bay/services/main'
 *
 *   queue.register('send-email', new SendEmailJob())
 *   await queue.dispatch('send-email', { to: 'user@example.com' })
 */

import type { QueueManager } from "../QueueManager.js";

let instance: QueueManager | undefined;

/** @internal Bind the singleton (called by BayProvider or by the app). */
export function setQueue(value: QueueManager): void {
	instance = value;
}

/** @internal Read the singleton (or `undefined` pre-boot). */
export function getQueue(): QueueManager | undefined {
	return instance;
}

const queue: QueueManager = new Proxy({} as QueueManager, {
	get(_target, prop) {
		// A module loader inspects what it imports before anyone uses it: it reads
		// `then` to decide whether the namespace is thenable, and various symbols
		// for interop and formatting. Throwing on those turns a plain
		// `import { setX } from ".../services/main"` into a crash at import time,
		// far from any real use. They are not members of what this stands in for,
		// so answer undefined and let a genuine access be the one that reports.
		if (typeof prop === 'symbol' || prop === 'then') {
			return undefined
		}
		if (!instance) {
			throw new Error(
				"[bay] QueueManager singleton accessed before BayProvider.boot() ran " +
					"or `setQueue(myQueue)` was called. Wire one of them first.",
			);
		}
		const value = Reflect.get(instance, prop, instance);
		return typeof value === "function" ? value.bind(instance) : value;
	},
});

export default queue;
