/**
 * Default `QueueManager` singleton — mirror of Adonis's
 * `import queue from '@adonisjs/queue/services/main'` shape.
 *
 * Populated by either:
 *   - `BayProvider.boot()`, when the app uses `() => import('@c9up/bay/provider')`
 *   - The app itself, via `_setQueue(myQueue)`, when it wires a
 *     custom-driver `QueueManager` outside the provider flow.
 *
 *   import queue from '@c9up/bay/services/main'
 *
 *   queue.register('send-email', new SendEmailJob())
 *   await queue.dispatch('send-email', { to: 'user@example.com' })
 */

import type { QueueManager } from "../QueueManager.js";

let _instance: QueueManager | undefined;

/** @internal Bind the singleton (called by BayProvider or by the app). */
export function _setQueue(instance: QueueManager): void {
	_instance = instance;
}

/** @internal Read the singleton (or `undefined` pre-boot). */
export function _getQueue(): QueueManager | undefined {
	return _instance;
}

const queue: QueueManager = new Proxy({} as QueueManager, {
	get(_target, prop) {
		if (!_instance) {
			throw new Error(
				"[bay] QueueManager singleton accessed before BayProvider.boot() ran " +
					"or `_setQueue(myQueue)` was called. Wire one of them first.",
			);
		}
		const value = Reflect.get(_instance, prop, _instance);
		return typeof value === "function" ? value.bind(_instance) : value;
	},
});

export default queue;
