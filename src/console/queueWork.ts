/**
 * `queue:work` — run the worker that takes jobs off the queue.
 *
 * The command a deployment runs as its own process, beside the HTTP one:
 *
 *     ream queue:work
 *     ream queue:work --queue=emails,notifications
 *     ream queue:work --concurrency=10
 *
 * It stays alive on purpose — `staysAlive` — and the loop it starts is the
 * application's, so a SIGTERM reaches `BayProvider.shutdown()`, which stops the
 * worker and lets the job in flight finish rather than dropping it.
 */

import { getQueue } from "../services/main.js";
import { type BayCommandClass, flag } from "./contract.js";

/** Split `--queue=a,b` into names, dropping the empties a trailing comma makes. */
export function parseQueues(value: string | undefined): string[] | undefined {
	if (value === undefined) return undefined;
	const names = value
		.split(",")
		.map((name) => name.trim())
		.filter((name) => name.length > 0);
	return names.length > 0 ? names : undefined;
}

export function queueWorkCommand(): BayCommandClass {
	return class QueueWork {
		static commandName = "queue:work";
		static description = "Process queued jobs until the process is stopped";
		// The worker needs the container: the queue it drains is the one the
		// provider booted, with the driver the config named.
		static options = { startApp: true, staysAlive: true };
		static flags = [
			flag("queue", "string", {
				description:
					"Comma-separated queues to serve, in order (default: the default queue)",
			}),
			flag("concurrency", "number", {
				description: "How many jobs to run at once (default: 1)",
			}),
		];

		declare queue?: string;
		declare concurrency?: number;

		async run(): Promise<void> {
			const manager = getQueue();
			if (!manager) {
				// Naming the wiring beats a stack trace out of the service proxy:
				// the usual cause is a project that never added the provider.
				throw new Error(
					"[bay] queue:work found no queue. Add `() => import('@c9up/bay/provider')` " +
						"to the providers in reamrc.ts, or call setQueue(myQueue) at boot.",
				);
			}

			const queues = parseQueues(this.queue);
			process.stdout.write(
				`[bay] worker started — queues: ${(queues ?? ["default"]).join(", ")}, concurrency: ${this.concurrency ?? 1}\n`,
			);

			await manager.work({
				queues,
				concurrency: this.concurrency,
			});
		}
	};
}
