/**
 * Memory queue driver — in-process queue for development and tests.
 */

import { DEFAULT_QUEUE } from "../Job.js";
import { type JobRecord, type QueueDriver, queueOf } from "../QueueManager.js";

export class MemoryDriver implements QueueDriver {
	/** One list per named queue, created on first use. */
	#pending: Map<string, JobRecord[]> = new Map();
	/**
	 * Jobs whose `runAt` has not arrived, oldest deadline first.
	 *
	 * Kept apart from the queues rather than filtered on the way out: a delayed
	 * job at the head of a list would otherwise be skipped over on every poll,
	 * and a queue whose head is not due would look empty while it is not.
	 */
	#delayed: JobRecord[] = [];
	#failedJobs: JobRecord[] = [];
	#maxFailedJobs: number;

	constructor(options?: { maxFailedJobs?: number }) {
		this.#maxFailedJobs = options?.maxFailedJobs ?? 1000;
	}

	#queue(name: string): JobRecord[] {
		const existing = this.#pending.get(name);
		if (existing !== undefined) return existing;
		const created: JobRecord[] = [];
		this.#pending.set(name, created);
		return created;
	}

	/** Move everything whose delay has elapsed into its queue. */
	#promoteDue(now = Date.now()): void {
		if (this.#delayed.length === 0) return;
		const due = this.#delayed.filter((job) => (job.runAt ?? 0) <= now);
		if (due.length === 0) return;
		this.#delayed = this.#delayed.filter((job) => (job.runAt ?? 0) > now);
		for (const job of due) {
			job.runAt = undefined;
			this.#queue(queueOf(job)).push(job);
		}
	}

	async push(job: JobRecord): Promise<void> {
		if (job.runAt !== undefined && job.runAt > Date.now()) {
			this.#delayed.push(job);
			this.#delayed.sort((a, b) => (a.runAt ?? 0) - (b.runAt ?? 0));
			return;
		}
		this.#queue(queueOf(job)).push(job);
	}

	async pop(
		queues: readonly string[] = [DEFAULT_QUEUE],
	): Promise<JobRecord | null> {
		this.#promoteDue();
		// In the order given: naming `['critical', 'default']` is how a worker
		// says which queue it would rather drain first.
		for (const name of queues) {
			const next = this.#pending.get(name)?.shift();
			if (next !== undefined) return next;
		}
		return null;
	}

	async fail(job: JobRecord, error: string): Promise<void> {
		job.error = error;
		job.status = "failed";
		this.#failedJobs.push(job);
		if (this.#failedJobs.length > this.#maxFailedJobs) {
			this.#failedJobs.splice(0, this.#failedJobs.length - this.#maxFailedJobs);
		}
	}

	async complete(_job: JobRecord): Promise<void> {
		// Nothing to do for memory driver
	}

	async retry(job: JobRecord): Promise<void> {
		job.status = "pending";
		this.#queue(queueOf(job)).push(job);
	}

	async failed(): Promise<JobRecord[]> {
		return [...this.#failedJobs];
	}

	/**
	 * Everything waiting, across every queue — a delayed job included.
	 *
	 * It is queued; it is simply not due. Leaving it out would report an empty
	 * queue to anything draining one before shutdown.
	 */
	async size(): Promise<number> {
		let total = this.#delayed.length;
		for (const jobs of this.#pending.values()) total += jobs.length;
		return total;
	}
}
