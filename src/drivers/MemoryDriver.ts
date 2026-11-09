/**
 * Memory queue driver — in-process queue for development and tests.
 */

import { DEFAULT_QUEUE } from "../Job.js";
import { type JobRecord, type QueueDriver, queueOf } from "../QueueManager.js";
import { snapshot } from "../record.js";

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

	/**
	 * Stored as a JSON copy, the way Redis stores it. By reference, a dispatcher
	 * that changed its object after dispatch changed the queued job, and a Date
	 * or a BigInt survived here and nowhere else — tests passed on behaviour
	 * production does not have.
	 */
	async push(job: JobRecord): Promise<void> {
		const stored = snapshot(job);
		if (stored.runAt !== undefined && stored.runAt > Date.now()) {
			this.#delayed.push(stored);
			this.#delayed.sort((a, b) => (a.runAt ?? 0) - (b.runAt ?? 0));
			return;
		}
		this.#queue(queueOf(stored)).push(stored);
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
		this.#failedJobs.push(snapshot(job));
		if (this.#failedJobs.length > this.#maxFailedJobs) {
			this.#failedJobs.splice(0, this.#failedJobs.length - this.#maxFailedJobs);
		}
	}

	async complete(_job: JobRecord): Promise<void> {
		// Nothing to do for memory driver
	}

	/** Back in its queue, or with the delayed jobs when it carries a backoff. */
	async retry(job: JobRecord): Promise<void> {
		job.status = "pending";
		await this.push(job);
	}

	async failed(): Promise<JobRecord[]> {
		return this.#failedJobs.map(snapshot);
	}

	/**
	 * The default queue, a delayed job included — the same answer the Redis
	 * driver gives, so a check written against one holds against the other.
	 * Counting every queue here made a drain-before-shutdown loop that passed
	 * in tests wait on the wrong number in production.
	 */
	async size(): Promise<number> {
		return this.sizeOf(DEFAULT_QUEUE);
	}

	/**
	 * One queue, a delayed job included: it is queued, simply not due, and
	 * leaving it out reports an empty queue to anything draining one.
	 */
	async sizeOf(queue: string): Promise<number> {
		const delayed = this.#delayed.filter((job) => queueOf(job) === queue);
		return (this.#pending.get(queue)?.length ?? 0) + delayed.length;
	}
}
