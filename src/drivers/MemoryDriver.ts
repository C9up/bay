/**
 * Memory queue driver — in-process queue for development and tests.
 */

import { DEFAULT_PRIORITY, DEFAULT_QUEUE } from "../Job.js";
import { type JobRecord, type QueueDriver, queueOf } from "../QueueManager.js";
import { snapshot } from "../record.js";

function priorityOf(job: JobRecord | undefined): number {
	return job?.priority ?? DEFAULT_PRIORITY;
}

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
	/** Dedup id → the job holding it, and until when (epoch ms) if it expires. */
	#dedup: Map<string, { jobId: string; expiresAt?: number }> = new Map();

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
	async push(job: JobRecord): Promise<string | undefined> {
		const dedup = job.dedup;
		if (dedup !== undefined) {
			const now = Date.now();
			const held = this.#dedup.get(dedup.id);
			if (held && (held.expiresAt === undefined || held.expiresAt > now)) {
				if (dedup.extend && dedup.ttl !== undefined) {
					held.expiresAt = now + dedup.ttl;
				}
				return held.jobId;
			}
			this.#dedup.set(dedup.id, {
				jobId: job.id,
				expiresAt: dedup.ttl === undefined ? undefined : now + dedup.ttl,
			});
		}
		this.#store(job);
		return undefined;
	}

	#store(job: JobRecord): void {
		const stored = snapshot(job);
		if (stored.runAt !== undefined && stored.runAt > Date.now()) {
			this.#delayed.push(stored);
			this.#delayed.sort((a, b) => (a.runAt ?? 0) - (b.runAt ?? 0));
			return;
		}
		this.#queue(queueOf(stored)).push(stored);
	}

	/** Let the next job with this dedup id be queued. */
	#release(job: JobRecord): void {
		const id = job.dedup?.id;
		if (id !== undefined && this.#dedup.get(id)?.jobId === job.id) {
			this.#dedup.delete(id);
		}
	}

	async pop(
		queues: readonly string[] = [DEFAULT_QUEUE],
	): Promise<JobRecord | null> {
		this.#promoteDue();
		// In the order given: naming `['critical', 'default']` is how a worker
		// says which queue it would rather drain first.
		// Within a queue the lowest priority number first, and the oldest of
		// those: the first one found, since jobs are appended in order.
		for (const name of queues) {
			const jobs = this.#pending.get(name);
			if (jobs === undefined || jobs.length === 0) continue;
			let best = 0;
			for (let index = 1; index < jobs.length; index++) {
				if (priorityOf(jobs[index]) < priorityOf(jobs[best])) best = index;
			}
			const [next] = jobs.splice(best, 1);
			if (next !== undefined) return next;
		}
		return null;
	}

	async fail(job: JobRecord, error: string): Promise<void> {
		job.error = error;
		job.status = "failed";
		this.#release(job);
		this.#failedJobs.push(snapshot(job));
		if (this.#failedJobs.length > this.#maxFailedJobs) {
			this.#failedJobs.splice(0, this.#failedJobs.length - this.#maxFailedJobs);
		}
	}

	async complete(job: JobRecord): Promise<void> {
		this.#release(job);
	}

	/**
	 * Back in its queue, or with the delayed jobs when it carries a backoff.
	 * Its dedup id stays held: the job still exists.
	 */
	async retry(job: JobRecord): Promise<void> {
		job.status = "pending";
		this.#store(job);
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
