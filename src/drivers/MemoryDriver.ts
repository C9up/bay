/**
 * Memory queue driver — in-process queue for development and tests.
 */

import type { ResolvedRetention } from "../Job.js";
import { DEFAULT_PRIORITY, DEFAULT_QUEUE } from "../Job.js";
import {
	type JobLookup,
	type JobRecord,
	type QueueDriver,
	queueOf,
} from "../QueueManager.js";
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
	/**
	 * Every job `getJob` can answer: those waiting or running, and the finished
	 * ones their retention keeps.
	 */
	#lookup: Map<string, JobLookup> = new Map();
	/**
	 * Finished job ids kept by a retention, oldest first, per status and queue
	 * — the set a `count` or an `age` prunes, as upstream prunes a queue's
	 * history whatever the job class.
	 */
	#history: Map<string, string[]> = new Map();

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
		this.#lookup.set(stored.id, { status: "pending", data: stored });
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
			if (next !== undefined) {
				this.#lookup.set(next.id, { status: "active", data: next });
				return next;
			}
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
		this.#finish(job, "failed", job.removeOnFail, error);
	}

	async complete(job: JobRecord): Promise<void> {
		this.#release(job);
		this.#finish(job, "completed", job.removeOnComplete);
	}

	/**
	 * Forget a finished job, or keep it for `getJob` and prune its queue's
	 * history by the retention's age and count — upstream's `#storeHistory`.
	 */
	#finish(
		job: JobRecord,
		status: "completed" | "failed",
		retention: ResolvedRetention | undefined,
		error?: string,
	): void {
		if (retention === undefined) {
			this.#lookup.delete(job.id);
			return;
		}
		const now = Date.now();
		this.#lookup.set(job.id, {
			status,
			data: snapshot(job),
			finishedAt: now,
			...(error === undefined ? {} : { error }),
		});
		const key = `${status}:${queueOf(job)}`;
		const ids = this.#history.get(key) ?? [];
		ids.push(job.id);
		this.#history.set(key, ids);
		const prune = (id: string | undefined): void => {
			if (id !== undefined) this.#lookup.delete(id);
		};
		if (retention.maxAge > 0) {
			const cutoff = now - retention.maxAge;
			while (ids.length > 0) {
				const oldest = this.#lookup.get(ids[0] ?? "");
				if (oldest !== undefined && (oldest.finishedAt ?? 0) >= cutoff) break;
				prune(ids.shift());
			}
		}
		if (retention.maxCount > 0) {
			while (ids.length > retention.maxCount) prune(ids.shift());
		}
	}

	async getJob(jobId: string): Promise<JobLookup | null> {
		const found = this.#lookup.get(jobId);
		if (found === undefined) return null;
		const data = snapshot(found.data);
		// Promoted lazily on a pop: a waiting job whose delay is not over is
		// delayed, whichever list it sits in.
		const status =
			found.status === "pending" &&
			data.runAt !== undefined &&
			data.runAt > Date.now()
				? "delayed"
				: found.status;
		return { ...found, status, data };
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
