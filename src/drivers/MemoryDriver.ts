/**
 * Memory queue driver — in-process queue for development.
 */

import type { Job, QueueDriver } from "../QueueManager.js";

export class MemoryDriver implements QueueDriver {
	#pending: Job[] = [];
	#failedJobs: Job[] = [];
	#maxFailedJobs: number;

	constructor(options?: { maxFailedJobs?: number }) {
		this.#maxFailedJobs = options?.maxFailedJobs ?? 1000;
	}

	async push(job: Job): Promise<void> {
		this.#pending.push(job);
	}

	async pop(): Promise<Job | null> {
		return this.#pending.shift() ?? null;
	}

	async fail(job: Job, error: string): Promise<void> {
		job.error = error;
		job.status = "failed";
		this.#failedJobs.push(job);
		if (this.#failedJobs.length > this.#maxFailedJobs) {
			this.#failedJobs.splice(0, this.#failedJobs.length - this.#maxFailedJobs);
		}
	}

	async complete(_job: Job): Promise<void> {
		// Nothing to do for memory driver
	}

	async retry(job: Job): Promise<void> {
		job.status = "pending";
		this.#pending.push(job);
	}

	async failed(): Promise<Job[]> {
		return [...this.#failedJobs];
	}

	async size(): Promise<number> {
		return this.#pending.length;
	}
}
