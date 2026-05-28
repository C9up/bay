/**
 * Redis queue driver — FIFO job queue with visibility timeout.
 *
 * Uses LMOVE (Redis 6.2+) for at-least-once delivery:
 * - pop() moves the job from pending → processing (atomic)
 * - complete() removes from processing
 * - If a worker crashes, the job stays in processing
 * - recoverStale() moves expired processing jobs back to pending
 *
 * Compatible with ioredis and node-redis clients.
 */

import type { Job, QueueDriver } from "../QueueManager.js";

export interface RedisClient {
	rpush(key: string, ...values: string[]): Promise<number>;
	lpop(key: string): Promise<string | null>;
	lmove?(
		source: string,
		destination: string,
		from: "LEFT" | "RIGHT",
		to: "LEFT" | "RIGHT",
	): Promise<string | null>;
	lrem(key: string, count: number, element: string): Promise<number>;
	llen(key: string): Promise<number>;
	lrange(key: string, start: number, stop: number): Promise<string[]>;
	del(key: string): Promise<number>;
	set(key: string, value: string, ...args: string[]): Promise<string | null>;
	get(key: string): Promise<string | null>;
}

function isValidJob(obj: unknown): obj is Job {
	if (typeof obj !== "object" || obj === null) return false;
	const j = obj as Record<string, unknown>;
	return (
		typeof j.id === "string" &&
		typeof j.name === "string" &&
		typeof j.attempts === "number" &&
		typeof j.maxAttempts === "number" &&
		typeof j.status === "string"
	);
}

export class RedisDriver implements QueueDriver {
	#client: RedisClient;
	#prefix: string;
	#visibilityTimeout: number;

	constructor(
		client: RedisClient,
		options?: { prefix?: string; visibilityTimeoutMs?: number },
	) {
		this.#client = client;
		this.#prefix = options?.prefix ?? "queue:";
		this.#visibilityTimeout = options?.visibilityTimeoutMs ?? 30_000;
	}

	#pendingKey = () => `${this.#prefix}pending`;
	#processingKey = () => `${this.#prefix}processing`;
	#failedKey = () => `${this.#prefix}failed`;
	#leaseKey = (jobId: string) => `${this.#prefix}lease:${jobId}`;

	async push(job: Job): Promise<void> {
		await this.#client.rpush(this.#pendingKey(), JSON.stringify(job));
	}

	async pop(): Promise<Job | null> {
		let raw: string | null = null;

		if (this.#client.lmove) {
			raw = await this.#client.lmove(
				this.#pendingKey(),
				this.#processingKey(),
				"LEFT",
				"RIGHT",
			);
		} else {
			raw = await this.#client.lpop(this.#pendingKey());
			if (raw) await this.#client.rpush(this.#processingKey(), raw);
		}

		if (!raw) return null;
		try {
			const parsed: unknown = JSON.parse(raw);
			if (!isValidJob(parsed)) {
				// Malformed payload — purge from `processing` so it can't sit
				// there indefinitely as a poison pill. recoverStale() also
				// catches survivors but pop()'s own move is the primary path.
				await this.#client.lrem(this.#processingKey(), 1, raw);
				return null;
			}
			await this.#client.set(
				this.#leaseKey(parsed.id),
				raw,
				"PX",
				String(this.#visibilityTimeout),
			);
			return parsed;
		} catch {
			await this.#client.lrem(this.#processingKey(), 1, raw);
			return null;
		}
	}

	async complete(job: Job): Promise<void> {
		await this.#removeFromProcessing(job);
		await this.#client.del(this.#leaseKey(job.id));
	}

	async fail(job: Job, error: string): Promise<void> {
		await this.#removeFromProcessing(job);
		await this.#client.del(this.#leaseKey(job.id));
		job.error = error;
		job.status = "failed";
		await this.#client.rpush(this.#failedKey(), JSON.stringify(job));
	}

	async retry(job: Job): Promise<void> {
		await this.#removeFromProcessing(job);
		await this.#client.del(this.#leaseKey(job.id));
		job.status = "pending";
		await this.#client.rpush(this.#pendingKey(), JSON.stringify(job));
	}

	async recoverStale(): Promise<number> {
		const processing = await this.#client.lrange(this.#processingKey(), 0, -1);
		let recovered = 0;
		for (const raw of processing) {
			let parsed: unknown;
			try {
				parsed = JSON.parse(raw);
			} catch {
				// Malformed JSON would otherwise sit in processing forever —
				// LREM purges it so the queue makes progress.
				await this.#client.lrem(this.#processingKey(), 1, raw);
				continue;
			}
			if (!isValidJob(parsed)) {
				await this.#client.lrem(this.#processingKey(), 1, raw);
				continue;
			}
			const lease = await this.#client.get(this.#leaseKey(parsed.id));
			if (lease === null) {
				await this.#client.lrem(this.#processingKey(), 1, raw);
				parsed.status = "pending";
				await this.#client.rpush(this.#pendingKey(), JSON.stringify(parsed));
				recovered++;
			}
		}
		return recovered;
	}

	/**
	 * Remove the entry for `job` from the processing list. The string in
	 * Redis is whatever pop() pushed, but QueueManager mutates `job` after
	 * pop returns (attempts++, status="processing", processedAt, then
	 * completed/failed/pending). LREM-ing on `JSON.stringify(job)` would
	 * therefore miss every real-world entry. Use the lease — set to the
	 * exact raw string at pop() time — and fall back to a list scan when
	 * the lease has expired (e.g. recoverStale already handled it).
	 */
	async #removeFromProcessing(job: Job): Promise<void> {
		const stored = await this.#client.get(this.#leaseKey(job.id));
		if (stored !== null) {
			const removed = await this.#client.lrem(this.#processingKey(), 1, stored);
			if (removed > 0) return;
		}
		// Lease missing or already-LREM'd entry not found — best-effort scan
		// matches by job id and removes the actual stored representation.
		const items = await this.#client.lrange(this.#processingKey(), 0, -1);
		for (const item of items) {
			let parsed: unknown;
			try {
				parsed = JSON.parse(item);
			} catch {
				continue;
			}
			if (isValidJob(parsed) && (parsed as { id: string }).id === job.id) {
				await this.#client.lrem(this.#processingKey(), 1, item);
				return;
			}
		}
	}

	async failed(): Promise<Job[]> {
		const raws = await this.#client.lrange(this.#failedKey(), 0, -1);
		return raws
			.map((r) => {
				try {
					const parsed: unknown = JSON.parse(r);
					return isValidJob(parsed) ? parsed : null;
				} catch {
					return null;
				}
			})
			.filter((j): j is Job => j !== null);
	}

	async size(): Promise<number> {
		return this.#client.llen(this.#pendingKey());
	}
}
