/**
 * Redis queue driver — FIFO job queue with visibility timeout.
 *
 * Uses LMOVE (Redis 6.2+) for at-least-once delivery:
 * - pop() moves the job from pending → processing (atomic)
 * - complete() removes from processing
 * - If a worker crashes, the job stays in processing
 * - recoverStale() moves expired processing jobs back to pending
 *
 * Without LMOVE (Redis <6.2) pop() falls back to a non-atomic lpop+rpush, which
 * downgrades delivery to at-most-once — a crash between the two commands drops
 * the in-flight job (recoverStale can't reclaim it, it was never in processing).
 * The constructor warns when the client lacks LMOVE so the downgrade isn't silent.
 *
 * The client must be ioredis-shaped: lowercase methods and positional options
 * (e.g. set(key, val, "PX", ms)). node-redis v4 (camelCase + options objects like
 * { PX: ms }) does NOT satisfy this interface and would drop the lease TTL — it
 * needs a thin adapter.
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

/**
 * Where the client comes from. A resolver is what lets a queue name its
 * connection (`quasarConnection("jobs")`) instead of being handed a client:
 * the driver is built synchronously, the first command that needs the client
 * is not.
 */
export type RedisClientSource =
	| RedisClient
	| (() => RedisClient | Promise<RedisClient>);

/**
 * LMOVE (Redis 6.2+) is what makes pop() atomic. Warned once, when the client
 * is resolved rather than at construction — a driver that names its connection
 * has no client to inspect yet.
 */
const warned = new WeakSet<object>();
function warnWithoutLmove(client: RedisClient): void {
	if (typeof client.lmove === "function" || warned.has(client)) return;
	warned.add(client);
	console.warn(
		"[bay] RedisDriver: client lacks LMOVE (Redis <6.2). pop() falls back to " +
			"a non-atomic lpop+rpush, downgrading delivery from at-least-once to " +
			"at-most-once — a crash between the two commands loses the in-flight job.",
	);
}

export class RedisDriver implements QueueDriver {
	#source: RedisClientSource;
	#resolved: RedisClient | undefined;
	#pending: Promise<RedisClient> | undefined;
	#prefix: string;
	#visibilityTimeout: number;

	/**
	 * The client, resolved once. Two workers racing on a cold queue must not
	 * each open their own connection, so the in-flight promise is shared.
	 */
	async #client(): Promise<RedisClient> {
		if (this.#resolved) return this.#resolved;
		if (typeof this.#source !== "function") {
			this.#resolved = this.#source;
			warnWithoutLmove(this.#resolved);
			return this.#resolved;
		}
		if (!this.#pending) {
			const resolver = this.#source;
			this.#pending = Promise.resolve(resolver()).then((client) => {
				this.#resolved = client;
				this.#pending = undefined;
				warnWithoutLmove(client);
				return client;
			});
		}
		return this.#pending;
	}

	constructor(
		source: RedisClientSource,
		options?: { prefix?: string; visibilityTimeoutMs?: number },
	) {
		this.#source = source;
		// A client handed in directly can be checked now, so the warning keeps
		// landing at construction as it always did. A named connection has no
		// client yet — it is checked when the connection resolves.
		if (typeof source !== "function") warnWithoutLmove(source);
		this.#prefix = options?.prefix ?? "queue:";
		const visibilityTimeout = options?.visibilityTimeoutMs ?? 30_000;
		// A non-positive / non-integer timeout makes pop()'s `SET … PX <ms>` fail
		// on a real Redis; the catch then removes the job from `processing` and
		// returns null — the in-flight job is silently LOST. Fail closed at config
		// time instead.
		if (!Number.isInteger(visibilityTimeout) || visibilityTimeout <= 0) {
			throw new Error(
				`[bay] RedisDriver visibilityTimeoutMs must be a positive integer (ms), got ${visibilityTimeout}`,
			);
		}
		this.#visibilityTimeout = visibilityTimeout;
	}

	#pendingKey = () => `${this.#prefix}pending`;
	#processingKey = () => `${this.#prefix}processing`;
	#failedKey = () => `${this.#prefix}failed`;
	#leaseKey = (jobId: string) => `${this.#prefix}lease:${jobId}`;

	async push(job: Job): Promise<void> {
		const client = await this.#client();
		await client.rpush(this.#pendingKey(), JSON.stringify(job));
	}

	async pop(): Promise<Job | null> {
		const client = await this.#client();
		let raw: string | null = null;

		if (client.lmove) {
			raw = await client.lmove(
				this.#pendingKey(),
				this.#processingKey(),
				"LEFT",
				"RIGHT",
			);
		} else {
			raw = await client.lpop(this.#pendingKey());
			if (raw) await client.rpush(this.#processingKey(), raw);
		}

		if (!raw) return null;
		try {
			const parsed: unknown = JSON.parse(raw);
			if (!isValidJob(parsed)) {
				// Malformed payload — purge from `processing` so it can't sit
				// there indefinitely as a poison pill. recoverStale() also
				// catches survivors but pop()'s own move is the primary path.
				await client.lrem(this.#processingKey(), 1, raw);
				return null;
			}
			await client.set(
				this.#leaseKey(parsed.id),
				raw,
				"PX",
				String(this.#visibilityTimeout),
			);
			return parsed;
		} catch {
			await client.lrem(this.#processingKey(), 1, raw);
			return null;
		}
	}

	async complete(job: Job): Promise<void> {
		const client = await this.#client();
		await this.#removeFromProcessing(job);
		await client.del(this.#leaseKey(job.id));
	}

	async fail(job: Job, error: string): Promise<void> {
		const client = await this.#client();
		await this.#removeFromProcessing(job);
		await client.del(this.#leaseKey(job.id));
		job.error = error;
		job.status = "failed";
		await client.rpush(this.#failedKey(), JSON.stringify(job));
	}

	async retry(job: Job): Promise<void> {
		const client = await this.#client();
		await this.#removeFromProcessing(job);
		await client.del(this.#leaseKey(job.id));
		job.status = "pending";
		await client.rpush(this.#pendingKey(), JSON.stringify(job));
	}

	async recoverStale(): Promise<number> {
		const client = await this.#client();
		const processing = await client.lrange(this.#processingKey(), 0, -1);
		let recovered = 0;
		for (const raw of processing) {
			let parsed: unknown;
			try {
				parsed = JSON.parse(raw);
			} catch {
				// Malformed JSON would otherwise sit in processing forever —
				// LREM purges it so the queue makes progress.
				await client.lrem(this.#processingKey(), 1, raw);
				continue;
			}
			if (!isValidJob(parsed)) {
				await client.lrem(this.#processingKey(), 1, raw);
				continue;
			}
			const lease = await client.get(this.#leaseKey(parsed.id));
			if (lease === null) {
				await client.lrem(this.#processingKey(), 1, raw);
				parsed.status = "pending";
				await client.rpush(this.#pendingKey(), JSON.stringify(parsed));
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
		const client = await this.#client();
		const stored = await client.get(this.#leaseKey(job.id));
		if (stored !== null) {
			const removed = await client.lrem(this.#processingKey(), 1, stored);
			if (removed > 0) return;
		}
		// Lease missing or already-LREM'd entry not found — best-effort scan
		// matches by job id and removes the actual stored representation.
		const items = await client.lrange(this.#processingKey(), 0, -1);
		for (const item of items) {
			let parsed: unknown;
			try {
				parsed = JSON.parse(item);
			} catch {
				continue;
			}
			if (isValidJob(parsed) && (parsed as { id: string }).id === job.id) {
				await client.lrem(this.#processingKey(), 1, item);
				return;
			}
		}
	}

	async failed(): Promise<Job[]> {
		const client = await this.#client();
		const raws = await client.lrange(this.#failedKey(), 0, -1);
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
		const client = await this.#client();
		return client.llen(this.#pendingKey());
	}
}
