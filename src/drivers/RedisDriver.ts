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

import { DEFAULT_QUEUE } from "../Job.js";
import { inProduction } from "../nodeEnv.js";
import { type JobRecord, type QueueDriver, queueOf } from "../QueueManager.js";

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
	/**
	 * Optional, like `lmove`. Present on ioredis; without it the failed list
	 * simply keeps its entries, which is what this driver did before.
	 */
	ltrim?(key: string, start: number, stop: number): Promise<string>;
	llen(key: string): Promise<number>;
	lrange(key: string, start: number, stop: number): Promise<string[]>;
	del(key: string): Promise<number>;
	set(key: string, value: string, ...args: string[]): Promise<string | null>;
	get(key: string): Promise<string | null>;
	/**
	 * Sorted-set commands, for delayed jobs. Optional like `lmove`: a client
	 * without them can still run a queue, and `push` refuses a job carrying a
	 * `delay` rather than running it early — which is the one thing a delay
	 * must not do.
	 */
	zadd?(key: string, score: number, member: string): Promise<number | string>;
	zrangebyscore?(
		key: string,
		min: number | string,
		max: number | string,
		...args: string[]
	): Promise<string[]>;
	zrem?(key: string, ...members: string[]): Promise<number>;
	zcard?(key: string): Promise<number>;
}

function isValidJob(obj: unknown): obj is JobRecord {
	if (typeof obj !== "object" || obj === null) return false;
	return (
		typeof Reflect.get(obj, "id") === "string" &&
		typeof Reflect.get(obj, "name") === "string" &&
		typeof Reflect.get(obj, "attempts") === "number" &&
		typeof Reflect.get(obj, "maxAttempts") === "number" &&
		typeof Reflect.get(obj, "status") === "string"
	);
}

/**
 * What a lease holds: the exact string pop() moved into `processing`, and the
 * worker that moved it.
 *
 * The owner is what makes renewal safe. Without it a worker whose lease had
 * already expired — its job recovered, re-popped by somebody else — would go on
 * extending the deadline of a job it no longer had any claim on. Upstream draws
 * the same line inside its renewal script: "Only the worker that currently owns
 * the lease may renew it."
 */
interface Lease {
	owner: string;
	raw: string;
}

/**
 * Read a lease back. A value written by an older version of this driver is the
 * raw job string on its own, with no owner — still usable for the one thing
 * `#removeFromProcessing` needs it for.
 */
function readLease(stored: string): { owner?: string; raw: string } {
	let parsed: unknown;
	try {
		parsed = JSON.parse(stored);
	} catch {
		return { raw: stored };
	}
	if (typeof parsed !== "object" || parsed === null) return { raw: stored };
	const raw = Reflect.get(parsed, "raw");
	const owner = Reflect.get(parsed, "owner");
	if (typeof raw !== "string" || typeof owner !== "string") {
		return { raw: stored };
	}
	return { owner, raw };
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
function checkLmove(client: RedisClient, allowNonAtomicPop: boolean): void {
	if (typeof client.lmove === "function") return;

	// A queue's whole promise is that a job it accepted gets run. Without LMOVE
	// the pop is `lpop` then `rpush`, and a crash between the two deletes the
	// job from pending before it reaches processing: nothing recovers it,
	// because nothing knows it existed. That is a different product, and in
	// production it must be asked for rather than fallen into.
	if (inProduction() && !allowNonAtomicPop) {
		throw new Error(
			"[bay] this Redis client has no LMOVE (Redis < 6.2), so pop() would be a non-atomic lpop+rpush — " +
				"a crash between the two loses the in-flight job, turning at-least-once delivery into at-most-once.\n" +
				"  Upgrade to Redis 6.2 or later, or pass `allowNonAtomicPop: true` to state that losing a job is acceptable here.",
		);
	}
	if (warned.has(client)) return;
	warned.add(client);

	// Said even when the deployment opted in: agreeing to lose a job once, in a
	// config file, is not the same as being reminded that this process is
	// running that way. The line has to be in the logs of the incident.
	const optedIn = inProduction() && allowNonAtomicPop;
	console.warn(
		"[bay] RedisDriver: client lacks LMOVE (Redis <6.2). pop() falls back to " +
			"a non-atomic lpop+rpush, downgrading delivery from at-least-once to " +
			"at-most-once — a crash between the two commands loses the in-flight job." +
			(optedIn
				? "\n  Running this way in PRODUCTION because allowNonAtomicPop was set."
				: ""),
	);
}

/**
 * A key prefix that ends in a separator.
 *
 * `:` is Redis's conventional namespace separator; a prefix already ending in
 * one of the usual separators is left alone.
 */
function withSeparator(prefix: string): string {
	if (prefix.length === 0) return prefix;
	return /[:.\-_/]$/.test(prefix) ? prefix : `${prefix}:`;
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
			checkLmove(this.#resolved, this.#allowNonAtomicPop);
			return this.#resolved;
		}
		if (!this.#pending) {
			const resolver = this.#source;
			this.#pending = Promise.resolve(resolver())
				.then((client) => {
					this.#resolved = client;
					checkLmove(client, this.#allowNonAtomicPop);
					return client;
				})
				// Cleared on failure too. Clearing only on success left the
				// REJECTED promise cached forever, so one transient outage at
				// startup broke every later call for the life of the process —
				// a permanent failure with no error of its own to explain it.
				.finally(() => {
					this.#pending = undefined;
				});
		}
		return this.#pending;
	}

	constructor(
		source: RedisClientSource,
		options?: {
			prefix?: string;
			visibilityTimeoutMs?: number;
			/**
			 * Accept the non-atomic pop on a Redis older than 6.2, in
			 * production. Off by default: losing an accepted job is a choice a
			 * deployment makes, not one a version check makes for it.
			 */
			allowNonAtomicPop?: boolean;
			/**
			 * How many times a job may be reclaimed from a stalled worker before
			 * it is filed as failed instead of pushed round again. Default `1`,
			 * upstream's default for the same setting.
			 *
			 * Unbounded recovery is a job that kills its worker taking the whole
			 * queue down with it, forever: the crash never reaches the failure
			 * path, so `attempts` never moves and `maxAttempts` never applies.
			 */
			maxStalledCount?: number;
			/**
			 * How many failed jobs to keep. Default `1000` — the ceiling the
			 * memory driver already had. `0` keeps every one of them.
			 *
			 * Only enforced when the client answers `ltrim`.
			 */
			maxFailedJobs?: number;
		},
	) {
		this.#source = source;
		// A client handed in directly can be checked now, so the warning keeps
		// landing at construction as it always did. A named connection has no
		// client yet — it is checked when the connection resolves.
		if (typeof source !== "function") {
			checkLmove(source, options?.allowNonAtomicPop ?? false);
		}
		// Normalised rather than documented: every key is built by concatenation
		// (`${prefix}pending`), so a prefix without a trailing separator yields
		// "myapppending" — unreadable, and able to collide with a neighbouring
		// prefix. Nothing warned, because nothing failed.
		this.#prefix = withSeparator(options?.prefix ?? "queue:");
		this.#allowNonAtomicPop = options?.allowNonAtomicPop ?? false;
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

		const maxStalled = options?.maxStalledCount ?? 1;
		if (!Number.isInteger(maxStalled) || maxStalled < 0) {
			throw new Error(
				`[bay] RedisDriver maxStalledCount must be a non-negative integer, got ${maxStalled}`,
			);
		}
		this.#maxStalledCount = maxStalled;

		const maxFailed = options?.maxFailedJobs ?? 1000;
		if (!Number.isInteger(maxFailed) || maxFailed < 0) {
			throw new Error(
				`[bay] RedisDriver maxFailedJobs must be a non-negative integer, got ${maxFailed}`,
			);
		}
		this.#maxFailedJobs = maxFailed;
	}

	/**
	 * Renew a lease at half its length: two chances to be heard before the
	 * deadline, so one slow round-trip does not hand a running job to somebody
	 * else. Read by `QueueManager` while a handler runs.
	 */
	get renewIntervalMs(): number {
		return Math.max(1, Math.floor(this.#visibilityTimeout / 2));
	}

	/**
	 * Where one queue's jobs wait.
	 *
	 * The default queue keeps the key it always had. Naming it
	 * `queue:default:pending` would have been tidier and would have orphaned
	 * every job already sitting in `queue:pending` at the moment of the upgrade
	 * — a silent loss, since nothing reads the old key afterwards.
	 */
	#pendingKey = (queue: string = DEFAULT_QUEUE) =>
		queue === DEFAULT_QUEUE
			? `${this.#prefix}pending`
			: `${this.#prefix}q:${queue}:pending`;
	#delayedKey = (queue: string = DEFAULT_QUEUE) =>
		queue === DEFAULT_QUEUE
			? `${this.#prefix}delayed`
			: `${this.#prefix}q:${queue}:delayed`;
	#processingKey = () => `${this.#prefix}processing`;
	#allowNonAtomicPop = false;
	#maxStalledCount = 1;
	#maxFailedJobs = 1000;
	/** This driver instance, as a lease owner. */
	#workerId = crypto.randomUUID();
	#failedKey = () => `${this.#prefix}failed`;
	#leaseKey = (jobId: string) => `${this.#prefix}lease:${jobId}`;

	async push(job: JobRecord): Promise<void> {
		const client = await this.#client();
		const queue = queueOf(job);
		if (job.runAt !== undefined && job.runAt > Date.now()) {
			if (!client.zadd) {
				// Pushing it to the list instead would run it now, which is the
				// one thing a delay exists to prevent.
				throw new Error(
					"This Redis client cannot hold a delayed job: it has no ZADD. " +
						"Use a client with sorted-set commands (ioredis has them), or dispatch without `delay`.",
				);
			}
			await client.zadd(
				this.#delayedKey(queue),
				job.runAt,
				JSON.stringify(job),
			);
			return;
		}
		await client.rpush(this.#pendingKey(queue), JSON.stringify(job));
	}

	async pop(
		queues: readonly string[] = [DEFAULT_QUEUE],
	): Promise<JobRecord | null> {
		const client = await this.#client();
		let raw: string | null = null;

		// In the order given, so a worker can say which queue it drains first.
		for (const queue of queues) {
			await this.#promoteDue(client, queue);
			raw = await this.#take(client, queue);
			if (raw !== null) break;
		}

		if (!raw) return null;

		// Only a payload that can never be run is purged. Everything past this
		// point is a REAL job that already sits in `processing`, and deleting
		// it there is the one thing that loses it for good: it is gone from
		// pending too, and recoverStale() scans processing, so nothing would
		// ever find it again.
		let parsed: unknown;
		try {
			parsed = JSON.parse(raw);
		} catch {
			// A poison pill: unparseable, and it would sit in processing
			// forever blocking nothing but wasting every recovery pass.
			await client.lrem(this.#processingKey(), 1, raw);
			return null;
		}
		if (!isValidJob(parsed)) {
			await client.lrem(this.#processingKey(), 1, raw);
			return null;
		}

		// A lease that cannot be written is a transient Redis failure, not a
		// bad job. The error propagates and the job STAYS in processing with
		// no lease, which is precisely the state recoverStale() puts back in
		// pending — so the delivery guarantee survives the blip.
		await client.set(
			this.#leaseKey(parsed.id),
			JSON.stringify({ owner: this.#workerId, raw } satisfies Lease),
			"PX",
			String(this.#visibilityTimeout),
		);
		return parsed;
	}

	/**
	 * Say the job is still being worked on, and push its deadline back.
	 *
	 * Answers `false` when there is nothing left to renew — the lease expired
	 * and the job was recovered, or it was recovered and re-popped by another
	 * worker, whose claim this one must not extend.
	 */
	async renew(job: JobRecord): Promise<boolean> {
		const client = await this.#client();
		const key = this.#leaseKey(job.id);
		const stored = await client.get(key);
		if (stored === null) return false;
		const lease = readLease(stored);
		if (lease.owner !== undefined && lease.owner !== this.#workerId) {
			return false;
		}
		await client.set(key, stored, "PX", String(this.#visibilityTimeout));
		return true;
	}

	async complete(job: JobRecord): Promise<void> {
		const client = await this.#client();
		await this.#removeFromProcessing(job);
		await client.del(this.#leaseKey(job.id));
	}

	async fail(job: JobRecord, error: string): Promise<void> {
		const client = await this.#client();
		await this.#removeFromProcessing(job);
		await client.del(this.#leaseKey(job.id));
		job.error = error;
		job.status = "failed";
		await this.#pushFailed(client, job);
	}

	async retry(job: JobRecord): Promise<void> {
		const client = await this.#client();
		await this.#removeFromProcessing(job);
		await client.del(this.#leaseKey(job.id));
		job.status = "pending";
		await client.rpush(this.#pendingKey(queueOf(job)), JSON.stringify(job));
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
			if (lease !== null) continue;

			// The LREM is the claim, and its RESULT decides who acts. Two
			// recovery passes overlapping — two workers, or one worker whose
			// pass ran long — both read the same expired entry, and both used to
			// push it back to pending: one job, delivered twice, from the
			// mechanism that exists to make delivery reliable. Exactly one LREM
			// can remove a given element, so exactly one pass continues past
			// here. (A crash between this and the RPUSH below still loses the
			// entry; closing that needs the whole pass in one server-side script,
			// which is how upstream does it.)
			const claimed = await client.lrem(this.#processingKey(), 1, raw);
			if (claimed === 0) continue;

			// A stall is not an attempt: the worker died before the handler
			// could fail, so `attempts` never moved and `maxAttempts` never
			// applied. A job that kills whatever picks it up was therefore
			// recovered forever, taking the queue with it. Counted separately,
			// and bounded — upstream bounds the same thing with the same
			// default, failing the job once it is exceeded.
			const stalled = (parsed.stalledCount ?? 0) + 1;
			if (stalled > this.#maxStalledCount) {
				parsed.stalledCount = stalled;
				parsed.status = "failed";
				parsed.error = `Stalled ${stalled} time(s) without completing (maxStalledCount ${this.#maxStalledCount})`;
				await this.#pushFailed(client, parsed);
				continue;
			}

			parsed.stalledCount = stalled;
			parsed.status = "pending";
			// Back to the queue it came from, not to the default one: a recovered
			// job whose queue nobody serves would never run again.
			await client.rpush(
				this.#pendingKey(queueOf(parsed)),
				JSON.stringify(parsed),
			);
			recovered++;
		}
		return recovered;
	}

	/**
	 * File a job as failed, keeping the list to `maxFailedJobs`.
	 *
	 * Unbounded, the failed list is a leak with no ceiling and no owner: nothing
	 * trims it, and `failed()` reads all of it in one LRANGE. The memory driver
	 * has capped its own at a thousand from the start; this is the same cap on
	 * the driver where the list actually survives a restart.
	 */
	async #pushFailed(client: RedisClient, job: JobRecord): Promise<void> {
		await client.rpush(this.#failedKey(), JSON.stringify(job));
		if (this.#maxFailedJobs === 0 || !client.ltrim) return;
		await client.ltrim(this.#failedKey(), -this.#maxFailedJobs, -1);
	}

	/**
	 * Remove the entry for `job` from the processing list. The string in
	 * Redis is whatever pop() pushed, but QueueManager mutates `job` after
	 * pop returns (attempts++, status="processing", processedAt, then
	 * completed/failed/pending). LREM-ing on `JSON.stringify(job)` would
	 * therefore miss every real-world entry. Use the lease — which carries
	 * the exact raw string pop() moved — and fall back to a list scan when
	 * the lease has expired (e.g. recoverStale already handled it).
	 */
	async #removeFromProcessing(job: JobRecord): Promise<void> {
		const client = await this.#client();
		const stored = await client.get(this.#leaseKey(job.id));
		if (stored !== null) {
			const { raw } = readLease(stored);
			const removed = await client.lrem(this.#processingKey(), 1, raw);
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
			if (isValidJob(parsed) && parsed.id === job.id) {
				await client.lrem(this.#processingKey(), 1, item);
				return;
			}
		}
	}

	async failed(): Promise<JobRecord[]> {
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
			.filter((j): j is JobRecord => j !== null);
	}

	/**
	 * How many jobs are waiting on `queue` — its list plus its delayed set.
	 *
	 * A delayed job is queued; it is simply not due. Counting only the list
	 * reported an empty queue to anything draining one before shutdown.
	 */
	async size(queue: string = DEFAULT_QUEUE): Promise<number> {
		const client = await this.#client();
		const waiting = await client.llen(this.#pendingKey(queue));
		const delayed = client.zcard
			? await client.zcard(this.#delayedKey(queue))
			: 0;
		return waiting + delayed;
	}

	/**
	 * Move `queue`'s due jobs out of the delayed set and onto its list.
	 *
	 * `ZREM` is the claim: two workers can read the same due entry, and only
	 * the one whose removal returns 1 owns it. Without that the job is pushed
	 * onto the list once per worker that saw it.
	 */
	async #promoteDue(client: RedisClient, queue: string): Promise<void> {
		if (!client.zrangebyscore || !client.zrem) return;
		const due = await client.zrangebyscore(
			this.#delayedKey(queue),
			0,
			Date.now(),
			"LIMIT",
			"0",
			"100",
		);
		for (const raw of due) {
			const claimed = await client.zrem(this.#delayedKey(queue), raw);
			if (claimed !== 1) continue;
			let parsed: unknown;
			try {
				parsed = JSON.parse(raw);
			} catch {
				// Already removed from the set; there is nothing runnable to push.
				continue;
			}
			if (!isValidJob(parsed)) continue;
			parsed.runAt = undefined;
			await client.rpush(this.#pendingKey(queue), JSON.stringify(parsed));
		}
	}

	/** Take the head of one queue, claiming it in `processing`. */
	async #take(client: RedisClient, queue: string): Promise<string | null> {
		if (client.lmove) {
			return client.lmove(
				this.#pendingKey(queue),
				this.#processingKey(),
				"LEFT",
				"RIGHT",
			);
		}
		const raw = await client.lpop(this.#pendingKey(queue));
		if (raw) await client.rpush(this.#processingKey(), raw);
		return raw;
	}
}
