/**
 * Redis queue driver — FIFO job queue with a visibility lease.
 *
 * Every move of a job between two places is ONE server-side script, so it
 * happens entirely or not at all:
 * - pop() promotes due delayed jobs, takes the head of a queue into
 *   `processing` and writes its lease, in one step
 * - complete()/fail()/retry() check the caller still holds the lease, then
 *   remove the entry and put it where it goes next, in one step
 * - recoverStale() re-checks the lease and moves an orphaned entry back to
 *   pending (or to failed), in one step per entry
 *
 * Done as separate commands, a job was removed from one list before being
 * written to the next, and a single dropped connection in between lost it for
 * good. Scripts need nothing newer than Redis 2.6.
 *
 * A lease is a token unique to one pop. Only the worker holding that token may
 * renew or settle the job: a worker whose lease lapsed — its job recovered and
 * taken by somebody else — can no longer touch the new owner's claim.
 */

import { createHash, randomUUID } from "node:crypto";
import { DEFAULT_QUEUE } from "../Job.js";
import { type JobRecord, type QueueDriver, queueOf } from "../QueueManager.js";

/**
 * The commands this driver issues. ioredis and a `@c9up/quasar` connection
 * have every one of them.
 */
export interface RedisClient {
	eval(
		script: string,
		numKeys: number,
		...args: (string | number)[]
	): Promise<unknown>;
	/** Optional: without it every script is sent in full. */
	evalsha?(
		sha1: string,
		numKeys: number,
		...args: (string | number)[]
	): Promise<unknown>;
	rpush(key: string, ...values: string[]): Promise<number>;
	zadd(key: string, score: number, member: string): Promise<number | string>;
	llen(key: string): Promise<number>;
	zcard(key: string): Promise<number>;
	lrange(key: string, start: number, stop: number): Promise<string[]>;
}

/** Options for {@link RedisDriver}, reachable from `drivers.redis()` too. */
export interface RedisDriverOptions {
	/** Prefix of every key. A separator is added when missing. Default `queue:`. */
	prefix?: string;
	/** How long a worker owns a job it took, in ms. Default `30_000`. */
	visibilityTimeoutMs?: number;
	/**
	 * How many times a job may be reclaimed from a stalled worker before it is
	 * filed as failed instead of pushed round again. Default `1`, upstream's
	 * default for the same setting.
	 *
	 * Unbounded recovery is a job that kills its worker taking the whole queue
	 * down with it, forever: the crash never reaches the failure path, so
	 * `attempts` never moves and `maxAttempts` never applies.
	 */
	maxStalledCount?: number;
	/** How many failed jobs to keep. Default `1000`. `0` keeps every one. */
	maxFailedJobs?: number;
}

function isValidJob(obj: unknown): obj is JobRecord {
	if (typeof obj !== "object" || obj === null) return false;
	const maxAttempts: unknown = Reflect.get(obj, "maxAttempts");
	return (
		typeof Reflect.get(obj, "id") === "string" &&
		typeof Reflect.get(obj, "name") === "string" &&
		typeof Reflect.get(obj, "attempts") === "number" &&
		typeof maxAttempts === "number" &&
		Number.isSafeInteger(maxAttempts) &&
		maxAttempts >= 1 &&
		typeof Reflect.get(obj, "status") === "string"
	);
}

/** What can be read off a record that cannot run. */
interface Unreadable {
	reason: string;
	id?: string;
	name?: string;
	queue?: string;
}

type Parsed = { ok: true; job: JobRecord } | ({ ok: false } & Unreadable);

function stringField(obj: object, key: string): string | undefined {
	const value: unknown = Reflect.get(obj, key);
	return typeof value === "string" ? value : undefined;
}

/** Read a stored record, saying what is wrong with one that cannot run. */
function parseJob(raw: string): Parsed {
	let parsed: unknown;
	try {
		parsed = JSON.parse(raw);
	} catch {
		return { ok: false, reason: "not JSON" };
	}
	if (isValidJob(parsed)) return { ok: true, job: parsed };
	if (typeof parsed !== "object" || parsed === null) {
		return { ok: false, reason: "not an object" };
	}
	return {
		ok: false,
		reason: "not a job record (id, name, attempts, maxAttempts >= 1, status)",
		id: stringField(parsed, "id"),
		name: stringField(parsed, "name"),
		queue: stringField(parsed, "queue"),
	};
}

/** A script, with the digest `EVALSHA` addresses it by. */
interface Script {
	lua: string;
	sha: string;
}

function script(lua: string): Script {
	return { lua, sha: createHash("sha1").update(lua).digest("hex") };
}

/**
 * Promote the due delayed jobs of each queue, take the first job found, and
 * lease it — one step, so a recovery pass can never see the job in
 * `processing` without its lease.
 *
 * The id is read straight off the record (`dispatch` writes it first); the
 * decoder is the fallback for a record written in another key order. A record
 * neither can read is taken without a lease, and the caller files it as failed.
 */
const ACQUIRE = script(String.raw`
local processing = KEYS[1]
local now = ARGV[1]
local token = ARGV[2]
local ttl = ARGV[3]
local lease_prefix = ARGV[4]

local function job_id(raw)
  local id = string.match(raw, '^{"id":"([^"\\]+)"')
  if id then return id end
  local ok, decoded = pcall(cjson.decode, raw)
  if ok and type(decoded) == 'table' and type(decoded.id) == 'string' and decoded.id ~= '' then
    return decoded.id
  end
  return nil
end

for i = 2, #KEYS, 2 do
  local pending = KEYS[i]
  local delayed = KEYS[i + 1]
  local due = redis.call('ZRANGEBYSCORE', delayed, '-inf', now, 'LIMIT', 0, 100)
  for _, raw in ipairs(due) do
    redis.call('ZREM', delayed, raw)
    redis.call('RPUSH', pending, raw)
  end
  local raw = redis.call('LPOP', pending)
  if raw then
    redis.call('RPUSH', processing, raw)
    local id = job_id(raw)
    if id then
      redis.call('SET', lease_prefix .. id, token, 'PX', ttl)
      return { raw, id }
    end
    return { raw }
  end
end
return false
`);

/** Extend a lease, only for the worker that holds it. */
const RENEW = script(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
`);

/**
 * Settle a job the caller holds: drop its entry and lease, and put it where it
 * goes next. A caller whose lease is gone changes nothing.
 *
 * KEYS: lease, processing, destination. ARGV: token, stored entry, mode
 * (`complete` | `list` | `zset` | `failed`), new entry, score or failed cap.
 */
const SETTLE = script(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return 0
end
redis.call('LREM', KEYS[2], 1, ARGV[2])
redis.call('DEL', KEYS[1])
local mode = ARGV[3]
if mode == 'list' then
  redis.call('RPUSH', KEYS[3], ARGV[4])
elseif mode == 'zset' then
  redis.call('ZADD', KEYS[3], ARGV[5], ARGV[4])
elseif mode == 'failed' then
  redis.call('RPUSH', KEYS[3], ARGV[4])
  local keep = tonumber(ARGV[5])
  if keep > 0 then
    redis.call('LTRIM', KEYS[3], -keep, -1)
  end
end
return 1
`);

/**
 * Move one orphaned entry out of `processing`, unless it is leased after all.
 * The LREM is the claim: of two passes over the same entry, one moves it.
 *
 * KEYS: processing, destination, lease (empty when the record has no id).
 * ARGV: stored entry, mode (`list` | `failed`), new entry, failed cap.
 */
const RECOVER = script(`
if KEYS[3] ~= '' and redis.call('EXISTS', KEYS[3]) == 1 then
  return 0
end
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then
  return 0
end
redis.call('RPUSH', KEYS[2], ARGV[3])
if ARGV[2] == 'failed' then
  local keep = tonumber(ARGV[4])
  if keep > 0 then
    redis.call('LTRIM', KEYS[2], -keep, -1)
  end
end
return 1
`);

/** What a pop holds: its lease token, and the exact entry it put in processing. */
interface Claim {
	token: string;
	raw: string;
	id: string;
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

function checkClient(client: RedisClient): void {
	if (typeof client.eval === "function") return;
	throw new Error(
		"[bay] RedisDriver needs EVAL (Redis 2.6+): every move of a job is one server-side script, " +
			"so that a dropped connection can never leave it half-moved. ioredis and @c9up/quasar connections have it.",
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

function isNoScript(err: unknown): boolean {
	return err instanceof Error && err.message.startsWith("NOSCRIPT");
}

/** The entry and id the acquire script answered with, or `null` for none. */
function readAcquired(reply: unknown): { raw: string; id?: string } | null {
	if (reply === null || reply === false || reply === undefined) return null;
	if (!Array.isArray(reply) || typeof reply[0] !== "string") {
		throw new Error("[bay] unexpected reply from the acquire script");
	}
	const id: unknown = reply[1];
	return { raw: reply[0], id: typeof id === "string" ? id : undefined };
}

/** A stable id for a record that has none of its own. */
function digestId(raw: string): string {
	return `invalid_${createHash("sha1").update(raw).digest("hex").slice(0, 16)}`;
}

export class RedisDriver implements QueueDriver {
	#source: RedisClientSource;
	#resolved: RedisClient | undefined;
	#pending: Promise<RedisClient> | undefined;
	#prefix: string;
	#visibilityTimeout: number;
	#maxStalledCount: number;
	#maxFailedJobs: number;
	/** This driver instance, as a lease owner. */
	#workerId = randomUUID();
	/**
	 * The lease each record this driver popped is held under. Keyed by the
	 * record object, which is what `QueueManager` hands back to settle it.
	 */
	#claims = new WeakMap<JobRecord, Claim>();

	/**
	 * The client, resolved once. Two workers racing on a cold queue must not
	 * each open their own connection, so the in-flight promise is shared.
	 */
	async #client(): Promise<RedisClient> {
		if (this.#resolved) return this.#resolved;
		if (typeof this.#source !== "function") {
			checkClient(this.#source);
			this.#resolved = this.#source;
			return this.#resolved;
		}
		if (!this.#pending) {
			const resolver = this.#source;
			this.#pending = Promise.resolve(resolver())
				.then((client) => {
					checkClient(client);
					this.#resolved = client;
					return client;
				})
				// Cleared on failure too. Clearing only on success left the
				// REJECTED promise cached forever, so one transient outage at
				// startup broke every later call for the life of the process.
				.finally(() => {
					this.#pending = undefined;
				});
		}
		return this.#pending;
	}

	constructor(source: RedisClientSource, options?: RedisDriverOptions) {
		this.#source = source;
		// A client handed in directly can be checked now. A named connection
		// has no client yet — it is checked when the connection resolves.
		if (typeof source !== "function") checkClient(source);
		// Normalised rather than documented: every key is built by concatenation
		// (`${prefix}pending`), so a prefix without a trailing separator yields
		// "myapppending" — unreadable, and able to collide with a neighbouring
		// prefix.
		this.#prefix = withSeparator(options?.prefix ?? "queue:");
		const visibilityTimeout = options?.visibilityTimeoutMs ?? 30_000;
		// `SET … PX <ms>` refuses anything but a positive integer, and a pop that
		// cannot lease would take a job and drop it. Fail closed at config time.
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
	 * The default queue keeps the key it always had: renaming it would orphan
	 * every job already sitting in `queue:pending` at the moment of an upgrade.
	 */
	#pendingKey(queue: string = DEFAULT_QUEUE): string {
		return queue === DEFAULT_QUEUE
			? `${this.#prefix}pending`
			: `${this.#prefix}q:${queue}:pending`;
	}
	#delayedKey(queue: string = DEFAULT_QUEUE): string {
		return queue === DEFAULT_QUEUE
			? `${this.#prefix}delayed`
			: `${this.#prefix}q:${queue}:delayed`;
	}
	#processingKey(): string {
		return `${this.#prefix}processing`;
	}
	#failedKey(): string {
		return `${this.#prefix}failed`;
	}
	#leaseKey(jobId: string): string {
		return `${this.#prefix}lease:${jobId}`;
	}

	/** Run a script by digest, sending it in full only when the server lacks it. */
	async #run(
		client: RedisClient,
		{ lua, sha }: Script,
		keys: string[],
		args: (string | number)[],
	): Promise<unknown> {
		if (client.evalsha) {
			try {
				return await client.evalsha(sha, keys.length, ...keys, ...args);
			} catch (err) {
				// Only "unknown script" falls through. Every other error may
				// have come after the script ran, and running it twice is not
				// safe.
				if (!isNoScript(err)) throw err;
			}
		}
		return client.eval(lua, keys.length, ...keys, ...args);
	}

	async push(job: JobRecord): Promise<void> {
		const client = await this.#client();
		const raw = JSON.stringify(job);
		const queue = queueOf(job);
		if (job.runAt !== undefined && job.runAt > Date.now()) {
			await client.zadd(this.#delayedKey(queue), job.runAt, raw);
			return;
		}
		await client.rpush(this.#pendingKey(queue), raw);
	}

	async pop(
		queues: readonly string[] = [DEFAULT_QUEUE],
	): Promise<JobRecord | null> {
		if (queues.length === 0) return null;
		const client = await this.#client();
		const keys = [this.#processingKey()];
		for (const queue of queues) {
			keys.push(this.#pendingKey(queue), this.#delayedKey(queue));
		}
		// A record that cannot run is filed and the next one taken, so a run of
		// bad entries does not cost a full idle delay each. Bounded, so a queue
		// of nothing else still returns.
		for (let tries = 0; tries < 100; tries++) {
			const token = `${this.#workerId}:${randomUUID()}`;
			const reply = readAcquired(
				await this.#run(client, ACQUIRE, keys, [
					Date.now(),
					token,
					this.#visibilityTimeout,
					`${this.#prefix}lease:`,
				]),
			);
			if (reply === null) return null;
			const parsed = parseJob(reply.raw);
			if (parsed.ok && reply.id !== undefined && parsed.job.id === reply.id) {
				// A promoted job keeps the `runAt` it was delayed with; it is due.
				parsed.job.runAt = undefined;
				this.#claims.set(parsed.job, {
					token,
					raw: reply.raw,
					id: reply.id,
				});
				return parsed.job;
			}
			const unreadable: Unreadable = parsed.ok
				? { reason: "an id the queue could not read", id: parsed.job.id }
				: parsed;
			await this.#quarantine(
				client,
				reply.raw,
				unreadable,
				reply.id === undefined ? undefined : { token, id: reply.id },
			);
		}
		return null;
	}

	/**
	 * File an entry that can never run as failed, with what is wrong with it.
	 *
	 * Deleting it was silent data loss: a record written by a newer version, or
	 * by hand, vanished with nothing to read about it anywhere.
	 */
	async #quarantine(
		client: RedisClient,
		raw: string,
		unreadable: Unreadable,
		held: { token: string; id: string } | undefined,
	): Promise<void> {
		const record = JSON.stringify(this.#deadLetter(raw, unreadable));
		process.stderr.write(
			`[bay] RedisDriver: filed an unreadable job record as failed (${unreadable.reason})\n`,
		);
		if (held) {
			await this.#run(
				client,
				SETTLE,
				[this.#leaseKey(held.id), this.#processingKey(), this.#failedKey()],
				[held.token, raw, "failed", record, this.#maxFailedJobs],
			);
			return;
		}
		await this.#run(
			client,
			RECOVER,
			[this.#processingKey(), this.#failedKey(), ""],
			[raw, "failed", record, this.#maxFailedJobs],
		);
	}

	#deadLetter(raw: string, unreadable: Unreadable): JobRecord {
		return {
			id: unreadable.id ?? digestId(raw),
			name: unreadable.name ?? "(unreadable)",
			payload: raw,
			attempts: 0,
			maxAttempts: 1,
			status: "failed",
			error: `Unreadable job record (${unreadable.reason}); the stored entry is the payload`,
			createdAt: Date.now(),
			queue: unreadable.queue ?? DEFAULT_QUEUE,
		};
	}

	/**
	 * Say the job is still being worked on, and push its deadline back.
	 *
	 * Answers `false` when this driver no longer holds the lease — it expired,
	 * or the job was recovered and taken by another worker, whose claim this
	 * one must not extend.
	 */
	async renew(job: JobRecord): Promise<boolean> {
		const claim = this.#claims.get(job);
		if (claim === undefined) return false;
		const client = await this.#client();
		const renewed = await this.#run(
			client,
			RENEW,
			[this.#leaseKey(claim.id)],
			[claim.token, this.#visibilityTimeout],
		);
		return renewed === 1;
	}

	async complete(job: JobRecord): Promise<void> {
		await this.#settle(job, "complete", this.#processingKey(), "", 0);
	}

	async fail(job: JobRecord, error: string): Promise<void> {
		job.error = error;
		job.status = "failed";
		await this.#settle(
			job,
			"failed",
			this.#failedKey(),
			JSON.stringify(job),
			this.#maxFailedJobs,
		);
	}

	async retry(job: JobRecord): Promise<void> {
		job.status = "pending";
		const queue = queueOf(job);
		if (job.runAt !== undefined && job.runAt > Date.now()) {
			await this.#settle(
				job,
				"zset",
				this.#delayedKey(queue),
				JSON.stringify(job),
				job.runAt,
			);
			return;
		}
		await this.#settle(
			job,
			"list",
			this.#pendingKey(queue),
			JSON.stringify(job),
			0,
		);
	}

	async #settle(
		job: JobRecord,
		mode: "complete" | "list" | "zset" | "failed",
		destination: string,
		entry: string,
		extra: number,
	): Promise<void> {
		const claim = this.#claims.get(job);
		if (claim === undefined) {
			process.stderr.write(
				`[bay] RedisDriver: job '${job.id}' was not taken by this worker; nothing to settle\n`,
			);
			return;
		}
		const client = await this.#client();
		const settled = await this.#run(
			client,
			SETTLE,
			[this.#leaseKey(claim.id), this.#processingKey(), destination],
			[claim.token, claim.raw, mode, entry, extra],
		);
		this.#claims.delete(job);
		if (settled !== 1) {
			process.stderr.write(
				`[bay] RedisDriver: job '${job.id}' lost its lease before it was settled; the worker that holds it now decides\n`,
			);
		}
	}

	async recoverStale(): Promise<number> {
		const client = await this.#client();
		const processing = await client.lrange(this.#processingKey(), 0, -1);
		let recovered = 0;
		for (const raw of processing) {
			const parsed = parseJob(raw);
			if (!parsed.ok) {
				// An entry the acquiring worker has not filed yet still carries
				// its lease, and the script leaves it alone.
				await this.#run(
					client,
					RECOVER,
					[
						this.#processingKey(),
						this.#failedKey(),
						parsed.id === undefined ? "" : this.#leaseKey(parsed.id),
					],
					[
						raw,
						"failed",
						JSON.stringify(this.#deadLetter(raw, parsed)),
						this.#maxFailedJobs,
					],
				);
				continue;
			}
			const job = parsed.job;
			// A stall is not an attempt: the worker died before the handler
			// could fail, so `attempts` never moved and `maxAttempts` never
			// applied. Counted separately, and bounded — upstream bounds the
			// same thing with the same default.
			const stalled = (job.stalledCount ?? 0) + 1;
			job.stalledCount = stalled;
			job.runAt = undefined;
			const exhausted = stalled > this.#maxStalledCount;
			if (exhausted) {
				job.status = "failed";
				job.error = `Stalled ${stalled} time(s) without completing (maxStalledCount ${this.#maxStalledCount})`;
			} else {
				job.status = "pending";
			}
			const moved = await this.#run(
				client,
				RECOVER,
				[
					this.#processingKey(),
					// Back to the queue it came from: a recovered job whose queue
					// nobody serves would never run again.
					exhausted ? this.#failedKey() : this.#pendingKey(queueOf(job)),
					this.#leaseKey(job.id),
				],
				[
					raw,
					exhausted ? "failed" : "list",
					JSON.stringify(job),
					this.#maxFailedJobs,
				],
			);
			if (moved === 1 && !exhausted) recovered++;
		}
		return recovered;
	}

	async failed(): Promise<JobRecord[]> {
		const client = await this.#client();
		const raws = await client.lrange(this.#failedKey(), 0, -1);
		const jobs: JobRecord[] = [];
		for (const raw of raws) {
			const parsed = parseJob(raw);
			if (parsed.ok) jobs.push(parsed.job);
		}
		return jobs;
	}

	/** Jobs waiting on the default queue, delayed ones included. */
	async size(): Promise<number> {
		return this.sizeOf(DEFAULT_QUEUE);
	}

	/**
	 * How many jobs are waiting on `queue` — its list plus its delayed set.
	 *
	 * A delayed job is queued; it is simply not due. Counting only the list
	 * reported an empty queue to anything draining one before shutdown.
	 */
	async sizeOf(queue: string): Promise<number> {
		const client = await this.#client();
		const waiting = await client.llen(this.#pendingKey(queue));
		const delayed = await client.zcard(this.#delayedKey(queue));
		return waiting + delayed;
	}
}
