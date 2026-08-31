import { afterEach, describe, expect, it, vi } from "vitest";
import {
	type RedisClient,
	RedisDriver,
} from "../../src/drivers/RedisDriver.js";
import type { Job } from "../../src/QueueManager.js";

/** Narrow away null/undefined without a `!` non-null assertion (which lies to the compiler). */
function defined<T>(value: T | null | undefined): T {
	if (value == null) throw new Error("expected a defined value");
	return value;
}

function createFakeRedis(opts?: { withLmove?: boolean }): {
	client: RedisClient;
	lists: Map<string, string[]>;
	keys: Map<string, string>;
} {
	const lists = new Map<string, string[]>();
	const keys = new Map<string, string>();

	const ensureList = (k: string): string[] => {
		const v = lists.get(k);
		if (v) return v;
		const fresh: string[] = [];
		lists.set(k, fresh);
		return fresh;
	};

	const client: RedisClient = {
		async rpush(key, ...values) {
			ensureList(key).push(...values);
			return ensureList(key).length;
		},
		async lpop(key) {
			const list = lists.get(key);
			if (!list || list.length === 0) return null;
			return list.shift() ?? null;
		},
		async lrem(key, _count, element) {
			const list = lists.get(key);
			if (!list) return 0;
			const idx = list.indexOf(element);
			if (idx < 0) return 0;
			list.splice(idx, 1);
			return 1;
		},
		async llen(key) {
			return lists.get(key)?.length ?? 0;
		},
		async lrange(key, start, stop) {
			const list = lists.get(key);
			if (!list) return [];
			const end = stop === -1 ? list.length : stop + 1;
			return list.slice(start, end);
		},
		async del(key) {
			const had = (lists.delete(key) ? 1 : 0) + (keys.delete(key) ? 1 : 0);
			return had;
		},
		async set(key, value) {
			keys.set(key, value);
			return "OK";
		},
		async get(key) {
			return keys.get(key) ?? null;
		},
	};

	if (opts?.withLmove !== false) {
		client.lmove = async (source, destination, _from, _to) => {
			const list = lists.get(source);
			if (!list || list.length === 0) return null;
			const v = list.shift();
			if (v === undefined) return null;
			ensureList(destination).push(v);
			return v;
		};
	}

	return { client, lists, keys };
}

function makeJob(over: Partial<Job> = {}): Job {
	return {
		id: "job_x",
		name: "send-email",
		payload: { to: "user@example.com" },
		attempts: 0,
		maxAttempts: 3,
		status: "pending",
		createdAt: 1700000000000,
		...over,
	};
}

describe("bay > RedisDriver > config validation", () => {
	it("rejects a non-positive / non-integer visibilityTimeoutMs (else pop() loses jobs)", () => {
		const fake = createFakeRedis();
		for (const bad of [0, -1, 1.5, Number.NaN]) {
			expect(
				() => new RedisDriver(fake.client, { visibilityTimeoutMs: bad }),
			).toThrow(/visibilityTimeoutMs must be a positive integer/);
		}
		// A valid positive integer — and the default — construct fine.
		expect(
			() => new RedisDriver(fake.client, { visibilityTimeoutMs: 5000 }),
		).not.toThrow();
		expect(() => new RedisDriver(fake.client)).not.toThrow();
	});
});

describe("bay > RedisDriver > push/pop", () => {
	it("push enqueues a serialized job onto the pending list", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client);
		await driver.push(makeJob());
		const pending = fake.lists.get("queue:pending") ?? [];
		expect(pending).toHaveLength(1);
		expect(JSON.parse(defined(pending[0])).id).toBe("job_x");
	});

	it("pop with LMOVE moves the job from pending → processing atomically", async () => {
		const fake = createFakeRedis({ withLmove: true });
		const driver = new RedisDriver(fake.client);
		await driver.push(makeJob({ id: "j1" }));
		const popped = await driver.pop();
		expect(popped?.id).toBe("j1");
		expect(fake.lists.get("queue:pending")).toEqual([]);
		expect(fake.lists.get("queue:processing")?.length).toBe(1);
	});

	it("pop falls back to LPOP+RPUSH when LMOVE is unavailable", async () => {
		const fake = createFakeRedis({ withLmove: false });
		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
		const driver = new RedisDriver(fake.client);
		// The at-most-once downgrade must be surfaced, not silent (audit 2026-06-13).
		expect(warn).toHaveBeenCalledWith(expect.stringContaining("at-most-once"));
		await driver.push(makeJob({ id: "j1" }));
		const popped = await driver.pop();
		expect(popped?.id).toBe("j1");
		expect(fake.lists.get("queue:processing")?.length).toBe(1);
		warn.mockRestore();
	});

	it("pop sets a lease key with PX visibilityTimeout", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client, { visibilityTimeoutMs: 5000 });
		await driver.push(makeJob({ id: "j1" }));
		const setSpy = vi.spyOn(fake.client, "set");
		await driver.pop();
		const args = setSpy.mock.calls[0];
		expect(args).toBeDefined();
		expect(args?.[0]).toBe("queue:lease:j1");
		expect(args?.slice(2)).toEqual(["PX", "5000"]);
	});

	it("pop returns null when the pending list is empty", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client);
		expect(await driver.pop()).toBeNull();
	});

	it("pop returns null when the popped value is malformed JSON", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client);
		await fake.client.rpush("queue:pending", "not-valid-json");
		expect(await driver.pop()).toBeNull();
	});

	it("pop returns null when the popped value is JSON but not a Job shape", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client);
		await fake.client.rpush("queue:pending", JSON.stringify({ foo: "bar" }));
		expect(await driver.pop()).toBeNull();
	});
});

describe("bay > RedisDriver > complete/fail/retry", () => {
	it("complete removes the job from processing and deletes its lease", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client);
		const job = makeJob({ id: "j1" });
		await driver.push(job);
		await driver.pop();
		await driver.complete(job);
		expect(fake.lists.get("queue:processing")?.length).toBe(0);
		expect(fake.keys.has("queue:lease:j1")).toBe(false);
	});

	it("fail moves the job to the failed list and records the error", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client);
		const job = makeJob({ id: "j1" });
		await driver.push(job);
		await driver.pop();
		await driver.fail(job, "transient failure");

		expect(fake.lists.get("queue:processing")?.length).toBe(0);
		const failedRaw = fake.lists.get("queue:failed")?.[0];
		expect(failedRaw).toBeDefined();
		const failed = JSON.parse(defined(failedRaw));
		expect(failed.error).toBe("transient failure");
		expect(failed.status).toBe("failed");
	});

	it("retry resets job to pending and re-enqueues it", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client);
		const job = makeJob({ id: "j1" });
		await driver.push(job);
		await driver.pop();
		await driver.retry(job);

		expect(fake.lists.get("queue:processing")?.length).toBe(0);
		const pending = fake.lists.get("queue:pending");
		expect(pending?.length).toBe(1);
		expect(JSON.parse(defined(defined(pending)[0])).status).toBe("pending");
	});
});

describe("bay > RedisDriver > recoverStale", () => {
	it("moves processing jobs whose lease has expired back to pending", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client);
		const j1 = makeJob({ id: "j1" });
		const j2 = makeJob({ id: "j2" });
		await driver.push(j1);
		await driver.push(j2);
		await driver.pop(); // j1 → processing + lease set
		await driver.pop(); // j2 → processing + lease set

		// Simulate j1's lease expiring (delete its lease key).
		fake.keys.delete("queue:lease:j1");

		const recovered = await driver.recoverStale();
		expect(recovered).toBe(1);
		// j1 should be back in pending; j2 still in processing.
		expect(fake.lists.get("queue:pending")?.length).toBe(1);
		expect(
			JSON.parse(defined(defined(fake.lists.get("queue:pending"))[0])).id,
		).toBe("j1");
		expect(fake.lists.get("queue:processing")?.length).toBe(1);
	});

	it("returns 0 when every processing job still has a valid lease", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client);
		await driver.push(makeJob({ id: "alive" }));
		await driver.pop();
		expect(await driver.recoverStale()).toBe(0);
	});

	it("skips malformed JSON in the processing list (no crash)", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client);
		await fake.client.rpush("queue:processing", "not-json");
		expect(await driver.recoverStale()).toBe(0);
	});

	it("skips processing entries that aren't valid Job shapes", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client);
		await fake.client.rpush(
			"queue:processing",
			JSON.stringify({ partial: true }),
		);
		expect(await driver.recoverStale()).toBe(0);
	});
});

describe("bay > RedisDriver > failed() / size()", () => {
	it("failed() returns parsed Job objects, filtering out malformed entries", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client);
		const j = makeJob({ id: "ok", error: "boom" });
		await fake.client.rpush("queue:failed", JSON.stringify(j));
		await fake.client.rpush("queue:failed", "garbage");
		await fake.client.rpush("queue:failed", JSON.stringify({ not: "a job" }));
		const failed = await driver.failed();
		expect(failed).toHaveLength(1);
		expect(failed[0]?.id).toBe("ok");
	});

	it("size() returns LLEN of the pending list", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client);
		await driver.push(makeJob({ id: "a" }));
		await driver.push(makeJob({ id: "b" }));
		expect(await driver.size()).toBe(2);
	});
});

describe("bay > RedisDriver > custom prefix", () => {
	it("respects a custom prefix on every key namespace", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client, { prefix: "myapp:" });
		await driver.push(makeJob({ id: "j" }));
		expect(fake.lists.has("myapp:pending")).toBe(true);
		await driver.pop();
		expect(fake.keys.has("myapp:lease:j")).toBe(true);
	});

	it("adds the separator a prefix is missing", async () => {
		// Every key is built by concatenation, so "myapp" produced
		// "myapppending" — unreadable, and able to collide with a neighbouring
		// prefix. Nothing warned, because nothing failed.
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client, { prefix: "myapp" });
		await driver.push(makeJob({ id: "j" }));
		expect(fake.lists.has("myapp:pending")).toBe(true);
		expect(fake.lists.has("myapppending")).toBe(false);
	});

	it("leaves a prefix that already ends in a separator alone", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client, { prefix: "myapp." });
		await driver.push(makeJob({ id: "j" }));
		expect(fake.lists.has("myapp.pending")).toBe(true);
	});
});

describe("bay > RedisDriver > processing-list fallback removal", () => {
	it("falls back to scanning the processing list when the lease is missing", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client);
		const job = makeJob({ id: "scanned" });
		await driver.push(job);
		const popped = await driver.pop();
		expect(popped?.id).toBe("scanned");
		// Manually drop the lease so RedisDriver#removeFromProcessing falls
		// through to the lrange/lrem scan path (lines 170-180 in RedisDriver.ts).
		await fake.client.del("queue:lease:scanned");
		await driver.complete(job);
		const remaining = fake.lists.get("queue:processing") ?? [];
		expect(remaining).toEqual([]);
	});

	it("scan ignores malformed JSON entries in the processing list", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client);
		const job = makeJob({ id: "real" });
		await fake.client.rpush("queue:processing", "{{not-json");
		await fake.client.rpush("queue:processing", JSON.stringify(job));
		await driver.complete(job);
		const remaining = fake.lists.get("queue:processing") ?? [];
		expect(remaining).toEqual(["{{not-json"]);
	});
});

describe("bay > RedisDriver > the shapes it refuses", () => {
	it("refuses a popped value that is not an object at all", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client);

		// Valid JSON that is not a job — a `null`, a number, an array. Trusting
		// it would put a non-job through the handler.
		for (const value of ["null", "42", '"a string"', "[]"]) {
			await fake.client.rpush("queue:pending", value);
			expect(await driver.pop(), value).toBeNull();
		}
	});

	it("refuses a job-shaped object with one field of the wrong type", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client);
		const complete = {
			id: "1",
			name: "welcome",
			attempts: 0,
			maxAttempts: 3,
			status: "pending",
			payload: {},
		};

		for (const field of ["id", "name", "attempts", "maxAttempts", "status"]) {
			await fake.client.rpush(
				"queue:pending",
				JSON.stringify({ ...complete, [field]: { wrong: true } }),
			);
			expect(await driver.pop(), field).toBeNull();
		}
	});
});

describe("bay > RedisDriver > removing a job whose lease is gone", () => {
	it("finds it by id and removes the entry that is actually stored", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client);
		await driver.push(makeJob({ id: "j1" }));
		const job = defined(await driver.pop());
		// The lease is what `complete` normally matches on; without it the
		// driver has to fall back to scanning the processing list, or the job
		// stays there forever and gets recovered as stale.
		await fake.client.del(`queue:lease:${job.id}`);

		await driver.complete(job);

		expect(fake.lists.get("queue:processing") ?? []).toHaveLength(0);
	});

	it("leaves the list alone when nothing in it matches", async () => {
		const fake = createFakeRedis();
		const driver = new RedisDriver(fake.client);
		await driver.push(makeJob({ id: "j1" }));
		const job = defined(await driver.pop());
		await fake.client.del(`queue:lease:${job.id}`);
		await fake.client.lrem("queue:processing", 1, JSON.stringify(job));
		await fake.client.rpush("queue:processing", "not json");
		await fake.client.rpush(
			"queue:processing",
			JSON.stringify({ ...job, id: "someone-else" }),
		);

		await driver.complete(job);

		expect(fake.lists.get("queue:processing") ?? []).toHaveLength(2);
	});
});

describe("bay > a Redis without LMOVE is not silently accepted in production", () => {
	const original = process.env.NODE_ENV;
	afterEach(() => {
		process.env.NODE_ENV = original;
	});

	/** A client from before Redis 6.2: everything but LMOVE. */
	const oldClient = () => {
		const { client } = createFakeRedis({ withLmove: false });
		return client;
	};

	it("warns outside production and keeps working", () => {
		process.env.NODE_ENV = "development";
		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

		expect(() => new RedisDriver(oldClient())).not.toThrow();

		expect(warn).toHaveBeenCalled();
		warn.mockRestore();
	});

	it("refuses in production", () => {
		process.env.NODE_ENV = "production";

		// The pop becomes lpop+rpush: a crash between the two deletes the job
		// from pending before it reaches processing, and nothing recovers it
		// because nothing knows it existed.
		expect(() => new RedisDriver(oldClient())).toThrow(/no LMOVE/);
	});

	it("refuses under the `prod` spelling too", () => {
		process.env.NODE_ENV = "prod";

		expect(() => new RedisDriver(oldClient())).toThrow(/no LMOVE/);
	});

	it("accepts it when the deployment says so explicitly — and says so loudly", () => {
		process.env.NODE_ENV = "production";
		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

		expect(
			() => new RedisDriver(oldClient(), { allowNonAtomicPop: true }),
		).not.toThrow();

		// Agreeing once in a config file is not the same as being reminded
		// that this process is running that way — the line has to be in the
		// logs of the incident.
		expect(warn).toHaveBeenCalledOnce();
		expect(String(warn.mock.calls[0]?.[0])).toContain("PRODUCTION");
		warn.mockRestore();
	});

	it("says nothing when the client has LMOVE", () => {
		process.env.NODE_ENV = "production";
		const { client } = createFakeRedis({ withLmove: true });

		expect(() => new RedisDriver(client)).not.toThrow();
	});
});

describe("bay > NODE_ENV aliases decide whether the lossy pop is accepted", () => {
	const original = process.env.NODE_ENV;
	afterEach(() => {
		process.env.NODE_ENV = original;
	});

	const oldClient = () => createFakeRedis({ withLmove: false }).client;

	it("folds every production spelling", () => {
		for (const value of ["prod", "production", "PROD", "Production"]) {
			process.env.NODE_ENV = value;
			expect(() => new RedisDriver(oldClient()), value).toThrow(/no LMOVE/);
		}
	});

	it("folds the development and test spellings, which only warn", () => {
		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
		for (const value of ["dev", "develop", "development", "test", "testing"]) {
			process.env.NODE_ENV = value;
			expect(() => new RedisDriver(oldClient()), value).not.toThrow();
		}
		warn.mockRestore();
	});

	it("treats an unset or unrecognised environment as not production", () => {
		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

		process.env.NODE_ENV = "staging";
		expect(() => new RedisDriver(oldClient())).not.toThrow();

		// Absent is not production either — the refusal is for a deployment
		// that says it is one.
		delete process.env.NODE_ENV;
		expect(() => new RedisDriver(oldClient())).not.toThrow();

		warn.mockRestore();
	});
});

describe("bay > a transient Redis failure does not swallow a reserved job", () => {
	it("leaves the job in processing when the lease cannot be written", async () => {
		const fake = createFakeRedis({ withLmove: true });
		const driver = new RedisDriver(fake.client);
		await driver.push(makeJob({ id: "j1" }));
		// The move succeeded; the lease write is what blips.
		fake.client.set = async () => {
			throw new Error("READONLY You can't write against a replica");
		};

		await expect(driver.pop()).rejects.toThrow(/READONLY/);

		// Deleting it here would lose it for good: it is already out of
		// pending, and recoverStale() only ever scans processing.
		expect(fake.lists.get("queue:processing")).toHaveLength(1);
	});

	it("and recoverStale puts that job back where a worker will see it", async () => {
		const fake = createFakeRedis({ withLmove: true });
		const driver = new RedisDriver(fake.client);
		await driver.push(makeJob({ id: "j1" }));
		const set = fake.client.set;
		fake.client.set = async () => {
			throw new Error("READONLY");
		};
		await driver.pop().catch(() => undefined);
		fake.client.set = set;

		const recovered = await driver.recoverStale();

		// No lease was ever written, which is exactly the state recoverStale
		// reclaims. At-least-once survives the blip.
		expect(recovered).toBe(1);
		expect(fake.lists.get("queue:pending")).toHaveLength(1);
		expect(fake.lists.get("queue:processing")).toHaveLength(0);
	});

	it("still purges a payload that can never be run", async () => {
		const fake = createFakeRedis({ withLmove: true });
		const driver = new RedisDriver(fake.client);
		await fake.client.rpush("queue:pending", "not json at all");

		expect(await driver.pop()).toBeNull();

		// A poison pill is the one case where deleting is right: it would sit
		// in processing forever, wasting every recovery pass.
		expect(fake.lists.get("queue:processing") ?? []).toHaveLength(0);
	});

	it("purges a JSON payload that is not a job either", async () => {
		const fake = createFakeRedis({ withLmove: true });
		const driver = new RedisDriver(fake.client);
		await fake.client.rpush("queue:pending", JSON.stringify({ nope: true }));

		expect(await driver.pop()).toBeNull();
		expect(fake.lists.get("queue:processing") ?? []).toHaveLength(0);
	});
});
