import { createHash } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import {
	type RedisClient,
	RedisDriver,
} from "../../src/drivers/RedisDriver.js";
import type { JobRecord } from "../../src/QueueManager.js";
import { quasarConnection } from "../../src/quasar.js";
import { defined } from "../__helpers__/defined.js";
import {
	describeRedis,
	entriesUnder,
	freshPrefix,
	intercept,
	useRedis,
} from "../__helpers__/redis.js";
import { scripting } from "../__helpers__/scripting.js";

function makeJob(over: Partial<JobRecord> = {}): JobRecord {
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

/** A client nothing is ever sent to: enough for what the constructor checks. */
function inertClient(): RedisClient {
	const never = async (): Promise<never> => {
		throw new Error("not connected");
	};
	return {
		evalsha: never,
		script: never,
		rpush: never,
		zadd: never,
		lrange: never,
	};
}

describe("bay > RedisDriver > config validation", () => {
	it("rejects a non-positive / non-integer visibilityTimeoutMs", () => {
		for (const bad of [0, -1, 1.5, Number.NaN]) {
			expect(
				() => new RedisDriver(inertClient(), { visibilityTimeoutMs: bad }),
			).toThrow(/visibilityTimeoutMs must be a positive integer/);
		}
		expect(
			() => new RedisDriver(inertClient(), { visibilityTimeoutMs: 5000 }),
		).not.toThrow();
	});

	/** A client lacking one of the two scripting commands. */
	function without(command: "evalsha" | "script"): object {
		const client = { ...inertClient() };
		Reflect.deleteProperty(client, command);
		return client;
	}

	it("refuses a client that cannot run a script by digest or load one", () => {
		for (const command of ["evalsha", "script"] as const) {
			expect(() => Reflect.construct(RedisDriver, [without(command)])).toThrow(
				/needs EVALSHA and SCRIPT LOAD/,
			);
		}
	});

	it("refuses it when the client comes from a resolver too", async () => {
		const driver: unknown = Reflect.construct(RedisDriver, [
			() => without("script"),
		]);
		if (!(driver instanceof RedisDriver)) throw new Error("expected a driver");
		await expect(driver.size()).rejects.toThrow(
			/needs EVALSHA and SCRIPT LOAD/,
		);
	});
});

/**
 * Quasar retires a connection whose socket ended for good (its retry strategy
 * gave up) and hands out a fresh one on the next `connection(name)`. A driver
 * that kept the first one failed every operation with "Connection is closed."
 * until the process restarted, even once Redis was back.
 */
describe("bay > RedisDriver > a connection that ended for good", () => {
	function fakeClient(status: string) {
		// Counts the scripts that actually ran, whatever loading took.
		const ran = vi.fn(() => 0);
		return {
			status,
			ran,
			...scripting(ran),
			rpush: vi.fn(async () => 1),
			zadd: vi.fn(async () => 1),
			lrange: vi.fn(async (): Promise<string[]> => []),
		};
	}

	it("asks its resolver again once the client it holds has ended", async () => {
		const first = fakeClient("ready");
		const second = fakeClient("ready");
		const resolver = vi
			.fn<() => RedisClient>()
			.mockReturnValueOnce(first)
			.mockReturnValueOnce(second);
		const driver = new RedisDriver(resolver);

		await driver.size();
		first.status = "end";
		await driver.size();
		await driver.size();

		expect(resolver).toHaveBeenCalledTimes(2);
		expect(first.ran).toHaveBeenCalledOnce();
		expect(second.ran).toHaveBeenCalledTimes(2);
	});

	it("keeps a client that closed but reconnects by itself", async () => {
		const client = fakeClient("ready");
		const resolver = vi.fn(() => client);
		const driver = new RedisDriver(resolver);

		await driver.size();
		client.status = "close";
		await driver.size();

		expect(resolver).toHaveBeenCalledOnce();
	});

	it("keeps a client handed in directly: replacing it is the caller's call", async () => {
		const client = fakeClient("end");
		const driver = new RedisDriver(client);

		await driver.size();

		expect(client.ran).toHaveBeenCalledOnce();
	});
});

/**
 * Scripts run only by digest. A server that does not know one answers
 * NOSCRIPT, which means the script did not run: the driver loads it and asks
 * again. Any other failure may have come after the script ran, so it is never
 * retried.
 */
describe("bay > RedisDriver > scripts by digest", () => {
	function recordingClient() {
		const calls: string[] = [];
		const commands = scripting(() => 0);
		const client: RedisClient = {
			evalsha: async (sha1) => {
				calls.push("evalsha");
				return commands.evalsha(sha1);
			},
			script: async (subcommand, lua) => {
				calls.push("script");
				return commands.script(subcommand, lua);
			},
			rpush: async () => 1,
			zadd: async () => 1,
			lrange: async () => [],
		};
		return { client, calls, commands };
	}

	it("loads a script the server does not know, then runs it by digest", async () => {
		const { client, calls, commands } = recordingClient();
		const driver = new RedisDriver(client);

		await expect(driver.size()).resolves.toBe(0);

		expect(calls).toEqual(["evalsha", "script", "evalsha"]);
		const [digest] = defined(commands.evalsha.mock.calls[0]);
		const [subcommand, lua] = defined(commands.script.mock.calls[0]);
		expect(subcommand).toBe("LOAD");
		expect(createHash("sha1").update(lua).digest("hex")).toBe(digest);
	});

	it("loads each script once, and only runs it by digest afterwards", async () => {
		const { client, calls } = recordingClient();
		const driver = new RedisDriver(client);

		await driver.size();
		await driver.size();
		await driver.size();

		expect(calls).toEqual([
			"evalsha",
			"script",
			"evalsha",
			"evalsha",
			"evalsha",
		]);
	});

	it("does not retry a script that failed for any other reason", async () => {
		const script = vi.fn(async () => "sha");
		const evalsha = vi.fn(async (): Promise<unknown> => {
			throw new Error("ECONNRESET");
		});
		const driver = new RedisDriver({
			evalsha,
			script,
			rpush: async () => 1,
			zadd: async () => 1,
			lrange: async () => [],
		});

		await expect(driver.size()).rejects.toThrow("ECONNRESET");

		expect(evalsha).toHaveBeenCalledOnce();
		expect(script).not.toHaveBeenCalled();
	});
});

describeRedis("bay > RedisDriver against a live Redis", () => {
	const redis = useRedis();
	const connection = () => redis.manager.connection("w0");

	async function driverFor(
		label: string,
		options: { visibilityTimeoutMs?: number } = {},
	): Promise<{ driver: RedisDriver; prefix: string }> {
		const prefix = freshPrefix(label);
		return {
			prefix,
			driver: new RedisDriver(await redis.client(), { prefix, ...options }),
		};
	}

	it("push enqueues a serialized job onto the pending list", async () => {
		const { driver, prefix } = await driverFor("push");
		await driver.push(makeJob());
		const pending = await connection().lrange(`${prefix}pending`, 0, -1);
		expect(pending).toHaveLength(1);
		expect(JSON.parse(defined(pending[0])).id).toBe("job_x");
	});

	it("pop moves the job into processing under a lease that expires", async () => {
		const { driver, prefix } = await driverFor("pop", {
			visibilityTimeoutMs: 5000,
		});
		await driver.push(makeJob({ id: "j1" }));
		const job = await driver.pop();
		expect(job?.id).toBe("j1");
		expect(await connection().llen(`${prefix}pending`)).toBe(0);
		expect(await connection().llen(`${prefix}processing`)).toBe(1);
		const ttl = await connection().pttl(`${prefix}lease:j1`);
		expect(ttl).toBeGreaterThan(0);
		expect(ttl).toBeLessThanOrEqual(5000);
	});

	it("pop returns null when the queue is empty, or when no queue is named", async () => {
		const { driver } = await driverFor("empty");
		expect(await driver.pop()).toBeNull();
		await driver.push(makeJob());
		expect(await driver.pop([])).toBeNull();
	});

	it("pop serves the queues in the order given", async () => {
		const { driver } = await driverFor("order");
		await driver.push(makeJob({ id: "low", queue: "low" }));
		await driver.push(makeJob({ id: "high", queue: "high" }));
		expect((await driver.pop(["high", "low"]))?.id).toBe("high");
		expect((await driver.pop(["high", "low"]))?.id).toBe("low");
	});

	it("complete removes the job from processing and deletes its lease", async () => {
		const { driver, prefix } = await driverFor("complete");
		await driver.push(makeJob({ id: "j1" }));
		await driver.complete(defined(await driver.pop()));
		expect(await entriesUnder(redis.manager, prefix)).toEqual({});
	});

	it("fail moves the job to the failed list and records the error", async () => {
		const { driver, prefix } = await driverFor("fail");
		await driver.push(makeJob({ id: "j1" }));
		await driver.fail(defined(await driver.pop()), "handler threw");
		expect(await connection().llen(`${prefix}processing`)).toBe(0);
		const failed = await driver.failed();
		expect(failed.map((j) => [j.id, j.status, j.error])).toEqual([
			["j1", "failed", "handler threw"],
		]);
	});

	it("retry puts the job back in its own queue, or in its delayed set when it carries a future runAt", async () => {
		const { driver, prefix } = await driverFor("retry");
		await driver.push(makeJob({ id: "now", queue: "emails" }));
		await driver.push(makeJob({ id: "later", queue: "emails" }));

		const now = defined(await driver.pop(["emails"]));
		await driver.retry(now);
		const later = defined(await driver.pop(["emails"]));
		later.runAt = Date.now() + 60_000;
		await driver.retry(later);

		expect(
			(await connection().lrange(`${prefix}q:emails:pending`, 0, -1)).map(
				(raw) => JSON.parse(raw).id,
			),
		).toEqual(["now"]);
		expect(await connection().zcard(`${prefix}q:emails:delayed`)).toBe(1);
		expect(await driver.sizeOf("emails")).toBe(2);
	});

	it("does not settle a record it never took", async () => {
		const { driver, prefix } = await driverFor("foreign");
		await driver.push(makeJob({ id: "j1" }));
		const before = await entriesUnder(redis.manager, prefix);
		await driver.complete(makeJob({ id: "j1" }));
		await driver.fail(makeJob({ id: "j1" }), "nope");
		expect(await entriesUnder(redis.manager, prefix)).toEqual(before);
	});

	it("recoverStale moves an expired job back to pending and leaves a leased one", async () => {
		const { driver, prefix } = await driverFor("recover");
		await driver.push(makeJob({ id: "a" }));
		await driver.push(makeJob({ id: "b" }));
		await driver.pop();
		await driver.pop();
		await connection().del(`${prefix}lease:a`);

		expect(await driver.recoverStale()).toBe(1);
		const pending = await connection().lrange(`${prefix}pending`, 0, -1);
		expect(pending.map((raw) => JSON.parse(raw).id)).toEqual(["a"]);
		expect(JSON.parse(defined(pending[0])).stalledCount).toBe(1);
		expect(await connection().llen(`${prefix}processing`)).toBe(1);
	});

	it("respects a custom prefix, adding the separator it is missing", async () => {
		const base = freshPrefix("custom").slice(0, -1);
		const driver = new RedisDriver(await redis.client(), { prefix: base });
		await driver.push(makeJob());
		expect(await connection().llen(`${base}:pending`)).toBe(1);
		expect(await connection().exists(`${base}pending`)).toBe(0);
	});

	it("loads each script once and addresses it by digest afterwards", async () => {
		const prefix = freshPrefix("evalsha");
		// Forget every script, so the first call has to send it.
		await connection().script("FLUSH");
		const calls: string[] = [];
		const client = intercept(await redis.client(), (command, run) => {
			calls.push(command);
			return run();
		});
		const driver = new RedisDriver(client, { prefix });
		await driver.push(makeJob({ id: "a" }));
		await driver.push(makeJob({ id: "b" }));

		await driver.pop();
		await driver.pop();

		// At most one load per script — two here, the push and the acquire
		// (another test file may load them first) — always after a digest
		// attempt, and the digest afterwards.
		const scripted = calls.filter((c) => c === "script" || c === "evalsha");
		expect(scripted.filter((c) => c === "script").length).toBeLessThanOrEqual(
			2,
		);
		expect(scripted[0]).toBe("evalsha");
		expect(scripted.at(-1)).toBe("evalsha");
	});

	for (const [label, raw] of [
		["unparseable JSON", "not json at all"],
		["JSON that is not a job", JSON.stringify({ nope: true })],
		[
			"a job whose maxAttempts was lost in serialisation",
			JSON.stringify({ ...makeJob({ id: "inf" }), maxAttempts: null }),
		],
	] as const) {
		it(`files ${label} as failed instead of deleting it`, async () => {
			const { driver, prefix } = await driverFor("unreadable");
			const stderr = vi
				.spyOn(process.stderr, "write")
				.mockImplementation(() => true);
			try {
				await connection().rpush(`${prefix}pending`, raw);
				await driver.push(makeJob({ id: "good" }));

				// The bad entry is filed and the next job taken in the same call.
				expect((await driver.pop())?.id).toBe("good");
				expect(stderr).toHaveBeenCalledWith(
					expect.stringMatching(/unreadable job record/),
				);
			} finally {
				stderr.mockRestore();
			}

			const failed = await driver.failed();
			expect(failed).toHaveLength(1);
			expect(failed[0]?.payload).toBe(raw);
			expect(failed[0]?.error).toMatch(/Unreadable job record/);
			expect(await connection().llen(`${prefix}processing`)).toBe(1);
		});
	}

	it("files an unreadable entry found in processing as failed too", async () => {
		const { driver, prefix } = await driverFor("unreadable-processing");
		await connection().rpush(`${prefix}processing`, "garbage");
		expect(await driver.recoverStale()).toBe(0);
		expect((await driver.failed()).map((job) => job.payload)).toEqual([
			"garbage",
		]);
		expect(await connection().llen(`${prefix}processing`)).toBe(0);
	});

	it("resolves a named quasar connection lazily", async () => {
		const prefix = freshPrefix("named");
		const driver = new RedisDriver(quasarConnection("w1"), { prefix });
		await driver.push(makeJob());
		expect(await driver.size()).toBe(1);
	});
});
