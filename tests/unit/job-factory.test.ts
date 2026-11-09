/**
 * Jobs and handler classes are built by a factory — the host container by
 * default — so a job can take its dependencies in its constructor.
 */
import { describe, expect, it } from "vitest";
import BayProvider, { type BayAppContext } from "../../src/BayProvider.js";
import { MemoryDriver } from "../../src/drivers/MemoryDriver.js";
import { defineConfig, type JobFactory } from "../../src/index.js";
import { Job } from "../../src/Job.js";
import { type JobHandler, QueueManager } from "../../src/QueueManager.js";

class Mailer {
	readonly sent: string[] = [];
	send(to: string): void {
		this.sent.push(to);
	}
}

class SendEmail extends Job<{ to: string }> {
	constructor(private readonly mailer: Mailer) {
		super();
	}
	async execute(): Promise<void> {
		this.mailer.send(this.payload.to);
	}
}

class Notify implements JobHandler {
	constructor(private readonly mailer: Mailer) {}
	async handle(payload: unknown): Promise<void> {
		this.mailer.send(String(payload));
	}
}

/** A container that knows how to build the two classes above. */
function containerFor(mailer: Mailer): JobFactory {
	return (cls) => {
		if (cls === SendEmail) return new SendEmail(mailer);
		if (cls === Notify) return new Notify(mailer);
		throw new Error(`cannot build ${cls.name}`);
	};
}

describe("bay > a job factory", () => {
	it("builds job classes, so constructor dependencies arrive", async () => {
		const mailer = new Mailer();
		const queue = new QueueManager(new MemoryDriver(), undefined, {
			jobFactory: containerFor(mailer),
		});
		await queue.dispatch(SendEmail, { to: "a@example.com" });
		await queue.processOne();
		expect(mailer.sent).toEqual(["a@example.com"]);
	});

	it("builds registered handler classes too", async () => {
		const mailer = new Mailer();
		const queue = new QueueManager(new MemoryDriver(), undefined, {
			jobFactory: containerFor(mailer),
		});
		queue.register("notify", Notify);
		await queue.dispatch("notify", "b@example.com");
		await queue.processOne();
		expect(mailer.sent).toEqual(["b@example.com"]);
	});

	it("counts a factory that throws as a failed attempt, not a crashed worker", async () => {
		const queue = new QueueManager(new MemoryDriver(), undefined, {
			jobFactory: () => {
				throw new Error("container: Mailer is not bound");
			},
		});
		await queue.dispatch(SendEmail, { to: "x" }, { maxAttempts: 2 });

		expect(await queue.processOne()).toBe(true);
		expect(await queue.size()).toBe(1);
		expect(await queue.processOne()).toBe(true);
		const failed = await queue.failedJobs();
		expect(failed.map((job) => [job.attempts, job.error])).toEqual([
			[2, "container: Mailer is not bound"],
		]);
	});

	it("refuses what a factory built when it is not the job asked for", async () => {
		const queue = new QueueManager(new MemoryDriver(), undefined, {
			jobFactory: () => ({ not: "a job" }),
		});
		await queue.dispatch(SendEmail, { to: "x" }, { maxAttempts: 1 });
		await queue.processOne();
		expect((await queue.failedJobs())[0]?.error).toMatch(
			/factory.*SendEmail.*not an instance/,
		);
	});
});

describe("bay > the provider's job factory", () => {
	function app(
		config: unknown,
		make?: (token: unknown) => Promise<unknown>,
	): BayAppContext {
		const bindings = new Map<unknown, () => unknown>();
		const built = new Map<unknown, unknown>();
		return {
			config: { get: () => config },
			container: {
				singleton(token: unknown, factory: () => unknown) {
					bindings.set(token, factory);
				},
				async resolve(token: unknown): Promise<unknown> {
					if (!built.has(token)) {
						const factory = bindings.get(token);
						if (!factory) throw new Error("unbound");
						built.set(token, await factory());
					}
					return built.get(token);
				},
				make,
			},
		};
	}

	async function managerOf(context: BayAppContext): Promise<QueueManager> {
		new BayProvider(context).register();
		const manager = await context.container.resolve(QueueManager);
		if (!(manager instanceof QueueManager)) throw new Error("no manager");
		return manager;
	}

	it("builds jobs through the host container by default", async () => {
		const mailer = new Mailer();
		const made: unknown[] = [];
		const manager = await managerOf(
			app(defineConfig({}), async (token) => {
				made.push(token);
				return new SendEmail(mailer);
			}),
		);
		await manager.dispatch(SendEmail, { to: "c@example.com" });
		await manager.processOne();
		expect(made).toEqual([SendEmail]);
		expect(mailer.sent).toEqual(["c@example.com"]);
	});

	it("lets the config's jobFactory win over the container", async () => {
		const mailer = new Mailer();
		let containerUsed = false;
		const manager = await managerOf(
			app(defineConfig({ jobFactory: containerFor(mailer) }), async () => {
				containerUsed = true;
				return undefined;
			}),
		);
		await manager.dispatch(SendEmail, { to: "d@example.com" });
		await manager.processOne();
		expect(containerUsed).toBe(false);
		expect(mailer.sent).toEqual(["d@example.com"]);
	});
});
