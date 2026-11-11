/**
 * `work()` refuses a setting it cannot honour instead of turning it into a
 * busy loop or a worker that silently serves nothing.
 */
import { describe, expect, it } from "vitest";
import { MemoryDriver } from "../../src/drivers/MemoryDriver.js";
import { QueueManager } from "../../src/QueueManager.js";

const NOT_A_DELAY = [
	Number.NaN,
	Number.POSITIVE_INFINITY,
	0,
	-1,
	2 ** 31,
	"soon",
];

describe("bay > work() options", () => {
	it("refuses an idleDelay or stalledInterval that is not a delay setTimeout can hold", async () => {
		const queue = new QueueManager(new MemoryDriver());
		for (const bad of NOT_A_DELAY) {
			await expect(queue.work({ idleDelay: bad }), String(bad)).rejects.toThrow(
				/idleDelay/,
			);
			await expect(
				queue.work({ stalledInterval: bad }),
				String(bad),
			).rejects.toThrow(/stalledInterval/);
		}
	});

	it("refuses the same in the config's worker block", async () => {
		const queue = new QueueManager(new MemoryDriver(), {
			stalledInterval: Number.NaN,
		});
		await expect(queue.work()).rejects.toThrow(/stalledInterval/);
	});

	it("refuses a queue list that serves nothing", async () => {
		const queue = new QueueManager(new MemoryDriver());
		for (const queues of [[], [""], ["  "], ["default", ""]]) {
			await expect(
				queue.work({ queues }),
				JSON.stringify(queues),
			).rejects.toThrow(/queues/);
		}
	});

	it("takes durations the way a config file writes them", async () => {
		const queue = new QueueManager(new MemoryDriver());
		let ran = 0;
		let done: (() => void) | undefined;
		const handled = new Promise<void>((resolve) => {
			done = resolve;
		});
		queue.register("x", {
			async handle() {
				ran++;
				done?.();
			},
		});
		await queue.dispatch("x", {});
		const working = queue.work({ idleDelay: "10ms", stalledInterval: "30s" });
		// The job running is the signal, not a guess at how long it takes.
		await handled;
		await queue.stop();
		await working;
		expect(ran).toBe(1);
	});
});
