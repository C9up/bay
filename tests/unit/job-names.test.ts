/**
 * A job class answers to `options.name` when it declares one, its class name
 * otherwise — and two classes never silently share a name.
 */
import { describe, expect, it } from "vitest";
import { MemoryDriver } from "../../src/drivers/MemoryDriver.js";
import { Job, jobNameOf } from "../../src/Job.js";
import { QueueManager } from "../../src/QueueManager.js";

function twoSendEmails(ran: string[]) {
	const Billing = class SendEmail extends Job {
		static override options = { name: "billing.send-email" };
		async execute(): Promise<void> {
			ran.push("billing");
		}
	};
	const Marketing = class SendEmail extends Job {
		async execute(): Promise<void> {
			ran.push("marketing");
		}
	};
	return { Billing, Marketing };
}

describe("bay > job names", () => {
	it("refuses a second class under a name another class already answers to", () => {
		const queue = new QueueManager(new MemoryDriver());
		const first = class SendEmail extends Job {
			async execute(): Promise<void> {}
		};
		const second = class SendEmail extends Job {
			async execute(): Promise<void> {}
		};
		queue.registerJob(first);
		expect(() => queue.registerJob(second)).toThrow(
			expect.objectContaining({
				code: "E_BAY_DUPLICATE_JOB",
				message: expect.stringMatching(
					/two job classes answer to 'SendEmail'.*options\.name/s,
				),
			}),
		);
		// The same class twice is not a conflict.
		expect(() => queue.registerJob(first)).not.toThrow();
	});

	it("refuses it on dispatch too, rather than running the other class's code", async () => {
		const ran: string[] = [];
		const queue = new QueueManager(new MemoryDriver());
		const { Marketing } = twoSendEmails(ran);
		const Other = class SendEmail extends Job {
			async execute(): Promise<void> {
				ran.push("other");
			}
		};
		queue.registerJob(Marketing);
		await expect(queue.dispatch(Other, undefined)).rejects.toThrow(
			/two job classes/,
		);
		expect(await queue.size()).toBe(0);
	});

	it("lets options.name tell two same-named classes apart", async () => {
		const ran: string[] = [];
		const { Billing, Marketing } = twoSendEmails(ran);
		const driver = new MemoryDriver();
		const queue = new QueueManager(driver);
		queue.registerJob(Billing);
		queue.registerJob(Marketing);

		await queue.dispatch(Marketing, undefined);
		await queue.dispatch(Billing, undefined);
		while (await queue.processOne()) {}

		expect(ran).toEqual(["marketing", "billing"]);
		expect([...queue.registeredJobs().keys()].sort()).toEqual([
			"SendEmail",
			"billing.send-email",
		]);
		expect(jobNameOf(Billing)).toBe("billing.send-email");
	});

	it("refuses an empty options.name", () => {
		const queue = new QueueManager(new MemoryDriver());
		const Blank = class Blank extends Job {
			static override options = { name: "  " };
			async execute(): Promise<void> {}
		};
		expect(() => queue.registerJob(Blank)).toThrow(/name/);
	});
});
