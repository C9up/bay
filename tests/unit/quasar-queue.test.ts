/**
 * A queue naming its quasar connection, against a real server: what is being
 * proved is that a QuasarConnection carries the list commands this driver
 * issues (LMOVE included), which a fake cannot show.
 *
 * Skipped, not failed, when no server answers.
 */

import { QuasarManager } from "@c9up/quasar";
import { setQuasar } from "@c9up/quasar/services/main";
import { afterAll, describe, expect, it } from "vitest";
import { RedisDriver } from "../../src/drivers/RedisDriver.js";
import { quasarConnection } from "../../src/quasar.js";

const url = process.env.REDIS_TEST_URL ?? "redis://127.0.0.1:6379";
const manager = new QuasarManager({
	connection: "main",
	connections: { main: { url, db: 15 } },
});

const live = await manager
	.connection()
	.ping()
	.then(() => true)
	.catch(() => false);

setQuasar(manager);

afterAll(async () => {
	await manager.quit();
});

describe.skipIf(!live)("a queue on a quasar connection", () => {
	it("pushes and pops through the connection quasar owns", async () => {
		const driver = new RedisDriver(quasarConnection("main"), {
			prefix: `bay-test:${process.pid}:`,
		});
		const job = {
			id: "j1",
			name: "send-mail",
			payload: { to: "a@b.c" },
			attempts: 0,
			maxAttempts: 3,
			status: "pending" as const,
			createdAt: Date.now(),
		};

		await driver.push(job);
		const popped = await driver.pop();

		expect(popped?.id).toBe("j1");
		if (popped) await driver.complete(popped);
	});

	it("resolves the connection only when a command needs it", async () => {
		// Building the driver must not dial: an app declaring a redis queue it
		// never uses should never open a socket.
		const before = manager.activeConnectionNames.length;
		const driver = new RedisDriver(quasarConnection("main"), {
			prefix: `bay-test:${process.pid}:lazy:`,
		});
		expect(manager.activeConnectionNames.length).toBe(before);

		await driver.size();
		expect(manager.activeConnectionNames).toContain("main");
	});
});
