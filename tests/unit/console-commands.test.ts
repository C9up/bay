/**
 * The two commands bay ships, and how the kernel finds them.
 *
 * `queue:work` and `make:job` reach a project through `reamrc.commands`, so the
 * loader's metadata is the contract: a name the kernel cannot read is a command
 * nobody can run, and nothing else in the package would notice.
 */

import * as fsp from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { argument, flag } from "../../src/console/contract.js";
import { getCommand, getMetaData } from "../../src/console/index.js";
import {
	jobStub,
	makeJobCommand,
	resolveJobName,
} from "../../src/console/makeJob.js";
import { parseQueues, queueWorkCommand } from "../../src/console/queueWork.js";
import { MemoryDriver } from "../../src/drivers/MemoryDriver.js";
import {
	directoryOf,
	discoverJobs,
	getJobsDir,
	setJobsDir,
} from "../../src/jobs.js";
import { QueueManager } from "../../src/QueueManager.js";
import { clearQueue, setQueue } from "../../src/services/main.js";

/** A directory of this test's own, removed afterwards. */
async function fixture(label: string): Promise<string> {
	const dir = path.join(
		os.tmpdir(),
		`bay-${label}-${process.pid}-${Math.random().toString(36).slice(2)}`,
	);
	await fsp.mkdir(dir, { recursive: true });
	return dir;
}

describe("bay > the commands the package ships", () => {
	it("publishes both under names the kernel can read", async () => {
		const metadata = await getMetaData();
		const names = metadata.map((entry) => entry.commandName).sort();
		expect(names).toEqual(["make:job", "queue:work"]);

		const work = metadata.find((entry) => entry.commandName === "queue:work");
		// The namespace is what groups a command in `ream list`; derived from the
		// name, so a command with no colon has none.
		expect(work?.namespace).toBe("queue");
		expect(work?.description).toBeTruthy();
		expect(work?.options).toMatchObject({ startApp: true, staysAlive: true });
		expect(work?.flags).toHaveLength(2);
	});

	it("hands back the class for a name it published, and null for anything else", async () => {
		const [first] = await getMetaData();
		if (first === undefined) throw new Error("expected a command");
		const found = await getCommand(first);
		expect(found?.commandName).toBe(first.commandName);

		expect(
			await getCommand({ ...first, commandName: "queue:nope" }),
		).toBeNull();
	});

	it("describes an argument and a flag the way the kernel reads them", () => {
		// `startServer` → `start-server`: the same dash-casing the framework's
		// own decorators apply, so a command declared here and one declared with
		// `@flags` present the same command line.
		expect(flag("dryRun", "boolean", { description: "d" })).toMatchObject({
			type: "boolean",
			propertyName: "dryRun",
			flagName: "dry-run",
			required: false,
		});
		expect(argument("jobName", { default: "x" })).toMatchObject({
			propertyName: "jobName",
			argumentName: "job-name",
			// A default makes it optional; without one it is required.
			required: false,
		});
		expect(argument("name").required).toBe(true);
	});
});

describe("bay > queue:work", () => {
	afterEach(() => {
		clearQueue();
	});

	it("serves the queues and the concurrency it was given", async () => {
		const queue = new QueueManager(new MemoryDriver());
		const work = vi
			.spyOn(queue, "work")
			.mockImplementation(async () => undefined);
		const stdout = vi
			.spyOn(process.stdout, "write")
			.mockImplementation(() => true);
		setQueue(queue);

		const QueueWork = queueWorkCommand();
		const command = new QueueWork();
		Object.assign(command, { queue: "critical, emails,", concurrency: 4 });
		await command.run();

		expect(work).toHaveBeenCalledWith({
			queues: ["critical", "emails"],
			concurrency: 4,
		});
		// The line says what it is serving: a worker that looks idle and a worker
		// serving a queue nobody dispatches to are the same picture without it.
		expect(stdout.mock.calls.flat().join("")).toContain("critical, emails");
		stdout.mockRestore();
	});

	it("names the wiring when no queue was ever bound", async () => {
		const QueueWork = queueWorkCommand();
		await expect(new QueueWork().run()).rejects.toThrow(
			/Add `\(\) => import\('@c9up\/bay\/provider'\)`/,
		);
	});

	it("reads --queue as a list, and nothing as nothing", () => {
		expect(parseQueues("emails,notifications")).toEqual([
			"emails",
			"notifications",
		]);
		expect(parseQueues(" critical , default ")).toEqual([
			"critical",
			"default",
		]);
		// A trailing comma must not name an empty queue nothing ever serves.
		expect(parseQueues("emails,")).toEqual(["emails"]);
		expect(parseQueues("")).toBeUndefined();
		expect(parseQueues(undefined)).toBeUndefined();
	});
});

describe("bay > make:job", () => {
	let cwd: string;
	let dir: string;

	beforeEach(async () => {
		cwd = process.cwd();
		dir = await fixture("make-job");
		process.chdir(dir);
	});

	afterEach(async () => {
		process.chdir(cwd);
		await fsp.rm(dir, { recursive: true, force: true });
	});

	it("writes a job where the config says jobs live", async () => {
		const MakeJob = makeJobCommand({ jobsDir: "app/jobs" });
		const command = new MakeJob();
		Object.assign(command, { name: "emails/SendWelcomeEmail" });
		const log = vi.spyOn(console, "log").mockImplementation(() => {});

		await command.run();

		const written = await fsp.readFile(
			path.join(dir, "app/jobs/emails/send_welcome_email.ts"),
			"utf8",
		);
		expect(written).toContain(
			"export default class SendWelcomeEmail extends Job<",
		);
		expect(log.mock.calls.flat().join(" ")).toContain("send_welcome_email.ts");
		log.mockRestore();
	});

	it("refuses to overwrite a job that is already there", async () => {
		const MakeJob = makeJobCommand({ jobsDir: "app/jobs" });
		const command = new MakeJob();
		Object.assign(command, { name: "SendEmail" });
		vi.spyOn(console, "log").mockImplementation(() => {});

		await command.run();
		// `wx`: the second write is an error, not a silent replacement of work
		// somebody had already done in that file.
		await expect(command.run()).rejects.toThrow(/EEXIST|exists/);
	});

	it("reports a name it cannot use instead of writing somewhere else", async () => {
		const MakeJob = makeJobCommand({ jobsDir: "app/jobs" });
		const command = new MakeJob();
		Object.assign(command, { name: "../../etc/passwd" });
		const error = vi.spyOn(console, "error").mockImplementation(() => {});

		await command.run();

		expect(process.exitCode).toBe(1);
		expect(error.mock.calls.flat().join(" ")).toContain("Invalid job name");
		process.exitCode = 0;
		error.mockRestore();
	});

	it("derives the class and the file from what was asked for", () => {
		const resolved = resolveJobName("emails/SendWelcomeEmail");
		expect(resolved).toEqual({
			dir: "emails",
			className: "SendWelcomeEmail",
			fileName: "send_welcome_email.ts",
		});
		expect(resolveJobName("send_email").className).toBe("SendEmail");
		expect(() => resolveJobName("9lives")).toThrow(/Invalid job name/);
	});

	it("scaffolds something that compiles against the class it extends", () => {
		const stub = jobStub("SendEmail");
		expect(stub).toContain("import { Job } from '@c9up/bay'");
		expect(stub).toContain("async execute(): Promise<void>");
		expect(stub).toContain("async failed(error: Error): Promise<void>");
	});
});

describe("bay > finding the job classes", () => {
	let dir: string;

	beforeEach(async () => {
		dir = await fixture("discover");
	});

	afterEach(async () => {
		await fsp.rm(dir, { recursive: true, force: true });
		setJobsDir("app/jobs");
	});

	it("reads a glob as the directory it starts with", () => {
		// The spelling upstream's config uses, so a config copied from there
		// finds the same files.
		expect(directoryOf("./app/jobs/**/*.{ts,js}")).toBe("app/jobs");
		expect(directoryOf("app/jobs")).toBe("app/jobs");
		expect(directoryOf("./app/jobs/")).toBe("app/jobs");
		expect(directoryOf("")).toBe(".");
	});

	it("registers a default-exported job class, and skips everything else", async () => {
		await fsp.writeFile(
			path.join(dir, "send_email.mjs"),
			`import { Job } from "${new URL("../../src/Job.ts", import.meta.url).href}";
export default class SendEmail extends Job { async execute() {} }
`,
		);
		await fsp.writeFile(
			path.join(dir, "helper.mjs"),
			"export default function notAJob() {}\n",
		);
		await fsp.writeFile(path.join(dir, "notes.txt"), "not a module\n");

		const found = await discoverJobs([dir]);

		expect(found.map((job) => job.name)).toEqual(["SendEmail"]);
	});

	it("resolves a location through the host, not the process's directory", async () => {
		// `app/jobs` means "under the application root". Resolving against
		// `process.cwd()` gave a worker launched from anywhere else an empty
		// discovery, and gave it silently.
		await fsp.writeFile(
			path.join(dir, "send_email.mjs"),
			`import { Job } from "${new URL("../../src/Job.ts", import.meta.url).href}";
export default class SendEmail extends Job { async execute() {} }
`,
		);
		const seen: string[] = [];
		const found = await discoverJobs(["app/jobs"], (location) => {
			seen.push(location);
			return dir;
		});

		expect(seen).toEqual(["app/jobs"]);
		expect(found.map((job) => job.name)).toEqual(["SendEmail"]);
	});

	it("refuses a jobs directory it cannot read, instead of reading it as empty", async () => {
		// A permission denial, a broken mount, a path that names a file: every
		// one of them used to produce a worker with no handlers and no message.
		// It accepted jobs and processed none of them.
		const notADirectory = path.join(dir, "jobs.txt");
		await fsp.writeFile(notADirectory, "not a directory\n");

		await expect(discoverJobs([notADirectory])).rejects.toThrow(
			/cannot read the jobs directory/,
		);
	});

	it("still treats a directory that does not exist yet as empty", async () => {
		// Naming where the jobs will go before writing the first one is normal.
		await expect(
			discoverJobs([path.join(dir, "not-created-yet")]),
		).resolves.toEqual([]);
	});

	it("refuses to come up with no handlers when EVERY job file failed", async () => {
		const stderr = vi
			.spyOn(process.stderr, "write")
			.mockImplementation(() => true);
		try {
			await fsp.writeFile(path.join(dir, "a.mjs"), "%%% not js\n");
			await fsp.writeFile(path.join(dir, "b.mjs"), "%%% not js either\n");

			// Skipping ONE broken file so the rest still run is deliberate.
			// Skipping all of them leaves a worker that accepts jobs and runs
			// nothing, which is a broken deploy rather than a warning.
			await expect(discoverJobs([dir])).rejects.toThrow(
				/every job file failed to load/,
			);
		} finally {
			stderr.mockRestore();
		}
	});

	it("reports a module it cannot load and keeps going", async () => {
		const stderr = vi
			.spyOn(process.stderr, "write")
			.mockImplementation(() => true);
		await fsp.writeFile(
			path.join(dir, "broken.mjs"),
			"this is not valid js %%%\n",
		);
		await fsp.writeFile(
			path.join(dir, "fine.mjs"),
			`import { Job } from "${new URL("../../src/Job.ts", import.meta.url).href}";
export default class Fine extends Job { async execute() {} }
`,
		);

		// One unfinished job file must not stop the worker from running every
		// other job — it is reported, not fatal.
		const found = await discoverJobs([dir]);

		expect(found.map((job) => job.name)).toEqual(["Fine"]);
		expect(stderr.mock.calls.flat().join("")).toContain("could not load");
		stderr.mockRestore();
	});

	it("treats a directory that does not exist yet as empty", async () => {
		// A project may name where its jobs will go before writing the first one.
		expect(await discoverJobs([path.join(dir, "nothing-here")])).toEqual([]);
	});

	it("remembers where make:job should write", () => {
		setJobsDir("src/queue");
		expect(getJobsDir()).toBe("src/queue");
	});
});
