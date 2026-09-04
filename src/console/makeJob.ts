/**
 * `make:job` — scaffold a job class.
 *
 *     ream make:job SendWelcomeEmail        → app/jobs/send_welcome_email.ts
 *     ream make:job emails/SendWelcomeEmail → app/jobs/emails/send_welcome_email.ts
 *
 * A subdirectory is allowed and is the only reason the name is not a plain
 * identifier: `..` and every other way out of the jobs directory is refused
 * before anything is written.
 */

import * as fsp from "node:fs/promises";
import * as path from "node:path";
import { argument, type BayCommandClass } from "./contract.js";

export interface MakeJobOptions {
	/** Directory the job files are scaffolded into. */
	jobsDir: string;
}

/** `SendWelcomeEmail` → `send_welcome_email`, `HTTPPing` → `http_ping`. */
export function toSnakeCase(name: string): string {
	return name
		.replace(/([a-z0-9])([A-Z])/g, "$1_$2")
		.replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2")
		.replace(/[-\s]+/g, "_")
		.toLowerCase();
}

/** `send_welcome_email` / `sendWelcomeEmail` → `SendWelcomeEmail`. */
export function toPascalCase(name: string): string {
	return name
		.split(/[_\-\s]+/)
		.filter((part) => part.length > 0)
		.map((part) => part.charAt(0).toUpperCase() + part.slice(1))
		.join("");
}

/**
 * Split `emails/SendWelcomeEmail` into the directory it goes in and the class
 * it declares, refusing anything that would leave the jobs directory.
 */
export function resolveJobName(input: string): {
	dir: string;
	className: string;
	fileName: string;
} {
	const parts = input.split("/").filter((part) => part.length > 0);
	const last = parts.pop();
	if (last === undefined || last.length === 0) {
		throw new Error(`Invalid job name: '${input}'`);
	}
	for (const part of [...parts, last]) {
		// `..` is the traversal; the rest are characters a filename has no
		// business carrying and a shell has opinions about.
		if (part === ".." || /[\\'";`]/.test(part)) {
			throw new Error(`Invalid job name: '${input}'`);
		}
	}
	const className = toPascalCase(last);
	if (!/^[A-Za-z][A-Za-z0-9]*$/.test(className)) {
		throw new Error(
			`Invalid job name: '${input}' — a job's name becomes a class name`,
		);
	}
	return {
		dir: parts.join("/"),
		className,
		fileName: `${toSnakeCase(last)}.ts`,
	};
}

/** The file a fresh job starts as. */
export function jobStub(className: string): string {
	return `import { Job } from '@c9up/bay'
import type { JobOptions } from '@c9up/bay'

interface ${className}Payload {
  // What \`dispatch(${className}, …)\` must be given.
}

export default class ${className} extends Job<${className}Payload> {
  static options: JobOptions = {
    // queue: 'default',
    // maxRetries: 3,
    // delay: '10s',
    // timeout: '1m',
  }

  async execute(): Promise<void> {
    // The work. Throwing is what makes the attempt fail.
    void this.payload
  }

  async failed(error: Error): Promise<void> {
    // Once the last attempt has failed — the alert or the cleanup.
    void error
  }
}
`;
}

export function makeJobCommand(options: MakeJobOptions): BayCommandClass {
	return class MakeJob {
		static commandName = "make:job";
		static description = "Scaffold a background job class";
		// Pure filesystem work: no reason to boot the app and open a connection.
		static options = { startApp: false };
		static args = [
			argument("name", {
				description: "Job class name, optionally under a subdirectory",
			}),
		];

		declare name: string;

		async run(): Promise<void> {
			let resolved: ReturnType<typeof resolveJobName>;
			try {
				resolved = resolveJobName(this.name);
			} catch (err) {
				console.error(
					`[bay] ${err instanceof Error ? err.message : String(err)}`,
				);
				process.exitCode = 1;
				return;
			}

			const dir = path.join(options.jobsDir, resolved.dir);
			const filePath = path.join(dir, resolved.fileName);
			await fsp.mkdir(dir, { recursive: true });
			// `wx`: an existing job is never clobbered, and the error says so.
			await fsp.writeFile(filePath, jobStub(resolved.className), {
				flag: "wx",
			});
			console.log(`Created ${filePath}`);
		}
	};
}
