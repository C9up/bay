/**
 * Finding the job classes an application wrote.
 *
 * A job class carries its own name, and a worker resolves a queued record by
 * that name — so the worker process has to have imported the class. Written by
 * hand that is a registration list to keep in step with a directory:
 *
 *     queue.registerJob(SendEmail)
 *     queue.registerJob(SendInvoice)   // …and the one nobody added
 *
 * `locations` in `config/queue.ts` is the directory instead. Every module under
 * it is imported once at boot, and a default export that is a job class is
 * registered under its own name.
 *
 * Directories, not globs. `'./app/jobs/**\/*.{ts,js}'` — the spelling upstream's
 * config uses — is accepted and read as the directory it starts with, so a
 * config copied from there works; bay ships no glob engine and adding a
 * dependency for one path shape is not worth it.
 */

import * as fsp from "node:fs/promises";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { isJobClass, type JobClass } from "./Job.js";

/** Where `make:job` writes, and where discovery looks when nothing is declared. */
export const DEFAULT_JOBS_DIR = "app/jobs";

let jobsDir = DEFAULT_JOBS_DIR;

/** @internal Told by the provider what the config declared. */
export function setJobsDir(dir: string): void {
	jobsDir = dir;
}

/** Where job files live — the first `locations` entry, or the default. */
export function getJobsDir(): string {
	return jobsDir;
}

/**
 * The directory a `locations` entry names.
 *
 * Everything from the first glob character on is dropped: `app/jobs/** /*.ts`
 * and `app/jobs` name the same directory, and the walk below is recursive
 * either way.
 */
export function directoryOf(location: string): string {
	const withoutGlob = location.split(/[*?[{]/)[0] ?? location;
	const trimmed = withoutGlob.replace(/\/+$/, "");
	return trimmed.replace(/^\.\//, "") || ".";
}

/** Every module file under `dir`, recursively. */
async function walk(dir: string, depth = 0): Promise<string[]> {
	// A jobs directory is a flat convention with the occasional subdirectory;
	// an unbounded walk would follow whatever happens to live under it.
	if (depth > 8) return [];
	let entries: import("node:fs").Dirent[];
	try {
		entries = await fsp.readdir(dir, { withFileTypes: true });
	} catch (err) {
		// A declared directory that does not exist YET is not an error: a project
		// can name where its jobs will go before writing the first one.
		//
		// Anything else is. Reading every failure as "empty" meant a permission
		// denial, a broken mount or a path pointing at a file produced a worker
		// with no handlers and nothing said — the queue accepted jobs and
		// processed none of them.
		if ((err as NodeJS.ErrnoException).code === "ENOENT") return [];
		throw new Error(
			`[bay] cannot read the jobs directory '${dir}': ${
				err instanceof Error ? err.message : String(err)
			}`,
			{ cause: err },
		);
	}
	const found: string[] = [];
	for (const entry of entries) {
		const full = path.join(dir, entry.name);
		if (entry.isDirectory()) {
			found.push(...(await walk(full, depth + 1)));
			continue;
		}
		// `.d.ts` is a declaration, not a module with a job in it.
		if (/\.d\.[cm]?ts$/.test(entry.name)) continue;
		if (/\.[cm]?[jt]s$/.test(entry.name)) found.push(full);
	}
	return found.sort();
}

/**
 * Import every module under `locations` and return the job classes they
 * default-export.
 *
 * A module that throws on import is reported and skipped: one unfinished job
 * file must not stop the worker from running every other job.
 */
/** Name what was exported, so the message points at the actual mistake. */
function describe(value: unknown): string {
	if (typeof value === "function")
		return `a function named '${value.name || "(anonymous)"}'`;
	if (value === null) return "null";
	return typeof value;
}

export async function discoverJobs(
	locations: readonly string[],
	/**
	 * Turn a configured location into an absolute path.
	 *
	 * The host supplies it — `app.makePath` on ream — because `app/jobs` means
	 * "under the application root", not "under whatever directory the process
	 * happens to have started in". Resolving against `process.cwd()` gave a
	 * worker launched from anywhere else a silent empty discovery.
	 *
	 * Absent, the old cwd-relative behaviour stands: bay is agnostic, and a host
	 * with no notion of an application root has nothing better to offer.
	 */
	resolveLocation: (location: string) => string = (location) =>
		path.resolve(location),
): Promise<JobClass[]> {
	const found: JobClass[] = [];
	let scanned = 0;
	/** Files that could not be imported at all. */
	const failures: string[] = [];
	/** Files that imported cleanly but held no Job. */
	const rejected: string[] = [];
	for (const location of locations) {
		for (const file of await walk(resolveLocation(directoryOf(location)))) {
			scanned += 1;
			let module: unknown;
			try {
				module = await import(pathToFileURL(path.resolve(file)).href);
			} catch (err) {
				const message = err instanceof Error ? err.message : String(err);
				failures.push(`${file}: ${message}`);
				process.stderr.write(`[bay] could not load '${file}': ${message}\n`);
				continue;
			}
			if (typeof module !== "object" || module === null) {
				rejected.push(`${file}: the module is not an object`);
				continue;
			}
			const exported = Reflect.get(module, "default");
			if (isJobClass(exported)) {
				found.push(exported);
				continue;
			}
			// Loaded fine, exported the wrong thing. Skipped in silence before,
			// which is the shape a rename or a forgotten `export default` takes:
			// the file is there, it compiles, and the job never runs.
			const what =
				exported === undefined
					? "no default export"
					: `a default export that is not a Job subclass (${describe(exported)})`;
			rejected.push(`${file}: ${what}`);
			process.stderr.write(`[bay] ignoring '${file}' — ${what}\n`);
		}
	}
	// Skipping ONE unusable file so the others still run is the point of the
	// catch above. Ending with nothing at all is a different thing: the worker
	// comes up, accepts jobs and processes none.
	//
	// The reason no longer matters. This used to fire only when every file
	// failed to LOAD, so a directory of files that all compiled and all exported
	// the wrong thing — a rename, a forgotten `export default` — produced an
	// empty discovery and a silent worker.
	if (found.length === 0 && scanned > 0) {
		throw new Error(
			`[bay] ${scanned} job file(s) were scanned and none yielded a Job, so the worker has no handlers:\n  ${[...failures, ...rejected].join("\n  ")}`,
		);
	}
	return found;
}
