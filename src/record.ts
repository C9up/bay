/**
 * Reading a stored job record back.
 *
 * Both drivers store a record as JSON — the memory driver too, so a test run
 * against it sees exactly what a worker reading Redis would — and both read it
 * back through here.
 */

import { E_BAY_UNSERIALIZABLE_JOB } from "./errors.js";
import type { JobRecord } from "./QueueManager.js";

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
export interface Unreadable {
	reason: string;
	id?: string;
	name?: string;
	queue?: string;
}

export type Parsed =
	| { ok: true; job: JobRecord }
	| ({ ok: false } & Unreadable);

function stringField(obj: object, key: string): string | undefined {
	const value: unknown = Reflect.get(obj, key);
	return typeof value === "string" ? value : undefined;
}

/** Read a stored record, saying what is wrong with one that cannot run. */
export function parseJob(raw: string): Parsed {
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

/**
 * A copy of `job` as JSON would carry it. Throws where JSON cannot carry it
 * — a BigInt — which is what the Redis driver does at the same point.
 */
export function snapshot(job: JobRecord): JobRecord {
	const parsed = parseJob(JSON.stringify(job));
	if (!parsed.ok) {
		throw new E_BAY_UNSERIALIZABLE_JOB([job.id, parsed.reason]);
	}
	return parsed.job;
}
