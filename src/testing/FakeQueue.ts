/**
 * In-memory `QueueDriver` for tests — captures every dispatched
 * job and exposes Adonis/Laravel-style `assertPushed` /
 * `assertNotPushed` helpers in the same shape as Rover's
 * `FakeMail`.
 *
 * Not re-exported from the main barrel; reach via
 * `@c9up/bay/testing` so production code never accidentally pulls
 * the fake into a runtime build.
 */

import type { Job, QueueDriver } from "../QueueManager.js";

export interface FakeQueuePredicate {
	/** Custom payload predicate — receives the job's `payload` and
	 *  returns `true` to match. The job name is always taken from the
	 *  positional `name` argument of `assertPushed` / `assertNotPushed`
	 *  — there is no `name` field on the predicate (would create a
	 *  silent override footgun where `assertPushed('a', { name: 'b' })`
	 *  matches 'b' but the error message says 'a'). */
	payloadMatches?: (payload: unknown) => boolean;
}

export type FakeQueuePredicateArg =
	| FakeQueuePredicate
	| ((job: Job) => boolean);

export class FakeQueue implements QueueDriver {
	#pushed: Job[] = [];

	async push(job: Job): Promise<void> {
		// Reject duplicate ids to surface the most common test-fixture
		// mistake — two `makeJob({ id: 'x' })` reused across pushes
		// silently corrupts later `fail`/`complete`/`retry` lookups.
		if (this.#pushed.some((j) => j.id === job.id)) {
			throw new Error(
				`FakeQueue: duplicate job id '${job.id}' — each push must use a unique id (the real drivers enforce this implicitly via crypto.randomUUID()).`,
			);
		}
		this.#pushed.push(job);
	}

	/** Always returns `null` — fake queues never auto-dispatch.
	 *  Tests that need handler execution should use the memory
	 *  driver directly. */
	async pop(): Promise<Job | null> {
		return null;
	}

	async fail(job: Job, error: string): Promise<void> {
		const found = this.#requireJob(job, "fail");
		found.status = "failed";
		found.error = error;
	}

	async complete(job: Job): Promise<void> {
		const found = this.#requireJob(job, "complete");
		found.status = "completed";
		found.processedAt = Date.now();
	}

	async retry(job: Job): Promise<void> {
		const found = this.#requireJob(job, "retry");
		found.attempts += 1;
		found.status = "pending";
		// Reset transient state from the prior failure so a retried
		// job's invariants match a fresh push (a real driver would
		// either drop these on requeue or rely on the worker to clear
		// them — we mirror "drop").
		found.error = undefined;
		found.processedAt = undefined;
	}

	/** Look up the captured copy of a job by id. Throws when the id
	 *  isn't present — silent no-op on a missing job is the most
	 *  insidious test bug (caller's local Job ref shows the new
	 *  status while the FakeQueue's internal capture is unchanged). */
	#requireJob(job: Job, verb: string): Job {
		const found = this.#pushed.find((j) => j.id === job.id);
		if (!found) {
			throw new Error(
				`FakeQueue.${verb}: job id '${job.id}' is not in the queue. Did you forget to push it, or is it from a different FakeQueue instance?`,
			);
		}
		return found;
	}

	async failed(): Promise<Job[]> {
		return this.#pushed
			.filter((j) => j.status === "failed")
			.map((j) => ({ ...j }));
	}

	async size(): Promise<number> {
		return this.#pushed.filter((j) => j.status === "pending").length;
	}

	/**
	 * Defensive snapshot of every captured job. Each entry is a
	 * shallow clone so test-side mutations can't bleed back into the
	 * internal capture store — avoids cross-test contamination.
	 */
	getPushed(): Job[] {
		return this.#pushed.map((j) => ({ ...j }));
	}

	reset(): void {
		this.#pushed = [];
	}

	assertPushed(name: string, predicate?: FakeQueuePredicateArg): void {
		const match = makeMatcher(name, predicate);
		if (this.#pushed.some(match)) return;
		throw new Error(
			`queue.assertPushed('${name}'${describePredicate(predicate)}) failed — no captured job matches.\n${describeCaptured(this.#pushed)}`,
		);
	}

	assertNotPushed(name: string, predicate?: FakeQueuePredicateArg): void {
		const match = makeMatcher(name, predicate);
		const found = this.#pushed.find(match);
		if (!found) return;
		throw new Error(
			`queue.assertNotPushed('${name}'${describePredicate(predicate)}) failed — at least one captured job matches.\n${describeCaptured(this.#pushed)}`,
		);
	}
}

function makeMatcher(
	name: string,
	predicate: FakeQueuePredicateArg | undefined,
): (j: Job) => boolean {
	// Function-form predicate — caller does ALL the matching, the
	// `name` arg is still a hard prerequisite.
	if (typeof predicate === "function") {
		return (j) => j.name === name && predicate(j);
	}
	if (predicate === undefined) {
		return (j) => j.name === name;
	}
	// Object-form: positional `name` is the contract; the predicate
	// narrows further via `payloadMatches`.
	return (j) => {
		if (j.name !== name) return false;
		if (predicate.payloadMatches && !predicate.payloadMatches(j.payload)) {
			return false;
		}
		return true;
	};
}

function describePredicate(
	predicate: FakeQueuePredicateArg | undefined,
): string {
	if (predicate === undefined) return "";
	if (typeof predicate === "function") return ", <function predicate>";
	// Empty object collapses to a name-only match — say so explicitly
	// so a mistaken `assertPushed('x', {})` is not mistaken for "I'm
	// narrowing by some predicate I forgot to fill in".
	if (Object.keys(predicate).length === 0)
		return ", <empty predicate (name-only)>";
	return `, ${JSON.stringify(predicate)}`;
}

function describeCaptured(captured: Job[]): string {
	if (captured.length === 0) return "Captured: (none)";
	const lines = captured.map(
		(j, i) =>
			`  [${i}] name="${j.name}" status=${j.status} attempts=${j.attempts}/${j.maxAttempts}`,
	);
	return `Captured (${captured.length}):\n${lines.join("\n")}`;
}
