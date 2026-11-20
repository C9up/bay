/**
 * The fluent builders behind `Job.dispatch()` and `Job.dispatchMany()` —
 * upstream's `JobDispatcher` / `JobBatchDispatcher`.
 *
 *     const { jobId } = await SendEmail.dispatch({ to }).toQueue('emails').in('5m')
 *     const { jobIds } = await SendEmail.dispatchMany(payloads).priority(1)
 *
 * Each sends through the queue bound to `services/main` (by the provider, or
 * by `setQueue`), as upstream's go through its `QueueManager`. Awaiting one is
 * what sends it; `run()` does the same explicitly.
 *
 * Upstream's `group()` and `with(adapter)` are not offered: bay has no job
 * groups, and one queue manager drives one store.
 */

import type { Duration, JobClass } from "./Job.js";
import type { DispatchOptions, QueueManager } from "./QueueManager.js";
import { getQueue } from "./services/main.js";

/** What `Job.dispatch()` answers once sent — upstream's `DispatchResult`. */
export interface DispatchResult {
	jobId: string;
}

/** What `Job.dispatchMany()` answers once sent. */
export interface DispatchManyResult {
	jobIds: string[];
}

function boundQueue(): QueueManager {
	const queue = getQueue();
	if (queue === undefined) {
		throw new Error(
			"[bay] Job.dispatch() needs a queue: register BayProvider, or bind one with setQueue(queue) from '@c9up/bay/services/main'.",
		);
	}
	return queue;
}

export class JobDispatcher<Payload> implements PromiseLike<DispatchResult> {
	readonly #job: JobClass<Payload>;
	readonly #payload: Payload;
	readonly #options: DispatchOptions = {};

	constructor(job: JobClass<Payload>, payload: Payload) {
		this.#job = job;
		this.#payload = payload;
	}

	/** Send it to this queue instead of the class's. */
	toQueue(queue: string): this {
		this.#options.queue = queue;
		return this;
	}

	/** Hold it back for this long before any worker may take it. */
	in(delay: Duration): this {
		this.#options.delay = delay;
		return this;
	}

	/** 1 to 10, lower runs first. */
	priority(priority: number): this {
		this.#options.priority = priority;
		return this;
	}

	/** Queue it only if no job of the same class holds this id. */
	dedup(options: { id: string; ttl?: Duration; extend?: boolean }): this {
		this.#options.dedup = options;
		return this;
	}

	async run(): Promise<DispatchResult> {
		const jobId = await boundQueue().dispatch(
			this.#job,
			this.#payload,
			this.#options,
		);
		return { jobId };
	}

	// biome-ignore lint/suspicious/noThenProperty: upstream's dispatcher is awaited directly (`await Job.dispatch(p)`), which is what a `then` is for.
	then<TResult1 = DispatchResult, TResult2 = never>(
		onFulfilled?:
			| ((value: DispatchResult) => TResult1 | PromiseLike<TResult1>)
			| null,
		onRejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
	): Promise<TResult1 | TResult2> {
		return this.run().then(onFulfilled, onRejected);
	}
}

export class JobBatchDispatcher<Payload>
	implements PromiseLike<DispatchManyResult>
{
	readonly #job: JobClass<Payload>;
	readonly #payloads: readonly Payload[];
	readonly #options: DispatchOptions = {};

	constructor(job: JobClass<Payload>, payloads: readonly Payload[]) {
		this.#job = job;
		this.#payloads = payloads;
	}

	toQueue(queue: string): this {
		this.#options.queue = queue;
		return this;
	}

	priority(priority: number): this {
		this.#options.priority = priority;
		return this;
	}

	async run(): Promise<DispatchManyResult> {
		const jobIds = await boundQueue().dispatchMany(
			this.#job,
			this.#payloads,
			this.#options,
		);
		return { jobIds };
	}

	// biome-ignore lint/suspicious/noThenProperty: awaited directly, as upstream's batch dispatcher is.
	then<TResult1 = DispatchManyResult, TResult2 = never>(
		onFulfilled?:
			| ((value: DispatchManyResult) => TResult1 | PromiseLike<TResult1>)
			| null,
		onRejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
	): Promise<TResult1 | TResult2> {
		return this.run().then(onFulfilled, onRejected);
	}
}
