/**
 * QueueManager — dispatch and process background jobs.
 *
 * Usage:
 *   queue.register('send-email', new SendEmailHandler())
 *   await queue.dispatch('send-email', { to: 'user@example.com' })
 *   queue.work()
 */

export interface Job {
	id: string;
	name: string;
	payload: unknown;
	attempts: number;
	maxAttempts: number;
	status: "pending" | "processing" | "completed" | "failed";
	error?: string;
	createdAt: number;
	processedAt?: number;
}

export interface JobHandler {
	handle(payload: unknown): Promise<void>;
}

export interface QueueDriver {
	push(job: Job): Promise<void>;
	pop(): Promise<Job | null>;
	fail(job: Job, error: string): Promise<void>;
	complete(job: Job): Promise<void>;
	retry(job: Job): Promise<void>;
	failed(): Promise<Job[]>;
	size(): Promise<number>;
}

export class QueueManager {
	private driver: QueueDriver;
	private handlers: Map<string, JobHandler | (new () => JobHandler)> =
		new Map();
	private running = false;
	private inflightPromise: Promise<boolean> | null = null;

	constructor(driver: QueueDriver) {
		this.driver = driver;
	}

	/** Register a job handler. */
	register(name: string, handler: JobHandler | (new () => JobHandler)): void {
		this.handlers.set(name, handler);
	}

	/** Dispatch a job to the queue. */
	async dispatch(
		name: string,
		payload: unknown,
		options?: { maxAttempts?: number },
	): Promise<string> {
		if (options?.maxAttempts !== undefined && options.maxAttempts < 1) {
			throw new Error("maxAttempts must be >= 1");
		}
		const id = `job_${crypto.randomUUID()}`;
		const job: Job = {
			id,
			name,
			payload,
			attempts: 0,
			maxAttempts: options?.maxAttempts ?? 3,
			status: "pending",
			createdAt: Date.now(),
		};
		await this.driver.push(job);
		return id;
	}

	/** Process the next job in the queue. */
	async processOne(): Promise<boolean> {
		const job = await this.driver.pop();
		if (!job) return false;

		const handlerOrClass = this.handlers.get(job.name);
		if (!handlerOrClass) {
			process.stderr.write(
				`QueueManager: no handler registered for job '${job.name}'\n`,
			);
			await this.driver.fail(job, `No handler registered for job: ${job.name}`);
			return true;
		}

		const handler =
			typeof handlerOrClass === "function"
				? new handlerOrClass()
				: handlerOrClass;
		job.attempts++;
		job.status = "processing";
		job.processedAt = Date.now();

		try {
			await handler.handle(job.payload);
			job.status = "completed";
			await this.driver.complete(job);
		} catch (err) {
			const errorMsg = err instanceof Error ? err.message : String(err);
			if (job.attempts < job.maxAttempts) {
				job.status = "pending";
				await this.driver.retry(job);
			} else {
				job.status = "failed";
				job.error = errorMsg;
				await this.driver.fail(job, errorMsg);
			}
		}

		return true;
	}

	/** Start processing jobs continuously. */
	async work(pollIntervalMs = 1000): Promise<void> {
		if (pollIntervalMs <= 0) {
			throw new Error("pollIntervalMs must be positive");
		}
		if (this.running) {
			throw new Error("QueueManager is already running");
		}
		this.running = true;
		while (this.running) {
			try {
				this.inflightPromise = this.processOne();
				const processed = await this.inflightPromise;
				if (!processed) {
					await new Promise((r) => setTimeout(r, pollIntervalMs));
				}
			} catch (err) {
				process.stderr.write(
					`QueueManager processOne error: ${err instanceof Error ? err.message : String(err)}\n`,
				);
				await new Promise((r) => setTimeout(r, pollIntervalMs));
			} finally {
				this.inflightPromise = null;
			}
		}
	}

	/** Await the currently in-flight processOne, if any. */
	async drain(): Promise<void> {
		if (this.inflightPromise) {
			await this.inflightPromise.catch(() => {});
		}
	}

	/** Stop the worker. */
	async stop(): Promise<void> {
		this.running = false;
		await this.drain();
	}

	/** Get failed jobs. */
	async failedJobs(): Promise<Job[]> {
		return this.driver.failed();
	}

	/** Get queue size. */
	async size(): Promise<number> {
		return this.driver.size();
	}
}
