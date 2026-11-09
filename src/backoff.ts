/**
 * How long a failed job waits before its next attempt — upstream's backoff
 * strategies, by their names and defaults.
 *
 *     static options = {
 *       maxRetries: 5,
 *       retry: { backoff: exponentialBackoff({ baseDelay: '1s', maxDelay: '5m' }) },
 *     }
 *
 * Without one a retry is queued again at once, and a job failing because a
 * service it calls is down spends every attempt within milliseconds.
 */

import { type Duration, toMilliseconds } from "./Job.js";

export interface BackoffConfig {
	strategy: "exponential" | "linear" | "fixed";
	/** The first delay. */
	baseDelay: Duration;
	/** The ceiling no delay goes past. */
	maxDelay?: Duration;
	/** Growth factor of the exponential strategy. Default `2`. */
	multiplier?: number;
	/** Spread each delay by up to a quarter either way. */
	jitter?: boolean;
}

/**
 * A strategy as data, in milliseconds — what a job record carries, so a
 * worker that never saw the job class still waits the way it asked.
 */
export interface ResolvedBackoff {
	strategy: BackoffConfig["strategy"];
	baseDelay: number;
	maxDelay?: number;
	multiplier?: number;
	jitter?: boolean;
}

/** Called once per dispatch; the strategy it returns is stored on the record. */
export type BackoffFactory = () => BackoffStrategy;

const STRATEGIES = new Set(["exponential", "linear", "fixed"]);

export class BackoffStrategy {
	readonly #config: ResolvedBackoff;

	constructor(config: BackoffConfig | ResolvedBackoff) {
		if (!STRATEGIES.has(config.strategy)) {
			throw new Error(
				`backoff strategy must be exponential, linear or fixed, got ${String(config.strategy)}`,
			);
		}
		const baseDelay = toMilliseconds(config.baseDelay, "backoff baseDelay");
		if (!(baseDelay > 0)) {
			throw new Error("backoff baseDelay must be greater than zero");
		}
		const maxDelay =
			config.maxDelay === undefined
				? undefined
				: toMilliseconds(config.maxDelay, "backoff maxDelay");
		if (maxDelay !== undefined && maxDelay <= baseDelay) {
			throw new Error("backoff maxDelay must be greater than baseDelay");
		}
		const multiplier = config.multiplier;
		if (multiplier !== undefined) {
			if (!Number.isFinite(multiplier) || multiplier <= 0) {
				throw new Error("backoff multiplier must be a number above zero");
			}
			if (config.strategy === "exponential" && multiplier < 1) {
				throw new Error("an exponential backoff multiplier must be >= 1");
			}
		}
		this.#config = {
			strategy: config.strategy,
			baseDelay,
			maxDelay,
			multiplier,
			jitter: config.jitter,
		};
	}

	/** The delay before attempt `attempt + 1`, after attempt `attempt` failed (1-based). */
	calculateDelay(attempt: number): number {
		if (!Number.isInteger(attempt) || attempt < 1) {
			throw new Error("backoff attempt number must be a whole number >= 1");
		}
		const { strategy, baseDelay, maxDelay, jitter } = this.#config;
		const multiplier = this.#config.multiplier ?? 2;
		let delay =
			strategy === "exponential"
				? baseDelay * multiplier ** (attempt - 1)
				: strategy === "linear"
					? baseDelay * attempt
					: baseDelay;
		delay = Math.min(delay, maxDelay ?? Number.POSITIVE_INFINITY);
		if (jitter) {
			const range = delay * 0.25;
			delay = Math.max(0, delay + (Math.random() - 0.5) * 2 * range);
		}
		return Math.floor(delay);
	}

	/** When the next attempt may run, as a date. */
	getNextRetryAt(attempt: number): Date {
		return new Date(Date.now() + this.calculateDelay(attempt));
	}

	/** The strategy as plain data, in milliseconds. */
	getConfig(): ResolvedBackoff {
		return { ...this.#config };
	}
}

/** 1s, 2s, 4s… up to 5m, with jitter — upstream's defaults. */
export function exponentialBackoff(
	config: Partial<Omit<BackoffConfig, "strategy">> = {},
): BackoffFactory {
	return () =>
		new BackoffStrategy({
			strategy: "exponential",
			baseDelay: "1s",
			maxDelay: "5m",
			multiplier: 2,
			jitter: true,
			...config,
		});
}

/** 5s, 10s, 15s… up to 2m — upstream's defaults. */
export function linearBackoff(
	config: Partial<Omit<BackoffConfig, "strategy">> = {},
): BackoffFactory {
	return () =>
		new BackoffStrategy({
			strategy: "linear",
			baseDelay: "5s",
			maxDelay: "2m",
			...config,
		});
}

/** The same delay every time. Default `10s`. */
export function fixedBackoff(delay: Duration = "10s"): BackoffFactory {
	return () => new BackoffStrategy({ strategy: "fixed", baseDelay: delay });
}

/** A strategy spelled out in full. */
export function customBackoff(config: BackoffConfig): BackoffFactory {
	return () => new BackoffStrategy(config);
}

/** A stored strategy read back, or `undefined` for one that is not. */
export function readBackoff(value: unknown): BackoffStrategy | undefined {
	if (typeof value !== "object" || value === null) return undefined;
	const strategy: unknown = Reflect.get(value, "strategy");
	const baseDelay: unknown = Reflect.get(value, "baseDelay");
	const maxDelay: unknown = Reflect.get(value, "maxDelay");
	const multiplier: unknown = Reflect.get(value, "multiplier");
	const jitter: unknown = Reflect.get(value, "jitter");
	if (
		(strategy !== "exponential" &&
			strategy !== "linear" &&
			strategy !== "fixed") ||
		typeof baseDelay !== "number" ||
		(maxDelay !== undefined && typeof maxDelay !== "number") ||
		(multiplier !== undefined && typeof multiplier !== "number") ||
		(jitter !== undefined && typeof jitter !== "boolean")
	) {
		return undefined;
	}
	try {
		return new BackoffStrategy({
			strategy,
			baseDelay,
			maxDelay,
			multiplier,
			jitter,
		});
	} catch {
		return undefined;
	}
}
