/**
 * Narrow away `undefined` without a `!` assertion.
 *
 * A `!` silences the compiler by asserting; this proves it, and a fixture that
 * stopped producing the value fails on the line that reads it rather than
 * several frames later on a property of `undefined`.
 */
export function defined<T>(value: T | null | undefined, what = "value"): T {
	if (value == null) throw new Error(`expected a defined ${what}`);
	return value;
}
