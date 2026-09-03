/**
 * Teach ream's `ContainerBindings` what `container.make('queue')` returns.
 *
 * ream declares that interface open on purpose: it registers its own entries
 * and expects each package to contribute the one it owns. Nothing filled this
 * one in, so resolving by the string token answered `unknown` and every call
 * site had to assert a type it could not prove.
 *
 * Loaded from the package barrel and from the provider, so registering bay is
 * enough — an application writes no `declare module` of its own.
 *
 * Type-only, and ream stays an OPTIONAL peer: nothing here reaches a runtime
 * import, and a `declare module` for a specifier that does not resolve is
 * simply inert.
 */

// Referenced so the augmentation below resolves the module it augments.
import type {} from "@c9up/ream/types";
import type { QueueManager } from "./QueueManager.js";

declare module "@c9up/ream/types" {
	interface ContainerBindings {
		/** The job queue, bound by `BayProvider`. */
		"bay.queue": QueueManager;
		/**
		 * The same binding under the name it had before the token carried its
		 * package. Kept bound so an existing `container.make(...)` resolves.
		 */
		queue: QueueManager;
	}
}
