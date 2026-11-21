/**
 * The two scripting commands of a fake Redis client, behaving as a server does.
 *
 * `script("LOAD", lua)` stores the script under its SHA1 digest; `evalsha`
 * answers NOSCRIPT for a digest it was never given, exactly like a server that
 * restarted or flushed its script cache, and otherwise hands the loaded script
 * to `reply` for the answer. The fakes cannot run Lua, so `reply` stands in for
 * what the script would have answered.
 */
import { createHash } from "node:crypto";
import { vi } from "vitest";

export function scripting(reply: (lua: string) => unknown = () => null) {
	const loaded = new Map<string, string>();
	return {
		evalsha: vi.fn(async (sha1: string): Promise<unknown> => {
			const lua = loaded.get(sha1);
			if (lua === undefined) throw new Error("NOSCRIPT No matching script");
			return reply(lua);
		}),
		script: vi.fn(async (subcommand: "LOAD", lua: string): Promise<string> => {
			if (subcommand !== "LOAD") throw new Error(`unexpected ${subcommand}`);
			const sha1 = createHash("sha1").update(lua).digest("hex");
			loaded.set(sha1, lua);
			return sha1;
		}),
	};
}
