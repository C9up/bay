/**
 * `ream configure @c9up/bay`.
 *
 * The hook is what makes `ream add` mean installed AND working: the provider
 * alone reads a config file that would not exist.
 */
import { describe, expect, it } from "vitest";
import { configure } from "../../src/configure.js";

function fakeCodemods() {
	const providers: string[] = [];
	const files: Array<{ path: string; content: string }> = [];
	const env: Record<string, string> = {};
	return {
		providers,
		files,
		env,
		codemods: {
			async addProvider(importPath: string) {
				providers.push(importPath);
			},
			async addEnvVars(vars: Record<string, string>) {
				Object.assign(env, vars);
			},
			async writeFile(path: string, content: string) {
				files.push({ path, content });
			},
		},
	};
}

describe("bay > configure", () => {
	it("registers the provider and writes the config it reads", async () => {
		const { providers, files, codemods } = fakeCodemods();

		await configure(codemods);

		expect(providers).toEqual(["@c9up/bay/provider"]);
		expect(files.map((f) => f.path)).toEqual(["config/queue.ts"]);
	});

	it("declares the environment variables the config reads", async () => {
		const { env, files, codemods } = fakeCodemods();

		await configure(codemods);

		// Writing the file without them leaves an application whose config asks
		// the environment for something nothing ever put there.
		expect(env).toHaveProperty("QUEUE_STORE");
		for (const key of Object.keys(env)) {
			expect(files[0]?.content).toContain(key);
		}
	});

	it("writes a config that imports from the package it configures", async () => {
		const { files, codemods } = fakeCodemods();

		await configure(codemods);

		// A stub importing the wrong package typechecks nowhere and is the one
		// mistake a generated file must not make.
		expect(files[0]?.content).toContain("from '@c9up/bay'");
	});
});
