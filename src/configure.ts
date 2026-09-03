/**
 * `ream configure @c9up/bay` — wire the job queue in one command.
 *
 * The provider alone is not enough: it reads `config/queue.ts`, and a package
 * registered without one falls back to a default that is rarely the one an
 * application wants. Writing both together is what makes `ream add` mean
 * installed AND working.
 */

interface Codemods {
	addProvider(importPath: string): Promise<void>;
	addEnvVars(vars: Record<string, string>): Promise<void>;
	writeFile(
		filePath: string,
		content: string,
		options?: { force?: boolean },
	): Promise<void>;
}

export async function configure(codemods: Codemods): Promise<void> {
	// The config below reads these, so they are declared here. Writing the file
	// without them leaves an application whose config asks the environment for
	// something nothing ever put there.
	await codemods.addEnvVars({
		QUEUE_DRIVER: "memory",
	});

	await codemods.addProvider("@c9up/bay/provider");
	await codemods.writeFile(
		"config/queue.ts",
		`import { defineConfig, drivers } from '@c9up/bay'
import env from '#start/env'

export default defineConfig({
  // Which adapter to run on. Memory forgets everything on restart, which is
  // what a single process in development wants and nothing else does.
  default: env.get('QUEUE_DRIVER', 'memory'),

  adapters: {
    memory: drivers.memory(),
    redis: drivers.redis({ connection: 'main' }),
  },

  // What the worker does between jobs: how long it waits after finding
  // nothing, and how often it reclaims jobs a crashed worker left behind.
  worker: {
    idleDelay: 2_000,
    stalledInterval: 30_000,
  },
})`,
	);
}
