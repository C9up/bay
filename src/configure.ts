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
	registerCommand(importPath: string): Promise<void>;
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
	// `queue:work` and `make:job` are the package's, not the binary's: a
	// project reaches them by listing the module, never by upgrading `ream`.
	await codemods.registerCommand("@c9up/bay/commands");
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
  // nothing, how often it reclaims jobs a crashed worker left behind, how many
  // it runs at once, and which named queues it serves.
  worker: {
    idleDelay: 2_000,
    stalledInterval: 30_000,
    concurrency: 1,
    // queues: ['critical', 'default'],
  },

  // Where the job classes live. Every module under here is imported at boot,
  // so a worker resolves a queued record by the class's own name.
  locations: ['app/jobs'],
})`,
	);
}
