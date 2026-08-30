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
	writeFile(
		filePath: string,
		content: string,
		options?: { force?: boolean },
	): Promise<void>;
}

export async function configure(codemods: Codemods): Promise<void> {
	await codemods.addProvider("@c9up/bay/provider");
	await codemods.writeFile(
		"config/queue.ts",
		`import { defineConfig, stores } from '@c9up/bay'
import env from '#start/env'

export default defineConfig({
  // Which store to run on. Memory forgets everything on restart, which is
  // what a single process in development wants and nothing else does.
  default: env.get('QUEUE_STORE', 'memory'),

  stores: {
    memory: stores.memory(),
    redis: stores.redis({ connection: 'main' }),
  },
})`,
	);
}
