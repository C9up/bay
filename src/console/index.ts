/**
 * `@c9up/bay/commands` — the commands bay ships.
 *
 *   // reamrc.ts, written by `configure()`
 *   commands: [() => import('@c9up/bay/commands')]
 *
 * A module answering `getMetaData()` / `getCommand()`, which is how a package
 * adds commands: by shipping them, never by a change to the `ream` binary.
 *
 * `jobsDir` is read through a getter, not captured: these classes are built
 * when the module is imported — before the application boots — while `run()`
 * happens after it, when the config exists.
 */

import { getJobsDir } from "../jobs.js";
import type { BayCommandClass } from "./contract.js";
import { makeJobCommand } from "./makeJob.js";
import { queueWorkCommand } from "./queueWork.js";

const COMMANDS: readonly BayCommandClass[] = [
	queueWorkCommand(),
	makeJobCommand({
		get jobsDir() {
			return getJobsDir();
		},
	}),
];

/** What the kernel reads to list a command without importing it. */
interface CommandMetaData {
	commandName: string;
	namespace: string | null;
	description: string;
	help?: string | string[];
	aliases: string[];
	options: Record<string, unknown>;
	args: readonly unknown[];
	flags: readonly unknown[];
}

function serialize(command: BayCommandClass): CommandMetaData {
	const colon = command.commandName.indexOf(":");
	return {
		commandName: command.commandName,
		namespace: colon === -1 ? null : command.commandName.slice(0, colon),
		description: command.description,
		help: command.help,
		aliases: [],
		options: { ...command.options },
		args: command.args ?? [],
		flags: command.flags ?? [],
	};
}

export async function getMetaData(): Promise<CommandMetaData[]> {
	return COMMANDS.map(serialize);
}

export async function getCommand(
	metadata: CommandMetaData,
): Promise<BayCommandClass | null> {
	return (
		COMMANDS.find((command) => command.commandName === metadata.commandName) ??
		null
	);
}

export type { BayCommandClass } from "./contract.js";
