import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		coverage: {
			provider: "v8",
			include: ["src/**"],
			exclude: ["src/**/*.d.ts"],
			reporter: ["text-summary", "json-summary"],
			// Set just under what the suite actually reaches, and now actually
			// run: `test:coverage` is a CI step, so these are a gate rather than
			// a wish. They read 98/97/92/98 while nothing ever checked them —
			// numbers from when bay was a driver and a dispatch loop, and out of
			// reach the moment it grew job classes, named queues and commands.
			// A threshold nothing runs cannot be wrong, which is the problem.
			thresholds: {
				lines: 94,
				statements: 93,
				branches: 87,
				functions: 95,
			},
		},
	},
});
