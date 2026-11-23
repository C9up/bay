import { Exception } from "@c9up/flare";
import { describe, expect, it } from "vitest";
import { errors } from "../../src/index.js";

describe("errors", () => {
	it("are exported from the entry as flare exceptions, one class per code", () => {
		const error = new errors.E_BAY_INVALID_ATTEMPTS(["attempts", "0"]);
		expect(error).toBeInstanceOf(Exception);
		expect(error).toMatchObject({
			code: "E_BAY_INVALID_ATTEMPTS",
			status: 500,
			message: "attempts must be a whole number >= 1, got 0",
		});
		for (const [name, ErrorClass] of Object.entries(errors)) {
			expect(ErrorClass.code).toBe(name);
		}
	});

	it("carries the job and the timeout on E_JOB_TIMEOUT", () => {
		const error = new errors.E_JOB_TIMEOUT(["send-email", 500]);
		expect(error).toBeInstanceOf(Exception);
		expect(error).toMatchObject({
			code: "E_JOB_TIMEOUT",
			status: 500,
			job: "send-email",
			timeout: 500,
			message: 'The job "send-email" has exceeded the timeout of 500ms',
		});
	});

	it("keeps the queue library's codes, statuses and messages", () => {
		expect(new errors.E_CONFIGURATION_ERROR(["no adapter"])).toMatchObject({
			code: "E_CONFIGURATION_ERROR",
			status: 500,
			message: "Configuration error. Reason: no adapter",
		});
		expect(new errors.E_JOB_NOT_FOUND(["send-email"])).toMatchObject({
			code: "E_JOB_NOT_FOUND",
			message: 'Requested job "send-email" is not registered',
		});
		expect(new errors.E_JOB_STALLED(["send-email", 1])).toMatchObject({
			code: "E_JOB_STALLED",
			message: 'The job "send-email" stalled more than the allowed 1 time(s)',
		});
		expect(new errors.E_ADAPTER_INIT_ERROR(["redis", "down"])).toMatchObject({
			code: "E_ADAPTER_INIT_ERROR",
			status: 500,
			message: 'Failed to initialize adapter "redis". Reason: down',
		});
	});
});
