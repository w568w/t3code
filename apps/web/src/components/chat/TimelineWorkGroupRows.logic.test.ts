import { EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { formatWaitCountdownNow } from "./TimelineWorkGroupRows.logic";

describe("formatWaitCountdownNow", () => {
  it("formats remaining seconds, minutes, and overdue countdowns", () => {
    const env = EnvironmentId.make("env-local");
    const nowMs = Date.parse("2026-06-01T12:00:00.000Z");
    expect(formatWaitCountdownNow("2026-06-01T12:00:42.000Z", env, nowMs)).toBe("in 42s");
    expect(formatWaitCountdownNow("2026-06-01T12:04:32.000Z", env, nowMs)).toBe("in 4m 32s");
    expect(formatWaitCountdownNow("2026-06-01T11:59:59.000Z", env, nowMs)).toBe("due now");
  });
});
