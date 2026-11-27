import { toServerClock } from "@t3tools/client-runtime/clock";
import type { EnvironmentId } from "@t3tools/contracts";
import { formatDuration } from "@t3tools/shared/orchestrationTimingCopy";
import { msg } from "gt-react";
import { m } from "~/i18n/messages";

export function formatWorkingTimerNow(startIso: string, environmentId: EnvironmentId): string {
  const startedAtMs = Date.parse(startIso);
  if (!Number.isFinite(startedAtMs)) {
    return m(msg("{seconds}s", { seconds: 0 }));
  }

  const elapsedSeconds = Math.max(
    0,
    Math.floor((toServerClock(environmentId, Date.now()) - startedAtMs) / 1000),
  );
  if (elapsedSeconds < 60) {
    return m(msg("{seconds}s", { seconds: elapsedSeconds }));
  }

  return formatDuration(elapsedSeconds * 1_000, m);
}

export function formatWaitCountdownNow(
  wakeAtIso: string,
  environmentId: EnvironmentId,
  nowMs = Date.now(),
): string {
  const remainingSeconds = Math.ceil(
    (Date.parse(wakeAtIso) - toServerClock(environmentId, nowMs)) / 1000,
  );
  if (!Number.isFinite(remainingSeconds) || remainingSeconds <= 0) {
    return m(msg("due now", { $context: "scheduled timer countdown" }));
  }
  return m(msg("in {duration}", { duration: formatDuration(remainingSeconds * 1_000, m) }));
}
