import { scopedThreadKey, scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { ThreadStructure } from "@t3tools/client-runtime/state/shell";
import type { EnvironmentId, ScheduledTask } from "@t3tools/contracts";
import { formatDuration } from "@t3tools/shared/orchestrationTimingCopy";
import * as Predicate from "effect/Predicate";
import { msg } from "gt-react";
import { m, type MessageResolver } from "~/i18n/messages";

export type ActiveScheduledTask = ScheduledTask & {
  readonly nextRunAt: string;
  readonly blockedByPolicy: undefined;
};

export interface ActiveThreadTimerWait {
  readonly id: string;
  readonly title: string;
  readonly isCron: boolean;
  readonly wakeAt?: string | undefined;
  readonly meta?: string | undefined;
  readonly prompt?: string | undefined;
}

const trimNonEmpty = (value: unknown): string | undefined =>
  typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;

const positiveNumber = (value: unknown): number | undefined =>
  typeof value === "number" && Number.isFinite(value) && value > 0 ? value : undefined;

/**
 * Extracts active timer/schedule waits for the running run (`activeRunId`).
 * Reads Antigravity `schedule` parameters from `item.input` when present.
 */
export function extractActiveTimerWaits(
  turnItems: ThreadStructure["turnItems"] | undefined,
  activeRunId: string | null | undefined,
  resolve: MessageResolver = m,
): ReadonlyArray<ActiveThreadTimerWait> {
  if (!activeRunId || !Array.isArray(turnItems) || turnItems.length === 0) return [];
  const waits: Array<ActiveThreadTimerWait> = [];
  for (const item of turnItems) {
    if (
      item.type !== "dynamic_tool" ||
      item.wait?.kind !== "timer" ||
      (item.status !== "running" && item.status !== "waiting") ||
      item.runId !== activeRunId
    ) {
      continue;
    }
    const raw = Predicate.isObject(item.input) ? item.input : undefined;
    const { cron, wakeAt } = item.wait;
    const duration = positiveNumber(raw?.DurationSeconds);
    const maxRuns = positiveNumber(raw?.MaxIterations);
    const condition = trimNonEmpty(raw?.TimerCondition);
    const meta = [
      duration !== undefined ? formatDuration(duration * 1_000, resolve) : undefined,
      maxRuns !== undefined
        ? resolve(
            msg("max {count} runs", { count: maxRuns, $context: "scheduled timer max iterations" }),
          )
        : undefined,
      raw?.IsDaemon === true
        ? resolve(msg("daemon", { $context: "scheduled timer flag" }))
        : undefined,
      raw?.ExactTiming === true
        ? resolve(msg("exact timing", { $context: "scheduled timer flag" }))
        : undefined,
      condition && condition !== "never"
        ? resolve(
            msg("cancel on {condition}", {
              condition,
              $context: "scheduled timer cancel condition",
            }),
          )
        : undefined,
    ]
      .filter(Boolean)
      .join(" · ");
    const prompt = trimNonEmpty(raw?.Prompt) ?? item.wait.prompt;
    waits.push({
      id: String(item.id),
      title:
        cron ??
        (wakeAt
          ? resolve(msg("Timer", { $context: "scheduled wait label" }))
          : resolve(msg("Schedule", { $context: "scheduled wait label" }))),
      isCron: cron !== undefined,
      ...(wakeAt ? { wakeAt } : {}),
      ...(meta ? { meta } : {}),
      ...(prompt ? { prompt } : {}),
    });
  }
  return waits;
}

/**
 * A task is actively scheduled iff it has a concrete upcoming trigger and is not policy-blocked.
 * Disabled tasks already have `nextRunAt === null`.
 */
export function isScheduledTaskActive<
  T extends Pick<ScheduledTask, "nextRunAt" | "blockedByPolicy">,
>(task: T): task is T & { readonly nextRunAt: string; readonly blockedByPolicy: undefined } {
  return task.nextRunAt !== null && task.blockedByPolicy === undefined;
}

function compareActiveScheduledTasks(
  left: Pick<ActiveScheduledTask, "id" | "nextRunAt">,
  right: Pick<ActiveScheduledTask, "id" | "nextRunAt">,
): number {
  const leftMs = Date.parse(left.nextRunAt);
  const rightMs = Date.parse(right.nextRunAt);
  const a = Number.isFinite(leftMs) ? leftMs : Infinity;
  const b = Number.isFinite(rightMs) ? rightMs : Infinity;
  return a !== b ? a - b : left.id.localeCompare(right.id);
}

export function sortActiveScheduledTasks(
  tasks: ReadonlyArray<ScheduledTask>,
): ReadonlyArray<ActiveScheduledTask> {
  return tasks.filter(isScheduledTaskActive).toSorted(compareActiveScheduledTasks);
}

export function formatNextScheduledRunLabel(
  nextRunAt: string,
  nowMs: number,
  resolve: MessageResolver = m,
): string {
  const diffMs = Date.parse(nextRunAt) - nowMs;
  if (!Number.isFinite(diffMs) || diffMs <= 0) {
    return resolve(msg("due now", { $context: "scheduled task next run" }));
  }
  const minutes = Math.ceil(diffMs / 60_000);
  if (minutes < 2) return resolve(msg("in under a minute"));
  if (minutes < 60) return resolve(msg("in {minutes}m", { minutes }));
  const hours = Math.round(minutes / 60);
  if (hours < 24) return resolve(msg("in {hours}h", { hours }));
  return resolve(msg("in {days}d", { days: Math.round(hours / 24) }));
}

export interface SidebarThreadScheduledSummary {
  readonly count: number;
  readonly nextRunAt: string;
  readonly title: string;
}

/**
 * Aggregates active thread-bound scheduled tasks across connected environments and reuses
 * unchanged summary references so memoized sidebar rows skip unrelated updates.
 */
export function aggregateSidebarScheduledTasks(
  environmentTasks: ReadonlyArray<{
    readonly environmentId: EnvironmentId;
    readonly tasks: ReadonlyArray<ScheduledTask> | null | undefined;
  }>,
  previous?: ReadonlyMap<string, SidebarThreadScheduledSummary>,
): ReadonlyMap<string, SidebarThreadScheduledSummary> {
  const grouped = new Map<string, { count: number; soonest: ActiveScheduledTask }>();
  for (const entry of environmentTasks) {
    if (!entry.tasks) continue;
    for (const task of entry.tasks) {
      if (task.threadId === null || !isScheduledTaskActive(task)) continue;
      const key = scopedThreadKey(scopeThreadRef(entry.environmentId, task.threadId));
      const existing = grouped.get(key);
      if (!existing) {
        grouped.set(key, { count: 1, soonest: task });
      } else {
        existing.count += 1;
        if (compareActiveScheduledTasks(task, existing.soonest) < 0) existing.soonest = task;
      }
    }
  }
  const next = new Map<string, SidebarThreadScheduledSummary>();
  let changed = previous === undefined || grouped.size !== previous.size;
  for (const [key, { count, soonest }] of grouped) {
    const prev = previous?.get(key);
    if (
      prev !== undefined &&
      prev.count === count &&
      prev.nextRunAt === soonest.nextRunAt &&
      prev.title === soonest.title
    ) {
      next.set(key, prev);
    } else {
      changed = true;
      next.set(key, { count, nextRunAt: soonest.nextRunAt, title: soonest.title });
    }
  }
  return changed ? next : previous!;
}
