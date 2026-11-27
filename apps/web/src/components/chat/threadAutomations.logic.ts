import { scopedThreadKey, scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { EnvironmentId, ScheduledTask } from "@t3tools/contracts";
import { msg } from "gt-react";
import { m, type MessageResolver } from "~/i18n/messages";

export type ActiveScheduledTask = ScheduledTask & {
  readonly nextRunAt: string;
  readonly blockedByPolicy: undefined;
};

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
