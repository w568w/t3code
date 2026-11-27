import { EnvironmentId, ScheduledTaskId, ThreadId, type ScheduledTask } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import {
  aggregateSidebarScheduledTasks,
  formatNextScheduledRunLabel,
  isScheduledTaskActive,
  sortActiveScheduledTasks,
} from "./threadAutomations.logic";

const makeTask = (overrides: Partial<ScheduledTask> = {}) =>
  ({
    id: ScheduledTaskId.make("task-1"),
    title: "Check CI",
    threadId: ThreadId.make("thread-1"),
    schedule: { type: "interval", everyMs: 900_000 },
    enabled: true,
    nextRunAt: "2026-06-01T12:15:00.000Z",
    lastRunStatus: "never",
    ...overrides,
  }) as ScheduledTask;

describe("threadAutomations.logic", () => {
  it("treats only tasks with nextRunAt and no blockedByPolicy as actively scheduled", () => {
    expect(isScheduledTaskActive(makeTask())).toBe(true);
    expect(isScheduledTaskActive(makeTask({ enabled: false, nextRunAt: null }))).toBe(false);
    expect(
      isScheduledTaskActive(makeTask({ schedule: { type: "webhook" } as never, nextRunAt: null })),
    ).toBe(false);
    expect(isScheduledTaskActive(makeTask({ lastRunStatus: "failed", nextRunAt: null }))).toBe(
      false,
    );
    expect(isScheduledTaskActive(makeTask({ blockedByPolicy: "Blocked by org policy" }))).toBe(
      false,
    );
  });

  it("sorts active scheduled tasks ascending by nextRunAt", () => {
    const later = makeTask({
      id: ScheduledTaskId.make("later"),
      nextRunAt: "2026-06-01T13:00:00.000Z",
    });
    const sooner = makeTask({
      id: ScheduledTaskId.make("sooner"),
      nextRunAt: "2026-06-01T12:10:00.000Z",
    });
    const paused = makeTask({
      id: ScheduledTaskId.make("paused"),
      enabled: false,
      nextRunAt: null,
    });
    expect(sortActiveScheduledTasks([later, paused, sooner]).map((t) => t.id)).toEqual([
      sooner.id,
      later.id,
    ]);
  });

  it("formats upcoming and overdue scheduled run times", () => {
    const nowMs = Date.parse("2026-06-01T12:00:00.000Z");
    expect(formatNextScheduledRunLabel("2026-06-01T11:59:00.000Z", nowMs)).toBe("due now");
    expect(formatNextScheduledRunLabel("2026-06-01T12:00:00.000Z", nowMs)).toBe("due now");
    expect(formatNextScheduledRunLabel("2026-06-01T12:00:45.000Z", nowMs)).toBe(
      "in under a minute",
    );
    expect(formatNextScheduledRunLabel("2026-06-01T12:11:00.000Z", nowMs)).toBe("in 11m");
    expect(formatNextScheduledRunLabel("2026-06-01T14:00:00.000Z", nowMs)).toBe("in 2h");
  });

  it("aggregates per-thread sidebar scheduled summaries and preserves unchanged entry references", () => {
    const envA = EnvironmentId.make("env-a");
    const t1a = makeTask({
      id: ScheduledTaskId.make("t1a"),
      title: "First",
      nextRunAt: "2026-06-01T12:10:00.000Z",
    });
    const t1b = makeTask({
      id: ScheduledTaskId.make("t1b"),
      title: "Second",
      nextRunAt: "2026-06-01T12:30:00.000Z",
    });
    const t2 = makeTask({
      id: ScheduledTaskId.make("t2"),
      threadId: ThreadId.make("thread-2"),
      title: "Other thread",
      nextRunAt: "2026-06-01T12:20:00.000Z",
    });
    const unbound = makeTask({ id: ScheduledTaskId.make("unbound"), threadId: null });

    const first = aggregateSidebarScheduledTasks([
      { environmentId: envA, tasks: [t1b, t1a, t2, unbound] },
    ]);
    const key1 = [...first.keys()].find((k) => k.endsWith("thread-1"))!;
    const key2 = [...first.keys()].find((k) => k.endsWith("thread-2"))!;
    expect(first.size).toBe(2);
    expect(first.get(key1)).toEqual({
      count: 2,
      nextRunAt: "2026-06-01T12:10:00.000Z",
      title: "First",
    });

    const second = aggregateSidebarScheduledTasks(
      [
        {
          environmentId: envA,
          tasks: [t1b, t1a, { ...t2, nextRunAt: "2026-06-01T12:45:00.000Z" }],
        },
      ],
      first,
    );
    expect(second.get(key1)).toBe(first.get(key1));
    expect(second.get(key2)).not.toBe(first.get(key2));
    expect(second.get(key2)?.nextRunAt).toBe("2026-06-01T12:45:00.000Z");
  });
});
