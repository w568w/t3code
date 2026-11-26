// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import type * as NodeStream from "node:stream";
import {
  CommandId,
  isOrchestrationV2WorkActive,
  MessageId,
  type NodeId,
  type OrchestrationV2Notification,
  type RunId,
  type ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import { stripAppImageRuntimeEnv } from "@t3tools/shared/appImageRuntimeEnv";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import { forkParked } from "../serverActivation.ts";
import * as CommandOutputHub from "./CommandOutputHub.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";

const DEFAULT_TIMEOUT_MS = 300_000;
const MAX_TIMEOUT_MS = 3_600_000;
const MAX_MONITORS_PER_THREAD = 8;
const MAX_WAKES_PER_MONITOR = 60;
const COALESCE_DEBOUNCE_MS = 250;
const COALESCE_MAX_WAIT_MS = 1_500;
const MAX_EVENT_LINES = 50;
const MAX_EVENT_CHARS = 4_096;
const MAX_SUMMARY_PREVIEW_CHARS = 100;
const MAX_PERSISTED_OUTPUT_CHARS = 16_384;
const KILL_GRACE_MS = 2_000;

export class ProcessMonitorError extends Schema.TaggedError<ProcessMonitorError>()(
  "ProcessMonitorError",
  {
    reason: Schema.Literals([
      "invalid_request",
      "limit_exceeded",
      "not_found",
      "spawn_failed",
      "orchestration_error",
    ]),
    message: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export interface StartProcessMonitorInput {
  readonly threadId: ThreadId;
  readonly runId: RunId | null;
  readonly nodeId: NodeId | null;
  readonly command: string;
  readonly description: string;
  readonly timeoutMs?: number | undefined;
  readonly persistent?: boolean | undefined;
  readonly waiting?: boolean | undefined;
}

export interface ProcessMonitorSummary {
  readonly monitorId: string;
  readonly turnItemId: TurnItemId;
  readonly threadId: ThreadId;
  readonly command: string;
  readonly description: string;
  readonly persistent: boolean;
  readonly waiting: boolean;
  readonly timeoutMs: number | null;
  readonly startedAt: string;
  readonly wakes: number;
}

export interface ProcessMonitorServiceShape {
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  readonly startMonitor: (
    input: StartProcessMonitorInput,
  ) => Effect.Effect<ProcessMonitorSummary, ProcessMonitorError>;
  readonly stopMonitor: (input: {
    readonly threadId: ThreadId;
    readonly monitorId: string;
  }) => Effect.Effect<
    { readonly stopped: boolean; readonly monitorId: string },
    ProcessMonitorError
  >;
  readonly listMonitors: (input: {
    readonly threadId: ThreadId;
  }) => Effect.Effect<ReadonlyArray<ProcessMonitorSummary>>;
}

export class ProcessMonitorService extends Context.Service<
  ProcessMonitorService,
  ProcessMonitorServiceShape
>()("t3/orchestration-v2/ProcessMonitorService") {}

type MonitorWorkItem =
  | { readonly _tag: "output"; readonly chunk: string }
  | { readonly _tag: "flush" }
  | { readonly _tag: "finish_output" }
  | {
      readonly _tag: "exit";
      readonly code: number | null;
      readonly signal: NodeJS.Signals | null;
      readonly spawnError?: Error;
    };

interface ActiveMonitor {
  readonly monitorId: string;
  readonly turnItemId: TurnItemId;
  readonly threadId: ThreadId;
  readonly runId: RunId | null;
  readonly nodeId: NodeId | null;
  readonly command: string;
  readonly description: string;
  readonly persistent: boolean;
  readonly waiting: boolean;
  readonly timeoutMs: number | null;
  readonly startedAt: string;
  readonly child: NodeChildProcess.ChildProcessByStdio<
    null,
    NodeStream.Readable,
    NodeStream.Readable
  >;
  readonly workQueue: Queue.Queue<MonitorWorkItem, Cause.Done>;
  wakes: number;
  wakesMuted: boolean;
  closed: boolean;
  stopReason: "explicit" | "external" | "timeout" | "wake_limit" | null;
  stdoutRemainder: string;
  pendingLines: Array<string>;
  outputTail: string;
  debounceFiber: Fiber.Fiber<void, never> | null;
  maxWaitFiber: Fiber.Fiber<void, never> | null;
  timeoutFiber: Fiber.Fiber<void, never> | null;
  killFiber: Fiber.Fiber<void, never> | null;
}

function appendTail(current: string, chunk: string): string {
  const combined = current + chunk;
  if (combined.length <= MAX_PERSISTED_OUTPUT_CHARS) return combined;
  return combined.slice(combined.length - MAX_PERSISTED_OUTPUT_CHARS);
}

function escapeXmlAttr(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function sanitizeEventBody(text: string): string {
  return text.replaceAll(/<\/\s*monitor-event\s*>/giu, "&lt;/monitor-event&gt;");
}

function summarizeLines(description: string, lines: ReadonlyArray<string>): string {
  const first = (lines[0] ?? "").trim().replaceAll(/\s+/gu, " ");
  const preview =
    first.length > MAX_SUMMARY_PREVIEW_CHARS
      ? `${first.slice(0, MAX_SUMMARY_PREVIEW_CHARS - 1)}…`
      : first;
  if (preview.length === 0) return `Monitor "${description}" reported new output`;
  const extra = lines.length > 1 ? ` (+${lines.length - 1} more)` : "";
  return `Monitor "${description}": ${preview}${extra}`;
}

function truncateEventPayload(lines: ReadonlyArray<string>): {
  readonly text: string;
  readonly truncated: boolean;
} {
  let truncated = false;
  let kept = lines;
  if (kept.length > MAX_EVENT_LINES) {
    kept = kept.slice(0, MAX_EVENT_LINES);
    truncated = true;
  }
  let joined = kept.join("\n");
  if (joined.length > MAX_EVENT_CHARS) {
    joined = `${joined.slice(0, MAX_EVENT_CHARS - 1)}…`;
    truncated = true;
  }
  return { text: joined, truncated };
}

function formatEventWakeText(
  monitor: ActiveMonitor,
  payload: { readonly text: string; readonly truncated: boolean },
  noteOverride?: string,
): string {
  const truncatedNote = payload.truncated
    ? "\n(output truncated; inspect monitor output for full log)"
    : "";
  const idAttr = escapeXmlAttr(monitor.monitorId);
  const descAttr = escapeXmlAttr(monitor.description);
  const body = sanitizeEventBody(payload.text) + truncatedNote;
  const footer =
    noteOverride ??
    `Background monitor "${monitor.description}" (${monitor.monitorId}) emitted new output and is still running. Call t3_monitor_stop with monitorId "${monitor.monitorId}" when you no longer need it.`;
  return [
    `<monitor-event monitor-id="${idAttr}" description="${descAttr}" state="running">`,
    body,
    "</monitor-event>",
    footer,
  ].join("\n");
}

function formatExitWakeText(input: {
  readonly monitor: ActiveMonitor;
  readonly outcome: "completed" | "failed" | "cancelled";
  readonly detail: string | undefined;
  readonly exitCode: number | undefined;
  readonly reason: string;
}): string {
  const idAttr = escapeXmlAttr(input.monitor.monitorId);
  const descAttr = escapeXmlAttr(input.monitor.description);
  return [
    `<monitor-event monitor-id="${idAttr}" description="${descAttr}" state="${input.outcome}"${input.exitCode === undefined ? "" : ` exit-code="${input.exitCode}"`}>`,
    ...(input.detail ? [sanitizeEventBody(input.detail)] : []),
    "</monitor-event>",
    `Background monitor "${input.monitor.description}" (${input.monitor.monitorId}) ${input.reason}.`,
  ].join("\n");
}

function killProcessGroup(
  child: NodeChildProcess.ChildProcessByStdio<null, NodeStream.Readable, NodeStream.Readable>,
  signal: NodeJS.Signals,
  detached: boolean,
): void {
  const pid = child.pid;
  if (pid === undefined) return;
  try {
    if (detached) {
      process.kill(-pid, signal);
    } else {
      child.kill(signal);
    }
  } catch {
    try {
      child.kill(signal);
    } catch {
      // Process already exited.
    }
  }
}

function toSummary(monitor: ActiveMonitor): ProcessMonitorSummary {
  return {
    monitorId: monitor.monitorId,
    turnItemId: monitor.turnItemId,
    threadId: monitor.threadId,
    command: monitor.command,
    description: monitor.description,
    persistent: monitor.persistent,
    waiting: monitor.waiting,
    timeoutMs: monitor.timeoutMs,
    startedAt: monitor.startedAt,
    wakes: monitor.wakes,
  };
}

const make = Effect.gen(function* () {
  const engine = yield* Orchestrator.OrchestratorV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const projects = yield* ProjectStore.ProjectStoreV2;
  const commandOutputHub = yield* CommandOutputHub.CommandOutputHub;
  const random = yield* SecureRandom.SecureRandom;
  const platform = yield* HostProcess.Platform;
  const detachProcessGroup = platform !== "win32";
  const services = yield* Effect.context<never>();
  const runForkWithServices = Effect.runForkWith(services);

  const byMonitorId = new Map<string, ActiveMonitor>();
  const byTurnItemId = new Map<TurnItemId, ActiveMonitor>();

  const interruptFiber = (fiber: Fiber.Fiber<void, never> | null) => {
    if (fiber !== null) {
      runForkWithServices(Fiber.interrupt(fiber));
    }
  };

  const clearFlushTimers = (monitor: ActiveMonitor) => {
    interruptFiber(monitor.debounceFiber);
    monitor.debounceFiber = null;
    interruptFiber(monitor.maxWaitFiber);
    monitor.maxWaitFiber = null;
  };

  const clearAllTimers = (monitor: ActiveMonitor) => {
    clearFlushTimers(monitor);
    interruptFiber(monitor.timeoutFiber);
    monitor.timeoutFiber = null;
    interruptFiber(monitor.killFiber);
    monitor.killFiber = null;
  };

  const scheduleForceKill = (monitor: ActiveMonitor) => {
    interruptFiber(monitor.killFiber);
    monitor.killFiber = runForkWithServices(
      Effect.sleep(Duration.millis(KILL_GRACE_MS)).pipe(
        Effect.andThen(
          Effect.sync(() => {
            monitor.killFiber = null;
            killProcessGroup(monitor.child, "SIGKILL", detachProcessGroup);
          }),
        ),
      ),
    );
  };

  const terminateChild = (monitor: ActiveMonitor) => {
    clearAllTimers(monitor);
    killProcessGroup(monitor.child, "SIGTERM", detachProcessGroup);
    scheduleForceKill(monitor);
  };

  const detachMonitor = (monitor: ActiveMonitor) => {
    byMonitorId.delete(monitor.monitorId);
    byTurnItemId.delete(monitor.turnItemId);
    clearAllTimers(monitor);
  };

  const recordStatus = Effect.fn("ProcessMonitorService.recordStatus")(function* (input: {
    readonly monitor: ActiveMonitor;
    readonly status: "running" | "completed" | "failed" | "cancelled" | "interrupted";
    readonly exitCode?: number | undefined;
    readonly output?: string | undefined;
    readonly wake?:
      | {
          readonly text: string;
          readonly notification: OrchestrationV2Notification;
        }
      | undefined;
  }) {
    const uuid = yield* random.uuidV4();
    yield* engine.dispatch({
      type: "process_monitor.record",
      commandId: CommandId.make(`server:process-monitor:${input.monitor.monitorId}:${uuid}`),
      threadId: input.monitor.threadId,
      runId: input.monitor.runId,
      nodeId: input.monitor.nodeId,
      turnItemId: input.monitor.turnItemId,
      command: input.monitor.command,
      description: input.monitor.description,
      status: input.status,
      ...(input.monitor.waiting ? { awaited: true as const } : {}),
      ...(input.exitCode === undefined ? {} : { exitCode: input.exitCode }),
      ...(input.output === undefined ? {} : { output: input.output }),
      ...(input.wake === undefined
        ? {}
        : {
            wake: {
              ...input.wake,
              messageId: MessageId.make(`message:monitor:${uuid}`),
            },
          }),
    });
  });

  const handleFlush = Effect.fn("ProcessMonitorService.handleFlush")(function* (
    monitor: ActiveMonitor,
  ) {
    clearFlushTimers(monitor);
    if (monitor.closed || monitor.wakesMuted || monitor.pendingLines.length === 0) {
      monitor.pendingLines = [];
      return;
    }
    const lines = monitor.pendingLines;
    monitor.pendingLines = [];
    monitor.wakes += 1;

    if (monitor.wakes > MAX_WAKES_PER_MONITOR) {
      const payload = truncateEventPayload(lines);
      const detail = payload.text;
      if (monitor.persistent) {
        monitor.wakesMuted = true;
        yield* recordStatus({
          monitor,
          status: "running",
          wake: {
            text: formatEventWakeText(
              monitor,
              payload,
              `Persistent monitor "${monitor.description}" (${monitor.monitorId}) reached the ${MAX_WAKES_PER_MONITOR}-event wake limit. Further output will continue streaming silently in the background without waking this thread. Call t3_monitor_stop with monitorId "${monitor.monitorId}" when you want to stop it.`,
            ),
            notification: {
              source: { kind: "monitor" },
              outcome: "updated",
              summary: `Monitor "${monitor.description}" muted further wakes (event limit reached)`,
              ...(detail.length > 0 ? { detail } : {}),
            },
          },
        });
        return;
      }

      monitor.stopReason = "wake_limit";
      monitor.closed = true;
      detachMonitor(monitor);
      terminateChild(monitor);
      Queue.endUnsafe(monitor.workQueue);
      yield* commandOutputHub.finish({
        threadId: monitor.threadId,
        itemId: monitor.turnItemId,
      });
      yield* recordStatus({
        monitor,
        status: "failed",
        output: monitor.outputTail,
        wake: {
          text: formatExitWakeText({
            monitor,
            outcome: "failed",
            detail,
            exitCode: undefined,
            reason: `was stopped after exceeding the ${MAX_WAKES_PER_MONITOR}-event wake limit`,
          }),
          notification: {
            source: { kind: "monitor" },
            outcome: "failed",
            summary: `Monitor "${monitor.description}" stopped (event limit reached)`,
            ...(detail.length > 0 ? { detail } : {}),
          },
        },
      });
      return;
    }

    const payload = truncateEventPayload(lines);
    const summary = summarizeLines(monitor.description, lines);
    yield* recordStatus({
      monitor,
      status: "running",
      wake: {
        text: formatEventWakeText(monitor, payload),
        notification: {
          source: { kind: "monitor" },
          outcome: "updated",
          summary,
          detail: payload.text,
        },
      },
    });
  });

  const handleExit = Effect.fn("ProcessMonitorService.handleExit")(function* (
    monitor: ActiveMonitor,
    exit: {
      readonly code: number | null;
      readonly signal: NodeJS.Signals | null;
      readonly spawnError?: Error;
    },
  ) {
    if (monitor.closed) return;
    monitor.closed = true;
    const remainder = monitor.stdoutRemainder.trim();
    if (remainder.length > 0) {
      monitor.pendingLines.push(remainder);
    }
    monitor.stdoutRemainder = "";
    const trailingLines = monitor.pendingLines;
    monitor.pendingLines = [];
    detachMonitor(monitor);

    const stopReason = monitor.stopReason;
    if (stopReason === "explicit" || stopReason === "external") {
      yield* commandOutputHub.finish({
        threadId: monitor.threadId,
        itemId: monitor.turnItemId,
      });
      return;
    }

    const payload = trailingLines.length > 0 ? truncateEventPayload(trailingLines) : null;
    const detail = payload?.text ?? (exit.spawnError ? exit.spawnError.message : undefined);

    yield* commandOutputHub.finish({
      threadId: monitor.threadId,
      itemId: monitor.turnItemId,
    });

    if (stopReason === "timeout") {
      yield* recordStatus({
        monitor,
        status: "cancelled",
        output: monitor.outputTail,
        wake: {
          text: formatExitWakeText({
            monitor,
            outcome: "cancelled",
            detail,
            exitCode: undefined,
            reason: `timed out after ${Math.round((monitor.timeoutMs ?? DEFAULT_TIMEOUT_MS) / 1000)}s`,
          }),
          notification: {
            source: { kind: "monitor" },
            outcome: "cancelled",
            summary: `Monitor "${monitor.description}" timed out`,
            ...(detail ? { detail } : {}),
          },
        },
      });
      return;
    }

    const exitCode = exit.code ?? undefined;
    const failed =
      exit.spawnError !== undefined ||
      (exitCode !== undefined ? exitCode !== 0 : exit.signal !== null);
    const status = failed ? "failed" : "completed";
    const reason = exit.spawnError
      ? `failed to start: ${exit.spawnError.message}`
      : exitCode !== undefined
        ? exitCode === 0
          ? "exited cleanly (exit 0)"
          : `exited with code ${exitCode}`
        : `exited on signal ${exit.signal ?? "unknown"}`;
    const summary = failed
      ? `Monitor "${monitor.description}" failed${exitCode === undefined ? "" : ` (exit ${exitCode})`}`
      : `Monitor "${monitor.description}" finished`;

    yield* recordStatus({
      monitor,
      status,
      exitCode,
      output: monitor.outputTail,
      wake: {
        text: formatExitWakeText({
          monitor,
          outcome: status,
          detail,
          exitCode,
          reason,
        }),
        notification: {
          source: { kind: "monitor" },
          outcome: status,
          summary,
          ...(detail ? { detail } : {}),
        },
      },
    });
  });

  const handleWorkItem = (monitor: ActiveMonitor, item: MonitorWorkItem) => {
    switch (item._tag) {
      case "output":
        return commandOutputHub.append({
          threadId: monitor.threadId,
          itemId: monitor.turnItemId,
          chunk: item.chunk,
        });
      case "flush":
        return handleFlush(monitor);
      case "finish_output":
        return commandOutputHub.finish({
          threadId: monitor.threadId,
          itemId: monitor.turnItemId,
        });
      case "exit":
        return handleExit(monitor, item);
    }
  };

  const scheduleFlush = (monitor: ActiveMonitor) => {
    if (monitor.closed || monitor.wakesMuted) return;
    interruptFiber(monitor.debounceFiber);
    monitor.debounceFiber = runForkWithServices(
      Effect.sleep(Duration.millis(COALESCE_DEBOUNCE_MS)).pipe(
        Effect.andThen(
          Effect.sync(() => {
            monitor.debounceFiber = null;
            if (!monitor.closed) {
              Queue.offerUnsafe(monitor.workQueue, { _tag: "flush" });
            }
          }),
        ),
      ),
    );

    if (monitor.maxWaitFiber === null) {
      monitor.maxWaitFiber = runForkWithServices(
        Effect.sleep(Duration.millis(COALESCE_MAX_WAIT_MS)).pipe(
          Effect.andThen(
            Effect.sync(() => {
              monitor.maxWaitFiber = null;
              if (!monitor.closed) {
                Queue.offerUnsafe(monitor.workQueue, { _tag: "flush" });
              }
            }),
          ),
        ),
      );
    }
  };

  const killExternally = (monitor: ActiveMonitor) => {
    if (monitor.closed) return;
    monitor.stopReason = "external";
    monitor.closed = true;
    detachMonitor(monitor);
    terminateChild(monitor);
    Queue.offerUnsafe(monitor.workQueue, { _tag: "finish_output" });
    Queue.endUnsafe(monitor.workQueue);
  };

  const stopMatchingMonitors = (
    predicate: (candidate: ActiveMonitor) => boolean,
    markInterrupted: boolean,
  ) =>
    Effect.forEach(
      [...byMonitorId.values()].filter(predicate),
      (monitor) =>
        Effect.gen(function* () {
          killExternally(monitor);
          if (markInterrupted) {
            yield* recordStatus({
              monitor,
              status: "interrupted",
              output: monitor.outputTail,
            }).pipe(Effect.ignoreCause);
          }
        }),
      { discard: true },
    );

  const start: ProcessMonitorServiceShape["start"] = Effect.fn("ProcessMonitorService.start")(
    function* () {
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          for (const monitor of byMonitorId.values()) {
            monitor.stopReason = "external";
            monitor.closed = true;
            clearAllTimers(monitor);
            killProcessGroup(monitor.child, "SIGTERM", detachProcessGroup);
            Queue.endUnsafe(monitor.workQueue);
          }
          byMonitorId.clear();
          byTurnItemId.clear();
        }),
      );

      yield* forkParked(
        Stream.runForEach(engine.streamDomainEvents, (event) => {
          switch (event.type) {
            case "turn-item.updated": {
              const monitor = byTurnItemId.get(event.payload.id);
              if (monitor !== undefined && !isOrchestrationV2WorkActive(event.payload.status)) {
                killExternally(monitor);
              }
              return Effect.void;
            }
            case "run.updated": {
              if (event.payload.status !== "rolled_back") return Effect.void;
              const rolledBackRunId = event.payload.id;
              return stopMatchingMonitors(
                (candidate) =>
                  candidate.threadId === event.threadId && candidate.runId === rolledBackRunId,
                true,
              );
            }
            case "thread.deleted":
            case "thread.archived":
            case "thread.settled":
              return stopMatchingMonitors(
                (candidate) => candidate.threadId === event.threadId,
                true,
              );
            default:
              return Effect.void;
          }
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("ProcessMonitorService event stream stopped", { cause }),
          ),
        ),
      );
    },
  );

  const startMonitor: ProcessMonitorServiceShape["startMonitor"] = Effect.fn(
    "ProcessMonitorService.startMonitor",
  )(function* (input) {
    const command = input.command.trim();
    const description = input.description.trim();
    if (command.length === 0 || description.length === 0) {
      return yield* new ProcessMonitorError({
        reason: "invalid_request",
        message: "Both `command` and `description` must be non-empty.",
      });
    }

    const threadMonitors = [...byMonitorId.values()].filter(
      (candidate) => candidate.threadId === input.threadId,
    );
    if (threadMonitors.length >= MAX_MONITORS_PER_THREAD) {
      return yield* new ProcessMonitorError({
        reason: "limit_exceeded",
        message: `This thread already has ${MAX_MONITORS_PER_THREAD} active monitors. Stop one before starting another.`,
      });
    }

    const thread = yield* projections.getThread(input.threadId).pipe(
      Effect.mapError(
        (cause) =>
          new ProcessMonitorError({
            reason: "orchestration_error",
            message: "Could not read thread metadata.",
            cause,
          }),
      ),
    );

    const projectOption = yield* projects.get(thread.projectId).pipe(
      Effect.mapError(
        (cause) =>
          new ProcessMonitorError({
            reason: "orchestration_error",
            message: "Could not read project workspace.",
            cause,
          }),
      ),
    );
    const cwd = thread.worktreePath ?? Option.getOrUndefined(projectOption)?.workspaceRoot;
    if (cwd === undefined) {
      return yield* new ProcessMonitorError({
        reason: "orchestration_error",
        message: "Could not determine working directory for this thread.",
      });
    }

    const persistent = input.persistent === true;
    const waiting = input.waiting ?? !persistent;
    const timeoutMs =
      input.timeoutMs !== undefined
        ? Math.min(Math.max(1_000, Math.round(input.timeoutMs)), MAX_TIMEOUT_MS)
        : persistent
          ? null
          : DEFAULT_TIMEOUT_MS;

    const uuid = yield* random.uuidV4();
    const monitorId = `mon_${uuid.replaceAll("-", "").slice(0, 12)}`;
    const turnItemId = TurnItemId.make(`turn-item:monitor:${monitorId}`);
    const startedAt = DateTime.formatIso(yield* DateTime.now);

    yield* engine
      .dispatch({
        type: "process_monitor.record",
        commandId: CommandId.make(`server:process-monitor-start:${monitorId}`),
        threadId: input.threadId,
        runId: input.runId,
        nodeId: input.nodeId,
        turnItemId,
        command,
        description,
        status: "running",
        ...(waiting ? { awaited: true as const } : {}),
      })
      .pipe(
        Effect.mapError(
          (cause) =>
            new ProcessMonitorError({
              reason: "orchestration_error",
              message: "Could not record the monitor turn item.",
              cause,
            }),
        ),
      );

    const child = yield* Effect.try({
      try: () =>
        NodeChildProcess.spawn(command, {
          cwd,
          shell: true,
          detached: detachProcessGroup,
          stdio: ["ignore", "pipe", "pipe"],
          env: stripAppImageRuntimeEnv(process.env),
        }),
      catch: (cause) =>
        new ProcessMonitorError({
          reason: "spawn_failed",
          message: `Failed to start monitor process: ${cause instanceof Error ? cause.message : String(cause)}`,
          cause,
        }),
    }).pipe(
      Effect.tapError((error) =>
        engine
          .dispatch({
            type: "process_monitor.record",
            commandId: CommandId.make(`server:process-monitor-spawn-fail:${monitorId}`),
            threadId: input.threadId,
            runId: input.runId,
            nodeId: input.nodeId,
            turnItemId,
            command,
            description,
            status: "failed",
            output: error.message,
          })
          .pipe(Effect.ignoreCause),
      ),
    );

    const workQueue = yield* Queue.unbounded<MonitorWorkItem, Cause.Done>();
    const monitor: ActiveMonitor = {
      monitorId,
      turnItemId,
      threadId: input.threadId,
      runId: input.runId,
      nodeId: input.nodeId,
      command,
      description,
      persistent,
      waiting,
      timeoutMs,
      startedAt,
      child,
      workQueue,
      wakes: 0,
      wakesMuted: false,
      closed: false,
      stopReason: null,
      stdoutRemainder: "",
      pendingLines: [],
      outputTail: "",
      debounceFiber: null,
      maxWaitFiber: null,
      timeoutFiber: null,
      killFiber: null,
    };

    byMonitorId.set(monitorId, monitor);
    byTurnItemId.set(turnItemId, monitor);

    runForkWithServices(
      Stream.fromQueue(workQueue).pipe(
        Stream.runForEach((item) =>
          handleWorkItem(monitor, item).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("ProcessMonitorService monitor worker step failed", {
                monitorId,
                step: item._tag,
                cause,
              }),
            ),
          ),
        ),
      ),
    );

    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");

    child.stdout.on("data", (chunk: string) => {
      if (monitor.closed) return;
      monitor.outputTail = appendTail(monitor.outputTail, chunk);
      Queue.offerUnsafe(workQueue, { _tag: "output", chunk });
      if (monitor.wakesMuted) return;
      const parts = (monitor.stdoutRemainder + chunk).split("\n");
      monitor.stdoutRemainder = parts.pop() ?? "";
      let added = false;
      for (const rawLine of parts) {
        const line = rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine;
        if (line.trim().length === 0) continue;
        monitor.pendingLines.push(line);
        added = true;
      }
      if (added) scheduleFlush(monitor);
    });

    child.stderr.on("data", (chunk: string) => {
      if (monitor.closed) return;
      monitor.outputTail = appendTail(monitor.outputTail, chunk);
      Queue.offerUnsafe(workQueue, { _tag: "output", chunk });
    });

    let exitQueued = false;
    const queueExit = (exit: {
      readonly code: number | null;
      readonly signal: NodeJS.Signals | null;
      readonly spawnError?: Error;
    }) => {
      if (exitQueued) return;
      exitQueued = true;
      Queue.offerUnsafe(workQueue, { _tag: "exit", ...exit });
      Queue.endUnsafe(workQueue);
    };

    child.on("error", (error) => {
      queueExit({ code: null, signal: null, spawnError: error });
    });

    child.on("close", (code, signal) => {
      queueExit({ code, signal });
    });

    if (timeoutMs !== null) {
      monitor.timeoutFiber = runForkWithServices(
        Effect.sleep(Duration.millis(timeoutMs)).pipe(
          Effect.andThen(
            Effect.sync(() => {
              monitor.timeoutFiber = null;
              if (monitor.closed) return;
              monitor.stopReason = "timeout";
              killProcessGroup(monitor.child, "SIGTERM", detachProcessGroup);
              scheduleForceKill(monitor);
            }),
          ),
        ),
      );
    }

    return toSummary(monitor);
  });

  const stopMonitor: ProcessMonitorServiceShape["stopMonitor"] = Effect.fn(
    "ProcessMonitorService.stopMonitor",
  )(function* (input) {
    const monitor = byMonitorId.get(input.monitorId);
    if (monitor === undefined || monitor.threadId !== input.threadId) {
      return { stopped: false, monitorId: input.monitorId };
    }
    monitor.stopReason = "explicit";
    monitor.closed = true;
    detachMonitor(monitor);
    terminateChild(monitor);
    Queue.endUnsafe(monitor.workQueue);
    yield* commandOutputHub.finish({
      threadId: monitor.threadId,
      itemId: monitor.turnItemId,
    });
    yield* recordStatus({
      monitor,
      status: "cancelled",
      output: monitor.outputTail,
    }).pipe(
      Effect.mapError(
        (cause) =>
          new ProcessMonitorError({
            reason: "orchestration_error",
            message: "Could not mark the monitor stopped.",
            cause,
          }),
      ),
    );
    return { stopped: true, monitorId: input.monitorId };
  });

  const listMonitors: ProcessMonitorServiceShape["listMonitors"] = (input) =>
    Effect.sync(() =>
      [...byMonitorId.values()]
        .filter((candidate) => candidate.threadId === input.threadId)
        .map(toSummary),
    );

  return ProcessMonitorService.of({
    start,
    startMonitor,
    stopMonitor,
    listMonitors,
  });
});

export const layer = Layer.effect(ProcessMonitorService, make);
