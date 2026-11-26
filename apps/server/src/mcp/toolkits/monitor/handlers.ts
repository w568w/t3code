import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import * as ProcessMonitorService from "../../../orchestration-v2/ProcessMonitorService.ts";
import * as ProjectionStore from "../../../orchestration-v2/ProjectionStore.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { loadCaller } from "../../threadAccess.ts";
import { MonitorToolkit } from "./tools.ts";

function toMcpFailure(cause: ProcessMonitorService.ProcessMonitorError): OrchestratorMcpFailure {
  return new OrchestratorMcpFailure({
    code:
      cause.reason === "invalid_request" ||
      cause.reason === "limit_exceeded" ||
      cause.reason === "not_found"
        ? "invalid_request"
        : "orchestration_error",
    message: cause.message,
  });
}

function toToolSummary(summary: ProcessMonitorService.ProcessMonitorSummary) {
  return {
    monitorId: summary.monitorId,
    command: summary.command,
    description: summary.description,
    persistent: summary.persistent,
    waiting: summary.waiting,
    timeoutMs: summary.timeoutMs,
    startedAt: summary.startedAt,
    wakes: summary.wakes,
  };
}

const handlers = {
  t3_monitor_start: McpToolAccess.actsAsCaller((input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const caller = yield* McpInvocationContext.requireThreadScope(scope, "t3_monitor_start");
      const { limits } = yield* loadCaller();
      if (limits.runtimeMode !== "full-access") {
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "Only full-access threads may start background process monitors.",
        });
      }
      const monitors = yield* ProcessMonitorService.ProcessMonitorService;
      const projections = yield* ProjectionStore.ProjectionStoreV2;

      const records = yield* projections
        .getThreadRecords(caller.thread.threadId, ["runs"], {
          runStatuses: ["preparing", "starting", "running"],
        })
        .pipe(Effect.orElseSucceed(() => null));
      const activeRun = records?.runs.findLast(
        (run) =>
          run.status === "preparing" || run.status === "starting" || run.status === "running",
      );

      const started = yield* monitors
        .startMonitor({
          threadId: caller.thread.threadId,
          runId: activeRun?.id ?? null,
          nodeId: activeRun?.rootNodeId ?? null,
          command: input.command,
          description: input.description,
          timeoutMs: input.timeoutMs,
          persistent: input.persistent,
          waiting: input.waiting,
        })
        .pipe(Effect.mapError(toMcpFailure));

      return toToolSummary(started);
    }),
  ),
  t3_monitor_stop: McpToolAccess.actsAsCaller((input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const caller = yield* McpInvocationContext.requireThreadScope(scope, "t3_monitor_stop");
      const monitors = yield* ProcessMonitorService.ProcessMonitorService;
      return yield* monitors
        .stopMonitor({
          threadId: caller.thread.threadId,
          monitorId: input.monitorId,
        })
        .pipe(Effect.mapError(toMcpFailure));
    }),
  ),
  t3_monitor_list: McpToolAccess.readsAsCaller(() =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const caller = yield* McpInvocationContext.requireThreadScope(scope, "t3_monitor_list");
      const monitors = yield* ProcessMonitorService.ProcessMonitorService;
      const active = yield* monitors.listMonitors({ threadId: caller.thread.threadId });
      return { monitors: active.map(toToolSummary) };
    }),
  ),
} satisfies McpToolAccess.Handlers<typeof MonitorToolkit.tools>;

export const layer = McpToolAccess.toLayer(MonitorToolkit, handlers);
