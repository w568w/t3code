import { OrchestratorMcpFailure, PositiveInt, TrimmedNonEmptyString } from "@t3tools/contracts";
import { McpSchema, Tool, Toolkit } from "effect/ai";
import * as Schema from "effect/Schema";

import * as ProcessMonitorService from "../../../orchestration-v2/ProcessMonitorService.ts";
import * as ProjectionStore from "../../../orchestration-v2/ProjectionStore.ts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";

const dependencies = [
  McpInvocationContext.McpInvocationContext,
  ProcessMonitorService.ProcessMonitorService,
  ProjectionStore.ProjectionStoreV2,
  ThreadManagementService.ThreadManagementService,
];

const MonitorSummarySchema = Schema.Struct({
  monitorId: Schema.String,
  command: Schema.String,
  description: Schema.String,
  persistent: Schema.Boolean,
  waiting: Schema.Boolean,
  timeoutMs: Schema.NullOr(Schema.Number),
  startedAt: Schema.String,
  wakes: Schema.Number,
});

const MonitorStartTool = Tool.make("t3_monitor_start", {
  description:
    "Start a background shell monitor whose stdout lines wake this thread as events. Prefer over sleep/polling loops. Non-persistent monitors keep this thread Waiting while idle. Stop with `t3_monitor_stop`.",
  parameters: Schema.Struct({
    command: TrimmedNonEmptyString.annotate({
      description: "Shell command to run. Each stdout line triggers an event wake.",
    }),
    description: TrimmedNonEmptyString.annotate({
      description: "Short label shown in the UI and event summaries.",
    }),
    timeoutMs: Schema.optional(
      PositiveInt.annotate({
        description: "Auto-stop deadline in ms. Default 300000 unless persistent.",
      }),
    ),
    persistent: Schema.optional(
      Schema.Boolean.annotate({
        description: "Run without a default timeout and without holding thread completion.",
      }),
    ),
    waiting: Schema.optional(
      Schema.Boolean.annotate({
        description: "Override whether this monitor keeps the thread Waiting while idle.",
      }),
    ),
  }),
  success: MonitorSummarySchema,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Start a background monitor")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, true)
  .annotate(McpSchema.EnabledWhen, McpInvocationContext.hiddenFromOutsideClients);

const MonitorStopTool = Tool.make("t3_monitor_stop", {
  description: "Stop a running background monitor started by `t3_monitor_start` on this thread.",
  parameters: Schema.Struct({
    monitorId: TrimmedNonEmptyString,
  }),
  success: Schema.Struct({
    stopped: Schema.Boolean,
    monitorId: Schema.String,
  }),
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Stop a background monitor")
  .annotate(Tool.Destructive, true)
  .annotate(McpSchema.EnabledWhen, McpInvocationContext.hiddenFromOutsideClients);

const MonitorListTool = Tool.make("t3_monitor_list", {
  description: "List active background monitors on this thread.",
  success: Schema.Struct({
    monitors: Schema.Array(MonitorSummarySchema),
  }),
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "List background monitors")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(McpSchema.EnabledWhen, McpInvocationContext.hiddenFromOutsideClients);

export const MonitorToolkit = Toolkit.make(MonitorStartTool, MonitorStopTool, MonitorListTool);
