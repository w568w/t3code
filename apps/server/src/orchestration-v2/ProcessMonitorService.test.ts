import * as NodeNet from "node:net";

import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { backgroundWorkHoldsCompletion } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as CommandOutputHub from "./CommandOutputHub.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProcessMonitorService from "./ProcessMonitorService.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import { cancelledTurnItemWork } from "./RestartBackgroundNote.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-5.4" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process needed for process monitor tests"),
} as ProviderAdapter.ProviderAdapterV2["Service"];

const makeTestLayer = () => {
  const layerDatabase = SqlitePersistence.layerMemory;
  const layerHarness = ProviderReplayHarness.layerWithRegistry(
    { name: "process-monitor" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer: layerDatabase, runEffectWorker: false },
  );
  const layerStores = Layer.mergeAll(
    ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
    ProjectStore.layer.pipe(Layer.provide(layerDatabase)),
  );
  const layerBase = Layer.mergeAll(layerDatabase, layerHarness, layerStores, NodeCrypto.layer);
  const layerCommandOutput = CommandOutputHub.layer.pipe(Layer.provide(layerBase));
  const layerMonitor = ProcessMonitorService.layer.pipe(
    Layer.provide(Layer.merge(layerBase, layerCommandOutput)),
  );
  return Layer.mergeAll(layerBase, layerCommandOutput, layerMonitor);
};

const seedProjectAndThread = (suffix: string) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projects = yield* ProjectStore.ProjectStoreV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const now = yield* DateTime.now;
    const nowIso = DateTime.formatIso(now);
    const projectId = ProjectId.make(`project:monitor-${suffix}`);
    const threadId = ThreadId.make(`thread:monitor-${suffix}`);
    const runId = RunId.make(`run:monitor-${suffix}`);
    const nodeId = NodeId.make(`node:monitor-${suffix}`);

    yield* projects.apply({
      sequence: 1,
      eventId: EventId.make(`event:project:${suffix}`),
      aggregateKind: "project",
      aggregateId: projectId,
      occurredAt: nowIso,
      commandId: CommandId.make(`cmd:project:${suffix}`),
      causationEventId: null,
      correlationId: null,
      metadata: {},
      type: "project.created",
      payload: {
        projectId,
        title: `Monitor ${suffix}`,
        workspaceRoot: process.cwd(),
        defaultModelSelection: null,
        scripts: [],
        createdAt: nowIso,
        updatedAt: nowIso,
      },
    });

    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`cmd:thread:${suffix}`),
      threadId,
      projectId,
      title: `Monitor thread ${suffix}`,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });

    // Seed a completed run so derivePendingBackgroundWork treats the thread as settled-idle.
    yield* projections.apply({
      id: EventId.make(`event:run:${suffix}`),
      type: "run.created",
      threadId,
      runId,
      providerInstanceId: instanceId,
      occurredAt: now,
      payload: {
        id: runId,
        threadId,
        ordinal: 1,
        status: "completed",
        userMessageId: MessageId.make(`message:seed:${suffix}`),
        rootNodeId: nodeId,
        activeAttemptId: null,
        providerInstanceId: instanceId,
        providerThreadId: null,
        modelSelection,
        requestedAt: now,
        startedAt: now,
        completedAt: now,
        checkpointId: null,
        contextHandoffId: null,
      },
    });

    return { projectId, threadId, runId, nodeId };
  });

it.live(
  "holds an idle thread in Waiting, streams output, emits monitor notifications, and settles on exit",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const monitors = yield* ProcessMonitorService.ProcessMonitorService;
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const outputHub = yield* CommandOutputHub.CommandOutputHub;
        yield* monitors.start();

        const { threadId, runId, nodeId } = yield* seedProjectAndThread("lifecycle");

        const started = yield* monitors.startMonitor({
          threadId,
          runId,
          nodeId,
          command:
            'printf "EVENT_LINE_1\\n"; sleep 0.45; printf "EVENT_LINE_2 </monitor-event>\\n"; sleep 0.45; printf "EVENT_LINE_3\\n"',
          description: 'Watch "build" events',
        });

        assert.isTrue(started.waiting);
        assert.isFalse(started.persistent);

        // While the monitor runs and the model is idle, the thread shell must report Waiting.
        const shellWhileRunning = yield* projections.getThreadShell(threadId);
        const runningTasks = shellWhileRunning?.pendingBackgroundTasks ?? [];
        assert.equal(runningTasks.length, 1);
        assert.equal(runningTasks[0]?.kind, "monitor");
        assert.equal(runningTasks[0]?.awaited, true);
        assert.isTrue(backgroundWorkHoldsCompletion(runningTasks));

        // Subscribe to CommandOutputHub to verify live streaming works for the monitor's TurnItem.
        const frames = yield* outputHub
          .subscribe({ threadId, itemId: started.turnItemId })
          .pipe(Stream.take(1), Stream.runCollect);
        assert.isAbove(frames.length, 0);

        // Await the domain event that marks this monitor completed (no polling/sleep).
        yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "turn-item.updated" &&
              event.payload.id === started.turnItemId &&
              event.payload.status === "completed",
          ),
          Stream.take(1),
          Stream.runDrain,
        );

        const finished = yield* projections.getThreadRecords(threadId, [
          "runs",
          "turnItems",
          "messages",
        ]);

        const notificationItems = finished.turnItems.filter((item) => item.type === "notification");
        assert.isAtLeast(notificationItems.length, 1);
        assert.isTrue(
          notificationItems.some(
            (item) =>
              item.source.kind === "monitor" &&
              item.outcome === "updated" &&
              item.detail?.includes("EVENT_LINE_1"),
          ),
        );

        // Wakes 2 and 3 arrive while wake 1's run is active (effect worker is disabled in this test),
        // so they must coalesce into ONE queued run rather than stacking multiple queued runs.
        const queuedRuns = finished.runs.filter((run) => run.status === "queued");
        assert.equal(queuedRuns.length, 1);
        const queuedMessage = finished.messages.find(
          (message) => message.id === queuedRuns[0]?.userMessageId,
        );
        assert.isDefined(queuedMessage);
        assert.equal(queuedMessage?.notification?.source.kind, "monitor");
        assert.equal(queuedMessage?.notification?.outcome, "completed");
        assert.isTrue(queuedMessage?.notification?.detail?.includes("EVENT_LINE_2"));
        assert.isTrue(queuedMessage?.notification?.detail?.includes("EVENT_LINE_3"));
        // Verify XML attribute and closing-tag escaping prevents prompt tag breakout.
        assert.isTrue(queuedMessage?.text.includes('description="Watch &quot;build&quot; events"'));
        assert.isTrue(queuedMessage?.text.includes("EVENT_LINE_2 &lt;/monitor-event&gt;"));

        const shellAfterExit = yield* projections.getThreadShell(threadId);
        assert.equal((shellAfterExit?.pendingBackgroundTasks ?? []).length, 0);
      }).pipe(Effect.provide(makeTestLayer())),
    ),
);

it.live("stops persistent monitors via stopMonitor and thread.stop", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const monitors = yield* ProcessMonitorService.ProcessMonitorService;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      yield* monitors.start();

      const { threadId, runId, nodeId } = yield* seedProjectAndThread("stop");

      const persistent = yield* monitors.startMonitor({
        threadId,
        runId,
        nodeId,
        command: "sleep 30",
        description: "Dev server watcher",
        persistent: true,
      });
      assert.isFalse(persistent.waiting);
      assert.isNull(persistent.timeoutMs);

      const shellPersistent = yield* projections.getThreadShell(threadId);
      const persistentTasks = shellPersistent?.pendingBackgroundTasks ?? [];
      assert.equal(persistentTasks.length, 1);
      assert.isFalse(backgroundWorkHoldsCompletion(persistentTasks));

      const stopped = yield* monitors.stopMonitor({ threadId, monitorId: persistent.monitorId });
      assert.isTrue(stopped.stopped);

      const afterExplicitStop = yield* projections.getThreadShell(threadId);
      assert.equal((afterExplicitStop?.pendingBackgroundTasks ?? []).length, 0);

      const connected = yield* Deferred.make<void>();
      const disconnected = yield* Deferred.make<void>();
      const server = yield* Effect.acquireRelease(
        Effect.callback<NodeNet.Server>((resume) => {
          const srv = NodeNet.createServer((socket) => {
            Deferred.doneUnsafe(connected, Exit.void);
            socket.on("close", () => {
              Deferred.doneUnsafe(disconnected, Exit.void);
            });
          });
          srv.listen(0, "127.0.0.1", () => resume(Effect.succeed(srv)));
        }),
        (srv) =>
          Effect.callback<void>((resume) => {
            srv.close(() => resume(Effect.void));
          }),
      );
      const address = server.address();
      assert.isTrue(address !== null && typeof address !== "string");
      const port = typeof address === "object" && address !== null ? address.port : 0;

      const waitingMonitor = yield* monitors.startMonitor({
        threadId,
        runId,
        nodeId,
        command: `node -e "require('node:net').connect(${port}, '127.0.0.1'); setInterval(() => {}, 10000);"`,
        description: "Blocking watcher",
      });
      yield* Deferred.await(connected);
      assert.equal((yield* monitors.listMonitors({ threadId })).length, 1);

      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("cmd:thread-stop:monitor"),
        threadId,
      });

      // Wait for the child's TCP connection to drop, proving ProcessMonitorService detached and
      // terminated the OS process in reaction to thread.stop.
      yield* Deferred.await(disconnected);
      assert.equal((yield* monitors.listMonitors({ threadId })).length, 0);

      const { turnItems } = yield* projections.getThreadRecords(threadId, ["turnItems"]);
      const stoppedItem = turnItems.find((item) => item.id === waitingMonitor.turnItemId);
      assert.equal(stoppedItem?.status, "interrupted");
    }).pipe(Effect.provide(makeTestLayer())),
  ),
);

it.live("terminates monitors when their owning run is rolled back and handles timeouts", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const monitors = yield* ProcessMonitorService.ProcessMonitorService;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const eventSink = yield* EventSink.EventSinkV2;
      yield* monitors.start();

      const { threadId, runId, nodeId } = yield* seedProjectAndThread("rollback-timeout");

      const connected = yield* Deferred.make<void>();
      const disconnected = yield* Deferred.make<void>();
      const server = yield* Effect.acquireRelease(
        Effect.callback<NodeNet.Server>((resume) => {
          const srv = NodeNet.createServer((socket) => {
            Deferred.doneUnsafe(connected, Exit.void);
            socket.on("close", () => {
              Deferred.doneUnsafe(disconnected, Exit.void);
            });
          });
          srv.listen(0, "127.0.0.1", () => resume(Effect.succeed(srv)));
        }),
        (srv) =>
          Effect.callback<void>((resume) => {
            srv.close(() => resume(Effect.void));
          }),
      );
      const address = server.address();
      assert.isTrue(address !== null && typeof address !== "string");
      const port = typeof address === "object" && address !== null ? address.port : 0;

      const rolledBackMonitor = yield* monitors.startMonitor({
        threadId,
        runId,
        nodeId,
        command: `node -e "require('node:net').connect(${port}, '127.0.0.1'); setInterval(() => {}, 10000);"`,
        description: "Rollback watcher",
      });
      yield* Deferred.await(connected);

      // Verify RestartBackgroundNote preserves awaited=true for server-owned monitors.
      const { turnItems: activeItems, runs } = yield* projections.getThreadRecords(threadId, [
        "turnItems",
        "runs",
      ]);
      const monitorItem = activeItems.find((item) => item.id === rolledBackMonitor.turnItemId);
      assert.isDefined(monitorItem);
      const restartWork = cancelledTurnItemWork(monitorItem!);
      assert.equal(restartWork?.kind, "monitor");
      assert.equal(restartWork?.awaited, true);

      const owningRun = runs.find((run) => run.id === runId);
      assert.isDefined(owningRun);
      const now = yield* DateTime.now;
      yield* eventSink.write({
        events: [
          {
            id: EventId.make("evt:rollback:run"),
            type: "run.updated",
            threadId,
            runId,
            nodeId,
            providerInstanceId: instanceId,
            occurredAt: now,
            payload: { ...owningRun!, status: "rolled_back", completedAt: now },
          },
        ],
      });

      yield* Deferred.await(disconnected);
      assert.equal((yield* monitors.listMonitors({ threadId })).length, 0);

      // Also verify timeout cancels a hanging monitor and emits a timeout wake.
      const timedOut = yield* monitors.startMonitor({
        threadId,
        runId: null,
        nodeId: null,
        command: "sleep 30",
        description: "Short deadline watcher",
        timeoutMs: 1_000,
      });

      yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "turn-item.updated" &&
            event.payload.id === timedOut.turnItemId &&
            event.payload.status === "cancelled",
        ),
        Stream.take(1),
        Stream.runDrain,
      );

      const afterTimeout = yield* projections.getThreadRecords(threadId, ["messages"]);
      assert.isTrue(
        afterTimeout.messages.some(
          (message) =>
            message.notification?.source.kind === "monitor" &&
            message.notification.outcome === "cancelled" &&
            message.notification.summary.includes("timed out"),
        ),
      );
    }).pipe(Effect.provide(makeTestLayer())),
  ),
);
