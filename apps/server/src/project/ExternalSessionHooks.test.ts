import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  ProjectId,
  ThreadId,
  type OrchestrationCommand,
  type OrchestrationProject,
  type OrchestrationReadModel,
  type OrchestrationThread,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { HttpBody, HttpClient, HttpRouter } from "effect/unstable/http";

import { decideOrchestrationCommand } from "../orchestration/decider.ts";
import { OrchestrationCommandInvariantError } from "../orchestration/Errors.ts";
import { createEmptyReadModel, projectEvent } from "../orchestration/projector.ts";
import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import {
  EXTERNAL_SESSIONS_ROUTE_PATH,
  externalSessionHooksRouteLayer,
  isLoopbackRemoteAddress,
} from "./ExternalSessionHooks.ts";

const PROJECT_ID = ProjectId.make("project-1");
const WORKSPACE_ROOT = "/tmp/external-session-project";
const NOW = "2026-09-07T10:00:00.000Z";

function toShell(thread: OrchestrationThread): OrchestrationThreadShell {
  return {
    id: thread.id,
    projectId: thread.projectId,
    title: thread.title,
    modelSelection: thread.modelSelection,
    runtimeMode: thread.runtimeMode,
    interactionMode: thread.interactionMode,
    branch: thread.branch,
    worktreePath: thread.worktreePath,
    latestTurn: thread.latestTurn,
    createdAt: thread.createdAt,
    updatedAt: thread.updatedAt,
    archivedAt: thread.archivedAt,
    settledOverride: thread.settledOverride,
    settledAt: thread.settledAt,
    session: thread.session,
    latestUserMessageAt: null,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
  };
}

/**
 * Drives real commands through the pure decider + projector (no engine/DB),
 * so assertions reflect the same projected read state a real server would
 * produce — mirroring how decider.import.test.ts exercises this pair.
 */
const makeInMemoryOrchestration = Effect.fn("makeInMemoryOrchestration")(function* () {
  const crypto = yield* Crypto.Crypto;
  let readModel: OrchestrationReadModel = createEmptyReadModel(NOW);
  let sequence = 0;
  let failNextDispatchOfType: OrchestrationCommand["type"] | null = null;

  const seedProject = (project: OrchestrationProject) =>
    Effect.gen(function* () {
      sequence += 1;
      readModel = yield* projectEvent(readModel, {
        sequence,
        eventId: EventId.make(`event-project-${project.id}`),
        aggregateKind: "project",
        aggregateId: project.id,
        type: "project.created",
        occurredAt: project.createdAt,
        commandId: CommandId.make(`command-project-${project.id}`),
        causationEventId: null,
        correlationId: CommandId.make(`command-project-${project.id}`),
        metadata: {},
        payload: {
          projectId: project.id,
          title: project.title,
          workspaceRoot: project.workspaceRoot,
          defaultModelSelection: null,
          scripts: [],
          createdAt: project.createdAt,
          updatedAt: project.createdAt,
        },
      });
    });

  const dispatch: OrchestrationEngine.OrchestrationEngineShape["dispatch"] = (
    command: OrchestrationCommand,
  ) => {
    // One-shot failure injection for the partial-failure recovery
    // regression test: a real transient dispatch error surfaces as an
    // `OrchestrationDispatchError`, not a defect, so it must flow through
    // this typed channel rather than the `Effect.orDie` below.
    if (failNextDispatchOfType === command.type) {
      failNextDispatchOfType = null;
      return Effect.fail(
        new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: "simulated transient dispatch failure",
        }),
      );
    }
    return Effect.gen(function* () {
      const produced = yield* decideOrchestrationCommand({ command, readModel }).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
      );
      const events = Array.isArray(produced) ? produced : [produced];
      for (const event of events) {
        sequence += 1;
        readModel = yield* projectEvent(readModel, { ...event, sequence });
      }
      return { sequence };
    }).pipe(Effect.orDie);
  };

  const engine = OrchestrationEngine.OrchestrationEngineService.of({
    readEvents: () => Stream.die("unused in this test"),
    readThreadEvents: () => Stream.die("unused in this test"),
    getThreadReplayStats: () => Effect.die("unused in this test"),
    dispatch,
    streamDomainEvents: Stream.empty,
    subscribeDomainEvents: Effect.die("unused in this test"),
    latestSequence: Effect.succeed(0),
  });

  const snapshots = ProjectionSnapshotQuery.ProjectionSnapshotQuery.of({
    getUserInputActivity: () => Effect.die("unused in this test"),
    getCommandReadModel: () => Effect.die("unused in this test"),
    getSnapshot: () => Effect.die("unused in this test"),
    getShellSnapshot: () => Effect.die("unused in this test"),
    getArchivedShellSnapshot: () => Effect.die("unused in this test"),
    searchThreads: () => Effect.die("unused in this test"),
    getSnapshotSequence: () => Effect.die("unused in this test"),
    getCounts: () => Effect.die("unused in this test"),
    getEventReplayStats: () => Effect.die("unused in this test"),
    getActiveProjectByWorkspaceRoot: (workspaceRoot) =>
      Effect.sync(() =>
        Option.fromNullishOr(
          readModel.projects.find(
            (project) => project.workspaceRoot === workspaceRoot && project.deletedAt === null,
          ),
        ),
      ),
    getProjectShellById: () => Effect.die("unused in this test"),
    getFirstActiveThreadIdByProjectId: () => Effect.die("unused in this test"),
    getImportedAgentSessionSources: () => Effect.die("unused in this test"),
    getThreadCheckpointContext: () => Effect.die("unused in this test"),
    getFullThreadDiffContext: () => Effect.die("unused in this test"),
    getThreadShellById: (threadId) =>
      Effect.sync(() =>
        Option.map(
          Option.fromNullishOr(readModel.threads.find((thread) => thread.id === threadId)),
          toShell,
        ),
      ),
    getThreadRuntimeContext: () => Effect.die("unused in this test"),
    getTurnStartMessage: () => Effect.die("unused in this test"),
    getThreadDetailById: (threadId) =>
      Effect.sync(() =>
        Option.fromNullishOr(readModel.threads.find((thread) => thread.id === threadId)),
      ),
    getThreadDetailSnapshot: () => Effect.die("unused in this test"),
  });

  return {
    engine,
    snapshots,
    seedProject,
    getThread: (threadId: ThreadId) => readModel.threads.find((thread) => thread.id === threadId),
    failNextDispatchOf: (type: OrchestrationCommand["type"]) => {
      failNextDispatchOfType = type;
    },
  };
});

const servicesLayer = (harness: Effect.Success<ReturnType<typeof makeInMemoryOrchestration>>) =>
  Layer.mergeAll(
    Layer.succeed(OrchestrationEngine.OrchestrationEngineService, harness.engine),
    Layer.succeed(ProjectionSnapshotQuery.ProjectionSnapshotQuery, harness.snapshots),
  );

const postHookEvent = (input: {
  readonly provider: "claude" | "codex";
  readonly pid: number;
  readonly cwd: string;
  readonly sessionId: string;
  readonly event: "start" | "end";
  readonly timestamp: string;
}) =>
  Effect.flatMap(HttpClient.HttpClient, (httpClient) =>
    httpClient.post(EXTERNAL_SESSIONS_ROUTE_PATH, { body: HttpBody.jsonUnsafe(input) }),
  );

/**
 * Anchors the TestClock to `NOW`, seeds a project at {@link WORKSPACE_ROOT},
 * and serves the route — the setup every hook-payload-validation test below
 * needs before it can post to {@link EXTERNAL_SESSIONS_ROUTE_PATH}.
 */
const setUpMarkerRouterHarness = Effect.fn("setUpMarkerRouterHarness")(function* () {
  yield* TestClock.setTime(Date.parse(NOW));
  const harness = yield* makeInMemoryOrchestration();
  yield* harness.seedProject({
    id: PROJECT_ID,
    title: "Project",
    workspaceRoot: WORKSPACE_ROOT,
    repositoryIdentity: null,
    defaultModelSelection: null,
    defaultThreadEnvMode: null,
    autoPull: false,
    faviconPath: null,
    projectIcon: null,
    scripts: [],
    createdAt: NOW,
    updatedAt: NOW,
    deletedAt: null,
  });
  yield* HttpRouter.serve(externalSessionHooksRouteLayer, {
    disableListenLog: true,
    disableLogger: true,
  }).pipe(Layer.provide(servicesLayer(harness)), Layer.build);
  return harness;
});

it.effect("materializes a read-only marker thread on start and settles it on end", () =>
  Effect.scoped(
    Effect.gen(function* () {
      // The route enforces a small future-timestamp grace window against real
      // wall-clock time; anchor the TestClock to this fixture's timestamps.
      yield* TestClock.setTime(Date.parse(NOW));
      const harness = yield* makeInMemoryOrchestration();
      yield* harness.seedProject({
        id: PROJECT_ID,
        title: "Project",
        workspaceRoot: WORKSPACE_ROOT,
        repositoryIdentity: null,
        defaultModelSelection: null,
        defaultThreadEnvMode: null,
        autoPull: false,
        faviconPath: null,
        projectIcon: null,
        scripts: [],
        createdAt: NOW,
        updatedAt: NOW,
        deletedAt: null,
      });
      yield* HttpRouter.serve(externalSessionHooksRouteLayer, {
        disableListenLog: true,
        disableLogger: true,
      }).pipe(Layer.provide(servicesLayer(harness)), Layer.build);

      const threadId = ThreadId.make("external:claude:session-abc");
      const startResponse = yield* postHookEvent({
        provider: "claude",
        pid: 4242,
        cwd: WORKSPACE_ROOT,
        sessionId: "session-abc",
        event: "start",
        timestamp: NOW,
      });
      expect(startResponse.status).toBe(200);
      expect(yield* startResponse.json).toEqual({ recorded: true, threadId });

      const afterStart = harness.getThread(threadId);
      expect(afterStart?.settledAt).toBeNull();
      expect(afterStart?.settledOverride).toBeNull();
      expect(afterStart?.activities.map((activity) => activity.kind)).toEqual([
        "external-session.started",
      ]);
      expect(afterStart?.activities[0]?.summary).toContain("pid 4242");

      // A duplicate start (hook fired twice) must not error or re-create.
      const duplicateStartResponse = yield* postHookEvent({
        provider: "claude",
        pid: 4242,
        cwd: WORKSPACE_ROOT,
        sessionId: "session-abc",
        event: "start",
        timestamp: NOW,
      });
      expect(yield* duplicateStartResponse.json).toEqual({
        recorded: false,
        reason: "already-recorded",
      });
      expect(harness.getThread(threadId)?.activities).toHaveLength(1);

      const endResponse = yield* postHookEvent({
        provider: "claude",
        pid: 4242,
        cwd: WORKSPACE_ROOT,
        sessionId: "session-abc",
        event: "end",
        timestamp: "2026-09-07T10:05:00.000Z",
      });
      expect(endResponse.status).toBe(200);
      expect(yield* endResponse.json).toEqual({ recorded: true, threadId });

      const afterEnd = harness.getThread(threadId);
      expect(afterEnd?.settledOverride).toBe("settled");
      expect(afterEnd?.settledAt).not.toBeNull();
      expect(afterEnd?.activities.map((activity) => activity.kind)).toEqual([
        "external-session.started",
        "external-session.ended",
      ]);
    }),
  ).pipe(Effect.provide(Layer.mergeAll(NodeHttpServer.layerTest, NodeServices.layer))),
);

it.effect("drops events it cannot attach to a known project or session", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse(NOW));
      const harness = yield* makeInMemoryOrchestration();
      yield* HttpRouter.serve(externalSessionHooksRouteLayer, {
        disableListenLog: true,
        disableLogger: true,
      }).pipe(Layer.provide(servicesLayer(harness)), Layer.build);

      const startWithoutProject = yield* postHookEvent({
        provider: "codex",
        pid: 99,
        cwd: "/tmp/never-opened-in-t3",
        sessionId: "session-orphan",
        event: "start",
        timestamp: NOW,
      });
      expect(yield* startWithoutProject.json).toEqual({
        recorded: false,
        reason: "project-not-found",
      });

      const endWithoutStart = yield* postHookEvent({
        provider: "codex",
        pid: 99,
        cwd: "/tmp/never-opened-in-t3",
        sessionId: "session-never-started",
        event: "end",
        timestamp: NOW,
      });
      expect(yield* endWithoutStart.json).toEqual({ recorded: false, reason: "unknown-session" });
    }),
  ).pipe(Effect.provide(Layer.mergeAll(NodeHttpServer.layerTest, NodeServices.layer))),
);

it("only accepts callers whose socket address is genuinely loopback", () => {
  expect(isLoopbackRemoteAddress("127.0.0.1")).toBe(true);
  expect(isLoopbackRemoteAddress("127.5.6.7")).toBe(true);
  expect(isLoopbackRemoteAddress("::1")).toBe(true);
  expect(isLoopbackRemoteAddress("::ffff:127.0.0.1")).toBe(true);
  expect(isLoopbackRemoteAddress("192.168.1.50")).toBe(false);
  expect(isLoopbackRemoteAddress("10.0.0.5")).toBe(false);
  expect(isLoopbackRemoteAddress("::ffff:10.0.0.5")).toBe(false);
  expect(isLoopbackRemoteAddress("2001:db8::1")).toBe(false);
  expect(isLoopbackRemoteAddress("")).toBe(false);
});

it.effect("rejects a hook payload larger than the byte cap before parsing it", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* setUpMarkerRouterHarness();

      const oversizedBody = "x".repeat(20 * 1_024);
      const response = yield* Effect.flatMap(HttpClient.HttpClient, (httpClient) =>
        httpClient.post(EXTERNAL_SESSIONS_ROUTE_PATH, {
          body: HttpBody.text(oversizedBody, "application/json"),
        }),
      );
      expect(response.status).toBe(413);
    }),
  ).pipe(Effect.provide(Layer.mergeAll(NodeHttpServer.layerTest, NodeServices.layer))),
);

it.effect("rejects a cwd/sessionId longer than the bounded field length", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* setUpMarkerRouterHarness();

      const tooLongCwd = yield* postHookEvent({
        provider: "claude",
        pid: 1,
        cwd: `${WORKSPACE_ROOT}/${"a".repeat(5_000)}`,
        sessionId: "session-too-long-cwd",
        event: "start",
        timestamp: NOW,
      });
      expect(tooLongCwd.status).toBe(400);

      const tooLongSessionId = yield* postHookEvent({
        provider: "claude",
        pid: 1,
        cwd: WORKSPACE_ROOT,
        sessionId: "s".repeat(5_000),
        event: "start",
        timestamp: NOW,
      });
      expect(tooLongSessionId.status).toBe(400);
    }),
  ).pipe(Effect.provide(Layer.mergeAll(NodeHttpServer.layerTest, NodeServices.layer))),
);

it.effect("rejects an implausible timestamp instead of persisting it", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* setUpMarkerRouterHarness();

      // NOW + 30 minutes: past the 5 minute future-clock-skew grace window.
      const tooFarFuture = yield* postHookEvent({
        provider: "claude",
        pid: 1,
        cwd: WORKSPACE_ROOT,
        sessionId: "session-future-timestamp",
        event: "start",
        timestamp: "2026-09-07T10:30:00.000Z",
      });
      expect(tooFarFuture.status).toBe(400);

      const notADate = yield* postHookEvent({
        provider: "claude",
        pid: 1,
        cwd: WORKSPACE_ROOT,
        sessionId: "session-garbage-timestamp",
        event: "start",
        timestamp: "not-a-timestamp",
      });
      expect(notADate.status).toBe(400);

      const absurdlyOld = yield* postHookEvent({
        provider: "claude",
        pid: 1,
        cwd: WORKSPACE_ROOT,
        sessionId: "session-ancient-timestamp",
        event: "start",
        timestamp: "1900-01-01T00:00:00.000Z",
      });
      expect(absurdlyOld.status).toBe(400);
    }),
  ).pipe(Effect.provide(Layer.mergeAll(NodeHttpServer.layerTest, NodeServices.layer))),
);

it.effect("does not append a duplicate 'ended' activity or re-settle on a retried end hook", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const harness = yield* setUpMarkerRouterHarness();
      const threadId = ThreadId.make("external:claude:session-end-retry");

      yield* postHookEvent({
        provider: "claude",
        pid: 1,
        cwd: WORKSPACE_ROOT,
        sessionId: "session-end-retry",
        event: "start",
        timestamp: NOW,
      });

      const firstEnd = yield* postHookEvent({
        provider: "claude",
        pid: 1,
        cwd: WORKSPACE_ROOT,
        sessionId: "session-end-retry",
        event: "end",
        timestamp: NOW,
      });
      expect(yield* firstEnd.json).toEqual({ recorded: true, threadId });
      const settledAtAfterFirstEnd = harness.getThread(threadId)?.settledAt;
      expect(settledAtAfterFirstEnd).not.toBeNull();

      // A retried "end" hook (network retry, at-least-once delivery) must be
      // a no-op: no second "ended" activity, no re-settle.
      const retriedEnd = yield* postHookEvent({
        provider: "claude",
        pid: 1,
        cwd: WORKSPACE_ROOT,
        sessionId: "session-end-retry",
        event: "end",
        timestamp: NOW,
      });
      expect(yield* retriedEnd.json).toEqual({ recorded: false, reason: "already-recorded" });

      const thread = harness.getThread(threadId);
      expect(
        thread?.activities.filter((activity) => activity.kind === "external-session.ended"),
      ).toHaveLength(1);
      expect(thread?.settledAt).toBe(settledAtAfterFirstEnd);
    }),
  ).pipe(Effect.provide(Layer.mergeAll(NodeHttpServer.layerTest, NodeServices.layer))),
);

it.effect("completes a missing start activity on retry after the append dispatch failed", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const harness = yield* setUpMarkerRouterHarness();
      const threadId = ThreadId.make("external:claude:session-partial-start");

      // Force `thread.create` to succeed but the follow-up
      // `thread.activity.append` dispatch to fail transiently — the exact
      // partial-failure sequence from the bug report.
      harness.failNextDispatchOf("thread.activity.append");
      const firstStart = yield* postHookEvent({
        provider: "claude",
        pid: 7,
        cwd: WORKSPACE_ROOT,
        sessionId: "session-partial-start",
        event: "start",
        timestamp: NOW,
      });
      expect(yield* firstStart.json).toEqual({ recorded: false, reason: undefined });
      const afterFailedStart = harness.getThread(threadId);
      expect(afterFailedStart).toBeDefined();
      expect(afterFailedStart?.activities).toHaveLength(0);

      // Retrying the same start hook must notice the thread exists but has
      // no start activity, and complete the missing step — not treat the
      // existing thread as already fully recorded.
      const retriedStart = yield* postHookEvent({
        provider: "claude",
        pid: 7,
        cwd: WORKSPACE_ROOT,
        sessionId: "session-partial-start",
        event: "start",
        timestamp: NOW,
      });
      expect(yield* retriedStart.json).toEqual({ recorded: true, threadId });
      const afterRetry = harness.getThread(threadId);
      expect(afterRetry?.activities.map((activity) => activity.kind)).toEqual([
        "external-session.started",
      ]);
    }),
  ).pipe(Effect.provide(Layer.mergeAll(NodeHttpServer.layerTest, NodeServices.layer))),
);
