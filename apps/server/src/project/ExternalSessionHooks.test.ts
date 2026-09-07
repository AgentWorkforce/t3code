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
import { HttpBody, HttpClient, HttpRouter } from "effect/unstable/http";

import { decideOrchestrationCommand } from "../orchestration/decider.ts";
import { createEmptyReadModel, projectEvent } from "../orchestration/projector.ts";
import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { EXTERNAL_SESSIONS_ROUTE_PATH, externalSessionHooksRouteLayer } from "./ExternalSessionHooks.ts";

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
  ) =>
    Effect.gen(function* () {
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
      Effect.sync(() => Option.fromNullishOr(readModel.threads.find((thread) => thread.id === threadId))),
    getThreadDetailSnapshot: () => Effect.die("unused in this test"),
  });

  return {
    engine,
    snapshots,
    seedProject,
    getThread: (threadId: ThreadId) => readModel.threads.find((thread) => thread.id === threadId),
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

it.effect("materializes a read-only marker thread on start and settles it on end", () =>
  Effect.scoped(
    Effect.gen(function* () {
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
