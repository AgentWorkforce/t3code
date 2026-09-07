import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  type OrchestrationCommand,
  type OrchestrationProject,
  type OrchestrationThreadShell,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";

import { ServerConfig } from "../../config.ts";
import { OrchestrationEngineService } from "../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as WorkspacePaths from "../../workspace/WorkspacePaths.ts";
import type { ProviderInstance } from "../ProviderDriver.ts";
import type { AgentRelayAdapterShape } from "../Services/AgentRelayAdapter.ts";
import { AgentRelayThreadDiscoveryReactor } from "../Services/AgentRelayThreadDiscoveryReactor.ts";
import { ProviderInstanceRegistry } from "../Services/ProviderInstanceRegistry.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import type {
  ProviderRuntimeBinding,
  ProviderRuntimeBindingWithMetadata,
  ProviderSessionDirectoryShape,
} from "../Services/ProviderSessionDirectory.ts";
import { AGENT_RELAY_DEFAULT_MODEL_SLUG } from "./AgentRelayProvider.ts";
import { makeAgentRelayThreadDiscoveryReactorLive } from "./AgentRelayThreadDiscoveryReactor.ts";

const AGENT_RELAY = ProviderDriverKind.make("agentrelay");
const INSTANCE_ID = ProviderInstanceId.make("agentrelay-workspace");

/** Polls with the *real* clock while `Effect.sleep`/`Schedule.spaced` inside
 * the reactor run on the virtual `TestClock` — the same combination
 * `AgentRelayAdapter.test.ts`'s `waitUntil` uses, since a sweep's real
 * filesystem work (creating the synthetic project directory) resolves on
 * Node's real event loop, not the virtual clock `TestClock.adjust` drives. */
const waitUntil = (predicate: () => boolean): Effect.Effect<void> =>
  Effect.gen(function* () {
    while (!predicate()) {
      yield* Effect.sleep(Duration.millis(10));
    }
  }).pipe(Effect.timeout("2 seconds"), TestClock.withLive, Effect.orDie);

/** A behaviorally-accurate in-memory `ProviderSessionDirectory` fake: real
 * `onConflict` semantics and a real `listBindings()` scan, so the reactor's
 * "already claimed" logic runs against the same shape it does in
 * production, without wiring the SQL-backed persistence layer. Plain
 * mutable state (not `Ref`) so test assertions and `waitUntil` predicates
 * can read it synchronously between/while the reactor's fiber runs. */
function makeFakeDirectory() {
  const bindings = new Map<ThreadId, ProviderRuntimeBindingWithMetadata>();

  const upsert: ProviderSessionDirectoryShape["upsert"] = (binding, options) =>
    Effect.sync(() => {
      const existing = bindings.get(binding.threadId);
      if (existing && options?.onConflict === "ignore") {
        return;
      }
      bindings.set(binding.threadId, {
        ...existing,
        ...binding,
        lastSeenAt: "2026-01-01T00:00:00.000Z",
      });
    });

  const shape: ProviderSessionDirectoryShape = {
    upsert,
    recordImportedTranscript: () => Effect.void,
    getProvider: () => Effect.die("unused"),
    getBinding: (threadId) => Effect.sync(() => Option.fromUndefinedOr(bindings.get(threadId))),
    listThreadIds: () => Effect.die("unused"),
    listBindings: () => Effect.sync(() => Array.from(bindings.values())),
  };
  // Synchronous, direct seeding for test setup (outside the Effect world) —
  // `upsert` itself is only ever invoked through the reactor under test.
  const seed = (binding: ProviderRuntimeBinding) =>
    bindings.set(binding.threadId, { ...binding, lastSeenAt: "2026-01-01T00:00:00.000Z" });
  return { shape, bindings, seed };
}

interface FakeAgent {
  readonly name: string;
  readonly status: "online" | "offline" | "unknown";
}

function makeAgentRelayAdapter(
  listWorkspaceAgents?: AgentRelayAdapterShape["listWorkspaceAgents"],
): AgentRelayAdapterShape {
  return {
    provider: AGENT_RELAY,
    capabilities: { sessionModelSwitch: "unsupported" },
    startSession: () => Effect.die("unused"),
    sendTurn: () => Effect.die("unused"),
    interruptTurn: () => Effect.die("unused"),
    respondToRequest: () => Effect.die("unused"),
    respondToUserInput: () => Effect.die("unused"),
    stopSession: () => Effect.die("unused"),
    listSessions: () => Effect.succeed([]),
    hasSession: () => Effect.succeed(false),
    readThread: () => Effect.die("unused"),
    rollbackThread: () => Effect.die("unused"),
    stopAll: () => Effect.die("unused"),
    streamEvents: Stream.empty,
    ...(listWorkspaceAgents ? { listWorkspaceAgents } : {}),
  };
}

function makeAgentRelayInstance(input: {
  readonly adapter: AgentRelayAdapterShape;
  readonly displayName?: string;
  readonly enabled?: boolean;
}): ProviderInstance {
  return {
    instanceId: INSTANCE_ID,
    driverKind: AGENT_RELAY,
    continuationIdentity: {
      driverKind: AGENT_RELAY,
      continuationKey: `agentrelay:instance:${INSTANCE_ID}`,
    },
    displayName: input.displayName,
    enabled: input.enabled ?? true,
    snapshot: {
      resolveMaintenance: () => Effect.die("unused"),
      getSnapshot: Effect.die("unused"),
      refresh: Effect.die("unused"),
      streamChanges: Stream.empty,
      applyUsageLimits: () => Effect.die("unused"),
    },
    adapter: input.adapter,
    textGeneration: {
      generateCommitMessage: () => Effect.die("unused"),
      generatePrContent: () => Effect.die("unused"),
      generateBranchName: () => Effect.die("unused"),
      generateThreadTitle: () => Effect.die("unused"),
    },
  };
}

function makeProject(id: ProjectId, workspaceRoot: string, title: string): OrchestrationProject {
  return {
    id,
    title,
    workspaceRoot,
    defaultModelSelection: null,
    scripts: [],
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    deletedAt: null,
  };
}

function makeThreadShell(
  id: ThreadId,
  projectId: ProjectId,
  title: string,
  modelSelection: { readonly instanceId: ProviderInstanceId; readonly model: string },
): OrchestrationThreadShell {
  return {
    id,
    projectId,
    title,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    latestTurn: null,
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    session: null,
    latestUserMessageAt: null,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
  };
}

/** Minimal but behaviorally real orchestration model: `dispatch` mutates it
 * the same way the real decider would for the four command types this
 * reactor issues, and the `ProjectionSnapshotQuery` mock reads back from it —
 * so `getActiveProjectByWorkspaceRoot` / `getThreadShellById` reflect exactly
 * what the reactor dispatched, not a canned fixture. */
function makeHarness(input: {
  readonly agents: ReadonlyArray<FakeAgent>;
  readonly enabled?: boolean;
  readonly seedBinding?: ProviderRuntimeBinding;
}) {
  let agents: ReadonlyArray<FakeAgent> = input.agents;
  const directory = makeFakeDirectory();
  if (input.seedBinding) {
    directory.seed(input.seedBinding);
  }
  const projectsByRoot = new Map<string, OrchestrationProject>();
  const threadsById = new Map<ThreadId, OrchestrationThreadShell>();
  const commands: Array<OrchestrationCommand> = [];

  const dispatch: OrchestrationEngineService["Service"]["dispatch"] = (command) =>
    Effect.sync(() => {
      commands.push(command);
      if (command.type === "project.create") {
        projectsByRoot.set(
          command.workspaceRoot,
          makeProject(command.projectId, command.workspaceRoot, command.title),
        );
      } else if (command.type === "thread.create") {
        threadsById.set(
          command.threadId,
          makeThreadShell(
            command.threadId,
            command.projectId,
            command.title,
            command.modelSelection,
          ),
        );
      } else if (command.type === "thread.settle") {
        const existing = threadsById.get(command.threadId);
        if (existing) {
          threadsById.set(command.threadId, {
            ...existing,
            settledOverride: "settled",
            settledAt: "2026-01-01T00:00:00.000Z",
          });
        }
      } else if (command.type !== "thread.activity.append") {
        throw new Error(`Unexpected command: ${command.type}`);
      }
      return { sequence: 1 };
    });

  const instance = makeAgentRelayInstance({
    ...(input.enabled !== undefined ? { enabled: input.enabled } : {}),
    adapter: makeAgentRelayAdapter(() => Effect.sync(() => agents)),
  });

  const dependencies = Layer.mergeAll(
    Layer.mock(ProviderInstanceRegistry)({
      listInstances: Effect.succeed([instance]),
    }),
    Layer.mock(ProjectionSnapshotQuery)({
      getActiveProjectByWorkspaceRoot: (workspaceRoot) =>
        Effect.sync(() => Option.fromUndefinedOr(projectsByRoot.get(workspaceRoot))),
      getThreadShellById: (threadId) =>
        Effect.sync(() => Option.fromUndefinedOr(threadsById.get(threadId))),
    }),
    Layer.mock(OrchestrationEngineService)({ dispatch }),
    Layer.succeed(ProviderSessionDirectory, directory.shape),
    WorkspacePaths.layer,
    ServerConfig.layerTest(process.cwd(), { prefix: "agentrelay-discovery-test-" }),
  ).pipe(Layer.provideMerge(NodeServices.layer));

  return {
    directory,
    commands,
    projectsByRoot,
    threadsById,
    setAgents: (next: ReadonlyArray<FakeAgent>) => {
      agents = next;
    },
    dependencies,
  };
}

const startReactor = <R, E>(
  dependencies: Layer.Layer<R, E, never>,
  options?: { readonly sweepIntervalMs?: number; readonly offlineDebounceMs?: number },
) =>
  Effect.gen(function* () {
    const reactor = yield* Effect.provide(
      Effect.service(AgentRelayThreadDiscoveryReactor),
      makeAgentRelayThreadDiscoveryReactorLive(options).pipe(Layer.provide(dependencies)),
    );
    yield* reactor.start();
  });

it.layer(NodeServices.layer)("AgentRelayThreadDiscoveryReactor", (it) => {
  it.effect("materializes a thread for an unclaimed online agent", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = makeHarness({ agents: [{ name: "Worker1", status: "online" }] });
        yield* startReactor(harness.dependencies);
        yield* waitUntil(() =>
          harness.commands.some((command) => command.type === "thread.create"),
        );

        const threadCreate = harness.commands.find((command) => command.type === "thread.create");
        assert.isDefined(threadCreate);
        if (threadCreate?.type !== "thread.create") return assert.fail("expected thread.create");
        assert.strictEqual(threadCreate.title, "Worker1");
        assert.deepStrictEqual(threadCreate.modelSelection, {
          instanceId: INSTANCE_ID,
          model: AGENT_RELAY_DEFAULT_MODEL_SLUG,
        });

        const projectCreate = harness.commands.find((command) => command.type === "project.create");
        assert.isDefined(projectCreate);
        if (projectCreate?.type !== "project.create") return assert.fail("expected project.create");
        const fs = yield* FileSystem.FileSystem;
        const exists = yield* fs.exists(projectCreate.workspaceRoot);
        assert.isTrue(exists);

        const bindings = yield* harness.directory.shape.listBindings();
        const binding = bindings.find((entry) => entry.threadId === threadCreate.threadId);
        assert.isDefined(binding);
        assert.deepStrictEqual(binding?.resumeCursor, { agentName: "Worker1" });
      }),
    ),
  );

  it.effect("does not double-materialize an agent already bound to a thread", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const existingThreadId = ThreadId.make("thread-already-bound");
        const harness = makeHarness({
          agents: [{ name: "Worker2", status: "online" }],
          seedBinding: {
            threadId: existingThreadId,
            provider: AGENT_RELAY,
            providerInstanceId: INSTANCE_ID,
            status: "stopped",
            resumeCursor: { agentName: "Worker2" },
          },
        });
        yield* startReactor(harness.dependencies);
        // Give the sweep a chance to run: since it must NOT dispatch
        // anything, wait on the real clock instead of a signal that never
        // arrives.
        yield* Effect.sleep(Duration.millis(50)).pipe(TestClock.withLive);

        assert.isUndefined(harness.commands.find((command) => command.type === "thread.create"));
        assert.isUndefined(harness.commands.find((command) => command.type === "project.create"));
      }),
    ),
  );

  it.effect(
    "settles a materialized thread after its agent stays offline past the debounce window",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const boundThreadId = ThreadId.make("thread-goes-offline");
          const harness = makeHarness({
            agents: [{ name: "Worker3", status: "online" }],
            seedBinding: {
              threadId: boundThreadId,
              provider: AGENT_RELAY,
              providerInstanceId: INSTANCE_ID,
              status: "running",
              resumeCursor: { agentName: "Worker3" },
            },
          });
          // A pre-existing thread needs a matching thread row for the
          // "already settled?" check the reactor runs before dispatching.
          harness.threadsById.set(
            boundThreadId,
            makeThreadShell(boundThreadId, ProjectId.make("existing-project"), "Worker3", {
              instanceId: INSTANCE_ID,
              model: AGENT_RELAY_DEFAULT_MODEL_SLUG,
            }),
          );

          yield* startReactor(harness.dependencies, {
            sweepIntervalMs: 1_000,
            offlineDebounceMs: 5_000,
          });
          // First sweep: online, seeds "last seen online".
          yield* Effect.sleep(Duration.millis(50)).pipe(TestClock.withLive);

          harness.setAgents([]);
          // Still within the debounce window: must not settle yet.
          yield* TestClock.adjust("2 seconds");
          yield* Effect.sleep(Duration.millis(50)).pipe(TestClock.withLive);
          assert.isUndefined(harness.commands.find((command) => command.type === "thread.settle"));

          // Past the debounce window: now it must settle.
          yield* TestClock.adjust("6 seconds");
          yield* waitUntil(() =>
            harness.commands.some((command) => command.type === "thread.settle"),
          );

          const settle = harness.commands.find((command) => command.type === "thread.settle");
          assert.isDefined(settle);
          if (settle?.type !== "thread.settle") return assert.fail("expected thread.settle");
          assert.strictEqual(settle.threadId, boundThreadId);
          assert.strictEqual(harness.threadsById.get(boundThreadId)?.settledOverride, "settled");
        }),
      ),
  );

  it.effect("leaves a Single-mode instance untouched (no listWorkspaceAgents)", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const singleModeInstance = makeAgentRelayInstance({
          adapter: makeAgentRelayAdapter(),
        });

        const commands: Array<OrchestrationCommand> = [];
        const dependencies = Layer.mergeAll(
          Layer.mock(ProviderInstanceRegistry)({
            listInstances: Effect.succeed([singleModeInstance]),
          }),
          Layer.mock(ProjectionSnapshotQuery)({}),
          Layer.mock(OrchestrationEngineService)({
            dispatch: (command) =>
              Effect.sync(() => commands.push(command)).pipe(Effect.as({ sequence: 1 })),
          }),
          Layer.succeed(ProviderSessionDirectory, makeFakeDirectory().shape),
          WorkspacePaths.layer,
          ServerConfig.layerTest(process.cwd(), { prefix: "agentrelay-discovery-single-test-" }),
        ).pipe(Layer.provideMerge(NodeServices.layer));

        yield* startReactor(dependencies);
        yield* Effect.sleep(Duration.millis(50)).pipe(TestClock.withLive);

        assert.strictEqual(commands.length, 0);
      }),
    ),
  );
});
