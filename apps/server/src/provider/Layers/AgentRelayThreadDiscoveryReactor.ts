/**
 * AgentRelayThreadDiscoveryReactorLive — auto-materializes T3 Code threads
 * for agents that are already running in a Workspace-mode Agent Relay
 * instance's workspace, however they were spawned (Agent Relay's own CLI,
 * its MCP tools, a fleet trigger, or an earlier T3 Code thread).
 *
 * Shaped like `ProviderSessionReaper.ts`: one polling sweep, forked once at
 * `start()` and left to run for the life of the server. See
 * `docs/internals/providers.md` ("Left out: auto-materializing threads for
 * already-running agents", now replaced by the design note this reactor
 * implements) for the investigation this is built on:
 *
 * - `workspaceRoot` is an opaque uniqueness key at the command-decider
 *   level (`commandInvariants.ts`'s `requireActiveProjectWorkspaceRootAbsent`
 *   only string-compares it) and checkpointing already no-ops on a
 *   non-git directory (`CheckpointReactor.ts`'s `isGitRepository` guard) —
 *   so a synthetic project directory needs to exist, but does not need to
 *   be git-initialized. Nothing writes to it since the actual agent runs on
 *   Agent Relay's side, not on this filesystem.
 * - `AgentSessionImporter.ts` already establishes the precedent this
 *   reactor follows for "materialize a thread that attaches instead of
 *   spawning": install a `ProviderSessionDirectory` binding carrying a
 *   `resumeCursor` *before* the thread becomes visible (`onConflict:
 *   "ignore"`), then dispatch `thread.create`. `ProviderService.startSession`
 *   already prefers a persisted binding's `resumeCursor` over spawning fresh
 *   (see its `effectiveResumeCursor` fallback) — the same mechanism
 *   `AgentRelayAdapter.startSession` reads via `isAgentRelayResumeCursor`.
 *   No new attach-signal plumbing is needed.
 *
 * One project per provider *instance*, not per agent: created lazily (via
 * `WorkspacePaths.normalizeWorkspaceRoot(..., { createIfMissing: true })`,
 * the same helper `project.create`'s own client-command normalizer uses)
 * the first time a sweep finds an unclaimed online agent for that instance,
 * under `<T3 home>/agent-relay/<instanceId>` — there is nothing for a human
 * to browse to and pick, so this never prompts a file dialog.
 *
 * @module AgentRelayThreadDiscoveryReactor
 */
import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  EventId,
  ProjectId,
  ProviderDriverKind,
  type ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schedule from "effect/Schedule";

import { ServerConfig } from "../../config.ts";
import { OrchestrationEngineService } from "../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { forkParked } from "../../serverActivation.ts";
import { WorkspacePaths } from "../../workspace/WorkspacePaths.ts";
import type { ProviderInstance } from "../ProviderDriver.ts";
import type { AgentRelayAdapterShape } from "../Services/AgentRelayAdapter.ts";
import type { AgentRelayWorkspaceAgentSummary } from "../Services/AgentRelayWorkspaceClient.ts";
import {
  AgentRelayThreadDiscoveryReactor,
  type AgentRelayThreadDiscoveryReactorShape,
} from "../Services/AgentRelayThreadDiscoveryReactor.ts";
import { ProviderInstanceRegistry } from "../Services/ProviderInstanceRegistry.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import { isAgentRelayResumeCursor } from "./AgentRelayAdapter.ts";
import { AGENT_RELAY_DEFAULT_MODEL_SLUG } from "./AgentRelayProvider.ts";

const AGENT_RELAY_DRIVER_KIND = ProviderDriverKind.make("agentrelay");

// How often to re-list every configured Workspace-mode instance's agents.
// Agent Relay's own presence push channel is unverified end-to-end (see
// `AgentRelayWorkspaceClientLive.ts`), so this reactor polls only —
// `waitForAgentOnline` already established that polling alone is sufficient
// for correctness here, just slower than a push would be.
const DEFAULT_SWEEP_INTERVAL_MS = 30_000;

// An agent missing from one `listAgents()` sweep is not proof it is gone —
// the same presence-reliability uncertainty documented above. Require it to
// stay unconfirmed online across several sweep cycles (a comfortable margin
// past a single transient miss) before treating it as offline.
const DEFAULT_OFFLINE_DEBOUNCE_MS = 120_000;

export interface AgentRelayThreadDiscoveryReactorLiveOptions {
  readonly sweepIntervalMs?: number;
  readonly offlineDebounceMs?: number;
}

function slugify(value: string, maxLength = 48): string {
  const slug = value.replace(/[^a-zA-Z0-9_-]/g, "-").slice(0, maxLength);
  return slug.length > 0 ? slug : "default";
}

interface AgentRelayWorkspaceInstance {
  readonly instanceId: ProviderInstanceId;
  readonly displayName: string | undefined;
  readonly listWorkspaceAgents: NonNullable<AgentRelayAdapterShape["listWorkspaceAgents"]>;
}

/** Only enabled Workspace-mode instances discover anything — Single mode has
 * no `listWorkspaceAgents` (see `AgentRelayAdapterShape`), and a disabled
 * instance's driver can still hold a live workspace client (its `create()`
 * does not gate that on `enabled`), so this must check both. */
function asAgentRelayWorkspaceInstance(
  instance: ProviderInstance,
): AgentRelayWorkspaceInstance | undefined {
  if (instance.driverKind !== AGENT_RELAY_DRIVER_KIND || !instance.enabled) {
    return undefined;
  }
  const listWorkspaceAgents = (instance.adapter as AgentRelayAdapterShape).listWorkspaceAgents;
  return listWorkspaceAgents
    ? { instanceId: instance.instanceId, displayName: instance.displayName, listWorkspaceAgents }
    : undefined;
}

const makeAgentRelayThreadDiscoveryReactor = (
  options?: AgentRelayThreadDiscoveryReactorLiveOptions,
) =>
  Effect.gen(function* () {
    const instanceRegistry = yield* ProviderInstanceRegistry;
    const directory = yield* ProviderSessionDirectory;
    const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
    const engine = yield* OrchestrationEngineService;
    const workspacePaths = yield* WorkspacePaths;
    const serverConfig = yield* ServerConfig;
    const path = yield* Path.Path;
    const crypto = yield* Crypto.Crypto;

    const sweepIntervalMs = Math.max(1, options?.sweepIntervalMs ?? DEFAULT_SWEEP_INTERVAL_MS);
    const offlineDebounceMs = Math.max(
      1,
      options?.offlineDebounceMs ?? DEFAULT_OFFLINE_DEBOUNCE_MS,
    );

    // Resolved synthetic project per instance, cached so a sweep that finds
    // several unclaimed agents for the same instance does not dispatch
    // `project.create` more than once before the read model catches up.
    const projectIdByInstance = new Map<ProviderInstanceId, ProjectId>();
    // `${instanceId}:${agentName}` -> last sweep time this agent was seen
    // online. Absence here (not "0") means "never confirmed online since
    // this reactor started" — see the seeding comment in `sweepInstance`.
    const lastOnlineAtMs = new Map<string, number>();

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const serverCommandId = (tag: string) =>
      crypto.randomUUIDv4.pipe(
        Effect.map((uuid) => CommandId.make(`server:agentrelay-discovery:${tag}:${uuid}`)),
      );

    const resolveInstanceProject = Effect.fn("resolveInstanceProject")(function* (
      instance: AgentRelayWorkspaceInstance,
    ) {
      const cached = projectIdByInstance.get(instance.instanceId);
      if (cached !== undefined) {
        return cached;
      }

      const workspaceRoot = path.join(
        serverConfig.baseDir,
        "agent-relay",
        slugify(instance.instanceId),
      );
      const normalizedRoot = yield* workspacePaths.normalizeWorkspaceRoot(workspaceRoot, {
        createIfMissing: true,
      });

      const existingProject =
        yield* projectionSnapshotQuery.getActiveProjectByWorkspaceRoot(normalizedRoot);
      if (Option.isSome(existingProject)) {
        projectIdByInstance.set(instance.instanceId, existingProject.value.id);
        return existingProject.value.id;
      }

      const projectId = ProjectId.make(yield* crypto.randomUUIDv4);
      const createdAt = yield* nowIso;
      yield* engine.dispatch({
        type: "project.create",
        commandId: yield* serverCommandId("project-create"),
        projectId,
        title: `Agent Relay: ${instance.displayName ?? instance.instanceId}`,
        workspaceRoot: normalizedRoot,
        createdAt,
      });
      projectIdByInstance.set(instance.instanceId, projectId);
      return projectId;
    });

    /** Agent names already bound to a thread for this instance, discovered
     * the same way `ProviderSessionReaper` already scans bindings — reusing
     * `ProviderSessionDirectory` instead of a second, parallel "is this
     * agent claimed" table. */
    const claimedAgentNamesForInstance = Effect.fn("claimedAgentNamesForInstance")(function* (
      instanceId: ProviderInstanceId,
    ) {
      const bindings = yield* directory.listBindings();
      const claimed = new Map<string, ThreadId>();
      for (const binding of bindings) {
        if (binding.provider !== AGENT_RELAY_DRIVER_KIND) continue;
        if (binding.providerInstanceId !== instanceId) continue;
        if (!isAgentRelayResumeCursor(binding.resumeCursor)) continue;
        claimed.set(binding.resumeCursor.agentName, binding.threadId);
      }
      return claimed;
    });

    const materializeThread = Effect.fn("materializeThread")(function* (input: {
      readonly instance: AgentRelayWorkspaceInstance;
      readonly agentName: string;
    }) {
      const projectId = yield* resolveInstanceProject(input.instance);
      const threadId = ThreadId.make(yield* crypto.randomUUIDv4);
      const createdAt = yield* nowIso;

      // Install the cursor before the thread becomes visible — the same
      // ordering `AgentSessionImporter` uses, so a client that opens this
      // thread the instant it appears attaches to `agentName` rather than
      // racing the spawn path.
      yield* directory.upsert(
        {
          threadId,
          provider: AGENT_RELAY_DRIVER_KIND,
          providerInstanceId: input.instance.instanceId,
          status: "stopped",
          resumeCursor: { agentName: input.agentName },
        },
        { onConflict: "ignore" },
      );

      yield* engine.dispatch({
        type: "thread.create",
        commandId: yield* serverCommandId("thread-create"),
        threadId,
        projectId,
        title: input.agentName,
        modelSelection: {
          instanceId: input.instance.instanceId,
          model: AGENT_RELAY_DEFAULT_MODEL_SLUG,
        },
        runtimeMode: DEFAULT_RUNTIME_MODE,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        branch: null,
        worktreePath: null,
        createdAt,
      });

      yield* engine.dispatch({
        type: "thread.activity.append",
        commandId: yield* serverCommandId("thread-discovered-activity"),
        threadId,
        activity: {
          id: EventId.make(yield* crypto.randomUUIDv4),
          tone: "info",
          kind: "agentrelay.agent.discovered",
          summary: `Attached to '${input.agentName}', already running in the Agent Relay workspace`,
          payload: {
            agentName: input.agentName,
            providerInstanceId: input.instance.instanceId,
          },
          turnId: null,
          createdAt,
        },
        createdAt,
      });
    });

    /** Reverse of `materializeThread`: an agent this reactor (or T3 Code
     * itself) previously bound to a thread has dropped out of
     * `listAgents()` for at least `offlineDebounceMs`. Settling (rather than
     * deleting or archiving) matches `ExternalSessionHooks`' "session ended"
     * marker — the thread and its history stay, just no longer shown live.
     * Sending it a new message unsettles it automatically (see
     * `decider.ts`'s `thread.turn-start-requested` handling), so nothing
     * here needs to reverse this if the agent comes back. */
    const markAgentOffline = Effect.fn("markAgentOffline")(function* (input: {
      readonly instanceId: ProviderInstanceId;
      readonly agentName: string;
      readonly threadId: ThreadId;
    }) {
      const thread = yield* projectionSnapshotQuery
        .getThreadShellById(input.threadId)
        .pipe(Effect.map(Option.getOrUndefined));
      // Already gone, already settled, or actively mid-turn (thread.settle
      // itself would reject that case) — nothing to do.
      if (!thread || thread.settledOverride === "settled") {
        return;
      }

      const createdAt = yield* nowIso;
      yield* engine.dispatch({
        type: "thread.activity.append",
        commandId: yield* serverCommandId("thread-offline-activity"),
        threadId: input.threadId,
        activity: {
          id: EventId.make(yield* crypto.randomUUIDv4),
          tone: "info",
          kind: "agentrelay.agent.offline",
          summary: `'${input.agentName}' is no longer running in the Agent Relay workspace`,
          payload: {
            agentName: input.agentName,
            providerInstanceId: input.instanceId,
          },
          turnId: null,
          createdAt,
        },
        createdAt,
      });
      yield* engine.dispatch({
        type: "thread.settle",
        commandId: yield* serverCommandId("thread-offline-settle"),
        threadId: input.threadId,
      });
    });

    const sweepInstance = Effect.fn("sweepInstance")(function* (
      instance: AgentRelayWorkspaceInstance,
    ) {
      const agents = yield* instance
        .listWorkspaceAgents()
        .pipe(Effect.orElseSucceed((): ReadonlyArray<AgentRelayWorkspaceAgentSummary> => []));
      const onlineNames = new Set(
        agents.filter((agent) => agent.status === "online").map((agent) => agent.name),
      );
      const claimed = yield* claimedAgentNamesForInstance(instance.instanceId);
      const now = yield* Clock.currentTimeMillis;

      for (const name of onlineNames) {
        lastOnlineAtMs.set(`${instance.instanceId}:${name}`, now);
        if (claimed.has(name)) continue;
        yield* materializeThread({ instance, agentName: name }).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("agentrelay.discovery.materialize-failed", {
              instanceId: instance.instanceId,
              agentName: name,
              cause,
            }),
          ),
        );
      }

      for (const [agentName, threadId] of claimed) {
        if (onlineNames.has(agentName)) continue;
        const key = `${instance.instanceId}:${agentName}`;
        const lastSeen = lastOnlineAtMs.get(key);
        if (lastSeen === undefined) {
          // First time this reactor has observed this binding: assume it
          // was online until now rather than settling a pre-existing
          // thread on the very first sweep after a server restart.
          lastOnlineAtMs.set(key, now);
          continue;
        }
        if (now - lastSeen < offlineDebounceMs) continue;
        yield* markAgentOffline({ instanceId: instance.instanceId, agentName, threadId }).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("agentrelay.discovery.offline-settle-failed", {
              instanceId: instance.instanceId,
              agentName,
              threadId,
              cause,
            }),
          ),
        );
      }
    });

    const sweep = Effect.gen(function* () {
      const instances = yield* instanceRegistry.listInstances;
      const workspaceInstances = instances.flatMap((instance) => {
        const workspaceInstance = asAgentRelayWorkspaceInstance(instance);
        return workspaceInstance ? [workspaceInstance] : [];
      });
      yield* Effect.forEach(workspaceInstances, sweepInstance, { discard: true });
    });

    const start: AgentRelayThreadDiscoveryReactorShape["start"] = () =>
      Effect.gen(function* () {
        yield* forkParked(
          sweep.pipe(
            Effect.catch((error: unknown) =>
              Effect.logWarning("agentrelay.discovery.sweep-failed", { error }),
            ),
            Effect.catchDefect((defect: unknown) =>
              Effect.logWarning("agentrelay.discovery.sweep-defect", { defect }),
            ),
            Effect.repeat(Schedule.spaced(Duration.millis(sweepIntervalMs))),
          ),
        );
        yield* Effect.logInfo("agentrelay.discovery.started", {
          sweepIntervalMs,
          offlineDebounceMs,
        });
      });

    return { start } satisfies AgentRelayThreadDiscoveryReactorShape;
  });

export const makeAgentRelayThreadDiscoveryReactorLive = (
  options?: AgentRelayThreadDiscoveryReactorLiveOptions,
) => Layer.effect(AgentRelayThreadDiscoveryReactor, makeAgentRelayThreadDiscoveryReactor(options));

export const AgentRelayThreadDiscoveryReactorLive = makeAgentRelayThreadDiscoveryReactorLive();
