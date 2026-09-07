/**
 * AgentRelayWorkspaceClientLive — real `@agent-relay/sdk` implementation of
 * {@link AgentRelayWorkspaceClientShape}.
 *
 * Two separate SDK surfaces are combined here, deliberately, because they are
 * not the same thing:
 *
 * - `createWorkspaceClient` (`@agent-relay/sdk/messaging`) is a thin,
 *   workspace-key-scoped pass-through over `@relaycast/sdk`'s raw client.
 *   It is the *only* surface that exposes `agents.spawn` — the reshaped
 *   `AgentRelay` facade's `.agents` (below) omits it. This is exactly what
 *   Agent Relay's own `add_agent`/`list_agents` MCP tools call under the
 *   hood (`packages/cli/src/cli/agent-relay-mcp.ts`'s `getRelay()` returns
 *   this same thin client, typed as `RelayCastLike`).
 * - `AgentRelay` (`@agent-relay/sdk`) is the richer facade with a live
 *   `addListener` event fan-in, used here only for presence. Listening
 *   requires at least one registered agent identity for events to fan
 *   through (`relay.workspace.register(...)`), so this module registers a
 *   lightweight, deterministically-named identity for that purpose alone —
 *   it never sends messages or spawns through it.
 *
 * Presence uncertainty: this module could not be verified against a live
 * Agent Relay workspace (see `docs/internals/providers.md`). The predicate
 * below matches both the raw messaging-level `agentOnline`/`agentOffline`
 * events (`@agent-relay/sdk`'s `messaging/types.ts`) and the newer
 * `agent.status.*` dotted events, because static reading of the SDK left it
 * unclear which (if either) reaches the top-level `addListener` fan-in for a
 * plain workspace-key registration. `AgentRelayAdapter.ts`'s
 * `waitForAgentOnline` races this against polling `listAgents`, so a spawn
 * still resolves correctly even if this listener never fires.
 *
 * @module AgentRelayWorkspaceClientLive
 */
import { AgentRelay } from "@agent-relay/sdk";
import { createWorkspaceClient, type RelayAgent } from "@agent-relay/sdk/messaging";
import * as Effect from "effect/Effect";

import { ProviderAdapterRequestError } from "../Errors.ts";
import type {
  AgentRelayPresenceStatus,
  AgentRelayWorkspaceAgentSummary,
  AgentRelayWorkspaceClientShape,
} from "../Services/AgentRelayWorkspaceClient.ts";

const PROVIDER = "agentrelay";

function describeError(cause: unknown): string {
  return cause instanceof Error ? cause.message : String(cause);
}

function toAgentSummary(agent: RelayAgent): AgentRelayWorkspaceAgentSummary {
  return {
    name: agent.name,
    status: agent.status === "online" || agent.status === "offline" ? agent.status : "unknown",
  };
}

/**
 * Narrow an arbitrary event from `AgentRelay#addListener`'s untyped predicate
 * overload down to a `(name, status)` presence transition, whichever of the
 * two event shapes described above it turns out to be. Returns `undefined`
 * for anything else so callers can ignore it.
 */
export function readPresenceTransition(
  event: unknown,
): { readonly name: string; readonly status: AgentRelayPresenceStatus } | undefined {
  if (typeof event !== "object" || event === null || !("type" in event)) return undefined;
  const record = event as Record<string, unknown>;
  const type = record.type;
  if (type === "agentOnline" || type === "agentOffline") {
    const agent = record.agent;
    const name =
      typeof agent === "object" &&
      agent !== null &&
      typeof (agent as { name?: unknown }).name === "string"
        ? (agent as { name: string }).name
        : undefined;
    if (!name) return undefined;
    return { name, status: type === "agentOnline" ? "online" : "offline" };
  }
  if (type === "agent.status.online" || type === "agent.status.offline") {
    const agentId = record.agentId;
    if (typeof agentId !== "string" || !agentId) return undefined;
    return { name: agentId, status: type === "agent.status.online" ? "online" : "offline" };
  }
  // Any other `agent.status.*` event (e.g. `connecting`, `error`) is
  // deliberately ignored rather than defaulted to "online" — this
  // module's presence uncertainty (see the module doc) cuts both ways:
  // guessing wrong here can resolve `waitForAgentOnline` for an agent
  // that isn't actually attachable yet. `AgentRelayAdapter`'s poll-based
  // fallback still covers every real transition even when this listener
  // drops one.
  return undefined;
}

/**
 * Build a stable, valid Relaycast agent name for the identity this server
 * registers solely to receive presence events. Deterministic per instance so
 * restarts adopt (rotate) the same identity instead of accumulating one
 * per boot.
 */
function rosterWatcherName(instanceId: string): string {
  const slug = instanceId.replace(/[^a-zA-Z0-9_-]/g, "-").slice(0, 48) || "default";
  return `t3code-roster-${slug}`;
}

export function makeAgentRelayWorkspaceClient(
  workspaceKey: string,
  instanceId: string,
): Effect.Effect<AgentRelayWorkspaceClientShape> {
  return Effect.sync(() => {
    const workspaceClient = createWorkspaceClient({ workspaceKey });
    // Built lazily and best-effort: a workspace that rejects registration
    // (bad key, offline) must not block listing/spawning, which have their
    // own error handling.
    let presenceRelay: AgentRelay | undefined;
    let presenceReady: Promise<void> | undefined;

    const ensurePresenceRelay = (): Promise<void> => {
      if (presenceReady) return presenceReady;
      const relay = new AgentRelay({ workspaceKey });
      presenceRelay = relay;
      presenceReady = relay.workspace
        .register({ name: rosterWatcherName(instanceId), type: "agent" })
        .then(() => undefined)
        .catch((cause) => {
          // Presence is a best-effort push channel — `waitForAgentOnline`'s
          // polling fallback covers this. Reset so a later transient failure
          // (e.g. the workspace was briefly unreachable at boot) gets
          // retried on the next `onPresenceChange` call instead of wedging.
          presenceReady = undefined;
          presenceRelay = undefined;
          Effect.runFork(
            Effect.logWarning("Agent Relay workspace presence registration failed.", {
              instanceId,
              detail: describeError(cause),
            }),
          );
        });
      return presenceReady;
    };

    const listAgents: AgentRelayWorkspaceClientShape["listAgents"] = (filter) =>
      Effect.tryPromise({
        try: () =>
          workspaceClient.agents.list(filter?.status ? { status: filter.status } : undefined),
        catch: (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "workspace.agents.list",
            detail: describeError(cause),
          }),
      }).pipe(Effect.map((agents) => agents.map((agent) => toAgentSummary(agent as RelayAgent))));

    const spawnAgent: AgentRelayWorkspaceClientShape["spawnAgent"] = (input) =>
      Effect.tryPromise({
        try: () =>
          workspaceClient.agents.spawn({
            name: input.name,
            cli: input.cli,
            ...(input.task ? { task: input.task } : {}),
          }),
        catch: (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "workspace.agents.spawn",
            detail: describeError(cause),
          }),
      }).pipe(
        Effect.map((result) => {
          const name =
            typeof result.name === "string" && result.name.trim().length > 0
              ? result.name
              : input.name;
          return { name };
        }),
      );

    const onPresenceChange: AgentRelayWorkspaceClientShape["onPresenceChange"] = (handler) => {
      let unsubscribe: (() => void) | undefined;
      let cancelled = false;
      void ensurePresenceRelay().then(() => {
        if (cancelled || !presenceRelay) return;
        // `"agent.status.*"` is the one wildcard-typed selector confirmed in
        // `RelayEventMap` (`packages/sdk/src/listeners.ts`) that plausibly
        // carries agent connectivity — `addListener` otherwise only accepts
        // exact dotted names or a `ListenerPredicate` *object* (not a plain
        // filter function), so a raw `agentOnline`/`agentOffline` selector
        // is not something this surface's string/predicate overloads support.
        unsubscribe = presenceRelay!.addListener("agent.status.*", (event) => {
          const transition = readPresenceTransition(event);
          if (transition) handler(transition.name, transition.status);
        });
      });
      return () => {
        cancelled = true;
        unsubscribe?.();
      };
    };

    return { listAgents, spawnAgent, onPresenceChange };
  });
}
