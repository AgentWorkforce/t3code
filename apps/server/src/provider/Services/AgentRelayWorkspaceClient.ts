/**
 * AgentRelayWorkspaceClient — shape for talking to an Agent Relay *workspace*
 * (as opposed to `AgentRelayAdapterShape`, which owns a single agent's PTY
 * attach socket). This is the collaborator `AgentRelayAdapter.ts` uses in
 * "workspace" mode to discover who is already running and to spawn a new
 * agent for a thread that has none yet.
 *
 * @module AgentRelayWorkspaceClient
 */
import * as Effect from "effect/Effect";

import type { ProviderAdapterRequestError } from "../Errors.ts";

export interface AgentRelayWorkspaceAgentSummary {
  readonly name: string;
  readonly status: "online" | "offline" | "unknown";
}

export interface AgentRelayWorkspaceSpawnInput {
  readonly name: string;
  readonly cli: string;
  readonly task?: string;
}

export type AgentRelayPresenceStatus = "online" | "offline";

export interface AgentRelayWorkspaceClientShape {
  readonly listAgents: (filter?: {
    readonly status?: AgentRelayPresenceStatus;
  }) => Effect.Effect<ReadonlyArray<AgentRelayWorkspaceAgentSummary>, ProviderAdapterRequestError>;

  readonly spawnAgent: (
    input: AgentRelayWorkspaceSpawnInput,
  ) => Effect.Effect<{ readonly name: string }, ProviderAdapterRequestError>;

  /**
   * Registers a presence-change callback and returns an unsubscribe
   * function. Fires best-effort for online/offline transitions this client
   * observes live; callers that need a guarantee (e.g. confirming a spawn
   * came online) should still race this against polling `listAgents` — see
   * `waitForAgentOnline` in `AgentRelayAdapter.ts`.
   */
  readonly onPresenceChange: (
    handler: (name: string, status: AgentRelayPresenceStatus) => void,
  ) => () => void;
}
