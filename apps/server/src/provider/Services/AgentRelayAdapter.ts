/**
 * AgentRelayAdapter — shape type for the Agent Relay provider adapter.
 *
 * Mirrors the naming pattern in `CursorAdapter.ts` / `GrokAdapter.ts`: a
 * driver bundles one adapter per instance as a captured closure, so this
 * module only retains the shape interface as a naming anchor.
 *
 * @module AgentRelayAdapter
 */
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";
import type { AgentRelayWorkspaceClientShape } from "./AgentRelayWorkspaceClient.ts";

/**
 * AgentRelayAdapterShape — per-instance Agent Relay adapter contract.
 */
export interface AgentRelayAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {
  /**
   * Present only when this instance is configured in Workspace mode (a
   * `AgentRelayWorkspaceClient` was supplied to `makeAgentRelayAdapter`).
   * `undefined` in Single mode, where there is nothing to discover.
   *
   * Exposed here (rather than reaching for a second, independently
   * constructed `AgentRelayWorkspaceClient`) so
   * `AgentRelayThreadDiscoveryReactor` reuses this instance's already-live
   * client instead of registering a second presence identity for the same
   * workspace.
   */
  readonly listWorkspaceAgents?: AgentRelayWorkspaceClientShape["listAgents"];
}
