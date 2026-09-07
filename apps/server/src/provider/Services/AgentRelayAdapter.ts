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

/**
 * AgentRelayAdapterShape — per-instance Agent Relay adapter contract.
 */
export interface AgentRelayAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {}
