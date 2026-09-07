import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";

export interface AgentRelayThreadDiscoveryReactorShape {
  /**
   * Start the background Agent Relay discovery reactor within the provided
   * scope. Shaped like `ProviderSessionReaper.start` — one long-lived sweep
   * loop, forked and left to run until the scope closes.
   */
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
}

export class AgentRelayThreadDiscoveryReactor extends Context.Service<
  AgentRelayThreadDiscoveryReactor,
  AgentRelayThreadDiscoveryReactorShape
>()("t3/provider/Services/AgentRelayThreadDiscoveryReactor") {}
