/**
 * AgentRelayProvider — status/snapshot helpers for the Agent Relay driver.
 *
 * Agent Relay has no local binary and no login flow to probe (see
 * `docs/internals/providers.md`), so unlike the CLI-backed providers in this
 * directory, the health check here never opens a network connection. It only
 * looks at whether a broker URL and API key are configured. Actually
 * connecting happens per-thread in `AgentRelayAdapter.startSession`, which
 * keeps a background health probe from silently attaching to (and stealing
 * input from) an already-running agent — the same "setup must not happen as
 * a health-check side effect" rule Grok and Antigravity follow.
 *
 * @module AgentRelayProvider
 */
import { type AgentRelaySettings, type ServerProviderModel } from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";

import {
  buildServerProvider,
  providerModelsFromSettings,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const AGENT_RELAY_PRESENTATION = {
  displayName: "Agent Relay",
  badgeLabel: "Early Access",
  showInteractionModeToggle: false,
} as const;

const EMPTY_CAPABILITIES = createModelCapabilities({ optionDescriptors: [] });

// Agent Relay does not expose a model catalog to t3code — the underlying
// agent's model is chosen inside Agent Relay, not here. This single entry
// gives the composer something to select so the thread has a model label.
// Exported so `AgentRelayThreadDiscoveryReactor` can stamp the same slug on
// threads it materializes for already-running agents, instead of forking a
// second "the model label" constant that could drift from this one.
export const AGENT_RELAY_DEFAULT_MODEL_SLUG = "relay-agent";

const AGENT_RELAY_BUILT_IN_MODELS: ReadonlyArray<ServerProviderModel> = [
  {
    slug: AGENT_RELAY_DEFAULT_MODEL_SLUG,
    name: "Relay Agent",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
];

function agentRelayModelsFromSettings(
  settings: AgentRelaySettings,
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(
    AGENT_RELAY_BUILT_IN_MODELS,
    settings.customModels,
    EMPTY_CAPABILITIES,
  );
}

export function buildInitialAgentRelayProviderSnapshot(
  settings: AgentRelaySettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    return buildServerProvider({
      presentation: AGENT_RELAY_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models: agentRelayModelsFromSettings(settings),
      probe: settings.enabled
        ? {
            installed: settings.brokerUrl.trim().length > 0,
            version: null,
            status: "warning",
            auth: { status: "unknown" },
            message: "Checking Agent Relay configuration...",
          }
        : {
            installed: false,
            version: null,
            status: "warning",
            auth: { status: "unknown" },
            message: "Agent Relay is disabled in T3 Code settings.",
          },
    });
  });
}

export function checkAgentRelayProviderStatus(
  settings: AgentRelaySettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    const models = agentRelayModelsFromSettings(settings);

    if (!settings.enabled) {
      return buildServerProvider({
        presentation: AGENT_RELAY_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Agent Relay is disabled in T3 Code settings.",
        },
      });
    }

    const brokerUrl = settings.brokerUrl.trim();
    if (!brokerUrl) {
      return buildServerProvider({
        presentation: AGENT_RELAY_PRESENTATION,
        enabled: true,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: "No broker URL configured. Paste the WebSocket URL from the Agent Relay CLI.",
        },
      });
    }

    const hasApiKey = settings.apiKey.trim().length > 0;
    return buildServerProvider({
      presentation: AGENT_RELAY_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "ready",
        auth: hasApiKey
          ? { status: "authenticated", type: "api_key", label: "Agent Relay API key" }
          : { status: "unauthenticated" },
        ...(hasApiKey
          ? {}
          : { message: "No API key configured. The broker may reject the connection." }),
      },
    });
  });
}
