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

const LOOPBACK_HOSTNAMES = new Set(["localhost", "127.0.0.1", "::1", "[::1]"]);

/** True when `brokerUrl` would send the configured API key in cleartext to
 * a *non-loopback* host — `AgentRelayAdapter.connect`/`postInput` send it
 * over plain `http://`/`ws://` as-is with no transport-level guard, since a
 * local, self-hosted broker (the primary documented setup — see
 * `docs/user/providers-agentrelay.md`) has no TLS to speak of. Requiring
 * `https://` unconditionally would break that primary case; this only
 * flags the case that actually matters, a plaintext credential leaving the
 * machine. */
function isCleartextRemoteBrokerUrl(brokerUrl: string): boolean {
  if (!/^http:\/\//i.test(brokerUrl)) return false;
  let hostname: string;
  try {
    hostname = new URL(brokerUrl).hostname.toLowerCase();
  } catch {
    return false;
  }
  return !LOOPBACK_HOSTNAMES.has(hostname);
}

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
          message: "No broker URL configured. Paste the base HTTP(S) URL from the Agent Relay CLI.",
        },
      });
    }

    // Workspace mode without a workspace key can never actually attach:
    // `AgentRelayDriver` only builds a `workspaceClient` when one is
    // configured, and `AgentRelayAdapter.startSession` fails every new
    // thread on that instance without it. Reporting "ready" here (the
    // previous behavior, which only checked `brokerUrl`/`apiKey`) hid that
    // until the first real session start.
    if (settings.mode === "workspace" && !settings.workspaceKey.trim()) {
      return buildServerProvider({
        presentation: AGENT_RELAY_PRESENTATION,
        enabled: true,
        checkedAt,
        models,
        probe: {
          installed: true,
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message:
            "Workspace mode needs a Relaycast workspace key to discover or spawn agents. Add one, or switch to Single agent mode.",
        },
      });
    }

    const hasApiKey = settings.apiKey.trim().length > 0;
    const cleartextWarning =
      hasApiKey && isCleartextRemoteBrokerUrl(brokerUrl)
        ? "This broker URL is plain http:// to a non-local host — the API key is sent in cleartext. Use https:// if the broker isn't running on this machine."
        : undefined;
    return buildServerProvider({
      presentation: AGENT_RELAY_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: cleartextWarning ? "warning" : "ready",
        auth: hasApiKey
          ? { status: "authenticated", type: "api_key", label: "Agent Relay API key" }
          : { status: "unauthenticated" },
        ...(cleartextWarning
          ? { message: cleartextWarning }
          : hasApiKey
            ? {}
            : { message: "No API key configured. The broker may reject the connection." }),
      },
    });
  });
}
