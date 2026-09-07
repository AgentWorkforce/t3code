/**
 * AgentRelayDriver — `ProviderDriver` for the Agent Relay broker.
 *
 * Unlike every other built-in driver, `create()` never spawns or resolves a
 * local executable — there is nothing to install or update, so maintenance
 * capabilities are always manual-only. The adapter (`AgentRelayAdapter.ts`)
 * owns the one real piece of lifecycle: the outbound WebSocket connection to
 * the broker. See `docs/internals/providers.md` for why.
 *
 * @module provider/Drivers/AgentRelayDriver
 */
import { AgentRelaySettings, ProviderDriverKind } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { makeAgentRelayTextGeneration } from "../../textGeneration/AgentRelayTextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeAgentRelayAdapter } from "../Layers/AgentRelayAdapter.ts";
import {
  buildInitialAgentRelayProviderSnapshot,
  checkAgentRelayProviderStatus,
} from "../Layers/AgentRelayProvider.ts";
import { makeAgentRelayWorkspaceClient } from "../Layers/AgentRelayWorkspaceClientLive.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import { withInstanceIdentity } from "./instanceIdentity.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";

const decodeAgentRelaySettings = Schema.decodeSync(AgentRelaySettings);

const DRIVER_KIND = ProviderDriverKind.make("agentrelay");

export type AgentRelayDriverEnv =
  | BackgroundPolicy.BackgroundPolicy
  | Crypto.Crypto
  | ServerSettingsService;

export const AgentRelayDriver: ProviderDriver<AgentRelaySettings, AgentRelayDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "Agent Relay",
    supportsMultipleInstances: true,
  },
  configSchema: AgentRelaySettings,
  defaultConfig: (): AgentRelaySettings => decodeAgentRelaySettings({}),
  create: ({ instanceId, displayName, accentColor, enabled, config }) =>
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsService;
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        driverKind: DRIVER_KIND,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const effectiveConfig = { ...config, enabled } satisfies AgentRelaySettings;

      // Workspace mode discovers/spawns agents through a Relaycast workspace
      // key; single mode attaches directly and has no use for this client.
      const workspaceClient =
        effectiveConfig.mode === "workspace" && effectiveConfig.workspaceKey.trim()
          ? yield* makeAgentRelayWorkspaceClient(effectiveConfig.workspaceKey, instanceId)
          : undefined;
      const adapter = yield* makeAgentRelayAdapter(effectiveConfig, {
        instanceId,
        ...(workspaceClient ? { workspaceClient } : {}),
      });
      const textGeneration = yield* makeAgentRelayTextGeneration;

      const checkProvider = checkAgentRelayProviderStatus(effectiveConfig).pipe(
        Effect.map(stampIdentity),
      );

      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const snapshot = yield* makeManagedServerProvider<
        ProviderSnapshotSettings<AgentRelaySettings>
      >({
        // No local binary — nothing this driver could offer to update.
        resolveMaintenance: () =>
          Effect.succeed(
            makeManualOnlyProviderMaintenanceCapabilities({
              provider: DRIVER_KIND,
              packageName: null,
            }),
          ),
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          buildInitialAgentRelayProviderSnapshot(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: `Failed to build Agent Relay snapshot: ${cause.message ?? String(cause)}`,
              cause,
            }),
        ),
      );

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        adapter,
        textGeneration,
      } satisfies ProviderInstance;
    }),
};
