import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { AgentRelaySettings } from "@t3tools/contracts";

import { checkAgentRelayProviderStatus } from "./AgentRelayProvider.ts";

const decodeAgentRelaySettings = Schema.decodeSync(AgentRelaySettings);

describe("checkAgentRelayProviderStatus", () => {
  it("reports the base HTTP(S) URL, not a WebSocket URL, when none is configured", async () => {
    const snapshot = await Effect.runPromise(
      checkAgentRelayProviderStatus(
        decodeAgentRelaySettings({ enabled: true, mode: "single", brokerUrl: "" }),
      ),
    );
    expect(snapshot.status).toBe("error");
    expect(snapshot.message).toMatch(/base HTTP\(S\) URL/i);
    expect(snapshot.message).not.toMatch(/websocket url/i);
  });

  it("reports an error, not ready, when Workspace mode has no workspace key", async () => {
    const snapshot = await Effect.runPromise(
      checkAgentRelayProviderStatus(
        decodeAgentRelaySettings({
          enabled: true,
          mode: "workspace",
          brokerUrl: "http://127.0.0.1:18797",
          apiKey: "test-key",
          workspaceKey: "",
        }),
      ),
    );
    // A Workspace-mode instance with a broker URL but no workspace key
    // still reported "ready" before this fix — every new session on it
    // would then fail in `AgentRelayAdapter.startSession` (no
    // `workspaceClient` gets built without a workspace key), only
    // surfacing the misconfiguration on the first real thread instead of
    // in Settings.
    expect(snapshot.status).toBe("error");
    expect(snapshot.message).toMatch(/workspace key/i);
  });

  it("reports ready for a fully-configured Workspace instance", async () => {
    const snapshot = await Effect.runPromise(
      checkAgentRelayProviderStatus(
        decodeAgentRelaySettings({
          enabled: true,
          mode: "workspace",
          brokerUrl: "http://127.0.0.1:18797",
          apiKey: "test-key",
          workspaceKey: "rk_live_test",
        }),
      ),
    );
    expect(snapshot.status).toBe("ready");
  });

  it("reports ready for a fully-configured Single agent instance, ignoring workspaceKey", async () => {
    const snapshot = await Effect.runPromise(
      checkAgentRelayProviderStatus(
        decodeAgentRelaySettings({
          enabled: true,
          mode: "single",
          brokerUrl: "http://127.0.0.1:18797",
          agentName: "Worker1",
          apiKey: "test-key",
          workspaceKey: "",
        }),
      ),
    );
    expect(snapshot.status).toBe("ready");
  });

  it("warns (not error) about a plaintext API key to a non-loopback broker", async () => {
    const snapshot = await Effect.runPromise(
      checkAgentRelayProviderStatus(
        decodeAgentRelaySettings({
          enabled: true,
          mode: "single",
          brokerUrl: "http://broker.example.com",
          agentName: "Worker1",
          apiKey: "test-key",
        }),
      ),
    );
    expect(snapshot.status).toBe("warning");
    expect(snapshot.message).toMatch(/cleartext/i);
  });

  it("does not warn about a plaintext API key to a loopback broker", async () => {
    const snapshot = await Effect.runPromise(
      checkAgentRelayProviderStatus(
        decodeAgentRelaySettings({
          enabled: true,
          mode: "single",
          brokerUrl: "http://127.0.0.1:18797",
          agentName: "Worker1",
          apiKey: "test-key",
        }),
      ),
    );
    expect(snapshot.status).toBe("ready");
  });

  it("does not warn about a non-loopback broker over https://", async () => {
    const snapshot = await Effect.runPromise(
      checkAgentRelayProviderStatus(
        decodeAgentRelaySettings({
          enabled: true,
          mode: "single",
          brokerUrl: "https://broker.example.com",
          agentName: "Worker1",
          apiKey: "test-key",
        }),
      ),
    );
    expect(snapshot.status).toBe("ready");
  });
});
