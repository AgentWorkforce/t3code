import * as NodeAssert from "node:assert/strict";

import { describe, it } from "vite-plus/test";

import { readPresenceTransition } from "./AgentRelayWorkspaceClientLive.ts";

describe("readPresenceTransition", () => {
  it("reads the legacy agentOnline/agentOffline messaging event shape", () => {
    NodeAssert.deepEqual(
      readPresenceTransition({ type: "agentOnline", agent: { name: "Worker" } }),
      { name: "Worker", status: "online" },
    );
    NodeAssert.deepEqual(
      readPresenceTransition({ type: "agentOffline", agent: { name: "Worker" } }),
      { name: "Worker", status: "offline" },
    );
  });

  it("reads the agent.status.* dotted event shape for online and offline only", () => {
    NodeAssert.deepEqual(
      readPresenceTransition({ type: "agent.status.offline", agentId: "Worker" }),
      { name: "Worker", status: "offline" },
    );
    NodeAssert.deepEqual(
      readPresenceTransition({ type: "agent.status.online", agentId: "Worker" }),
      { name: "Worker", status: "online" },
    );
  });

  it("ignores agent.status.* events that are neither online nor offline", () => {
    // Previously defaulted anything but literal "offline" to "online" —
    // an intermediate/unrelated status (connecting, error, a future
    // status this module doesn't know about yet) could resolve
    // `waitForAgentOnline` for an agent that was never actually
    // attachable. Whitelisting is the safer default: a missed real
    // transition still gets caught by `AgentRelayAdapter`'s polling
    // fallback, but a wrongly-guessed one cannot.
    NodeAssert.equal(
      readPresenceTransition({ type: "agent.status.active", agentId: "Worker" }),
      undefined,
    );
    NodeAssert.equal(
      readPresenceTransition({ type: "agent.status.idle", agentId: "Worker" }),
      undefined,
    );
    NodeAssert.equal(
      readPresenceTransition({ type: "agent.status.connecting", agentId: "Worker" }),
      undefined,
    );
  });

  it("ignores events that are not a recognized presence transition", () => {
    NodeAssert.equal(readPresenceTransition({ type: "message.created" }), undefined);
    NodeAssert.equal(readPresenceTransition({ type: "agentOnline", agent: {} }), undefined);
    NodeAssert.equal(
      readPresenceTransition({ type: "agent.status.offline", agentId: "" }),
      undefined,
    );
    NodeAssert.equal(readPresenceTransition(null), undefined);
    NodeAssert.equal(readPresenceTransition("agentOnline"), undefined);
    NodeAssert.equal(readPresenceTransition(42), undefined);
  });
});
