// @effect-diagnostics nodeBuiltinImport:off
import * as NodeHttp from "node:http";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { FetchHttpClient } from "effect/unstable/http";
import { WebSocketServer, type WebSocket as WsSocket } from "ws";

import { AgentRelaySettings, ThreadId, type ProviderRuntimeEvent } from "@t3tools/contracts";

import type { AgentRelayWorkspaceClientShape } from "../Services/AgentRelayWorkspaceClient.ts";
import { makeAgentRelayAdapter } from "./AgentRelayAdapter.ts";

const decodeAgentRelaySettings = Schema.decodeSync(AgentRelaySettings);

/**
 * Real shape confirmed both from source (`crates/broker/src/protocol.rs`'s
 * `BrokerEvent::WorkerStream` and its serde round-trip test) and live
 * against a real `agent-relay-broker`: discriminated by `kind` (not
 * `type`), payload in `chunk` (not `data`), and carrying the worker's
 * `name` since the broker broadcasts every worker on the connection.
 */
const WorkerStreamFrame = Schema.Struct({
  kind: Schema.Literal("worker_stream"),
  name: Schema.String,
  stream: Schema.String,
  chunk: Schema.String,
  offset: Schema.optional(Schema.Number),
});
const encodeWorkerStreamFrame = Schema.encodeSync(Schema.fromJsonString(WorkerStreamFrame));

interface RecordedInput {
  readonly name: string;
  readonly data: string;
  readonly apiKeyHeader: string | undefined;
}

/**
 * A real local server standing in for `agent-relay-broker`: a plain HTTP
 * server (for `POST /api/input/:name`, recording each call) with the WS
 * server for `/ws` attached to the same port — the same single-port shape
 * the real broker uses.
 */
function startMockBroker(): Promise<{
  readonly server: WebSocketServer;
  readonly httpServer: NodeHttp.Server;
  readonly url: string;
  readonly inputs: RecordedInput[];
}> {
  const inputs: RecordedInput[] = [];
  return new Promise((resolve) => {
    const httpServer = NodeHttp.createServer((req, res) => {
      const match = /^\/api\/input\/([^/]+)$/.exec(req.url ?? "");
      if (!match || req.method !== "POST") {
        res.writeHead(404).end();
        return;
      }
      const chunks: Buffer[] = [];
      req.on("data", (chunk: Buffer) => chunks.push(chunk));
      req.on("end", () => {
        const body = JSON.parse(Buffer.concat(chunks).toString("utf8")) as { data: string };
        inputs.push({
          name: decodeURIComponent(match[1]!),
          data: body.data,
          apiKeyHeader: req.headers["x-api-key"] as string | undefined,
        });
        res
          .writeHead(200, { "Content-Type": "application/json" })
          .end(JSON.stringify({ name: match[1], bytes_written: body.data.length }));
      });
    });
    const server = new WebSocketServer({ server: httpServer, path: "/ws" });
    httpServer.listen(0, "127.0.0.1", () => {
      const address = httpServer.address();
      const port = typeof address === "object" && address !== null ? address.port : 0;
      resolve({ server, httpServer, url: `http://127.0.0.1:${port}`, inputs });
    });
  });
}

/**
 * Forks a one-shot Node event wait and yields the current fiber a couple of
 * turns before returning, so the forked fiber has actually reached its
 * `.once(...)` registration before the caller triggers whatever produces
 * the event. Without this, a synchronous trigger can fire before the
 * scheduler ever runs the newly forked fiber, and the `.once` listener
 * attaches too late to see it.
 */
function forkNodeEventWait<A>(register: (resume: (value: A) => void) => void) {
  return Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(
      Effect.callback<A>((resume) => {
        register((value) => resume(Effect.succeed(value)));
      }).pipe(Effect.timeout("2 seconds"), TestClock.withLive, Effect.orDie),
    );
    yield* Effect.yieldNow;
    yield* Effect.yieldNow;
    return fiber;
  });
}

const forkConnectionWait = (server: WebSocketServer) =>
  forkNodeEventWait<WsSocket>((resume) => server.once("connection", resume));

/**
 * A workspace client whose `spawnAgent` immediately marks the spawned name
 * "online" for `listAgents`, so `waitForAgentOnline`'s polling fallback
 * resolves on its very first check with no clock manipulation needed —
 * `onPresenceChange` is left a no-op here to specifically exercise that
 * fallback rather than the (unverifiable, see AgentRelayWorkspaceClientLive.ts)
 * presence push path.
 */
const makeFakeWorkspaceClient = () =>
  Effect.gen(function* () {
    const spawnCalls = yield* Ref.make<ReadonlyArray<string>>([]);
    const online = yield* Ref.make<ReadonlyArray<string>>([]);
    const shape: AgentRelayWorkspaceClientShape = {
      listAgents: () =>
        Ref.get(online).pipe(
          Effect.map((names) => names.map((name) => ({ name, status: "online" as const }))),
        ),
      spawnAgent: (input) =>
        Effect.gen(function* () {
          yield* Ref.update(spawnCalls, (calls) => [...calls, input.name]);
          yield* Ref.update(online, (names) => [...names, input.name]);
          return { name: input.name };
        }),
      onPresenceChange: () => () => {},
    };
    return { shape, spawnCalls };
  });

/**
 * Collects every event the adapter emits from the moment this is called.
 * Forked once per test (not re-subscribed per wait) so a wait started after
 * the fact still sees events published earlier in the same test.
 */
const makeEventCollector = (streamEvents: Stream.Stream<ProviderRuntimeEvent>) =>
  Effect.gen(function* () {
    const events = yield* Ref.make<ReadonlyArray<ProviderRuntimeEvent>>([]);
    yield* Stream.runForEach(streamEvents, (event) =>
      Ref.update(events, (current) => [...current, event]),
    ).pipe(Effect.forkScoped);
    // Give the forked subscriber a turn to attach to the PubSub before the
    // caller triggers the action that publishes the event it wants.
    yield* Effect.yieldNow;
    yield* Effect.yieldNow;
    return events;
  });

const waitForEvent = <T extends ProviderRuntimeEvent["type"]>(
  events: Ref.Ref<ReadonlyArray<ProviderRuntimeEvent>>,
  eventType: T,
): Effect.Effect<Extract<ProviderRuntimeEvent, { type: T }>> =>
  Effect.gen(function* () {
    while (true) {
      const current = yield* Ref.get(events);
      const found = current.find(
        (event): event is Extract<ProviderRuntimeEvent, { type: T }> => event.type === eventType,
      );
      if (found) return found;
      yield* Effect.sleep(Duration.millis(10));
    }
  }).pipe(Effect.timeout("2 seconds"), TestClock.withLive, Effect.orDie);

const waitUntil = (predicate: () => boolean): Effect.Effect<void> =>
  Effect.gen(function* () {
    while (!predicate()) {
      yield* Effect.sleep(Duration.millis(10));
    }
  }).pipe(Effect.timeout("2 seconds"), TestClock.withLive, Effect.orDie);

const agentRelayAdapterTestLayer = Layer.provideMerge(NodeServices.layer, FetchHttpClient.layer);

const makeTestAdapter = (brokerUrl: string, agentName = "Worker1", apiKey = "test-key") =>
  makeAgentRelayAdapter(
    decodeAgentRelaySettings({ enabled: true, mode: "single", brokerUrl, agentName, apiKey }),
  );

it.layer(agentRelayAdapterTestLayer)("AgentRelayAdapterLive", (it) => {
  it.effect("streams worker_stream frames as content.delta and posts input over HTTP", () =>
    Effect.gen(function* () {
      const { server, httpServer, url, inputs } = yield* Effect.promise(startMockBroker);
      yield* Effect.addFinalizer(() => Effect.sync(() => httpServer.close()));

      const adapter = yield* makeTestAdapter(url, "Worker1");
      const events = yield* makeEventCollector(adapter.streamEvents);
      const connectionFiber = yield* forkConnectionWait(server);

      const threadId = ThreadId.make("agentrelay-worker-stream");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
      const brokerSocket = yield* Fiber.join(connectionFiber);

      const ready = yield* waitForEvent(events, "session.state.changed");
      assert.deepEqual(ready.payload, {
        state: "ready",
        reason: "Connected to the Agent Relay broker.",
      });

      // The exact frame captured live from a real `agent-relay-broker`
      // running a real `claude --version` under its PTY.
      brokerSocket.send(
        '{"chunk":"2.1.263 (Claude Code)\\r\\n\\u001b[?25h","kind":"worker_stream","name":"Worker1","offset":29,"stream":"stdout"}',
      );
      const delta = yield* waitForEvent(events, "content.delta");
      assert.equal(delta.payload.streamKind, "command_output");
      assert.equal(delta.payload.delta, "2.1.263 (Claude Code)\r\n[?25h");

      yield* adapter.sendTurn({ threadId, input: "hello agent" });
      yield* waitUntil(() => inputs.length === 1);
      assert.deepEqual(inputs[0], {
        name: "Worker1",
        data: "hello agent\n",
        apiKeyHeader: "test-key",
      });

      const started = yield* waitForEvent(events, "turn.started");
      assert.isDefined(started.turnId);

      yield* adapter.stopSession(threadId);
      yield* waitForEvent(events, "session.exited");
    }),
  );

  it.effect("ignores worker_stream frames for a different worker on the same connection", () =>
    Effect.gen(function* () {
      // The broker broadcasts every worker on the connection over one
      // socket — a session must only react to its own agent's frames.
      const { server, httpServer, url } = yield* Effect.promise(startMockBroker);
      yield* Effect.addFinalizer(() => Effect.sync(() => httpServer.close()));

      const adapter = yield* makeTestAdapter(url, "Worker1");
      const events = yield* makeEventCollector(adapter.streamEvents);
      const connectionFiber = yield* forkConnectionWait(server);

      const threadId = ThreadId.make("agentrelay-multiplex-filter");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
      const brokerSocket = yield* Fiber.join(connectionFiber);
      yield* waitForEvent(events, "session.state.changed");

      brokerSocket.send(
        encodeWorkerStreamFrame({
          kind: "worker_stream",
          name: "SomeOtherWorker",
          stream: "stdout",
          chunk: "not for us\n",
        }),
      );
      brokerSocket.send(
        encodeWorkerStreamFrame({
          kind: "worker_stream",
          name: "Worker1",
          stream: "stdout",
          chunk: "for us\n",
        }),
      );

      const delta = yield* waitForEvent(events, "content.delta");
      assert.equal(delta.payload.delta, "for us\n");
      assert.isUndefined(
        (yield* Ref.get(events)).find(
          (event) => event.type === "content.delta" && event.payload.delta === "not for us\n",
        ),
      );

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("completes a turn once the broker goes quiet", () =>
    Effect.gen(function* () {
      const { server, httpServer, url } = yield* Effect.promise(startMockBroker);
      yield* Effect.addFinalizer(() => Effect.sync(() => httpServer.close()));

      const adapter = yield* makeTestAdapter(url);
      const events = yield* makeEventCollector(adapter.streamEvents);
      const connectionFiber = yield* forkConnectionWait(server);

      const threadId = ThreadId.make("agentrelay-idle-complete");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
      yield* Fiber.join(connectionFiber);
      yield* waitForEvent(events, "session.state.changed");

      yield* adapter.sendTurn({ threadId, input: "run the tests" });
      yield* waitForEvent(events, "turn.started");

      // No further output arrives: the idle watchdog should complete the
      // turn on its own once TURN_IDLE_COMPLETE_MS has elapsed.
      yield* TestClock.adjust(Duration.millis(1_500));

      const completed = yield* waitForEvent(events, "turn.completed");
      assert.deepEqual(completed.payload, { state: "completed", stopReason: null });

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("interrupting a turn posts Ctrl-C as input and completes it as cancelled", () =>
    Effect.gen(function* () {
      const { server, httpServer, url, inputs } = yield* Effect.promise(startMockBroker);
      yield* Effect.addFinalizer(() => Effect.sync(() => httpServer.close()));

      const adapter = yield* makeTestAdapter(url);
      const events = yield* makeEventCollector(adapter.streamEvents);
      const connectionFiber = yield* forkConnectionWait(server);

      const threadId = ThreadId.make("agentrelay-interrupt");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
      yield* Fiber.join(connectionFiber);
      yield* waitForEvent(events, "session.state.changed");

      yield* adapter.sendTurn({ threadId, input: "run forever" });
      yield* waitUntil(() => inputs.length === 1);

      yield* adapter.interruptTurn(threadId);
      yield* waitUntil(() => inputs.length === 2);
      assert.equal(inputs[1]!.data, "");

      const completed = yield* waitForEvent(events, "turn.completed");
      assert.deepEqual(completed.payload, { state: "cancelled", stopReason: null });

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("has no session before startSession and none after stopSession", () =>
    Effect.gen(function* () {
      const { httpServer, url } = yield* Effect.promise(startMockBroker);
      yield* Effect.addFinalizer(() => Effect.sync(() => httpServer.close()));

      const adapter = yield* makeTestAdapter(url);
      const threadId = ThreadId.make("agentrelay-hasSession");
      assert.isFalse(yield* adapter.hasSession(threadId));

      // `hasSession` flips true as soon as `startSession` records the
      // context, independent of the underlying socket finishing its
      // handshake — no need to wait on the mock broker here.
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
      assert.isTrue(yield* adapter.hasSession(threadId));

      yield* adapter.stopSession(threadId);
      assert.isFalse(yield* adapter.hasSession(threadId));
    }),
  );

  it.effect("rejects starting a session with no broker URL configured", () =>
    Effect.gen(function* () {
      const adapter = yield* makeAgentRelayAdapter(
        decodeAgentRelaySettings({
          enabled: true,
          mode: "single",
          brokerUrl: "",
          agentName: "Worker1",
          apiKey: "",
        }),
      );
      const threadId = ThreadId.make("agentrelay-missing-url");
      const failure = yield* adapter
        .startSession({ threadId, runtimeMode: "full-access" })
        .pipe(Effect.flip);
      assert.equal(failure._tag, "ProviderAdapterValidationError");
    }),
  );

  it.effect("rejects starting a session with no agent name configured", () =>
    Effect.gen(function* () {
      const adapter = yield* makeAgentRelayAdapter(
        decodeAgentRelaySettings({
          enabled: true,
          mode: "single",
          brokerUrl: "http://127.0.0.1:1",
          agentName: "",
          apiKey: "",
        }),
      );
      const threadId = ThreadId.make("agentrelay-missing-agent-name");
      const failure = yield* adapter
        .startSession({ threadId, runtimeMode: "full-access" })
        .pipe(Effect.flip);
      assert.equal(failure._tag, "ProviderAdapterValidationError");
    }),
  );
});

it.layer(agentRelayAdapterTestLayer)("AgentRelayAdapterLive workspace mode", (it) => {
  const makeWorkspaceTestAdapter = (
    brokerUrl: string,
    workspaceClient: AgentRelayWorkspaceClientShape | undefined,
  ) =>
    makeAgentRelayAdapter(
      decodeAgentRelaySettings({
        enabled: true,
        mode: "workspace",
        brokerUrl,
        apiKey: "test-key",
        workspaceKey: "rk_live_test",
        defaultSpawnCli: "claude",
      }),
      workspaceClient ? { workspaceClient } : {},
    );

  it.effect(
    "spawns a new agent, waits for it online, and filters frames by its resolved name",
    () =>
      Effect.gen(function* () {
        const { server, httpServer, url, inputs } = yield* Effect.promise(startMockBroker);
        yield* Effect.addFinalizer(() => Effect.sync(() => httpServer.close()));

        const { shape: workspaceClient, spawnCalls } = yield* makeFakeWorkspaceClient();
        const adapter = yield* makeWorkspaceTestAdapter(url, workspaceClient);
        const events = yield* makeEventCollector(adapter.streamEvents);
        const connectionFiber = yield* forkConnectionWait(server);

        const threadId = ThreadId.make("agentrelay-workspace-spawn");
        const session = yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
        const brokerSocket = yield* Fiber.join(connectionFiber);

        assert.deepEqual(yield* Ref.get(spawnCalls), ["t3code-agentrelay-workspace-spawn"]);
        assert.deepEqual(session.resumeCursor, { agentName: "t3code-agentrelay-workspace-spawn" });
        yield* waitForEvent(events, "session.state.changed");

        // A frame for some unrelated worker on the same broker connection
        // must not reach this thread, only one addressed to the spawned name.
        brokerSocket.send(
          encodeWorkerStreamFrame({
            kind: "worker_stream",
            name: "unrelated-worker",
            stream: "stdout",
            chunk: "ignore me\n",
          }),
        );
        brokerSocket.send(
          encodeWorkerStreamFrame({
            kind: "worker_stream",
            name: "t3code-agentrelay-workspace-spawn",
            stream: "stdout",
            chunk: "hello\n",
          }),
        );
        const delta = yield* waitForEvent(events, "content.delta");
        assert.equal(delta.payload.delta, "hello\n");

        yield* adapter.sendTurn({ threadId, input: "go" });
        yield* waitUntil(() => inputs.length === 1);
        assert.equal(inputs[0]!.name, "t3code-agentrelay-workspace-spawn");

        yield* adapter.stopSession(threadId);
      }),
  );

  it.effect("reconnecting with a persisted resume cursor does not spawn again", () =>
    Effect.gen(function* () {
      const { server, httpServer, url } = yield* Effect.promise(startMockBroker);
      yield* Effect.addFinalizer(() => Effect.sync(() => httpServer.close()));

      const { shape: workspaceClient, spawnCalls } = yield* makeFakeWorkspaceClient();
      const adapter = yield* makeWorkspaceTestAdapter(url, workspaceClient);
      const connectionFiber = yield* forkConnectionWait(server);

      const threadId = ThreadId.make("agentrelay-workspace-resume");
      const session = yield* adapter.startSession({
        threadId,
        runtimeMode: "full-access",
        resumeCursor: { agentName: "already-running-agent" },
      });
      yield* Fiber.join(connectionFiber);

      assert.deepEqual(yield* Ref.get(spawnCalls), []);
      assert.deepEqual(session.resumeCursor, { agentName: "already-running-agent" });

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("rejects starting a new thread with no workspace client configured", () =>
    Effect.gen(function* () {
      const adapter = yield* makeWorkspaceTestAdapter("http://127.0.0.1:1", undefined);
      const threadId = ThreadId.make("agentrelay-workspace-missing-client");
      const failure = yield* adapter
        .startSession({ threadId, runtimeMode: "full-access" })
        .pipe(Effect.flip);
      assert.equal(failure._tag, "ProviderAdapterValidationError");
    }),
  );
});
