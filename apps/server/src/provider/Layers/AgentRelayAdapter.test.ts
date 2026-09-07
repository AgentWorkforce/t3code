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
interface MockBrokerInputFailure {
  /** When true, the next `/api/input/:name` request 500s instead of
   * recording the input — lets a test exercise `postInput` failure without
   * a second server. Auto-resets after one failed request. */
  failNext: boolean;
}

function startMockBroker(): Promise<{
  readonly server: WebSocketServer;
  readonly httpServer: NodeHttp.Server;
  readonly url: string;
  readonly inputs: RecordedInput[];
  readonly inputFailure: MockBrokerInputFailure;
}> {
  const inputs: RecordedInput[] = [];
  const inputFailure: MockBrokerInputFailure = { failNext: false };
  return new Promise((resolve) => {
    const httpServer = NodeHttp.createServer((req, res) => {
      const match = /^\/api\/input\/([^/]+)$/.exec(req.url ?? "");
      if (!match || req.method !== "POST") {
        res.writeHead(404).end();
        return;
      }
      if (inputFailure.failNext) {
        inputFailure.failNext = false;
        res.writeHead(500).end();
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
    // `ws` does not close its own connections/server when the Node HTTP
    // server it was attached to closes, so every call site's
    // `httpServer.close()` finalizer would otherwise leave any still-open
    // WebSocket (a test failing or interrupted before `stopSession` closes
    // the client socket) keeping the event loop alive. Tie its lifetime to
    // the HTTP server's here once, instead of every call site remembering
    // a second `server.close()`.
    httpServer.on("close", () => server.close());
    httpServer.listen(0, "127.0.0.1", () => {
      const address = httpServer.address();
      const port = typeof address === "object" && address !== null ? address.port : 0;
      resolve({ server, httpServer, url: `http://127.0.0.1:${port}`, inputs, inputFailure });
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
const makeFakeWorkspaceClient = (options?: { readonly spawnDelayMs?: number }) =>
  Effect.gen(function* () {
    const spawnCalls = yield* Ref.make<ReadonlyArray<string>>([]);
    const online = yield* Ref.make<ReadonlyArray<string>>([]);
    const activeSpawns = yield* Ref.make(0);
    const maxConcurrentSpawns = yield* Ref.make(0);
    const shape: AgentRelayWorkspaceClientShape = {
      listAgents: () =>
        Ref.get(online).pipe(
          Effect.map((names) => names.map((name) => ({ name, status: "online" as const }))),
        ),
      spawnAgent: (input) =>
        Effect.gen(function* () {
          const active = yield* Ref.updateAndGet(activeSpawns, (n) => n + 1);
          yield* Ref.update(maxConcurrentSpawns, (max) => Math.max(max, active));
          if (options?.spawnDelayMs) {
            // A real (not virtual-clock) delay, so two `startSession`
            // calls fired concurrently actually overlap in wall-clock
            // time — wide enough to expose the race this simulates if
            // `startSession` isn't serialized per thread.
            yield* Effect.sleep(Duration.millis(options.spawnDelayMs)).pipe(TestClock.withLive);
          }
          yield* Ref.update(spawnCalls, (calls) => [...calls, input.name]);
          yield* Ref.update(online, (names) => [...names, input.name]);
          yield* Ref.update(activeSpawns, (n) => n - 1);
          return { name: input.name };
        }),
      onPresenceChange: () => () => {},
    };
    return { shape, spawnCalls, maxConcurrentSpawns };
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
  waitForMatchingEvent(
    events,
    (event): event is Extract<ProviderRuntimeEvent, { type: T }> => event.type === eventType,
  );

/**
 * Like `waitForEvent`, but for when a test needs a *specific* occurrence of
 * an event type rather than the first one ever recorded — `events` only
 * ever grows, so a second `waitForEvent(events, "session.state.changed")`
 * call after an earlier one already matched would just find that same
 * first event again, not wait for a new one.
 */
const waitForMatchingEvent = <A extends ProviderRuntimeEvent>(
  events: Ref.Ref<ReadonlyArray<ProviderRuntimeEvent>>,
  predicate: (event: ProviderRuntimeEvent) => event is A,
): Effect.Effect<A> =>
  Effect.gen(function* () {
    while (true) {
      const current = yield* Ref.get(events);
      const found = current.find(predicate);
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
      // Tagged `"assistant_text"`, not `"command_output"`: Agent Relay has
      // no structured split between "model output" and "tool output" —
      // it's all one raw terminal stream — and `ProviderRuntimeIngestion`
      // only turns `"assistant_text"` deltas into visible transcript
      // content, silently dropping every other stream kind.
      assert.equal(delta.payload.streamKind, "assistant_text");
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

  it.effect("completes a turn once the broker never responds at all", () =>
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
      // Let the forked watchdog actually reach its first `Queue.take` /
      // `Effect.sleep` race before advancing the clock — otherwise this
      // adjust can run before the fiber the scheduler hasn't gotten to yet
      // registers its sleep, and the sleep starts counting from the
      // already-advanced time instead of firing.
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      // No output ever arrives: the watchdog first waits up to
      // TURN_FIRST_ACTIVITY_TIMEOUT_MS (30s) for *any* activity before
      // falling back to the same idle-complete behavior it applies between
      // frames (TURN_IDLE_COMPLETE_MS, 1.5s) once that grace period runs
      // out too.
      yield* TestClock.adjust(Duration.millis(30_000));
      yield* TestClock.adjust(Duration.millis(1_500));

      const completed = yield* waitForEvent(events, "turn.completed");
      assert.deepEqual(completed.payload, { state: "completed", stopReason: null });

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect(
    "does not complete a turn during startup latency, only after real idle-quiet following output",
    () =>
      Effect.gen(function* () {
        const { server, httpServer, url } = yield* Effect.promise(startMockBroker);
        yield* Effect.addFinalizer(() => Effect.sync(() => httpServer.close()));

        const adapter = yield* makeTestAdapter(url);
        const events = yield* makeEventCollector(adapter.streamEvents);
        const connectionFiber = yield* forkConnectionWait(server);

        const threadId = ThreadId.make("agentrelay-slow-start");
        yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
        const brokerSocket = yield* Fiber.join(connectionFiber);
        yield* waitForEvent(events, "session.state.changed");

        yield* adapter.sendTurn({ threadId, input: "run the tests" });
        yield* waitForEvent(events, "turn.started");
        yield* Effect.yieldNow;
        yield* Effect.yieldNow;

        // Startup latency longer than TURN_IDLE_COMPLETE_MS (1.5s), but well
        // inside TURN_FIRST_ACTIVITY_TIMEOUT_MS (30s): the turn must still
        // be active when output finally arrives — this is exactly the
        // premature-completion bug being regression-tested.
        yield* TestClock.adjust(Duration.millis(5_000));
        assert.isUndefined(
          (yield* Ref.get(events)).find((event) => event.type === "turn.completed"),
        );

        brokerSocket.send(
          encodeWorkerStreamFrame({
            kind: "worker_stream",
            name: "Worker1",
            stream: "stdout",
            chunk: "still working\n",
          }),
        );
        // Give the incoming frame's handler a turn to run and nudge the
        // watchdog's activity queue before the next clock advance.
        yield* waitForEvent(events, "content.delta");
        yield* Effect.yieldNow;
        yield* Effect.yieldNow;

        assert.isUndefined(
          (yield* Ref.get(events)).find((event) => event.type === "turn.completed"),
        );

        // Now the broker actually goes quiet: idle-complete fires off the
        // *real* idle window (1.5s after the last frame), not the startup
        // grace period.
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

  it.effect("reconnects after a remote close with code 1000 instead of stopping the session", () =>
    Effect.gen(function* () {
      const { server, httpServer, url } = yield* Effect.promise(startMockBroker);
      yield* Effect.addFinalizer(() => Effect.sync(() => httpServer.close()));

      const adapter = yield* makeTestAdapter(url);
      const events = yield* makeEventCollector(adapter.streamEvents);
      const firstConnection = yield* forkConnectionWait(server);

      const threadId = ThreadId.make("agentrelay-reconnect-1000");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
      const brokerSocket = yield* Fiber.join(firstConnection);
      yield* waitForEvent(events, "session.state.changed");

      // A *remote* close with code 1000 — normal closure, but not one
      // this adapter asked for (`ctx.stopped` is only set by its own
      // `stopSession`/`stopSessionInternal`). Broker restarts close this
      // way; treating every 1000 as a deliberate local stop (the
      // previous behavior) tore the session down and skipped the
      // reconnect/backoff loop entirely on exactly the closes it exists
      // for.
      const secondConnection = yield* forkConnectionWait(server);
      brokerSocket.close(1000, "broker restarting");

      const errorEvent = yield* waitForMatchingEvent(
        events,
        (event): event is Extract<ProviderRuntimeEvent, { type: "session.state.changed" }> =>
          event.type === "session.state.changed" && event.payload.state === "error",
      );
      assert.equal(errorEvent.payload.state, "error");

      // RECONNECT_DELAYS_MS[0].
      yield* TestClock.adjust(Duration.millis(1_000));
      // A new connection arriving proves `connect()` ran again instead
      // of the session being torn down for good.
      yield* Fiber.join(secondConnection);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect(
    "aborts the active turn and keeps the session in error when the broker disconnects mid-turn",
    () =>
      Effect.gen(function* () {
        const { server, httpServer, url } = yield* Effect.promise(startMockBroker);
        yield* Effect.addFinalizer(() => Effect.sync(() => httpServer.close()));

        const adapter = yield* makeTestAdapter(url);
        const events = yield* makeEventCollector(adapter.streamEvents);
        const connectionFiber = yield* forkConnectionWait(server);

        const threadId = ThreadId.make("agentrelay-disconnect-mid-turn");
        yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
        const brokerSocket = yield* Fiber.join(connectionFiber);
        yield* waitForEvent(events, "session.state.changed");

        yield* adapter.sendTurn({ threadId, input: "run the tests" });
        yield* waitForEvent(events, "turn.started");

        // The broker drops the connection with no socket left to ever
        // deliver this turn's output. The turn must be aborted immediately
        // here, not left for the idle watchdog to eventually (and
        // incorrectly) mark "completed" against a session with no socket.
        // `.terminate()` (not `.close()`) simulates a real abnormal
        // disconnect: 1006 is a reserved code the WebSocket protocol
        // forbids ever sending explicitly, so `ws` rejects `.close(1006)`
        // outright — `.terminate()` drops the TCP connection without a
        // close handshake, which is what actually produces a 1006 on the
        // other end.
        brokerSocket.terminate();

        const completed = yield* waitForEvent(events, "turn.completed");
        assert.deepEqual(completed.payload, { state: "cancelled", stopReason: null });

        const [session] = yield* adapter.listSessions();
        assert.equal(session?.status, "error");

        yield* adapter.stopSession(threadId);
      }),
  );

  it.effect("normalizes a broker URL with a trailing slash before deriving ws/input routes", () =>
    Effect.gen(function* () {
      const { server, httpServer, url, inputs } = yield* Effect.promise(startMockBroker);
      yield* Effect.addFinalizer(() => Effect.sync(() => httpServer.close()));

      // A doubled "//ws" or "//api/input/<name>" would never match the
      // mock broker's exact-path routing (`WebSocketServer`'s `path: "/ws"`
      // and the `/^\/api\/input\/([^/]+)$/` regex respectively) — this
      // test fails by timing out (connection) or by `inputs` staying empty
      // (POST) if the trailing slash was not stripped.
      const adapter = yield* makeTestAdapter(`${url}/`);
      const events = yield* makeEventCollector(adapter.streamEvents);
      const connectionFiber = yield* forkConnectionWait(server);

      const threadId = ThreadId.make("agentrelay-trailing-slash");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
      yield* Fiber.join(connectionFiber);
      yield* waitForEvent(events, "session.state.changed");

      yield* adapter.sendTurn({ threadId, input: "go" });
      yield* waitUntil(() => inputs.length === 1);
      assert.equal(inputs[0]!.data, "go\n");

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("rolls back turn registration and emits turn.aborted when postInput fails", () =>
    Effect.gen(function* () {
      const { server, httpServer, url, inputFailure } = yield* Effect.promise(startMockBroker);
      yield* Effect.addFinalizer(() => Effect.sync(() => httpServer.close()));

      const adapter = yield* makeTestAdapter(url);
      const events = yield* makeEventCollector(adapter.streamEvents);
      const connectionFiber = yield* forkConnectionWait(server);

      const threadId = ThreadId.make("agentrelay-postinput-fails");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
      yield* Fiber.join(connectionFiber);
      yield* waitForEvent(events, "session.state.changed");

      inputFailure.failNext = true;
      const failure = yield* adapter
        .sendTurn({ threadId, input: "this never reaches the broker" })
        .pipe(Effect.flip);
      assert.equal(failure._tag, "ProviderAdapterRequestError");

      // `turn.started` went out (registered before the POST, so an
      // in-flight response would already have somewhere to attach), so a
      // failed POST must also publish a terminal event for it rather than
      // leaving that turn permanently open.
      const aborted = yield* waitForEvent(events, "turn.aborted");
      assert.equal(aborted.turnId, (yield* waitForEvent(events, "turn.started")).turnId);

      // The rollback must leave the session able to accept a normal turn
      // afterwards — proving `ctx.activeTurnId`/`ctx.session`/`ctx.turns`
      // were actually restored, not left pointing at the failed turn.
      inputFailure.failNext = false;
      const { turnId } = yield* adapter.sendTurn({ threadId, input: "try again" });
      assert.isDefined(turnId);

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

  it.effect("does not persist a resume cursor in Single mode", () =>
    Effect.gen(function* () {
      const { httpServer, url } = yield* Effect.promise(startMockBroker);
      yield* Effect.addFinalizer(() => Effect.sync(() => httpServer.close()));

      // `resumeCursor` only means something in Workspace mode (which
      // spawned agent to reattach to). Persisting it in Single mode too
      // meant switching an instance from Single to Workspace left the old
      // single-mode agent's name behind as a stale cursor — the next
      // Workspace start would skip spawning and silently attach to that
      // unrelated agent.
      const adapter = yield* makeTestAdapter(url);
      const threadId = ThreadId.make("agentrelay-single-no-resume-cursor");
      const session = yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
      assert.isUndefined(session.resumeCursor);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("caps retained turn history instead of growing it without bound", () =>
    Effect.gen(function* () {
      const { server, httpServer, url } = yield* Effect.promise(startMockBroker);
      yield* Effect.addFinalizer(() => Effect.sync(() => httpServer.close()));

      const adapter = yield* makeTestAdapter(url);
      const events = yield* makeEventCollector(adapter.streamEvents);
      const connectionFiber = yield* forkConnectionWait(server);

      const threadId = ThreadId.make("agentrelay-turn-history-cap");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
      yield* Fiber.join(connectionFiber);
      // Wait for "ready", not just the connection being accepted:
      // `sendTurn` falls back to polling `awaitSessionConnected` on a
      // virtual-clock sleep otherwise, which never fires without a
      // `TestClock.adjust` this test has no reason to do.
      yield* waitForEvent(events, "session.state.changed");

      // MAX_RETAINED_TURNS is 50 — send well past it. Each call steers the
      // same still-open turn (the mock broker never sends a reply, so the
      // watchdog never completes it), matching the common "keep typing"
      // case that actually grows this array in practice.
      for (let i = 0; i < 55; i++) {
        yield* adapter.sendTurn({ threadId, input: `message ${i}` });
      }

      const thread = yield* adapter.readThread(threadId);
      assert.equal(thread.turns.length, 50);

      yield* adapter.stopSession(threadId);
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

  it.effect("serializes overlapping startSession calls for the same thread", () =>
    Effect.gen(function* () {
      const { httpServer, url } = yield* Effect.promise(startMockBroker);
      yield* Effect.addFinalizer(() => Effect.sync(() => httpServer.close()));

      const {
        shape: workspaceClient,
        spawnCalls,
        maxConcurrentSpawns,
      } = yield* makeFakeWorkspaceClient({
        spawnDelayMs: 20,
      });
      const adapter = yield* makeWorkspaceTestAdapter(url, workspaceClient);

      const threadId = ThreadId.make("agentrelay-workspace-concurrent-start");
      // Two callers racing `startSession` for the same thread (e.g. a
      // duplicate client request) must not both observe "no session yet"
      // and spawn/attach independently — the loser's socket and spawned
      // agent would be orphaned when the winner's `sessions.set` silently
      // overwrote its context.
      yield* Effect.all(
        [
          adapter.startSession({ threadId, runtimeMode: "full-access" }),
          adapter.startSession({ threadId, runtimeMode: "full-access" }),
        ],
        { concurrency: "unbounded" },
      );

      assert.equal(yield* Ref.get(maxConcurrentSpawns), 1);
      assert.equal((yield* Ref.get(spawnCalls)).length, 2);

      yield* adapter.stopSession(threadId);
    }),
  );
});
