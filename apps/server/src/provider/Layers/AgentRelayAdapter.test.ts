// @effect-diagnostics nodeBuiltinImport:off
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { WebSocketServer, type WebSocket as WsSocket } from "ws";

import { AgentRelaySettings, ThreadId, type ProviderRuntimeEvent } from "@t3tools/contracts";

import { makeAgentRelayAdapter } from "./AgentRelayAdapter.ts";

const decodeAgentRelaySettings = Schema.decodeSync(AgentRelaySettings);

const WorkerStreamFrame = Schema.Struct({
  type: Schema.Literal("worker_stream"),
  data: Schema.String,
});
const encodeWorkerStreamFrame = Schema.encodeSync(Schema.fromJsonString(WorkerStreamFrame));

const SendInputFrame = Schema.Struct({ type: Schema.Literal("sendInput"), data: Schema.String });
const decodeSendInputFrame = Schema.decodeUnknownSync(Schema.fromJsonString(SendInputFrame));

function startMockBroker(): Promise<{ readonly server: WebSocketServer; readonly url: string }> {
  return new Promise((resolve) => {
    const server = new WebSocketServer({ port: 0 }, () => {
      const address = server.address();
      const port = typeof address === "object" && address !== null ? address.port : 0;
      resolve({ server, url: `ws://127.0.0.1:${port}` });
    });
  });
}

/**
 * Forks a one-shot Node event wait and yields the current fiber a couple of
 * turns before returning, so the forked fiber has actually reached its
 * `.once(...)` registration before the caller triggers whatever produces
 * the event. Without this, a synchronous trigger (e.g. `sendFrame` on an
 * already-open socket) can fire before the scheduler ever runs the newly
 * forked fiber, and the `.once` listener attaches too late to see it.
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

const forkMessageWait = (socket: WsSocket) =>
  forkNodeEventWait<string>((resume) =>
    socket.once("message", (data: Buffer) => resume(data.toString("utf8"))),
  );

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

const agentRelayAdapterTestLayer = NodeServices.layer;

const makeTestAdapter = (brokerUrl: string, apiKey = "test-key") =>
  makeAgentRelayAdapter(decodeAgentRelaySettings({ enabled: true, brokerUrl, apiKey }));

it.layer(agentRelayAdapterTestLayer)("AgentRelayAdapterLive", (it) => {
  it.effect("streams worker_stream frames as content.delta and forwards sendInput", () =>
    Effect.gen(function* () {
      const { server, url } = yield* Effect.promise(startMockBroker);
      yield* Effect.addFinalizer(() => Effect.sync(() => server.close()));

      const adapter = yield* makeTestAdapter(url);
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

      brokerSocket.send(
        encodeWorkerStreamFrame({ type: "worker_stream", data: "$ echo hi\nhi\n" }),
      );
      const delta = yield* waitForEvent(events, "content.delta");
      assert.equal(delta.payload.streamKind, "command_output");
      assert.equal(delta.payload.delta, "$ echo hi\nhi\n");

      const inboundInputFiber = yield* forkMessageWait(brokerSocket);
      yield* adapter.sendTurn({ threadId, input: "hello agent" });
      const inbound = yield* Fiber.join(inboundInputFiber);
      assert.deepEqual(decodeSendInputFrame(inbound), { type: "sendInput", data: "hello agent\n" });

      const started = yield* waitForEvent(events, "turn.started");
      assert.isDefined(started.turnId);

      yield* adapter.stopSession(threadId);
      yield* waitForEvent(events, "session.exited");
    }),
  );

  it.effect("completes a turn once the broker goes quiet", () =>
    Effect.gen(function* () {
      const { server, url } = yield* Effect.promise(startMockBroker);
      yield* Effect.addFinalizer(() => Effect.sync(() => server.close()));

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

  it.effect("interrupting a turn sends Ctrl-C and completes it as cancelled", () =>
    Effect.gen(function* () {
      const { server, url } = yield* Effect.promise(startMockBroker);
      yield* Effect.addFinalizer(() => Effect.sync(() => server.close()));

      const adapter = yield* makeTestAdapter(url);
      const events = yield* makeEventCollector(adapter.streamEvents);
      const connectionFiber = yield* forkConnectionWait(server);

      const threadId = ThreadId.make("agentrelay-interrupt");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
      const brokerSocket = yield* Fiber.join(connectionFiber);
      yield* waitForEvent(events, "session.state.changed");

      const firstInputFiber = yield* forkMessageWait(brokerSocket);
      yield* adapter.sendTurn({ threadId, input: "run forever" });
      yield* Fiber.join(firstInputFiber);

      const interruptFiber = yield* forkMessageWait(brokerSocket);
      yield* adapter.interruptTurn(threadId);
      const interruptFrame = yield* Fiber.join(interruptFiber);

      assert.deepEqual(decodeSendInputFrame(interruptFrame), { type: "sendInput", data: "" });
      const completed = yield* waitForEvent(events, "turn.completed");
      assert.deepEqual(completed.payload, { state: "cancelled", stopReason: null });

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("has no session before startSession and none after stopSession", () =>
    Effect.gen(function* () {
      const { server, url } = yield* Effect.promise(startMockBroker);
      yield* Effect.addFinalizer(() => Effect.sync(() => server.close()));

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
        decodeAgentRelaySettings({ enabled: true, brokerUrl: "", apiKey: "" }),
      );
      const threadId = ThreadId.make("agentrelay-missing-url");
      const failure = yield* adapter
        .startSession({ threadId, runtimeMode: "full-access" })
        .pipe(Effect.flip);
      assert.equal(failure._tag, "ProviderAdapterValidationError");
    }),
  );
});
