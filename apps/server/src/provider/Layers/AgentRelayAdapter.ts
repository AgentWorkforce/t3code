/**
 * AgentRelayAdapterLive — Agent Relay broker (terminal/PTY transport).
 *
 * Unlike every other adapter in this directory, Agent Relay does not spawn
 * or own a local subprocess. It opens an outbound WebSocket connection to an
 * already-running Agent Relay broker (`agent-relay-broker`, a separate
 * process this server does not manage) and attaches to one already-running
 * agent the same way Agent Relay's own external terminal clients do:
 * receiving `worker_stream` terminal-output frames over `/ws` and sending
 * keystrokes via a separate HTTP POST. See `docs/internals/providers.md`
 * for why this is v1 scope and what a v2 structured-protocol adapter would
 * add.
 *
 * Wire format: verified directly against Agent Relay's own source
 * (`crates/broker/src/protocol.rs`'s `BrokerEvent::WorkerStream` and its
 * serde round-trip test, plus `@agent-relay/harness-driver`'s
 * `HarnessDriverClient`/`BrokerTransport`), and confirmed live against a
 * real `agent-relay-broker` — not a guess:
 *
 * - Output: every event over `/ws` is a JSON object discriminated by
 *   `kind` (not `type`). A `worker_stream` event carries `name`, `stream`,
 *   `chunk` (not `data`), and an optional `offset`. The broker broadcasts
 *   every worker on it over the same socket, so `parseAgentRelayFrame`
 *   returns the frame's `name` and `handleIncomingText` filters by
 *   `ctx.agentName` — a session must not react to another worker's output.
 * - Auth: both `/ws` and the HTTP API take the API key as an `X-API-Key`
 *   header, not `Authorization: Bearer`.
 * - Input: `POST {brokerBaseUrl}/api/input/{name}` with JSON body
 *   `{ data: string }`, not a message sent over `/ws`. Agent Relay also
 *   has a higher-throughput streaming input WebSocket
 *   (`/api/input/{name}/stream`, with acks and keepalives) that this
 *   adapter does not use — the plain POST is simpler and was enough to
 *   verify correct end-to-end.
 *
 * Workspace mode: when `agentRelaySettings.mode === "workspace"`, a thread
 * with no agent bound to it yet spawns one through `workspaceClient` and
 * waits for it to come online (see `waitForAgentOnline`) before
 * connecting — everything from `connect()` down is untouched, reused exactly
 * as v1 built it. See `docs/internals/providers.md` for the credential
 * caveat that comes with this.
 *
 * @module AgentRelayAdapterLive
 */
import {
  type AgentRelaySettings,
  EventId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  ProviderDriverKind,
  ProviderInstanceId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";
import WebSocket from "ws";

import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import { type AgentRelayAdapterShape } from "../Services/AgentRelayAdapter.ts";
import type { AgentRelayWorkspaceClientShape } from "../Services/AgentRelayWorkspaceClient.ts";

const PROVIDER = ProviderDriverKind.make("agentrelay");

// Reconnect backoff after an unexpected close. The last entry repeats for
// every attempt beyond it, so a broker that stays down does not get hammered.
const RECONNECT_DELAYS_MS = [1_000, 2_000, 5_000, 10_000, 30_000];

// Turns have no native "done" signal over a raw terminal stream. A turn is
// considered complete once the broker has been quiet for this long after the
// last worker_stream frame. This is a heuristic, not a protocol guarantee —
// see the "protocol traps" note in docs/internals/providers.md.
const TURN_IDLE_COMPLETE_MS = 1_500;

// The idle-quiet heuristic above only makes sense once the agent has
// actually started responding. Starting that 1.5s countdown immediately
// after `postInput` (the original behavior) settled the turn during
// ordinary startup latency — broker round-trip, agent cold start — before
// any output existed, so a slightly slow agent had its turn marked
// "completed" while still working, and every frame after that arrived with
// no active turn to attach to. This bounds how long the watchdog waits for
// the *first* frame before falling back to the same idle-complete behavior
// as before; it is deliberately far more generous than the between-frames
// quiet window above.
const TURN_FIRST_ACTIVITY_TIMEOUT_MS = 30_000;

// How long to wait for a freshly-spawned agent to report itself online (via
// `workspaceClient.onPresenceChange`, raced against polling `listAgents`)
// before giving up. Agent Relay's own `add_agent` MCP tool documents spawns
// as fire-and-forget, so this has to be generous — CLI installs and cold
// starts are not instant.
const AGENT_SPAWN_WAIT_TIMEOUT = Duration.seconds(90);
const AGENT_SPAWN_POLL_INTERVAL = Duration.seconds(2);

// `startSession` returns as soon as the WebSocket connect is *initiated*
// (`connect()` is fire-and-forget — see its own comment) so the session is
// still `"connecting"` when orchestration's first `sendTurn` call lands
// moments later; the handshake has not necessarily finished. Give it a
// short grace window to reach `"ready"` before treating that as a real
// failure, since the alternative is every new Agent Relay thread's first
// message reliably losing this race and erroring out.
const CONNECT_WAIT_TIMEOUT = Duration.seconds(15);
const CONNECT_POLL_INTERVAL = Duration.millis(50);

// `ctx.turns` retains this many most-recent entries per session — see
// `sendTurn`'s trim comment for why an unbounded history isn't needed here.
const MAX_RETAINED_TURNS = 50;

/** Durable per-thread continuation state for workspace mode, round-tripped
 * through `ProviderSession.resumeCursor` / `ProviderSessionDirectory` the
 * same way `CodexResumeCursorSchema` persists a rollout id. Absent (or
 * invalid) means "no agent bound to this thread yet — spawn one".
 *
 * Exported for `AgentRelayThreadDiscoveryReactor`, which scans persisted
 * bindings to tell whether an agent already has a thread before
 * materializing one for it — the same "which field carries the attach
 * signal" question `startSession` answers below. */
export const AgentRelayResumeCursorSchema = Schema.Struct({ agentName: Schema.String });
export const isAgentRelayResumeCursor = Schema.is(AgentRelayResumeCursorSchema);

/** `https://broker.example.com` -> `wss://broker.example.com/ws`. */
function toWsUrl(brokerBaseUrl: string): string {
  return `${brokerBaseUrl.replace(/^http/, "ws")}/ws`;
}

/** `https://broker.example.com` -> `https://broker.example.com/api/input/<name>`. */
function toInputUrl(brokerBaseUrl: string, agentName: string): string {
  return `${brokerBaseUrl}/api/input/${encodeURIComponent(agentName)}`;
}

/** Derive a valid, deterministic Relaycast agent name for a thread's spawn,
 * so retrying `startSession` on the same thread (before a resume cursor is
 * persisted) asks for the same name instead of leaking one per attempt. */
function spawnAgentNameForThread(threadId: ThreadId): string {
  const slug = String(threadId)
    .replace(/[^a-zA-Z0-9_-]/g, "-")
    .slice(0, 40);
  return `t3code-${slug}`;
}

/**
 * Waits for `name` to report online, racing a live presence subscription
 * against polling `listAgents` — belt and suspenders, since this module
 * could not confirm from static reading alone that Agent Relay's presence
 * events reach `workspaceClient.onPresenceChange` for every workspace (see
 * `AgentRelayWorkspaceClientLive.ts`). Polling alone is sufficient for
 * correctness; the subscription only makes the common case faster.
 */
function waitForAgentOnline(
  workspaceClient: AgentRelayWorkspaceClientShape,
  name: string,
): Effect.Effect<void, ProviderAdapterRequestError> {
  const awaitPresenceEvent = Effect.callback<void>((resume) => {
    const unsubscribe = workspaceClient.onPresenceChange((eventName, status) => {
      if (eventName === name && status === "online") resume(Effect.void);
    });
    return Effect.sync(unsubscribe);
  });

  const pollUntilOnline = Effect.gen(function* () {
    while (true) {
      const agents = yield* workspaceClient.listAgents().pipe(Effect.orElseSucceed(() => []));
      if (agents.some((agent) => agent.name === name && agent.status === "online")) return;
      yield* Effect.sleep(AGENT_SPAWN_POLL_INTERVAL);
    }
  });

  return Effect.race(awaitPresenceEvent, pollUntilOnline).pipe(
    Effect.timeoutOption(AGENT_SPAWN_WAIT_TIMEOUT),
    Effect.flatMap((result) =>
      Option.isSome(result)
        ? Effect.void
        : new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "startSession",
            detail: `Agent Relay did not report '${name}' online within ${Duration.toSeconds(AGENT_SPAWN_WAIT_TIMEOUT)}s of spawning it. The spawn may still be starting up — try sending another message to retry the attach.`,
          }),
    ),
  );
}

/**
 * Blocks while `ctx.session.status` is still `"connecting"`, so a `sendTurn`
 * that lands right after `startSession` waits out the WebSocket handshake
 * instead of immediately erroring. Returns as soon as the status moves to
 * anything else (`"ready"` from `handleOpen`, or `"error"` from a failed
 * connect/close) — `sendTurn`'s existing status check is what turns an
 * `"error"` outcome here into the right user-facing message.
 */
function awaitSessionConnected(
  ctx: AgentRelaySessionContext,
): Effect.Effect<void, ProviderAdapterRequestError> {
  const pollUntilSettled = Effect.gen(function* () {
    // `ctx.stopped` also exits the wait: a session stopped mid-connect
    // (e.g. `stopSession` racing a still-connecting `sendTurn`) should fail
    // fast on the next status check instead of polling until the timeout.
    while (ctx.session.status === "connecting" && !ctx.stopped) {
      yield* Effect.sleep(CONNECT_POLL_INTERVAL);
    }
  });

  return pollUntilSettled.pipe(Effect.timeoutOption(CONNECT_WAIT_TIMEOUT), Effect.asVoid);
}

export interface AgentRelayAdapterLiveOptions {
  /** Selections are honored when routed to this instance id. Defaults to
   * the legacy built-in instance id (`agentrelay`). */
  readonly instanceId?: ProviderInstanceId;
  /** Required (and only used) when `agentRelaySettings.mode === "workspace"`:
   * discovers and spawns agents in the configured Relaycast workspace. */
  readonly workspaceClient?: AgentRelayWorkspaceClientShape;
}

interface AgentRelaySessionContext {
  readonly threadId: ThreadId;
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  /** Base HTTP(S) broker URL (`agentRelaySettings.brokerUrl`, trimmed).
   * `/ws` and `/api/input/<name>` are both derived from this. */
  readonly brokerBaseUrl: string;
  /** The specific worker this session attaches to. Resolved once at
   * `startSession` (single mode: `agentRelaySettings.agentName`; workspace
   * mode: the resumed or freshly-spawned agent's name) and fixed for the
   * life of the session — reconnects keep attaching to the same agent.
   * The broker's `/ws` broadcasts every worker on it, so every incoming
   * frame is filtered against this before the session reacts to it. */
  readonly agentName: string;
  socket: WebSocket | undefined;
  reconnectAttempt: number;
  activeTurnId: TurnId | undefined;
  readonly activitySignals: Queue.Queue<void>;
  turnWatchdogFiber: Fiber.Fiber<void> | undefined;
  readonly turns: Array<{ readonly id: TurnId; readonly items: Array<unknown> }>;
  stopped: boolean;
}

/**
 * Parse one incoming `/ws` frame. Returns `undefined` for anything that is
 * not a `worker_stream` event for some worker (non-JSON, a different
 * `kind`, or a missing `name`/`chunk`) so an unrecognized or irrelevant
 * broker message is ignored instead of tearing down the session. Matching
 * `name` against the session's target agent is the caller's job
 * (`handleIncomingText`) — the broker broadcasts every worker on it over
 * this one socket.
 */
function parseAgentRelayFrame(
  raw: string,
): { readonly name: string; readonly text: string } | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return undefined;
  }
  if (typeof parsed !== "object" || parsed === null) return undefined;
  const record = parsed as Record<string, unknown>;
  if (record.kind !== "worker_stream") return undefined;
  if (typeof record.name !== "string" || typeof record.chunk !== "string") return undefined;
  return { name: record.name, text: record.chunk };
}

function describeError(cause: unknown): string {
  return cause instanceof Error ? cause.message : String(cause);
}

export function makeAgentRelayAdapter(
  agentRelaySettings: AgentRelaySettings,
  options?: AgentRelayAdapterLiveOptions,
) {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("agentrelay");
    const crypto = yield* Crypto.Crypto;
    const context = yield* Effect.context<never>();
    const fork = Effect.runForkWith(context);
    const httpClient = yield* HttpClient.HttpClient;

    const sessions = new Map<ThreadId, AgentRelaySessionContext>();
    const threadLocksRef = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());
    const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const randomUUIDv4 = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv4",
            detail: "Failed to generate an Agent Relay runtime identifier.",
            cause,
          }),
      ),
    );
    const nextEventId = randomUUIDv4.pipe(Effect.map((id) => EventId.make(id)));
    const makeEventStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });

    const offerRuntimeEvent = (event: ProviderRuntimeEvent) =>
      PubSub.publish(runtimeEventPubSub, event).pipe(Effect.asVoid);

    /** Run a background (socket-callback-triggered) effect, logging instead
     * of losing failures — there is no caller left to observe them. */
    const dispatch = (effect: Effect.Effect<void, ProviderAdapterRequestError>): void => {
      fork(
        effect.pipe(
          Effect.catchCause((cause) =>
            Effect.logError("Agent Relay adapter background task failed.", { cause }),
          ),
        ),
      );
    };

    // Serializes `startSession` per thread — see its call site for why:
    // spawning/waiting-online and installing the session context both
    // happen inside the lock, matching `CursorAdapter`'s pattern.
    const getThreadSemaphore = (threadId: string) =>
      SynchronizedRef.modifyEffect(threadLocksRef, (current) => {
        const existing = Option.fromNullishOr(current.get(threadId));
        return Option.match(existing, {
          onNone: () =>
            Semaphore.make(1).pipe(
              Effect.map((semaphore) => {
                const next = new Map(current);
                next.set(threadId, semaphore);
                return [semaphore, next] as const;
              }),
            ),
          onSome: (semaphore) => Effect.succeed([semaphore, current] as const),
        });
      });

    const withThreadLock = <A, E, R>(threadId: string, effect: Effect.Effect<A, E, R>) =>
      Effect.flatMap(getThreadSemaphore(threadId), (semaphore) => semaphore.withPermit(effect));

    const requireSession = (
      threadId: ThreadId,
    ): Effect.Effect<AgentRelaySessionContext, ProviderAdapterSessionNotFoundError> => {
      const ctx = sessions.get(threadId);
      if (!ctx || ctx.stopped) {
        return Effect.fail(
          new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }),
        );
      }
      return Effect.succeed(ctx);
    };

    /** `POST /api/input/{name}` — see the module doc for why this is a
     * separate HTTP call rather than a message over the `/ws` socket. */
    const postInput = (
      ctx: AgentRelaySessionContext,
      data: string,
    ): Effect.Effect<void, ProviderAdapterRequestError> =>
      Effect.gen(function* () {
        const apiKey = agentRelaySettings.apiKey.trim();
        let request = HttpClientRequest.post(toInputUrl(ctx.brokerBaseUrl, ctx.agentName)).pipe(
          HttpClientRequest.bodyJsonUnsafe({ data }),
        );
        if (apiKey) {
          request = request.pipe(HttpClientRequest.setHeader("X-API-Key", apiKey));
        }
        yield* httpClient.execute(request).pipe(Effect.flatMap(HttpClientResponse.filterStatusOk));
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "sendInput",
              detail: `Failed to send input to the Agent Relay broker: ${describeError(cause)}`,
              cause,
            }),
        ),
      );

    const completeActiveTurn = (
      ctx: AgentRelaySessionContext,
      turnId: TurnId,
      state: "completed" | "cancelled",
    ) =>
      Effect.gen(function* () {
        if (ctx.activeTurnId !== turnId) return;
        ctx.activeTurnId = undefined;
        // Does not interrupt `ctx.turnWatchdogFiber` itself: the watchdog's
        // own timeout branch calls this and then returns, so interrupting
        // here would be the fiber interrupting itself mid-step. A caller on
        // a different fiber (`interruptTurn`) is responsible for stopping
        // the watchdog before it calls this.
        ctx.turnWatchdogFiber = undefined;
        const updatedAt = yield* nowIso;
        const { activeTurnId: _activeTurnId, ...rest } = ctx.session;
        // Preserve "error" rather than unconditionally restoring "ready":
        // a disconnect during this turn (`handleClose`) already set status
        // to "error" and cleared `ctx.socket`. Without this check, this
        // watchdog-driven completion would resurrect the session to
        // "ready" with no socket to serve it — `sendTurn` would then
        // accept a next message it can never actually deliver.
        const status = ctx.session.status === "error" ? "error" : "ready";
        ctx.session = { ...rest, status, updatedAt };
        yield* offerRuntimeEvent({
          type: "turn.completed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          turnId,
          payload: { state, stopReason: null },
        });
      });

    const startTurnWatchdog = (
      ctx: AgentRelaySessionContext,
      turnId: TurnId,
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        // Wait for the first sign of activity before starting the
        // idle-quiet countdown — see `TURN_FIRST_ACTIVITY_TIMEOUT_MS`'s
        // comment. If nothing ever arrives within that bound, fall through
        // to the same idle-complete behavior the loop below applies
        // between frames, rather than hanging forever.
        yield* Effect.raceFirst(
          Queue.take(ctx.activitySignals),
          Effect.sleep(Duration.millis(TURN_FIRST_ACTIVITY_TIMEOUT_MS)),
        );
        while (ctx.activeTurnId === turnId) {
          const woke = yield* Effect.raceFirst(
            Effect.sleep(Duration.millis(TURN_IDLE_COMPLETE_MS)).pipe(
              Effect.as("timeout" as const),
            ),
            Queue.take(ctx.activitySignals).pipe(Effect.as("activity" as const)),
          );
          if (woke === "timeout") {
            yield* completeActiveTurn(ctx, turnId, "completed");
            return;
          }
        }
      }).pipe(Effect.catch(() => Effect.void));

    const scheduleReconnect = (ctx: AgentRelaySessionContext) =>
      Effect.gen(function* () {
        const attempt = ctx.reconnectAttempt;
        ctx.reconnectAttempt = attempt + 1;
        const delayMs = RECONNECT_DELAYS_MS[Math.min(attempt, RECONNECT_DELAYS_MS.length - 1)]!;
        yield* Effect.sleep(Duration.millis(delayMs));
        if (ctx.stopped) return;
        connect(ctx);
      }).pipe(Effect.forkIn(ctx.scope));

    const handleOpen = (ctx: AgentRelaySessionContext) =>
      Effect.gen(function* () {
        const liveCtx = sessions.get(ctx.threadId);
        if (liveCtx !== ctx || ctx.stopped) return;
        ctx.reconnectAttempt = 0;
        const updatedAt = yield* nowIso;
        const { lastError: _lastError, ...clearedSession } = ctx.session;
        ctx.session = { ...clearedSession, status: "ready", updatedAt };
        yield* offerRuntimeEvent({
          type: "session.state.changed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: { state: "ready", reason: "Connected to the Agent Relay broker." },
        });
      });

    const handleIncomingText = (ctx: AgentRelaySessionContext, raw: string) =>
      Effect.gen(function* () {
        const liveCtx = sessions.get(ctx.threadId);
        if (liveCtx !== ctx || ctx.stopped) return;
        const frame = parseAgentRelayFrame(raw);
        // Not a worker_stream frame, or another worker's output — the
        // broker broadcasts every worker on this connection.
        if (!frame || frame.name !== ctx.agentName) return;
        yield* Queue.offer(ctx.activitySignals, undefined);
        yield* offerRuntimeEvent({
          type: "content.delta",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          ...(ctx.activeTurnId ? { turnId: ctx.activeTurnId } : {}),
          // `"command_output"` (what this reads as, structurally) is for a
          // structured adapter to stream a tool call's output alongside its
          // own separate `"assistant_text"` — `ProviderRuntimeIngestion`
          // only turns `"assistant_text"` deltas into visible message
          // content and silently drops every other stream kind. Agent
          // Relay has no such split: the raw terminal stream *is* the
          // entire response, so it has to be tagged `"assistant_text"` to
          // ever reach the transcript at all. Confirmed live: broker
          // frames arrived and the session reached "ready" with this
          // tagged as `"command_output"`, but nothing rendered.
          payload: { streamKind: "assistant_text", delta: frame.text },
        });
      });

    const handleClose = (ctx: AgentRelaySessionContext, code: number, reason: string) =>
      Effect.gen(function* () {
        const liveCtx = sessions.get(ctx.threadId);
        // `ctx.stopped` is the authoritative "we asked for this" signal:
        // `stopSessionInternal` sets it synchronously before ever calling
        // `socket.close()`, so by the time this handler's "close" event
        // fires for our own intentional close, this guard has already
        // returned. Reaching past it therefore means the *other* side
        // closed the socket — including a clean code 1000, which `ws`
        // servers send for perfectly ordinary reasons (a broker restart,
        // for instance). Treating every 1000 as deliberate (the previous
        // behavior) tore the session down and permanently skipped the
        // reconnect/backoff loop below on exactly the closes it exists
        // for. Every non-local close now goes through the same
        // error-then-reconnect path regardless of code.
        if (liveCtx !== ctx || ctx.stopped) return;
        ctx.socket = undefined;
        const detail = reason.trim() || `Broker connection closed (code ${code}).`;
        const updatedAt = yield* nowIso;
        ctx.session = { ...ctx.session, status: "error", updatedAt, lastError: detail };
        yield* offerRuntimeEvent({
          type: "session.state.changed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: { state: "error", reason: detail },
        });
        // Abort any in-flight turn now instead of leaving it to the idle
        // watchdog: with no socket, nothing will ever deliver its output,
        // and (per `completeActiveTurn`'s status guard above) letting the
        // watchdog's own timeout fire later would just complete the turn
        // against a session already marked "error".
        const turnId = ctx.activeTurnId;
        if (turnId !== undefined) {
          const watchdog = ctx.turnWatchdogFiber;
          ctx.turnWatchdogFiber = undefined;
          if (watchdog) {
            yield* Fiber.interrupt(watchdog);
          }
          yield* completeActiveTurn(ctx, turnId, "cancelled");
        }
        yield* scheduleReconnect(ctx);
      });

    const handleSocketError = (ctx: AgentRelaySessionContext, detail: string) =>
      Effect.logWarning("Agent Relay broker socket reported an error.", {
        threadId: ctx.threadId,
        detail,
      });

    const handleConnectFailure = (ctx: AgentRelaySessionContext, detail: string) =>
      Effect.gen(function* () {
        const liveCtx = sessions.get(ctx.threadId);
        if (liveCtx !== ctx || ctx.stopped) return;
        const updatedAt = yield* nowIso;
        ctx.session = { ...ctx.session, status: "error", updatedAt, lastError: detail };
        yield* offerRuntimeEvent({
          type: "session.state.changed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: {
            state: "error",
            reason: `Could not connect to the Agent Relay broker: ${detail}`,
          },
        });
      });

    // Plain (non-Effect) glue: opens the socket and bridges its callback
    // API into the Effect world via `dispatch`, the same shape `NodePtyAdapter`
    // uses for `onData`/`onExit`. Not itself an Effect because `new WebSocket`
    // and `.on(...)` registration are synchronous and side-effecting.
    const connect = (ctx: AgentRelaySessionContext): void => {
      if (ctx.stopped) return;
      const apiKey = agentRelaySettings.apiKey.trim();
      const wsUrl = toWsUrl(ctx.brokerBaseUrl);
      let socket: WebSocket;
      try {
        socket = apiKey
          ? new WebSocket(wsUrl, { headers: { "X-API-Key": apiKey } })
          : new WebSocket(wsUrl);
      } catch (cause) {
        dispatch(handleConnectFailure(ctx, describeError(cause)));
        return;
      }
      ctx.socket = socket;
      socket.on("open", () => dispatch(handleOpen(ctx)));
      socket.on("message", (data) => dispatch(handleIncomingText(ctx, data.toString("utf8"))));
      socket.on("close", (code, reason) =>
        dispatch(handleClose(ctx, code, reason.toString("utf8"))),
      );
      // `close` always follows `error` for a `ws` client socket, so the
      // reconnect decision lives entirely in `handleClose`; this only logs.
      socket.on("error", (error) => dispatch(handleSocketError(ctx, describeError(error))));
    };

    const stopSessionInternal = (ctx: AgentRelaySessionContext) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        ctx.stopped = true;
        const socket = ctx.socket;
        ctx.socket = undefined;
        if (
          socket &&
          (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)
        ) {
          // Best-effort — the socket is being discarded either way.
          yield* Effect.try(() => socket.close(1000, "session stopped")).pipe(Effect.ignore);
        }
        yield* Scope.close(ctx.scope, Exit.void).pipe(Effect.ignore);
        sessions.delete(ctx.threadId);
        yield* offerRuntimeEvent({
          type: "session.exited",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: { exitKind: "graceful" },
        });
      });

    const startSession: AgentRelayAdapterShape["startSession"] = (input) =>
      // Serialized per thread: without this, two overlapping `startSession`
      // calls for the same thread (e.g. a duplicate client request) could
      // both observe no existing context, both spawn/wait in Workspace
      // mode, and race to install their own `ctx` into `sessions` — the
      // loser's socket and workspace agent are then orphaned instead of
      // torn down. Matches `CursorAdapter`'s `withThreadLock`.
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          if (input.provider !== undefined && input.provider !== PROVIDER) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
            });
          }
          if (!agentRelaySettings.brokerUrl.trim()) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "Agent Relay broker URL is not configured for this instance.",
            });
          }

          // Resolve which agent this thread attaches to. Single mode always
          // targets the one configured `agentName`. Workspace mode: a resume
          // cursor from a prior `startSession` on this thread means an agent
          // is already bound — reconnect to that same one; no cursor means
          // this is the thread's first session, so spawn a fresh agent and
          // wait for it to come online before attaching, so a brand-new
          // Agent Relay thread never requires a pre-existing target the way
          // single mode does.
          let targetAgentName: string;
          if (agentRelaySettings.mode === "workspace") {
            if (isAgentRelayResumeCursor(input.resumeCursor)) {
              targetAgentName = input.resumeCursor.agentName;
            } else {
              const workspaceClient = options?.workspaceClient;
              if (!workspaceClient) {
                return yield* new ProviderAdapterValidationError({
                  provider: PROVIDER,
                  operation: "startSession",
                  issue:
                    "Agent Relay is in Workspace mode but no workspace key is configured for this instance.",
                });
              }
              const requestedName = spawnAgentNameForThread(input.threadId);
              const spawned = yield* workspaceClient
                .spawnAgent({
                  name: requestedName,
                  cli: agentRelaySettings.defaultSpawnCli,
                  ...(input.title ? { task: input.title } : {}),
                })
                .pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProviderAdapterRequestError({
                        provider: PROVIDER,
                        method: "startSession",
                        detail: `Failed to spawn an Agent Relay worker '${requestedName}': ${cause.detail}`,
                        cause,
                      }),
                  ),
                );
              yield* waitForAgentOnline(workspaceClient, spawned.name);
              targetAgentName = spawned.name;
            }
          } else {
            const configuredName = agentRelaySettings.agentName.trim();
            if (!configuredName) {
              return yield* new ProviderAdapterValidationError({
                provider: PROVIDER,
                operation: "startSession",
                issue:
                  "Agent Relay Single agent mode requires an agent name: the broker's WebSocket stream carries every worker on it, and T3 Code needs a name to tell them apart.",
              });
            }
            targetAgentName = configuredName;
          }
          // Strip trailing slashes: `toWsUrl`/`toInputUrl` each append their
          // own leading `/`, so a URL saved with one (e.g.
          // "https://broker.example.com/") would otherwise derive "//ws" and
          // "//api/input/<name>" — the broker's exact-path routing rejects
          // both.
          const brokerBaseUrl = agentRelaySettings.brokerUrl.trim().replace(/\/+$/, "");

          const existing = sessions.get(input.threadId);
          if (existing) {
            yield* stopSessionInternal(existing);
          }

          const scope = yield* Scope.make();
          const activitySignals = yield* Queue.sliding<void>(1);
          const now = yield* nowIso;
          const session: ProviderSession = {
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            status: "connecting",
            runtimeMode: input.runtimeMode,
            ...(input.cwd ? { cwd: input.cwd } : {}),
            threadId: input.threadId,
            // Only meaningful in Workspace mode, where it's how a later
            // `startSession` on this thread knows which spawned agent to
            // reattach to instead of spawning another. Persisting it in
            // Single mode too meant switching an instance from Single to
            // Workspace left the old single-mode agent's name behind as a
            // stale resume cursor — the next Workspace start would skip
            // spawning and silently attach to that unrelated agent.
            ...(agentRelaySettings.mode === "workspace"
              ? { resumeCursor: { agentName: targetAgentName } }
              : {}),
            createdAt: now,
            updatedAt: now,
          };
          const ctx: AgentRelaySessionContext = {
            threadId: input.threadId,
            session,
            scope,
            brokerBaseUrl,
            agentName: targetAgentName,
            socket: undefined,
            reconnectAttempt: 0,
            activeTurnId: undefined,
            activitySignals,
            turnWatchdogFiber: undefined,
            turns: [],
            stopped: false,
          };
          sessions.set(input.threadId, ctx);

          yield* offerRuntimeEvent({
            type: "session.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: {},
          });
          yield* offerRuntimeEvent({
            type: "thread.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: {},
          });

          connect(ctx);
          return session;
        }),
      );

    const sendTurn: AgentRelayAdapterShape["sendTurn"] = (input) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(input.threadId);
        if (ctx.session.status === "connecting") {
          yield* awaitSessionConnected(ctx);
        }
        if (ctx.session.status !== "ready" && ctx.session.status !== "running") {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "sendInput",
            detail: "Agent Relay session is not connected to the broker yet.",
          });
        }
        const text = input.input?.trim();
        if (!text) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue:
              "Turn requires non-empty text. Agent Relay's terminal transport cannot carry attachments.",
          });
        }
        // Register the turn *before* posting input, not after: `postInput`
        // is an HTTP round-trip, and the broker can start streaming
        // `worker_stream` output back over the already-open `/ws` socket
        // before that POST even resolves. `handleIncomingText` reads
        // `ctx.activeTurnId` to stamp outgoing `content.delta` events —
        // registering it only after `postInput` succeeded left a real
        // window where the first frames of a response arrived with no
        // active turn, and before `turn.started` had even been published.
        const isNewTurn = ctx.activeTurnId === undefined;
        const turnId = ctx.activeTurnId ?? TurnId.make(yield* randomUUIDv4);
        const previousActiveTurnId = ctx.activeTurnId;
        const previousSession = ctx.session;
        const previousTurnsLength = ctx.turns.length;

        ctx.activeTurnId = turnId;
        ctx.turns.push({ id: turnId, items: [{ input: text }] });
        const updatedAt = yield* nowIso;
        ctx.session = { ...ctx.session, status: "running", activeTurnId: turnId, updatedAt };

        if (isNewTurn) {
          yield* offerRuntimeEvent({
            type: "turn.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            turnId,
            payload: {},
          });
          ctx.turnWatchdogFiber = yield* startTurnWatchdog(ctx, turnId).pipe(
            Effect.forkIn(ctx.scope),
          );
        } else {
          // Steering an in-flight turn: nudge the watchdog so a user still
          // typing does not race the idle-completion timer.
          yield* Queue.offer(ctx.activitySignals, undefined);
        }

        const posted = yield* postInput(ctx, `${text}\n`).pipe(Effect.result);
        if (Result.isFailure(posted)) {
          // The broker never got this input — undo the registration above
          // so the session doesn't sit on a permanently "running" turn
          // nothing will ever complete. `turn.started` already went out to
          // any subscriber, so tell them it's over too.
          if (isNewTurn) {
            const watchdog = ctx.turnWatchdogFiber;
            ctx.turnWatchdogFiber = undefined;
            if (watchdog) {
              yield* Fiber.interrupt(watchdog);
            }
            yield* offerRuntimeEvent({
              type: "turn.aborted",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: input.threadId,
              turnId,
              payload: { reason: posted.failure.detail },
            });
          }
          ctx.activeTurnId = previousActiveTurnId;
          ctx.session = previousSession;
          ctx.turns.length = previousTurnsLength;
          return yield* posted.failure;
        }

        // `rollbackThread` is a no-op here (`supportsConversationRollback`
        // is false, so orchestration never actually calls it — see its own
        // comment) and there's no other reader that needs the full
        // history, only `readThread`'s most-recent view. Without a cap,
        // `ctx.turns` grows one entry per message for the life of a
        // session with nothing to ever trim it.
        if (ctx.turns.length > MAX_RETAINED_TURNS) {
          ctx.turns.splice(0, ctx.turns.length - MAX_RETAINED_TURNS);
        }

        return { threadId: input.threadId, turnId };
      });

    const interruptTurn: AgentRelayAdapterShape["interruptTurn"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const turnId = ctx.activeTurnId;
        if (turnId === undefined) return;
        // Stop the watchdog from this (different) fiber before completing
        // the turn -- `completeActiveTurn` itself never self-interrupts.
        const watchdog = ctx.turnWatchdogFiber;
        ctx.turnWatchdogFiber = undefined;
        if (watchdog) {
          yield* Fiber.interrupt(watchdog);
        }
        // Ctrl-C: the terminal-native interrupt signal, matching how a human
        // attached to the same broker session would cancel a running command.
        // Best-effort, same as the original WS-send version was — a
        // failed cancel should not fail the interrupt itself.
        yield* postInput(ctx, "\u0003").pipe(Effect.ignore);
        yield* completeActiveTurn(ctx, turnId, "cancelled");
      });

    const respondToRequest: AgentRelayAdapterShape["respondToRequest"] = (threadId, requestId) =>
      new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "respondToRequest",
        detail: `Agent Relay's terminal transport has no pending approval requests (thread ${threadId}, request ${requestId}).`,
      });

    const respondToUserInput: AgentRelayAdapterShape["respondToUserInput"] = (
      threadId,
      requestId,
    ) =>
      new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "respondToUserInput",
        detail: `Agent Relay's terminal transport has no pending user-input requests (thread ${threadId}, request ${requestId}).`,
      });

    const readThread: AgentRelayAdapterShape["readThread"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        return { threadId, turns: ctx.turns };
      });

    const rollbackThread: AgentRelayAdapterShape["rollbackThread"] = (threadId, numTurns) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        if (!Number.isInteger(numTurns) || numTurns < 1) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "rollbackThread",
            issue: "numTurns must be an integer >= 1.",
          });
        }
        // `capabilities.supportsConversationRollback` is false: a raw
        // terminal has no native conversation state to rewind, only a live
        // process. Orchestration checks the capability before calling this
        // (see docs/internals/overview.md#turn-completion-and-checkpoints),
        // so in practice this never runs — it returns the thread unchanged
        // rather than pretending turns were dropped.
        return { threadId, turns: ctx.turns };
      });

    const stopSession: AgentRelayAdapterShape["stopSession"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = sessions.get(threadId);
        if (!ctx) return;
        yield* stopSessionInternal(ctx);
      });

    const listSessions: AgentRelayAdapterShape["listSessions"] = () =>
      Effect.sync(() => Array.from(sessions.values(), (c) => ({ ...c.session })));

    const hasSession: AgentRelayAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => {
        const ctx = sessions.get(threadId);
        return ctx !== undefined && !ctx.stopped;
      });

    const stopAll: AgentRelayAdapterShape["stopAll"] = () =>
      Effect.forEach(sessions.values(), stopSessionInternal, { discard: true });

    yield* Effect.addFinalizer(() =>
      Effect.forEach(sessions.values(), stopSessionInternal, { discard: true }).pipe(
        Effect.catch((cause) =>
          Effect.logError("Failed to emit Agent Relay session shutdown event.", { cause }),
        ),
        Effect.tap(() => PubSub.shutdown(runtimeEventPubSub)),
      ),
    );

    const streamEvents = Stream.fromPubSub(runtimeEventPubSub);

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "unsupported", supportsConversationRollback: false },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      readThread,
      rollbackThread,
      stopAll,
      streamEvents,
      ...(options?.workspaceClient
        ? { listWorkspaceAgents: options.workspaceClient.listAgents }
        : {}),
    } satisfies AgentRelayAdapterShape;
  });
}
