/**
 * AgentRelayAdapterLive — Agent Relay broker (terminal/PTY transport).
 *
 * Unlike every other adapter in this directory, Agent Relay does not spawn
 * or own a local subprocess. It opens an outbound WebSocket connection to an
 * already-running Agent Relay broker (`agent-relay-broker`, a separate
 * process this server does not manage) and attaches to one already-running
 * agent the same way Agent Relay's own external terminal clients do:
 * receiving `worker_stream` terminal-output frames and replying with
 * `sendInput` keystroke frames over the same socket. See
 * `docs/internals/providers.md` for why this is v1 scope and what a v2
 * structured-protocol adapter would add.
 *
 * Wire-format note: the exact JSON shape of `worker_stream` / `sendInput`
 * frames is not vendored into this repo, so `parseAgentRelayFrame` and
 * `encodeSendInputFrame` below are the single, isolated boundary that
 * assumes a shape (`{ type: "worker_stream", data: string }` in,
 * `{ type: "sendInput", data: string }` out). If the real broker's frames
 * differ, only these two functions need to change.
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
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import WebSocket from "ws";

import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import { type AgentRelayAdapterShape } from "../Services/AgentRelayAdapter.ts";

const PROVIDER = ProviderDriverKind.make("agentrelay");

// Reconnect backoff after an unexpected close. The last entry repeats for
// every attempt beyond it, so a broker that stays down does not get hammered.
const RECONNECT_DELAYS_MS = [1_000, 2_000, 5_000, 10_000, 30_000];

// Turns have no native "done" signal over a raw terminal stream. A turn is
// considered complete once the broker has been quiet for this long after the
// last worker_stream frame. This is a heuristic, not a protocol guarantee —
// see the "protocol traps" note in docs/internals/providers.md.
const TURN_IDLE_COMPLETE_MS = 1_500;

export interface AgentRelayAdapterLiveOptions {
  /** Selections are honored when routed to this instance id. Defaults to
   * the legacy built-in instance id (`agentrelay`). */
  readonly instanceId?: ProviderInstanceId;
}

interface AgentRelaySessionContext {
  readonly threadId: ThreadId;
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  socket: WebSocket | undefined;
  reconnectAttempt: number;
  activeTurnId: TurnId | undefined;
  readonly activitySignals: Queue.Queue<void>;
  turnWatchdogFiber: Fiber.Fiber<void> | undefined;
  readonly turns: Array<{ readonly id: TurnId; readonly items: Array<unknown> }>;
  stopped: boolean;
}

/**
 * Parse one incoming text frame. Returns `undefined` for anything that does
 * not match the assumed `worker_stream` shape (non-JSON, a different
 * `type`, or a missing text field) so an unrecognized broker message is
 * ignored instead of tearing down the session.
 */
function parseAgentRelayFrame(raw: string): { readonly text: string } | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return undefined;
  }
  if (typeof parsed !== "object" || parsed === null) return undefined;
  const record = parsed as Record<string, unknown>;
  if (record.type !== "worker_stream" && record.type !== "workerStream") return undefined;
  const text =
    typeof record.data === "string"
      ? record.data
      : typeof record.chunk === "string"
        ? record.chunk
        : typeof record.text === "string"
          ? record.text
          : undefined;
  return text !== undefined ? { text } : undefined;
}

function encodeSendInputFrame(data: string): string {
  return JSON.stringify({ type: "sendInput", data });
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

    const sessions = new Map<ThreadId, AgentRelaySessionContext>();
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

    const sendFrame = (ctx: AgentRelaySessionContext, payload: string): boolean => {
      const socket = ctx.socket;
      if (!socket || socket.readyState !== WebSocket.OPEN) return false;
      try {
        socket.send(payload);
        return true;
      } catch {
        return false;
      }
    };

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
        const { activeTurnId: _activeTurnId, ...readySession } = ctx.session;
        ctx.session = { ...readySession, status: "ready", updatedAt };
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
        if (!frame) return;
        yield* Queue.offer(ctx.activitySignals, undefined);
        yield* offerRuntimeEvent({
          type: "content.delta",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          ...(ctx.activeTurnId ? { turnId: ctx.activeTurnId } : {}),
          payload: { streamKind: "command_output", delta: frame.text },
        });
      });

    const handleClose = (ctx: AgentRelaySessionContext, code: number, reason: string) =>
      Effect.gen(function* () {
        const liveCtx = sessions.get(ctx.threadId);
        if (liveCtx !== ctx || ctx.stopped) return;
        ctx.socket = undefined;
        // 1000 is a normal close either side can initiate — `stopSession`
        // closes with this code, so treat it as a deliberate disconnect
        // rather than something to reconnect from.
        if (code === 1000) {
          yield* stopSessionInternal(ctx);
          return;
        }
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
      let socket: WebSocket;
      try {
        socket = apiKey
          ? new WebSocket(agentRelaySettings.brokerUrl, {
              headers: { authorization: `Bearer ${apiKey}` },
            })
          : new WebSocket(agentRelaySettings.brokerUrl);
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
          createdAt: now,
          updatedAt: now,
        };
        const ctx: AgentRelaySessionContext = {
          threadId: input.threadId,
          session,
          scope,
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
      });

    const sendTurn: AgentRelayAdapterShape["sendTurn"] = (input) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(input.threadId);
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
        if (!sendFrame(ctx, encodeSendInputFrame(`${text}\n`))) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "sendInput",
            detail: "Failed to send input to the Agent Relay broker socket.",
          });
        }

        const isNewTurn = ctx.activeTurnId === undefined;
        const turnId = ctx.activeTurnId ?? TurnId.make(yield* randomUUIDv4);
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
        sendFrame(ctx, encodeSendInputFrame("\u0003"));
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
    } satisfies AgentRelayAdapterShape;
  });
}
