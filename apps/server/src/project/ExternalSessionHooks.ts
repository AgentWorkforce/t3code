/**
 * ExternalSessionHooks - visibility for Claude Code / Codex sessions started
 * outside T3 Code entirely.
 *
 * Agent Relay is the primary way sessions get discovered going forward. This
 * module is the deliberately small fallback for the case Agent Relay does not
 * cover: a bare `claude` or `codex` invocation in a raw terminal, launched
 * without T3 Code and without Agent Relay. Both CLIs support global lifecycle
 * hooks (Claude Code's `SessionStart`/`SessionEnd` in `~/.claude/settings.json`,
 * Codex's hooks in `~/.codex/config.toml`) that fire regardless of what
 * launched them and can `curl` this endpoint. See `docs/user/external-sessions.md`
 * for the exact hook configuration.
 *
 * T3 Code never owns the reported process: there is no PTY, no provider
 * session binding, and nothing to attach or resume. The only artifact this
 * module produces is a settled thread carrying two informational activity
 * entries ("started" / "ended") in a project that already exists for the
 * reported `cwd`. If no project matches, the event is dropped — this module
 * intentionally does not create projects for directories the user has never
 * opened in T3 Code.
 *
 * @module ExternalSessionHooks
 */
import {
  CommandId,
  DEFAULT_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  EventId,
  IsoDateTime,
  PositiveInt,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";

import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { collectUint8StreamText } from "../stream/collectUint8StreamText.ts";

export const EXTERNAL_SESSIONS_ROUTE_PATH = "/api/external-sessions";

/**
 * Every marker thread this module creates uses this id prefix (see
 * {@link externalSessionThreadId}) and nothing else in the codebase mints
 * thread ids shaped this way. That makes the prefix a durable, already
 * persisted signal for "this thread is a read-only external-session marker"
 * that other server code can check without a new projected field — see
 * {@link isExternalSessionMarkerThreadId} and its use in
 * `ProviderCommandReactor.ts` to refuse starting a live provider session on
 * one of these threads.
 */
const EXTERNAL_SESSION_THREAD_ID_PREFIX = "external:";

/** Generous but bounded: real hook payloads are a path and a session id. */
const MAX_EXTERNAL_SESSION_HOOK_STRING_LENGTH = 4_096;

/** Comfortably above two max-length strings plus JSON structure overhead. */
const MAX_EXTERNAL_SESSION_HOOK_BODY_BYTES = 16 * 1_024;

/** Tolerates ordinary clock skew between the hook's host and this server. */
const EXTERNAL_SESSION_HOOK_TIMESTAMP_FUTURE_GRACE_MS = 5 * 60 * 1_000;

/** Absolute floor used to reject garbage timestamps; needs no "now" reference. */
const EXTERNAL_SESSION_HOOK_TIMESTAMP_MIN_MS = Date.parse("2000-01-01T00:00:00.000Z");

export const ExternalSessionHookProvider = Schema.Literals(["claude", "codex"]);
export type ExternalSessionHookProvider = typeof ExternalSessionHookProvider.Type;

export const ExternalSessionHookEventKind = Schema.Literals(["start", "end"]);
export type ExternalSessionHookEventKind = typeof ExternalSessionHookEventKind.Type;

/** A non-empty string, bounded so a hostile caller cannot smuggle megabytes into `cwd`/`sessionId`. */
const BoundedHookString = TrimmedNonEmptyString.check(
  Schema.isMaxLength(MAX_EXTERNAL_SESSION_HOOK_STRING_LENGTH),
);

/**
 * An ISO date-time string, structurally sane enough to persist as `createdAt`
 * on the marker thread and its activities (see
 * {@link recordExternalSessionHookEvent}) without skewing thread ordering —
 * reject garbage instead of silently clamping it. This only checks what a
 * pure schema predicate can check without reading the clock (parseable, not
 * absurdly old); the "not too far in the future" bound needs real "now" and
 * is enforced separately in {@link externalSessionHooksRouteLayer} via
 * Effect's `Clock`.
 */
const PlausibleIsoDateTime = IsoDateTime.check(
  Schema.makeFilter((value: string) => {
    const parsedMs = Date.parse(value);
    if (Number.isNaN(parsedMs)) {
      return "must be a valid ISO 8601 date-time string";
    }
    if (parsedMs < EXTERNAL_SESSION_HOOK_TIMESTAMP_MIN_MS) {
      return "is implausibly far in the past";
    }
    return true;
  }),
);

/** Body a lifecycle hook posts to {@link EXTERNAL_SESSIONS_ROUTE_PATH}. */
export const ExternalSessionHookPayload = Schema.Struct({
  provider: ExternalSessionHookProvider,
  pid: PositiveInt,
  cwd: BoundedHookString,
  sessionId: BoundedHookString,
  event: ExternalSessionHookEventKind,
  timestamp: PlausibleIsoDateTime,
});
export type ExternalSessionHookPayload = typeof ExternalSessionHookPayload.Type;

export interface ExternalSessionHookResult {
  readonly recorded: boolean;
  readonly threadId?: string;
  readonly reason?: "project-not-found" | "already-recorded" | "unknown-session";
}

const PROVIDER_LABEL: Record<ExternalSessionHookProvider, string> = {
  claude: "Claude Code",
  codex: "Codex",
};

// Threads/sessions reference provider *instance* ids, never driver kinds
// directly, but an unconfigured built-in provider's instance id defaults to
// its driver kind (see ClaudeAdapter's `claudeAgent` fallback and Codex's
// `codex` default instance). A marker thread referencing an instance the user
// later renames or removes is rendered "unavailable" by the provider layer,
// the same fallback real threads get — it is never a crash.
function providerInstanceId(provider: ExternalSessionHookProvider): ProviderInstanceId {
  return ProviderInstanceId.make(provider === "claude" ? "claudeAgent" : "codex");
}

function defaultModel(provider: ExternalSessionHookProvider): string {
  const driverKind = ProviderDriverKind.make(provider === "claude" ? "claudeAgent" : "codex");
  return DEFAULT_MODEL_BY_PROVIDER[driverKind] ?? DEFAULT_MODEL;
}

function externalSessionThreadId(input: {
  readonly provider: ExternalSessionHookProvider;
  readonly sessionId: string;
}): ThreadId {
  return ThreadId.make(`${EXTERNAL_SESSION_THREAD_ID_PREFIX}${input.provider}:${input.sessionId}`);
}

/**
 * True for a thread id this module minted (see {@link externalSessionThreadId}).
 * These threads never had a real provider session behind them, so nothing
 * should ever start one now — used by `ProviderCommandReactor.ts` to refuse
 * turn starts against a read-only external-session marker thread.
 */
export function isExternalSessionMarkerThreadId(threadId: string): boolean {
  return threadId.startsWith(EXTERNAL_SESSION_THREAD_ID_PREFIX);
}

/**
 * Record a `start` or `end` lifecycle event reported by a Claude Code /
 * Codex hook running outside T3 Code. Best-effort and idempotent: a repeated
 * `start` for the same session id is a no-op, and an `end` for a session T3
 * never saw a `start` for is a no-op.
 */
export const recordExternalSessionHookEvent = Effect.fn("recordExternalSessionHookEvent")(
  function* (payload: ExternalSessionHookPayload) {
    const engine = yield* OrchestrationEngine.OrchestrationEngineService;
    const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
    const crypto = yield* Crypto.Crypto;
    const threadId = externalSessionThreadId(payload);
    const existingThread = yield* snapshots.getThreadShellById(threadId);

    const appendStartActivity = Effect.all({
      commandId: crypto.randomUUIDv4,
      activityId: crypto.randomUUIDv4,
    }).pipe(
      Effect.flatMap(({ commandId, activityId }) =>
        engine.dispatch({
          type: "thread.activity.append",
          commandId: CommandId.make(commandId),
          threadId,
          activity: {
            id: EventId.make(activityId),
            tone: "info",
            kind: "external-session.started",
            summary: `${PROVIDER_LABEL[payload.provider]} session started outside T3 Code (pid ${payload.pid})`,
            payload: { pid: payload.pid, cwd: payload.cwd, sessionId: payload.sessionId },
            turnId: null,
            createdAt: payload.timestamp,
          },
          createdAt: payload.timestamp,
        }),
      ),
    );

    if (payload.event === "end") {
      if (Option.isNone(existingThread)) {
        return { recorded: false, reason: "unknown-session" } satisfies ExternalSessionHookResult;
      }
      // Idempotent: once the thread is settled its "ended" marker is already
      // recorded. Without this check a retried hook call (network retry,
      // at-least-once delivery) appends another "ended" activity and
      // re-settles the thread on every retry.
      if (existingThread.value.settledAt !== null) {
        return { recorded: false, reason: "already-recorded" } satisfies ExternalSessionHookResult;
      }
      yield* engine.dispatch({
        type: "thread.activity.append",
        commandId: CommandId.make(yield* crypto.randomUUIDv4),
        threadId,
        activity: {
          id: EventId.make(yield* crypto.randomUUIDv4),
          tone: "info",
          kind: "external-session.ended",
          summary: `${PROVIDER_LABEL[payload.provider]} session ended (pid ${payload.pid})`,
          payload: { pid: payload.pid, cwd: payload.cwd, sessionId: payload.sessionId },
          turnId: null,
          createdAt: payload.timestamp,
        },
        createdAt: payload.timestamp,
      });
      // Best-effort: a thread whose session is somehow live, or that was
      // archived/deleted since, must not block recording the "ended" marker.
      yield* engine
        .dispatch({
          type: "thread.settle",
          commandId: CommandId.make(yield* crypto.randomUUIDv4),
          threadId,
        })
        .pipe(
          Effect.catch((cause) =>
            Effect.logWarning("Could not settle an external session's marker thread", {
              threadId,
              cause,
            }),
          ),
        );
      return { recorded: true, threadId } satisfies ExternalSessionHookResult;
    }

    if (Option.isSome(existingThread)) {
      // The thread already exists, but `thread.create` succeeding does not
      // guarantee the follow-up start-activity dispatch also succeeded (a
      // transient failure between the two leaves a thread with no start
      // marker). Check for the actual activity instead of trusting thread
      // existence alone, so a retry of the same start hook completes the
      // missing step rather than being swallowed as "already-recorded".
      const existingDetail = yield* snapshots.getThreadDetailById(threadId, {
        activityKinds: ["external-session.started"],
      });
      const hasStartActivity =
        Option.isSome(existingDetail) &&
        existingDetail.value.activities.some(
          (activity) => activity.kind === "external-session.started",
        );
      if (hasStartActivity) {
        return { recorded: false, reason: "already-recorded" } satisfies ExternalSessionHookResult;
      }
      yield* appendStartActivity;
      return { recorded: true, threadId } satisfies ExternalSessionHookResult;
    }
    const project = yield* snapshots.getActiveProjectByWorkspaceRoot(payload.cwd);
    if (Option.isNone(project)) {
      return { recorded: false, reason: "project-not-found" } satisfies ExternalSessionHookResult;
    }

    yield* engine.dispatch({
      type: "thread.create",
      commandId: CommandId.make(yield* crypto.randomUUIDv4),
      threadId,
      projectId: project.value.id,
      title: `External ${PROVIDER_LABEL[payload.provider]} session`,
      modelSelection: {
        instanceId: providerInstanceId(payload.provider),
        model: defaultModel(payload.provider),
      },
      runtimeMode: DEFAULT_RUNTIME_MODE,
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      branch: null,
      worktreePath: null,
      createdAt: payload.timestamp,
      historyImport: true,
    });
    yield* appendStartActivity;
    return { recorded: true, threadId } satisfies ExternalSessionHookResult;
  },
);

const decodeExternalSessionHookPayload = Schema.decodeUnknownEffect(ExternalSessionHookPayload);

/**
 * True when the raw remote address that opened this connection is the local
 * machine. Compared against the socket's own `remoteAddress`, never a
 * client-suppliable header, so this holds regardless of what host this
 * server is bound to (LAN, Tailscale, T3 Connect) — a hook config always
 * targets `127.0.0.1` (see `docs/user/external-sessions.md`), so a request
 * arriving from anywhere else cannot be a legitimate lifecycle hook.
 */
export function isLoopbackRemoteAddress(remoteAddress: string): boolean {
  const normalized = remoteAddress.startsWith("::ffff:")
    ? remoteAddress.slice("::ffff:".length)
    : remoteAddress;
  return normalized === "127.0.0.1" || normalized === "::1" || normalized.startsWith("127.");
}

/**
 * `POST /api/external-sessions` — see module docs. Restricted to genuinely
 * loopback callers (see {@link isLoopbackRemoteAddress}): this route has no
 * other authentication, and once the server is reachable over the network
 * (LAN, Tailscale, T3 Connect) an unauthenticated write endpoint would let
 * any network client fabricate unlimited marker threads.
 */
export const externalSessionHooksRouteLayer = HttpRouter.add(
  "POST",
  EXTERNAL_SESSIONS_ROUTE_PATH,
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;

    const remoteAddress = Option.getOrUndefined(request.remoteAddress);
    if (remoteAddress === undefined || !isLoopbackRemoteAddress(remoteAddress)) {
      return HttpServerResponse.text(
        "External session hooks are only accepted from the local machine.",
        { status: 403 },
      );
    }

    // Reject an oversized declared length before reading anything.
    const contentLengthHeader = request.headers["content-length"];
    if (contentLengthHeader !== undefined) {
      const declaredBytes = Number(contentLengthHeader);
      if (!Number.isFinite(declaredBytes) || declaredBytes > MAX_EXTERNAL_SESSION_HOOK_BODY_BYTES) {
        return HttpServerResponse.text("External session hook payload is too large.", {
          status: 413,
        });
      }
    }

    // Cap the actual bytes read too: Content-Length can be absent or wrong
    // (chunked transfer, a lying client), so the declared-length check alone
    // is not a real bound.
    const collected = yield* collectUint8StreamText({
      stream: request.stream,
      maxBytes: MAX_EXTERNAL_SESSION_HOOK_BODY_BYTES,
    }).pipe(Effect.orElseSucceed(() => null));
    if (collected === null || collected.truncated) {
      return HttpServerResponse.text("External session hook payload is too large.", {
        status: 413,
      });
    }

    const bodyJson = yield* Effect.try(() => {
      if (collected.text.length === 0) {
        return null;
      }
      // Raw parse ahead of `decodeExternalSessionHookPayload` below, which is
      // the actual schema validation; this only needs an `unknown` value to
      // hand it, and the body is already byte-capped above.
      // @effect-diagnostics-next-line preferSchemaOverJson:off
      return JSON.parse(collected.text) as unknown;
    }).pipe(Effect.orElseSucceed(() => null));
    const payload =
      bodyJson === null
        ? null
        : yield* decodeExternalSessionHookPayload(bodyJson).pipe(Effect.orElseSucceed(() => null));
    if (payload === null) {
      return HttpServerResponse.text("Invalid external session hook payload.", { status: 400 });
    }

    // The schema only checks that `timestamp` parses and isn't absurdly old
    // (see PlausibleIsoDateTime); the future bound needs real "now".
    const nowMs = yield* Clock.currentTimeMillis;
    if (Date.parse(payload.timestamp) - nowMs > EXTERNAL_SESSION_HOOK_TIMESTAMP_FUTURE_GRACE_MS) {
      return HttpServerResponse.text("External session hook timestamp is too far in the future.", {
        status: 400,
      });
    }

    const result = yield* recordExternalSessionHookEvent(payload).pipe(
      Effect.catch((cause) =>
        Effect.logWarning("Failed to record an external session hook event", { cause }).pipe(
          Effect.as({ recorded: false, reason: undefined } as const),
        ),
      ),
    );
    return HttpServerResponse.jsonUnsafe(result, { status: 200 });
  }),
);
