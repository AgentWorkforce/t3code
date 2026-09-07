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
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";

import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";

export const EXTERNAL_SESSIONS_ROUTE_PATH = "/api/external-sessions";

export const ExternalSessionHookProvider = Schema.Literals(["claude", "codex"]);
export type ExternalSessionHookProvider = typeof ExternalSessionHookProvider.Type;

export const ExternalSessionHookEventKind = Schema.Literals(["start", "end"]);
export type ExternalSessionHookEventKind = typeof ExternalSessionHookEventKind.Type;

/** Body a lifecycle hook posts to {@link EXTERNAL_SESSIONS_ROUTE_PATH}. */
export const ExternalSessionHookPayload = Schema.Struct({
  provider: ExternalSessionHookProvider,
  pid: PositiveInt,
  cwd: TrimmedNonEmptyString,
  sessionId: TrimmedNonEmptyString,
  event: ExternalSessionHookEventKind,
  timestamp: IsoDateTime,
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
  return ThreadId.make(`external:${input.provider}:${input.sessionId}`);
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

    if (payload.event === "end") {
      if (Option.isNone(existingThread)) {
        return { recorded: false, reason: "unknown-session" } satisfies ExternalSessionHookResult;
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
      return { recorded: false, reason: "already-recorded" } satisfies ExternalSessionHookResult;
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
    yield* engine.dispatch({
      type: "thread.activity.append",
      commandId: CommandId.make(yield* crypto.randomUUIDv4),
      threadId,
      activity: {
        id: EventId.make(yield* crypto.randomUUIDv4),
        tone: "info",
        kind: "external-session.started",
        summary: `${PROVIDER_LABEL[payload.provider]} session started outside T3 Code (pid ${payload.pid})`,
        payload: { pid: payload.pid, cwd: payload.cwd, sessionId: payload.sessionId },
        turnId: null,
        createdAt: payload.timestamp,
      },
      createdAt: payload.timestamp,
    });
    return { recorded: true, threadId } satisfies ExternalSessionHookResult;
  },
);

const decodeExternalSessionHookPayload = Schema.decodeUnknownEffect(ExternalSessionHookPayload);

/**
 * `POST /api/external-sessions` — see module docs. Unauthenticated by
 * design, same as this server's other loopback-oriented local tooling
 * surfaces: the payload only ever produces a read-only informational marker,
 * never code execution or a live session, so the worst a forged POST can do
 * is add a fake marker thread.
 */
export const externalSessionHooksRouteLayer = HttpRouter.add(
  "POST",
  EXTERNAL_SESSIONS_ROUTE_PATH,
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const bodyJson = yield* request.json.pipe(Effect.orElseSucceed(() => null));
    const payload =
      bodyJson === null
        ? null
        : yield* decodeExternalSessionHookPayload(bodyJson).pipe(
            Effect.orElseSucceed(() => null),
          );
    if (payload === null) {
      return HttpServerResponse.text("Invalid external session hook payload.", { status: 400 });
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
