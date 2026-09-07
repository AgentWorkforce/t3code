# External sessions

Agent Relay is the primary way T3 Code discovers and controls sessions. This feature is a small
safety net for the case Agent Relay does not cover: a `claude` or `codex` you start directly in a
plain terminal, bypassing T3 Code and Agent Relay entirely. Without it, a session like that leaves
no trace anywhere in T3 Code. With it, T3 Code shows a marker so you at least know the session
existed — a read-only note, not a controllable thread. You do not need this for anything already
visible through T3 Code or Agent Relay.

## What you get

Once configured, starting or ending a bare `claude`/`codex` session on the same machine adds a
thread showing the working directory, the process id, and the time it started (and, once it ends,
that it ended). You cannot resume, attach to, or send messages to the original process from that
thread — T3 Code never ran it and has nothing to reconnect to. The thread only appears in a project
already open in T3 Code for that working directory; a directory you have never opened in T3 Code
gets no marker.

## Set up the Claude Code hook

Add a `SessionStart` and `SessionEnd` hook to your global `~/.claude/settings.json` (not a
per-project `.claude/settings.json` — this needs to fire for every `claude` invocation on the
machine, not just ones inside a T3 Code project). Requires `jq` and `curl`. Replace `3773` if your
T3 Code server runs on a different port:

```json
{
  "hooks": {
    "SessionStart": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "jq --arg pid \"$PPID\" --arg ts \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\" '{provider:\"claude\", pid: ($pid|tonumber), cwd: .cwd, sessionId: .session_id, event:\"start\", timestamp: $ts}' | curl -fsS --max-time 1 -X POST http://127.0.0.1:3773/api/external-sessions -H 'Content-Type: application/json' -d @- >/dev/null 2>&1 &"
          }
        ]
      }
    ],
    "SessionEnd": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "jq --arg pid \"$PPID\" --arg ts \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\" '{provider:\"claude\", pid: ($pid|tonumber), cwd: .cwd, sessionId: .session_id, event:\"end\", timestamp: $ts}' | curl -fsS --max-time 1 -X POST http://127.0.0.1:3773/api/external-sessions -H 'Content-Type: application/json' -d @- >/dev/null 2>&1 &"
          }
        ]
      }
    ]
  }
}
```

Merge these into your existing `hooks` object if you already have other `SessionStart`/`SessionEnd`
hooks configured. The trailing `&` and `--max-time 1` keep a slow or unreachable T3 Code server
from delaying Claude Code's startup or shutdown.

## Set up the Codex hook

Codex reads lifecycle hooks from `~/.codex/config.toml`. Add an inline `[hooks]` table (or point
`hooks.json` at equivalent commands, if you prefer keeping them out of `config.toml`):

```toml
[hooks.session_start]
command = ["bash", "-c", "jq --arg pid \"$PPID\" --arg ts \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\" '{provider:\"codex\", pid: ($pid|tonumber), cwd: .cwd, sessionId: .session_id, event:\"start\", timestamp: $ts}' | curl -fsS --max-time 1 -X POST http://127.0.0.1:3773/api/external-sessions -H \"Content-Type: application/json\" -d @- >/dev/null 2>&1 &"]

[hooks.session_end]
command = ["bash", "-c", "jq --arg pid \"$PPID\" --arg ts \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\" '{provider:\"codex\", pid: ($pid|tonumber), cwd: .cwd, sessionId: .session_id, event:\"end\", timestamp: $ts}' | curl -fsS --max-time 1 -X POST http://127.0.0.1:3773/api/external-sessions -H \"Content-Type: application/json\" -d @- >/dev/null 2>&1 &"]
```

Codex's hooks configuration is newer and still evolving. If `session_start`/`session_end` are not
the exact table names your installed Codex version expects, check `codex --help` or that version's
release notes for the current hook event names — the payload T3 Code needs (`cwd`, `session_id`)
stays the same either way.

## Limitations

- Read-only marker only — this is not session import. T3 Code does not scan `~/.claude/projects`
  or `~/.codex/sessions` for past transcripts, and this feature does not make an external session
  resumable or attachable.
- Only covers sessions on the same machine as the T3 Code server the hook posts to.
- Only appears for a working directory that already has a project open in T3 Code.
