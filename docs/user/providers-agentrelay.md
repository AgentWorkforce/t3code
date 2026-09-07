# Agent Relay

Agent Relay is a different kind of provider: instead of installing and logging in to
a CLI on the environment's machine, T3 Code attaches to agents that Agent Relay is
already running elsewhere. It streams an agent's terminal into the thread and sends
what you type back as input, the same way Agent Relay's own terminal clients attach.

There are two modes.

## Workspace mode (recommended)

Point T3 Code at an Agent Relay workspace once, and every agent already running
there — however it was spawned (Agent Relay's CLI, its MCP tools, a fleet trigger,
or an earlier T3 Code thread) — shows up as a thread automatically, not just the
ones you started from T3 Code. Starting a **new** thread on this instance spawns
a fresh agent through Agent Relay instead of requiring one to already exist.

In **Settings → Providers**, add an Agent Relay instance, set **Mode** to
**Workspace**, and enter:

- **Workspace key** — the Relaycast workspace key (`rk_live_...`) from the Agent
  Relay CLI (`agent-relay workspace` or wherever you provisioned it).
- **Broker URL** — the base URL of the `agent-relay-broker` process itself, e.g.
  `https://broker.example.com` (no `/ws` or other path — T3 Code derives both the
  output stream and the input endpoint from this one URL).
- **API key** — the broker's own attach credential (separate from the workspace
  key — see "Two different credentials" below).
- **Spawn CLI** — which CLI (Claude Code, Codex, Gemini, ...) Agent Relay launches
  for a brand-new thread.

A broker can run more than one agent at once, and T3 Code tells them apart by
name: whichever agent Workspace mode resolves for a thread (spawned fresh or
resumed from a prior session) is the only one that thread's output and input
apply to, even though the underlying connection carries every agent on that
broker.

Starting a thread that already has an agent bound to it (including one it spawned
itself in a previous session) reconnects to that same agent rather than spawning
another.

New agents in the workspace appear as threads within about 30 seconds of coming
online, each named after the agent and pre-attached — open one and it connects
immediately, the same as any other thread. If an agent stops running, its
thread settles automatically after a couple of minutes rather than sitting
there looking live forever; sending it a new message brings it back if the
agent returns.

### Two different credentials

The workspace key and the broker URL/API key are not interchangeable, and Agent
Relay does not currently derive one from the other. The workspace key lists and
spawns agents through Agent Relay's hosted Relaycast service; the broker URL and
API key are a separate credential for the `agent-relay-broker` process's own
attach API. You need both configured for Workspace mode to actually connect once
an agent is found or spawned. See `docs/internals/providers.md` for why.

## Single agent mode (legacy)

Attach to exactly one already-running agent with no discovery: set **Mode** to
**Single agent**, and enter the **Broker URL**, **API key**, and **Agent name**
for that one agent, as printed by the Agent Relay CLI. The agent name is
required here — the broker's connection carries every agent running on it, and
without a name T3 Code has no way to tell them apart. Nothing is spawned and
nothing else in the workspace is visible from this instance. Use this when you
only ever want T3 Code to see one specific agent.

## Credentials are Agent Relay's, not T3 Code's

T3 Code does not manage sign-in for the agents Agent Relay runs — no CLI login, no
OAuth flow, no stored account. Whatever provider the underlying agent itself uses
(Claude, Codex, or otherwise) is authenticated on Agent Relay's side, not from
T3 Code.

## What you see

Because this is a raw terminal stream, Agent Relay threads look different from other
providers: there is no plan view, no tool-call cards, and no approval prompts — just
the agent's terminal output as it happens, and a text box that sends what you type
followed by Enter. Turn completion is a best-effort guess based on the terminal going
quiet, not a signal from the agent, so an agent that pauses mid-task can briefly show
as done before more output arrives.

## Reconnecting

If the connection to the broker drops, T3 Code retries with increasing delays and
shows the thread as disconnected in the meantime. Stopping the thread's session
closes the connection; starting it again (or sending a new message) reconnects.
Because multiple clients can attach to the same broker session, you can keep an
external Agent Relay terminal open on the same agent while a T3 Code thread is
attached to it.
