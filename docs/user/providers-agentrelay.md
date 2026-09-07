# Agent Relay

Agent Relay is a different kind of provider: instead of installing and logging in to
a CLI on the environment's machine, T3 Code attaches to an agent that Agent Relay is
already running elsewhere. It streams that agent's terminal into the thread and sends
what you type back as input, the same way Agent Relay's own terminal clients attach.

## How to start

Get a broker URL and API key from the Agent Relay CLI for the agent you want to
attach to. In **Settings → Providers**, add an Agent Relay instance and enter:

- **Broker URL** — the WebSocket URL for the Agent Relay broker, for example
  `wss://broker.example.com/ws`.
- **API key** — the attach token for that broker session.

Enable the instance and start a thread on it. T3 Code connects immediately; there is
nothing to install.

## Credentials are Agent Relay's, not T3 Code's

T3 Code does not manage sign-in for the agent Agent Relay is running — no CLI login,
no OAuth flow, no stored account. The broker URL and API key are the only
credentials this provider needs, and they only grant access to attach to that one
already-running agent. Whatever provider the underlying agent itself uses (Claude,
Codex, or otherwise) is authenticated on Agent Relay's side, not from T3 Code.

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
