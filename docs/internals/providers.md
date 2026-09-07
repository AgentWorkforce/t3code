# Provider constraints

Orchestration records intent and state without knowing which provider runs a thread. Provider
protocols, account ownership, permissions, and capabilities belong at the
[adapter boundary](../../apps/server/src/provider/Services/ProviderAdapter.ts). Normalize there
instead of spreading provider checks through reactors and clients.

A driver kind identifies an integration; an instance identifies one configuration and account
lifecycle. Route work by instance, so two accounts using the same driver do not share mutable
session or catalog state.

## Process and account isolation

T3-managed OpenCode chat uses one server per thread. Its MCP registrations are directory-scoped, while
T3's MCP connection is thread-scoped. Sharing a chat server between threads in one directory would
let them replace each other's connection. Catalog and text-generation work can share the
[instance-owned helper](../../apps/server/src/provider/OpenCodeServerOwner.ts), which closes
after an idle period. External OpenCode servers remain externally owned and can require an
external restart to pick up configuration changes.

OpenCode also stores persistent approval grants per directory. Automatic full-access replies use
`once` so they cannot widen a supervised thread's permissions on a shared external server.
See the [adapter](../../apps/server/src/provider/Layers/OpenCodeAdapter.ts).

Antigravity separates account profiles per instance while sharing installed executables across the
environment. It forces file-based credential storage because the native macOS keychain entry would
otherwise be shared across instances. The launch environment removes ambient Google credentials,
so an instance cannot silently use another account or billing project. The agent also resolves
its user-global skill directories under that profile, so the profile links those two directories
back to the user's real `~/.gemini`; MCP servers, hooks, and rules there stay out of the profile.
See [profile isolation](../../apps/server/src/provider/antigravityAuthSupport.ts).

The [Antigravity installer](../../apps/server/src/provider/AntigravityInstallation.ts) outlives
client connections and provider-instance rebuilds. Releases are immutable, with an atomic pointer
selecting the version for new processes. Running processes hold leases on their version. Updates
and removal must respect those leases instead of replacing executables under a running agent.

## Agent Relay attaches instead of spawning

Every other adapter owns a local subprocess: `startSession` spawns it, `stopSession`
kills it, and the adapter is the sole client of its stdio. [Agent Relay's
adapter](../../apps/server/src/provider/Layers/AgentRelayAdapter.ts) does neither —
it opens an outbound WebSocket to a broker process this server does not manage, and
attaches to an agent Agent Relay is already running. That agent's lifecycle is
independent of the thread: stopping the T3 Code session closes this client's socket,
not the agent, and other clients (including Agent Relay's own terminal UI) can be
attached to the same broker session concurrently. Assumptions elsewhere in this
directory that the adapter is the only thing writing to the process (e.g. approval
gating) do not hold here — a message another attached client typed can appear as
input this adapter never sent.

v1 speaks only the broker's terminal transport (`worker_stream` frames in,
`sendInput` frames out), so there are no structured turn, tool-call, or approval
events — turn completion is inferred from the terminal going quiet
(`TURN_IDLE_COMPLETE_MS`), not reported by the agent. Agent Relay's structured
`AgentEventEnvelope` protocol (`@agent-relay/harness-driver`) would remove that
heuristic and add real approvals, but only two Agent Relay harnesses speak it
natively today; adopting it is a distinct v2 adapter, not a v1 extension.

### Workspace mode: two credential domains that do not bridge today

`AgentRelaySettings.mode` adds a second shape (`"workspace"`) alongside the
original single-agent mode, following `AntigravitySettings.authMethod`'s
flat-struct-with-a-selector pattern rather than a schema union. In workspace mode,
[`AgentRelayAdapter.startSession`](../../apps/server/src/provider/Layers/AgentRelayAdapter.ts)
spawns an agent for a thread that has none yet (via `AgentRelayWorkspaceClient`,
[`Live`](../../apps/server/src/provider/Layers/AgentRelayWorkspaceClientLive.ts)),
waits for it to report online, and persists the resolved agent name as the thread's
`ProviderSession.resumeCursor` — the same mechanism `CodexSessionRuntime.ts` uses to
persist a rollout id — so reconnects and restarts attach to the same agent instead
of spawning a new one every time.

This was built against a concrete finding from reading Agent Relay's own source
(`agent-relay-mcp.ts`, `local-agent.ts`, `harness-driver/src/transport.ts`,
`sdk/src/agent-relay.ts`): **there is no existing, non-interactive way to derive a
write-capable broker attach credential from a workspace key.** They are two
separate systems:

- `list_agents`/`add_agent` (and the `AgentRelay` SDK facade behind them) talk to
  the hosted **Relaycast** workspace — a messaging/identity control plane, scoped
  by a `rk_live_...` workspace key. Spawning specifically goes through the raw
  pass-through thin client (`createWorkspaceClient` in
  `@agent-relay/sdk/messaging`, the same one `agent-relay-mcp.ts`'s `getRelay()`
  returns) — the nicer typed `AgentRelay#agents` facade omits `spawn` entirely.
  Neither surface's agent/node records (`RelayAgent`, `RelayNode`) carry a broker
  URL or API key.
- The actual PTY attach (`HarnessDriverClient`/`BrokerTransport` in
  `@agent-relay/harness-driver`) is a *different* credential: the local
  `agent-relay-broker` process's own `X-API-Key`-authenticated HTTP/WS API
  (`/ws`, `/api/input/{name}/stream`), resolved by the CLI's `attach` command from
  `--broker-url`/`--api-key`, `RELAY_BROKER_URL`/`RELAY_BROKER_API_KEY`, or
  `connection.json` — never from a workspace key. (Relay's own tracked gap here is
  issue #1382, "attach pairs broker URL/key from different sources".)

So workspace mode still asks for both a workspace key (discovery/spawn) *and* a
broker URL/API key (attach) — it cannot derive one from the other. If Agent Relay
ever adds a workspace-key-derivable attach credential (a `brokerUrl`/`apiKey` on
`RelayAgent`/`RelayNode`, or a new MCP tool), that field is exactly what should
replace the second credential here.

A second, narrower gap: the real per-agent attach protocol
(`harness-driver/src/transport.ts`) is a **two-channel**, ack/keepalive-based
protocol (`GET/PUT .../delivery-mode`, a shared `/ws?sinceSeq=` event stream
multiplexed by agent `name`, and a separate `/api/input/{name}/stream` for
writes with `pty_input_ready`/`pty_input_ack` flow control) — nothing like the v1
adapter's single bidirectional `worker_stream`/`sendInput` socket. Reproducing
that protocol is out of scope here for the same reason the v1 wire-format gap
above is: it is a distinct v2 adapter. Workspace mode's `brokerUrl` setting is
therefore a T3-Code-side convention layered on the *existing* v1 guess — a
`{name}` placeholder (or a `?agent=<name>` fallback) substituted by
`buildWorkspaceAttachUrl` — not a real Agent Relay contract. Whoever builds the v2
adapter should replace both the frame format and this URL convention together
against `harness-driver`'s real transport.

Presence (used to detect a freshly-spawned agent coming online, and to notice one
going offline) uses `AgentRelay#addListener("agent.status.*", ...)` — the one
wildcard-typed selector confirmed in `packages/sdk/src/listeners.ts`. This could
not be verified end-to-end against a live workspace, so `waitForAgentOnline`
races it against polling `listAgents()` every two seconds (bounded by
`AGENT_SPAWN_WAIT_TIMEOUT`, 90s); polling alone is sufficient for correctness.

### Left out: auto-materializing threads for already-running agents

`AgentRelayWorkspaceClient` can already list every agent in a workspace and watch
presence, which is the primitive auto-discovery (surfacing an agent nobody started
from T3 Code as a thread) needs. What it does not do is turn that into a thread:
`thread.create` (`apps/server/src/orchestration/decider.ts`) requires a
`projectId`, and every existing thread-creation path is a deliberate user action.
Silently materializing a thread per discovered agent needs a product decision this
change does not make on its own — at minimum, which project houses them and
whether every agent in a workspace should really become a thread unasked. A
follow-up background reactor (shaped like
`apps/server/src/provider/Layers/ProviderSessionReaper.ts`) is the right place to
wire `AgentRelayWorkspaceClient.listAgents`/`onPresenceChange` into `thread.create`
dispatch once that's decided.

## Setup must not happen as a health-check side effect

Opening a provider session can start MCP servers, run hooks, or launch a login browser.
[Grok probes](../../apps/server/src/provider/Layers/GrokProvider.ts) avoid authentication and
session creation for this reason. Antigravity likewise reserves authenticated catalog sessions for
explicit setup or model refresh; background checks use initialization only.

[Antigravity sign-in](../../apps/server/src/provider/AntigravityAuth.ts) belongs to the initiating
T3 auth session. The client carries the return URL back to the environment because the provider's
loopback listener may be on another machine. Forward only the callback for the owned pending flow;
a successful callback HTTP request is not proof that provider authentication finished. The native
process owns token exchange and storage.

Antigravity sign-out closes admission to new processes and stops existing processes before clearing account
metadata. Otherwise a helper or resumed session could retain the old account. Cached model lists
do not establish current access, and an authoritative empty catalog must clear the old list.

Antigravity text-generation helpers deny tool requests, but native hooks and MCP configuration can
run before the prompt. They reject profiles with such configuration before launch. Prompt
instructions and tool denial do not create a native sandbox.
See [helper constraints](../../apps/server/src/textGeneration/AntigravityTextGeneration.ts).

## Provider updates run only through the owning installer

A one-click update is offered only when the resolved executable's path proves which installer owns
it. Homebrew and npm are proven by the real path (symlinks followed): a versioned keg or cask under
`brew --prefix`, or `<prefix>/lib/node_modules/<pkg>/` (Windows: the shim beside `node_modules`).
Native installer layouts and the global bin directories of pnpm, Bun, and Vite+ may match on either
the resolved path or its real target, since those installers place real files or their own symlinks
there. Anything unproven stays manual-only but still reports the version gap. npm updates pin
`--prefix` because the `npm` on `PATH` can belong to a different Node than the one that owns the
provider. Homebrew
compares against `brew info` since casks trail npm by hours; native installs share npm's version
train, so the registry stays authoritative for them.
See the [resolver](../../apps/server/src/provider/providerMaintenance.ts).

Ownership is cached per instance and re-read immediately before an update runs. The
[runner](../../apps/server/src/provider/providerMaintenanceRunner.ts) refuses when the lock key
changed since the advisory, and reports success only when the refreshed provider is still installed
with a readable, current version.

## Protocol traps

Codex async questions arrive as notifications and are answered with a new user message. There is
no pending RPC response to send. Blocking questions still use the request/response path. The
[adapter](../../apps/server/src/provider/Layers/CodexAdapter.ts) distinguishes them; the
[decider](../../apps/server/src/orchestration/decider.ts) records an async answer and its user
message together.

An async question can outlive the turn or a server restart. The engine reads that request's
durable activity before resolving it because the in-memory command snapshot omits old activities.
Do not infer that a request has disappeared merely because it is outside the recent window.

Capabilities must describe what the provider can actually do. Antigravity can capture workspace
checkpoints but cannot roll back its conversation. The [checkpoint boundary](./overview.md#turn-completion-and-checkpoints)
therefore rejects revert before touching files. Native permission and question option IDs must
also survive normalization; a display label is not necessarily a valid reply.

## Attachments and stored history

Attachments live outside the project workspace. [ProviderService](../../apps/server/src/provider/Layers/ProviderService.ts)
puts their environment-local paths in turn input and lets adapters choose native input formats.
A path in the prompt does not grant filesystem access. Keep provider sandbox and approval rules
in force; copying uploads into the project to bypass them changes that boundary.

File attachments introduced a replay compatibility limit. Image-only clients cannot decode
file-bearing messages, and an image-only server can fail the entire environment's startup when
replaying one such event. Rollouts and downgrades must account for persisted history as well as
current client support.

Model classification has its own [manifest constraints](./model-manifest.md). Assistant-reference
handling is documented under [citations](./assistant-citations.md).
