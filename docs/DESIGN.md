# mcpwarp Go CLI/TUI — design & scoping

Status: decided (§13). Date: 2026-09-05.

Sources: `research-sdk.md`, `research-nodecli.md`, `research-stack.md`, plus direct reads of `mcpwarp-cli` (parity source of truth), `ws-mixer-go` (SDK consumed), `ws-mixer-spec` (wire/SDK contract). Paths relative to `/Users/anatoly/Developer/projects/mcpwarp/`.

## 1. Goal & non-goals

**Goal:** a single static Go binary, `mcpwarp`, reaching behavioural parity with the Node CLI (`mcpwarp-cli`, npm `mcpwarp`) — same commands, config/credential files, exit codes, wire behaviour — plus a TUI as the default `up` experience. Ships via goreleaser to brew/scoop/deb/rpm, replacing Node once parity is verified. **Parity gate**: Node's existing e2e suites (conformance-adapter driven) run against the Go binary (§10).

**Non-goals for v1:** no new server-side operations (protocol fixed, Go CLI is a client); no redesign of config schema, credential format, or app-protocol envelope — port behaviour, deviations called out (§2, §5); no feature Node lacks except the static personal-access-token mode via `MCPWARP_TOKEN` (§5) and the additive TUI; no `--json`, dropped from v1 (§2, §9); no `.cmd`/`.bat` shim support via `cmd.exe /c`, deferred (§7); not OS keychain for v1 (§5); not a rewrite of `mcpwarp-saas`; not a Rust rewrite — evaluated and rejected by the owner (§13).

## 2. Command surface

1:1 mapping, decided (§13):

| Node (`mcpwarp-cli`) | Go | Behaviour change |
|---|---|---|
| `mcpwarp login [--no-browser]` | same | Same device-flow UX; `login` stays a plain/huh prompt (§9) |
| `mcpwarp logout` | same | Best-effort revoke (5s budget) then unconditional creds delete |
| `mcpwarp up` | `+ [--no-tui]` | TUI default when stdin and stdout are both TTYs; `--no-tui` forces plain output; non-TTY auto-degrades (§9) |
| `mcpwarp status` | same | Same content, no network call |
| `mcpwarp whoami [--refresh]` | same | Same |
| Global: `--version`, `--verbose`, `--config`, `--issuer`, `--connect-url` | same, same precedence | `--issuer` selects a distinct credentials file via `sha256(issuer)[:16]` |
| Exit codes 0/1/2, plus 130/143 on SIGINT/SIGTERM | Same 0/1/2; 128+signal on shutdown (`shutdown.ts:6,110-111`) | No change |
| stdout=product, stderr=logs, ✓/!/✗, NO_COLOR | same; `slog` replaces pino | TUI owns the screen for `up`; logs go to file (§9), visible via `l` or `--no-tui`; non-TTY emits plain lines plus JSON log lines on stderr |
| (none) | `+ mcpwarp dashboard` | Go-only addition, decided 2026-09-12: prints the dashboard URL from `MCPWARP_WEB_URL` (or its default), then opens the browser; no Node counterpart |

`--json` and `--token` are both dropped from v1 (§5, §13) — no command removed; `dashboard` is the one addition.

Also Go-only, like `dashboard`: an update notice (`internal/update`), gh-CLI-style — checks GitHub releases at most once per 24h and prints a two-line notice on stderr when a newer tag exists, never self-replaces.

## 3. Architecture

Package layout (Go modules, module path `github.com/mcpwarp/cli`, decided — §13):

```
cmd/mcpwarp/            main() is a dozen lines: cli.Execute + last-resort KillAllLiveChildren/os.Exit
internal/cli/           cobra wiring, exit-code mapping, signal relay (Execute/runRoot)
internal/shutdown/      the bounded signal/fatal-exit handler registry (Register/Run/FatalExit)
internal/config/        load/validate config.json, same schema as Node
internal/auth/          device flow + PKCE, credential store, refresh lock, token provider seam
internal/tunnel/        wraps ws-mixer-go Dial/Conn; register/unregister; reconnect via ws-mixer-go v0.11.1's Client (§4)
internal/registry/      name/id/url table from `registered`; resolveByHost (Node: tunnel/registry.ts)
internal/relay/         raw-HTTP-over-stream relay to bridge's listener; never throws (Node: http/*, forward/*)
internal/bridge/        stdio child + local HTTP server per config entry; NDJSON framing; JSON-RPC routing
internal/supervisor/    child lifecycle: spawn, backoff, restart, disable/enable, replaceChild
internal/appproto/      the {"mcpwarp":{"v":1,"op":...}} envelope: encode/decode, op table, FrameQueue (one frame in flight, §8)
internal/eventbus/      typed events → TUI and/or plain renderer
internal/tui/           bubbletea v2 program(s): `up` screen, huh-based prompts for `login`
internal/output/        plain-mode renderer (tables, spinners) — --no-tui / non-TTY path
```

**Goroutine/ownership model.** One process per `mcpwarp up` invocation (same as Node — no daemonization). No `errgroup`: `runUp` (`internal/cli/up.go`) blocks on `deps.BlockForever(ctx)` (`<-ctx.Done()`) until a signal cancels ctx or a fatal disconnect calls `shutdown.FatalExit`.

- **main goroutine**: flag parsing (cobra, `internal/cli`), then `runUp` blocking as above until shutdown.
- **tunnel goroutine** (owns `*wsmixer.Conn`): the one shared callback goroutine ws-mixer-go promises — `OnStream`/`OnApp`/`OnDrain` fire here, in wire order (`research-sdk.md` item 5). Callbacks decode, push an event, return — never block on I/O/locks or call bridge/supervisor synchronously; anything blocking runs on a goroutine the callback only *launches*.
- **per-child supervisor goroutine** (one per server): owns spawn/restart/backoff. On replace it calls into the bridge **in-process**, mirroring Node's `supervisor.ts:427` calling `Bridge.replaceChild(child)` directly, not over HTTP. Loopback HTTP is a *different* boundary (`internal/relay`).
- **per-stream goroutines**: `OnStream` handoff, one per accepted stream. `internal/registry.resolveByHost` picks the target service from the `Host` header — `OPEN` carries no metadata (`src/forward/relay.ts:9-17`) — then `internal/relay` refuses any path but `/mcp` (§8) and proxies the rest via the `Forwarder` using `http.ReadRequest`/`(*http.Response).Write` over the stream (simpler than Node's hand-written parser/writer). Never-throws: a failure writes an HTTP-shaped error response; only unrecoverable ones fall back to `stream.Reset`.
- **event bus consumer goroutine(s)**: the TUI's `Program`, or the plain renderer for `--no-tui`/non-TTY — one per run, by TTY detection.

**Event bus is three channels, not one** — a single drop-oldest channel would silently drop `registered`/`disable`/`enable`/`conflict`/`error` frames, a correctness bug: **Control** (lossless, backpressures the producer) carries `ConnStateChanged`, `ServerStateChanged{name, state, restarts}`, `StreamOpened/Closed`, `ServiceChanged{name, kind, state, code, message, url, hint}` and `AppError{code, message, service?, hint?}`; both feed §9's last-error line. `ServiceChanged` is one service's registration transition (§9's STATE vocabulary), published by `internal/tunnel` once per change, in config order, after whatever moved it (a welcome's register, an app message, a local `d`/`e`): every per-service outcome — a register refusal, `SERVER_DISABLED`, a dashboard `disable`, a take-over, a whole-frame refusal (§8) — goes out only as `ServiceChanged`, never also as an `AppError`. `AppError` stays for the connection: `CONNECTION_LIMIT`, `OVERLOADED` (once per frame, §8), an `error` op answering an unregister or no frame, plus `PATH_REFUSED` for a service's first refused non-`/mcp` request (§8) and `SERVER_FAILED` for a stdio child given up on (§7); **Telemetry** (`LogLine`, drop-oldest, cap 64); **Metrics** (`MetricSample` — e.g. `ping_rtt`, `bytes_total` — drop-oldest; `bytes_total` comes from the TUI's once-a-second `Stats()` poll, which publishes nothing else, §9). All three make the TUI purely a *consumer*: bridge/supervisor/tunnel code never imports `internal/tui`, only `internal/eventbus`.

**Startup ordering.** Stdio bridges start and bind before the tunnel dials (`up.ts:92` startBridge loop, then `up.ts:126` startTunnel) — `registered` never routes to a dead listener.

**Shutdown ordering.** `internal/shutdown` runs a small ordered handler registry (`Register`/`Run`/`FatalExit`) sequentially, bounded by a shared 5s `Deadline` (Node-parity) — a hung handler gets whatever it managed; per-step budgets inside each handler are the real guard, not the shared deadline. `internal/cli/up.go` registers exactly two handlers, in order: (1) **tunnel** — `Unregister` (2s budget) then `Close` (drains `client_requested`, bounded ≤5s internally, closes the forwarder); (2) **servers** — every supervisor's `StopContext` concurrently, then every bridge's `CloseContext` concurrently, then `bridge.KillAllLiveChildren`, then `bus.Close()`, then wait for the renderer goroutine to finish. A second signal/fatal-exit mid-sequence, or the sequence running past `Deadline`, skips straight to `shutdown.OnForceExit`'s hooks (`bridge.KillAllLiveChildren`) before exiting. `Tunnel.Close` also honours `ctx`, so the shared deadline bounds its relay/send waits too, not just `relayShutdownBudget`/`appSendTimeout`.

## 4. SDK gap decision

**Load-bearing decision.** `ws-mixer-go` didn't implement what `CLIENT-SDK.md` mandates, already present in the JS SDK Node uses:

| Required by spec | ws-mixer-go today | JS SDK |
|---|---|---|
| Token-provider callback (per dial, not cached) | `ClientOptions.Token string` — static | Has `token: provider` fn |
| 401 → one refresh retry, then fatal | Not implemented; `Dial` returns opaque `fmt.Errorf`, no HTTP status | Implemented |
| Reconnect/backoff (full per-close-code table, WIRE.md §2.9) | Out of scope by design (`client.go:17-19`) | Implemented |
| Lifecycle callbacks + structured disconnect reason (`OnConnect` (carries the welcome), `OnDisconnect(reason)`) | Not implemented — only `OnStream`/`OnApp`/`OnDrain` exist (`client.go:20-35`); no disconnect-reason type | Implemented |
| Client-initiated `Drain` (WIRE.md §2.10 rule 14: `drain{reason:"client_requested"}` on SIGINT) | Already usable — no role check in the code; "Server-only" is a stale doc-comment (`drain.go:20`) | Implemented |
| `Stats()`/counters accessor — spec **MUST** (`CLIENT-SDK.md:20`) | None — only path is implementing `Metrics` yourself | `client.stats()` (`.../ws-mixer/dist/client.d.ts:216`) |

Missing lifecycle callbacks are a hard blocker, not a reconnect nicety: §8's register-on-`welcome` and §5's `markAccepted()` both need a welcome signal that doesn't exist today. `Conn.Drain` is already role-agnostic and usable today for SIGINT-drain (§3's shutdown sequence calls it as-is) — "Server-only" is a stale doc-comment, not missing code.

**Decided: option A** (§13). `ws-mixer-go` grows to spec in the ws-mixer-go session, greenlit by the owner; the CLI stays a thin consumer. The scoped hybrid (CLI-side reconnect state machine + token seam in `internal/tunnel`) is dropped.

**Scope for the ws-mixer-go session, v0.4.0:** `TokenProvider func(ctx context.Context) (string, error)` with one 401 refresh-retry; lifecycle callbacks `OnConnect`/`OnDisconnect(reason)` with a disconnect-reason type matching `CLIENT-SDK.md:17` verbatim (`phase`, `wsCode`, `errorCode`, `httpStatus`, `fatal`, `message`); a `Connect`/`Client` wrapper implementing the full WIRE.md §2.9 reconnect/backoff table (full-jitter, base 1000ms, cap 60000ms — distinct from the supervisor cap, §7); `Stats()`, feeding the TUI's connection panel (§9); the `Drain` doc fix plus a client `drain{client_requested}` on shutdown.

v0.4.0 is implemented in the working tree (2026-09-05) but untagged (`git tag`: `v0.3.0`); "today" above describes v0.3.0. No `OnWelcome`: `OnConnect func(c *Conn, welcome *WelcomeMsg)` (`wsmixer/client_reconnect.go:286`).

**Sequencing:** `mcpwarp up`'s tunnel path can't reach reconnect parity until ws-mixer-go v0.4.0 ships (M3, §12); config (§6), auth (§5), bridge/supervisor (§7), app-protocol (§8), and the TUI shell have no dependency on the gap and can start now.

## 5. Auth

Device flow (RFC 8628) + PKCE (S256) via `golang.org/x/oauth2`: `Config.DeviceAuth(ctx)` → poll `Config.DeviceAccessToken(ctx, da)` (`research-stack.md:12`). Maps onto Node's flow, including `authorization_pending`/`slow_down`/`expired_token`/`access_denied`/`invalid_grant` handling. Same defaults: `authUrl=https://auth.mcpwarp.io`, `realm=mcpwarp`, `client_id=mcpwarp-cli`, `scope="openid offline_access"`, flag > env > default.

**Credential storage — parity with Node, byte-for-byte:**
- Path: `~/.mcpwarp/credentials/<sha256(issuer)[:16]>.json`, one file per issuer. Fields: `access_token, refresh_token, expires_at(ms), refresh_expires_at?, token_type, scope, sub, email?, preferred_username?, issuer, client_id, saved_at`. No `id_token`.
- Atomic write: temp file same dir → write/fsync → `chmod 0600` → rename; directory `0700`. Load never throws: missing→nil, EACCES→warn+nil, corrupt→warn-once+nil, issuer-mismatch→nil.
- Staleness: `expires_at - 60s` passed. Cross-process refresh lock: `<creds>.lock`, `O_EXCL`, payload `{pid, createdAt, token}`, stale after 60s, retry every 100ms, 45s timeout, reap via rename-then-unlink.
- Token-provider contract: fresh → return; else lock → re-read → refresh-if-stale → persist. `markAccepted()` fires on `welcome`; asked again while unaccepted → force refresh. Refresh rotation mandatory. `invalid_grant` clears creds, surfaces "run `mcpwarp login`"; a proactive-refresh failure with a still-valid token warns and returns the old token. A `TokenRefreshError` (a failed refresh, whether of a stale token or a forced refresh of an unaccepted one: auth server unreachable, lock timeout or the dial deadline passing while another process holds the lock, persist failure) is wrapped in `wsmixer.ErrTokenUnavailable` by `internal/tunnel`, so ws-mixer-go v0.7.0 treats it as a failed dial and reconnects with normal backoff; any other provider error is fatal (the SDK closes the client, the CLI exits).
- JWT decoded **without signature verification**, display-only — same as Node.

**Static token mode — new, not in Node, decided (§13).** Tokens are personal access tokens (PATs), created, listed and revoked only in the web dashboard (Settings → Access tokens) — there is no CLI command to mint one. Opaque string, prefix `mcpwarp_pat_`, stored hashed on the SaaS side. `MCPWARP_TOKEN` env var only; no `--token` flag (a token in shell history is a worse leak than one in a file). When set, the token provider returns it verbatim as the Bearer to the tunnel — no exchange, no refresh, no decode, no credentials file involved. An invalid, revoked, or expired PAT fails via the same HTTP 401 / WS fatal-after-one-retry path as OAuth mode (§4) — nothing new in error handling. Shipped: `whoami` under `MCPWARP_TOKEN` skips the credentials file, prints `auth: using MCPWARP_TOKEN (personal access token)`, and warns if the value lacks the `mcpwarp_pat_` prefix — no sub/email lookup. The tunnel/register protocol is otherwise unchanged.

**Storage: file for v1, decided (§13), shared with Node.** Same paths, JSON fields, permissions as Node — a `login` with either CLI is honored by both. OS keychain and other hardening deferred: `zalando/go-keyring` is CGO-free but its macOS backend has no prompt gate (issue #110) and headless Linux has no Secret Service session, needing the same file fallback anyway. `adrg/xdg` isn't used for v1 either, for the same reason — it'd relocate files away from Node's fixed path; XDG-correct paths are a legitimate, called-out breaking migration once Node retires.

## 6. Config

Same file (`~/.mcpwarp/config.json`, or `--config`), schema, validation, exit codes as Node:
```json
{"servers":[
  {"name":"...", "kind":"stdio", "command":"...", "args":[...], "env":{...}, "cwd":"..."},
  {"name":"...", "kind":"http", "url":"..."}
]}
```
`name` matches `^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`, 1-30 chars, unique. `servers` holds 1-100 entries (`config.MaxServers`), a cap kept from v0.2.x, when every server went out in one `register`; 100 is the tunnel's per-message cap. The CLI now registers in frames of at most 100 names, one frame in flight (§8), so only a tunnel with a lower cap answers `TOO_MANY_SERVICES`. `http` entries require an `http(s)` URL with no query string. Unknown keys are errors (strict, matches Node's zod). Env vars: `MCPWARP_AUTH_URL`, `MCPWARP_AUTH_REALM`, `MCPWARP_AUTH_CLIENT_ID`, `MCPWARP_CONNECT_URL` (default `wss://connect.mcpwarp.io`), `MCPWARP_WEB_URL`, `MCPWARP_DEBUG`. Validation errors exit 2, one per line, `servers[N].field: msg`; JSON errors report line/col; missing file prints the example config.

`name` is also the public URL slug (`https://<name>-<username>.<domain>/mcp`), hence the tighter pattern and length. Renaming a server in config creates a *new* server with a new URL on the next `up` — the old one lingers in the dashboard, offline, until deleted there.

**Plain `encoding/json` + hand-written validation, not koanf** — a handful of fields, no multi-source merging (what koanf is for); `Decoder.DisallowUnknownFields()` plus ~30 lines of validation gets Node's exact error format for free.

## 7. Bridge & supervisor

Port list (verified against `mcpwarp-cli/src/bridge/{stdio-child,supervisor,http-bridge}.ts`, `src/tunnel/*`; `research-nodecli.md`):

| Behaviour | Detail | Parity note |
|---|---|---|
| Process spawn | `shell:false`, hidden window on Windows, own process group on POSIX, env = process + config env, `cwd` | `SysProcAttr{Setpgid: true}` on POSIX |
| Kill | SIGTERM → SIGKILL after 2s; POSIX kills whole group; last-resort SIGKILL on exit | Port as-is |
| Framing | NDJSON on child stdout, 16 MiB line cap, non-JSON skipped (debug); stdin backpressure-aware | Port as-is |
| stderr | Piped at debug level, `[name]`-prefixed, shown under `--verbose` | Port as-is |
| Supervisor backoff | Full-jitter, base 1000ms, cap 30000ms; 10 crashes without 60s healthy uptime → `failed` | Distinct from SDK reconnect cap (60000ms, §2.9) |
| Supervisor states | `healthy/restarting/failed/disabled/stopped`; `enable` = respawn + re-register; a restart (`r`) joins a spawn or crash backoff already in flight rather than spawning beside it | Port as-is; the supervisor's internal state name stays `healthy`, but the STATE column displays it as `active` (so stdio and http rows — which, having no supervisor, show their registration state `pending`/`active`/`rejected`/`disabled`, §9 — share one vocabulary) |
| Lazy respawn | A POST carrying a method (never a bare client response, which belongs to the dead child's id space) that finds the bridge's child exited calls the bridge's optional `EnsureChild` hook (bounded 5s) before routing; the supervisor's `EnsureRunning` behind it returns at once if `healthy`, respawns a `disabled` child via the `enable` path and waits for `healthy`, joins a spawn already in flight (concurrent callers, `enable` and a restart share one spawn), and refuses `failed`, a crash backoff, `stopped`, and a `disable` in flight or landing mid-wait (the `disable` wins). `up` wires the hook (`ensureChildFor`) behind a guard on the tunnel registry's row (`lazyRespawnRefusal`): no respawn for a local `d` (§9), a dashboard disable (`disable` op or `SERVER_DISABLED`, also while a re-register of that name is unanswered), a take-over (§8), or before the tunnel has started. The guard runs once up front and again as `EnsureRunning`'s `allow`, under the supervisor's lock right before a spawn from `disabled`; `up` marks the registry before every `Disable()` (`Controller.Disable` unregisters first, the tunnel applies a `disable` before calling `OnDisable`), so a racing `d` or dashboard disable is always seen. A refusal keeps the usual 502 `local server is not running` | Go-only: a request routed after the CLI applied a dashboard enable (or a resurrecting `registered`) but before the respawned child is healthy waits for it instead of getting 502 |
| `replaceChild` | Swaps child after restart; listener/port/session untouched; in-flight requests get `-32000 "local server restarted"` | Port as-is |
| HTTP bridge routing | By JSON-RPC shape — serves session-based and stateless transports | Port as-is |
| id rewriting | Client ids rewritten to bridge-minted monotonic ids | Port as-is |
| Progress-token namespacing | `p<internalId>` prefix | Port as-is |
| Legacy GET SSE | 256-message drop-oldest backlog, most-recent GET wins, no resumability | Port as-is |
| HTTP head cap | 64 KiB | Port as-is |
| Origin check | No `Origin`, literal `"null"`, or `http://127.0.0.1*`/`http://localhost*`, else 403 (`http-bridge.ts:187-190`) | Literally — not scheme-agnostic |
| Stream routing & relay | `OPEN` carries no metadata; route by `Host` header via `internal/registry.resolveByHost`, relay to bridge's listener (§3) | Go: `http.ReadRequest`/`Response.Write` over the stream; never-throws |

**Windows: Job Objects** (`TerminateJobObject` kills the whole tree) improve on Node's single-process kill, behaviour-preserving — in scope for v1 (§13).

**Documented asymmetry.** `Close()` on an already-exited child is a no-op on both platforms, so a grandchild it spawned survives — Node parity. On Windows that's only true for the hard-kill path: the Job Object's `KILL_ON_JOB_CLOSE` means releasing its handle on the normal-exit path also kills any grandchild still alive in it — Windows kills survivors POSIX/Node would leave running.

**`.cmd`/`.bat` shims are a separate question, deferred (§13).** Shims don't spawn because `shell:false` is enforced; `os/exec` could invoke them via `cmd.exe /c`, but that's a config-semantics change and an injection surface. Node's behaviour and documented workaround carry forward.

## 8. App protocol

Envelope: `{"mcpwarp":{"v":1,"op":...}}` wrapping `SendApp`'s body on stream 0 (`mcpwarp` is the CLI's own to marshal — `research-sdk.md:73`). In this section a dotted or lettered section number (§2.6, §3.2, §6A.7) is ws-mixer-spec MCPWARP.md's; a bare one (§3, §9) is this document's.

| Direction | Op | Payload | Notes |
|---|---|---|---|
| out | `register` | `{services:[{name,kind}]}` | On every `welcome` (the configured list in config order minus local `d` and taken-over names, ≤100 per frame), on `e` or a dashboard `enable`, and for a transient retry (below); one frame in flight (below) |
| out | `unregister` | `{services:[{name}]}` | On `d`, and on shutdown (below) |
| in | `registered` | `{services:[{name,id,url,created}], errors:[{name,code,message}]}` | Authoritative on reconnect — resurrects dashboard-disabled servers (a locally-disabled one is excluded from the reconnect batch entirely, §9). Settles the outstanding register |
| in | `unregistered` | `{services:[{name,id}]}` | Settles the outstanding unregister |
| in | `disable` | `{id,reason}` | May arrive before id is known locally → pending-disables map keyed by id |
| in | `enable` | `{id,name}` | Resolved by name |
| in | `conflict` | `{id,name,reason}` | Per-name take-over (below): that name reads `taken_over` (`TAKEN_OVER`) and stays out of every register batch until a local `e`, others stay live; non-fatal. Counts only for a name this connection holds (a `registered` on it carried a success for it); a conflict for a name only in an outstanding register (its reply decides) or refused is ignored (MCPWARP.md §3.3, §2.7). Resolved by name, `id` only cross-checked (a mismatch warns, still applies); `reason` is `taken_over`, the default when absent |
| in | `error` | `{code,message}` | `UNSUPPORTED_VERSION` (fatal, exit 1), `OVERLOADED` (re-send, below), `CONNECTION_LIMIT` (non-fatal, best-effort — see close-code note below). Any other code (`TOO_MANY_SERVICES`, `UNKNOWN_OP`, `BAD_REQUEST`, `INTERNAL`, unknown) answers the outstanding frame, nothing re-sent (§2.6 One in flight): a register's names read `rejected` with that code (§6A.6 G7; `TOO_MANY_SERVICES` with a remove-some hint — the edge refuses a register over its cap whole, with no `registered`; §6 caps the config at 100 and frames carry ≤100, so only a tunnel with a lower cap sends it), an unregister is settled as if `unregistered` (warn, §6A.7 Correlation); with no frame outstanding, an `AppError` |
| register errors | — | `QUOTA_EXCEEDED`, `CONFLICT`, `INVALID_NAME`, `USERNAME_REQUIRED`, ... → `rejected`; `SERVER_DISABLED` → `disabled` (`DISABLED`), waits for `enable`; transient (§2.5: `INTERNAL`, `NO_RESPONDERS`, `TIMEOUT`, `ABORTED`, `CONNECTION_LOST`, `MALFORMED_REPLY`, `UNAVAILABLE`, `SERVICE_ERROR`) → stays `pending`, retried (below) | Per name, non-fatal, except the fatal rule below |

**Same name from two sessions.** The tunnel keys a server by owner and name, and the newer registration wins: when another connection of the same account (a second `mcpwarp up`, another machine) registers a name this connection holds, the tunnel moves that one name over, resets its open streams here, and sends this connection `conflict` for it. "Holds" means a `registered` on this connection carried a success for the name: a `conflict` for a name only in a register still awaiting its reply (that reply says what happened) or one the tunnel refused is ignored. Only that name is affected; there is no `drain{replaced}` and no connection-wide close. The CLI marks the name `taken_over` (registry rejection `TAKEN_OVER`; a take-over hint plus the reason as the `last error`) and drops its URL from the dashboard (§9); routing stays the tunnel's call, so relay doesn't second-guess it. The take-over is sticky, like ws-mixer-go's own `mcpwarp` layer (its DECISIONS.md D-2026-10-14-03, which closes the whole client): the name is left out of every later register batch, so a reconnect can't take it back and bounce the other session, and a dashboard `enable` for it is ignored. Only a local `e` re-registers it — taking it back, since the newer registration wins again. Plain mode has no `e`, so there the name stays lost until restart. A stdio child keeps running, unlike a dashboard disable, since the fix is usually stopping the other `up` or a rename, then `e`. A re-register from the same session is idempotent, never a take-over, and a connection the tunnel is draining hands its names to its replacement silently, with no `conflict`.

**Drain hand-over (ws-mixer-go v0.9.0).** On a server `drain` the SDK dials the replacement in parallel; the drained connection keeps serving its in-flight streams past the replacement's welcome and is retired at the drain deadline + 1s or once empty. Its end is reported flagged `Superseded` and may arrive after the replacement's `OnConnect`, so `internal/tunnel` ignores it rather than publishing `disconnected` over the live connection's `connected`. App messages are bound per connection in `OnConnect` (before its register is sent), so one still arriving on the drained connection after that is dropped rather than applied over the live state; `ClientConfig.OnApp` only catches a new connection's frames until its own handler is bound. A connection that ended (not drained) can still deliver messages it received before its end: a `registered`, `unregistered`, `disable` or `enable` delivered after the end (its disconnect handled, or its `Done` closed at delivery) is dropped, while `conflict` and `error` still apply (MCPWARP.md §2.7 Ended connection).

A close with code 4014 (ws-mixer's application-level close, error code 0x0e) and reason prefix `CONNECTION_LIMIT:` is server 0.3.1+'s per-account concurrent-agent-connection cap (non-fatal): ws-mixer-go (since v0.4.1, still in v0.11.1) reconnects after a connected-phase 4014 at random(0, cap) (this CLI's cap is 30s), same as 4009. The close is surfaced as an `AppError{Code:"CONNECTION_LIMIT"}` with a reconnect-guidance `Hint`, same as the app-level `error` row above — 4009 is ws-mixer's own `ENHANCE_YOUR_CALM` meaning and is not this; the tunnel never sends it for the connection cap.

Go encode/decode: a flat struct can't express op-specific payloads — two-pass decode or `json.RawMessage` per-op in a switch. **Unknown op/version**: ignored-with-log, not fatal.

**Frames in flight** (ws-mixer-spec MCPWARP.md §2.6, §6A.7; `appproto.FrameQueue`, driven by `internal/tunnel` on its dispatcher goroutine; replaces Node's 1s pause / 64-deep drop-oldest queue, MCPWARP.md §11 row 49). At most one register/unregister outstanding per connection; the next goes out after its `registered`/`unregistered`, a whole-frame `error` op, or the connection's end. The rest queue FIFO in frames of ≤100 names (§2.1 Batch): a later intent for a name drops its earlier queued one (register-then-unregister before either goes out leaves the unregister), and an intent joins the last queued frame when that has the same op and room. A welcome abandons whatever was outstanding on the old connection, cancels scheduled retries, and puts the full register list ahead of the queue, dropping queued unregisters (a new connection holds nothing). **`OVERLOADED`** means the outstanding frame was dropped unprocessed: the slot stays reserved and the same frame is re-sent after 1s, 2s, 4s … capped 30s (`appproto.ResendDelay`, `min(1000 × 2^(n−1), 30000)` ms, n counting that frame's consecutive `OVERLOADED`s). During the pause a register intent folds into a paused register's re-send while it stays within 100 names, an unregister takes its name out of it and queues behind; a re-send left with no names isn't sent. While connected and not closing, nothing is dropped. Only a frame's first `OVERLOADED` publishes an `AppError` (hint `tunnel busy; re-sending automatically`); repeats log at debug.

**Transient retry** (§3.2 Transient retry, Transient per name). A reply's transient-coded names, and any name of the answered register the reply leaves out (§6A.7 Correlation; shown as `INTERNAL` `unlisted in reply`), stay `pending` and are re-sent on their own after `ResendDelay(n)`, n counting consecutive all-transient replies — carried across reconnects, reset by a reply with any non-transient entry (whose transient names then wait the n = 1 delay) or by an `unregistered`. A disconnect cancels a scheduled retry; the next welcome registers anyway. A `d`, `e` or take-over ends a name's series. The pending row carries the series' first transient code (`eventbus.ServiceChanged`'s one departure from §6A.4), and only that first error logs at warn.

**Fatal rule** (§6A.6 G5, judged on the overall service state, not one reply; `checkFatal` after every reply, whole-frame error and retry). Exit 1 with `No service could be registered.` only when this process never had a register succeed, nothing is outstanding, queued or scheduled for a retry, and every configured service not taken over or locally disabled ended refused with a non-transient code other than `SERVER_DISABLED` (a whole-frame `error` op counts whatever its code, §6A.6 G7; MCPWARP.md §11 row 50). A `SERVER_DISABLED` name keeps the process up (its `enable` arrives on this connection); a transient register error never counts. A whole-frame refusal of a first connection's register (`TOO_MANY_SERVICES`, `BAD_REQUEST`, ...) is therefore fatal when it leaves every service refused. Never while closing.

**Shutdown unregister** (§3.2 Unregister; `Tunnel.Unregister`, 2s budget, §3's shutdown step 1). Marks closing: scheduled retries are cancelled, registers queued or paused by `OVERLOADED` dropped, no register goes out afterwards, and one dropped by `OVERLOADED` from now on is abandoned. Then one unregister for every configured name not taken over (§3.3 Per name) goes out at once, even beside a register still on the wire (the one exception to one in flight; replies still settle by frame order), or queues behind an outstanding unregister. The wait ends when every frame is settled (`unregistered`, or an `error` op answering it), the connection ends, or the budget runs out. Unlike §3.2, which skips the unregister when no register went out on the live connection or every one was dropped by `OVERLOADED` with no `registered`, the CLI sends it whenever a connection is up; a duplicate is harmless (§2.3: names not held are silently absent). `Close` then stops the queue for good: nothing is written after it, not even for a welcome already queued.

**Path rule** (§4.3; ws-mixer-spec DECISIONS.md D-2026-10-24-02; `internal/relay`). Only `/mcp` is forwarded. The raw request-line target, any `scheme://authority` stripped and cut at the first `?` or `#`, is compared exactly and case-sensitively with `/mcp`, never decoded or cleaned (`/%6Dcp`, `/a/../mcp` are not `/mcp`). A query is allowed: the `Forwarder` sends to the target URL's own path, the inbound query passed through as received (§6 forbids a query on an `http` entry's `url`, and a bridge URL has none). Any other path (`/mcp/`, `/mcp/sub`, `/MCP`, `/sse`, `/messages`) answers 404 `{"error":"not_found"}` before any service lookup, so 404 beats 503 unknown service and 503 disabled (§4.4 row 2); the first such refusal per known service, once the 404 is written, logs a warn naming the path and publishes `AppError{PATH_REFUSED}` (the path in its message, the public `/mcp` URL in its hint) via `relay.Options.OnPathRefused`; later ones log at debug, and a refusal for an unknown host only ever at debug. Before that, `*` and authority-form answer 400 `unsupported request target form`, and `\mcp` fails Go's request parsing and answers 400 `bad request`. Every refusal from the path check on (404 path, 503 `{"error":"service unavailable"}` for an unknown service per §4.8 row 3b, 503 disabled, 502 not configured) drains the unread request body before `CLOSE` rather than RESETting (§4.4 Ending a refused stream); the earlier 400s for target form and a missing `Host` don't.

## 9. TUI

**bubbletea v2, pinned exact version, decided (§13)** (`research-stack.md`) — v1 is frozen; v2 is young but the only forward-looking choice. Isolated in `internal/tui` behind the event bus, with no `--no-tui`-default fallback plan. `tview` was evaluated and rejected by the owner. Companion libs: `bubbles` + `lipgloss` v2, `huh` for prompts.

**`up` screen** (fed by the event bus, §3): connection state; per-server table — NAME, KIND, STATE, RESTARTS, URL (no streams column; stream events carry no server name) — with aggregate streams/bytes/latency in the header; latency from `MetricSample{Kind:"ping_rtt"}` (`busMetrics.PingRTT`, not a `LogLine`); bytes polled from `Stats()` once a second (`BytesTransferred` fires per DATA frame — too hot to publish per call), the poll's only job; last error; scrollable log tail. STATE displays a healthy supervisor as `active`, not `healthy`, so a stdio row reads the same as an http row's registration state. STATE also reflects registration, carried by `ServiceChanged` (§3), so a row changes as soon as the tunnel's state does. Every row starts `pending`, and so does every name a welcome re-sends, until its reply; a transient failure stays `pending` while its retry runs (§8). A name refused per service (`QUOTA_EXCEEDED`, `CONFLICT`, `INVALID_NAME`, `USERNAME_REQUIRED`, ...) or by a whole-frame refusal (`TOO_MANY_SERVICES`, ..., §8) shows `rejected` (error style, no URL) until a later successful registration flips it back to `active` — that takes `e` or a reconnect re-sending it (raising the quota alone re-registers nothing). A name a `conflict` op took away (another session of the account registered it, §8) shows `taken_over` (`TAKEN_OVER`) — styled like `rejected`, no URL, nothing for `c` to copy, but named apart because the fix is `e`, not a config or plan change — and stays so across reconnects and dashboard enables until `e` reclaims it. `SERVER_DISABLED` and a dashboard `disable` show `disabled` with code `DISABLED`, since both resume on a dashboard enable. A local `d` shows `disabled` with no code whatever came before (pending, rejected, or a reply still in flight, which then lands disabled rather than active), and the following `e` shows `pending` until its reply. First match wins, as `internal/tunnel`'s `serviceState` orders it (ws-mixer-spec MCPWARP.md §6A.5): local `d`, take-over, awaiting a reply, `SERVER_DISABLED`, other refusal, dashboard disable, active. An event with a code or hint also sets the `last error` line (`[CODE] name: message` over the hint), sticky like an `AppError`'s. For stdio rows the registration only overrides a healthy supervisor state, so `restarting`/`failed` stay visible. A URL too wide for its column moves to its own line under the row instead of being cut with `…`. A `disconnected` header carries the tunnel's disconnect reason in parentheses (`ConnStateChanged.Reason`, e.g. `DISCONNECTED (connection limit)`); a local quit carries none, and once connected the reason is dropped.

**Keybindings (decided):** `q` quit, `?` help, arrows or `j`/`k` move selection, `l` toggle log pane, `pgup`/`pgdown`/`ctrl+u`/`ctrl+d` page/half-page the log pane, `c` copy the selected server's full public URL, `r` restart selected server, `d` disable selected server, `e` enable selected server.

`c` writes the URL both via OSC 52 (bubbletea's `SetClipboard`; the path that works over SSH, and inside tmux once `set -g set-clipboard on` is set) and via the native tool (`pbcopy`, `clip.exe`, `wl-copy`/`xclip`/`xsel`; the path that works in macOS Terminal.app, which ignores OSC 52), the latter run off the update loop and bounded by a 2s timeout plus a 2s wait for the killed tool's I/O to close (~4s worst case). A transient notice confirms it, or says there's nothing to copy for a row with no URL, and clears itself after a few seconds without replacing the `last error` line. Secondary text (footer descriptions, hints, log prefixes) uses an explicit gray chosen from the terminal's reported background (`tea.RequestBackgroundColor`), not ANSI 8, which disappears on mid-gray themes; footer keys are bold in the default foreground.

**TTY requirement.** The TUI path requires *both* stdin and stdout to be a TTY (`defaultUpIsTTY`), not stdout alone — it reads keypresses too, so either being redirected degrades to the plain renderer. A `kill -INT` while the TUI owns the screen races bubbletea's own signal handling benignly: bubbletea restores the terminal either way.

`d` queues `unregister` for that one service, marking it locally disabled in the registry, and then stops the child (supervisor state `disabled`, same as a remote disable) — in that order, so the lazy-respawn guard (§7) already sees the mark; `e` respawns it and sends `register` for it. Both reuse the existing ops (§8) — no new protocol. For an http server, which has no child, only the unregister/register half applies. Consequence: while locally disabled, the dashboard shows the service as absent, not paused; an agent-paused dashboard state is a possible later SaaS addition, out of scope here.

A local `d` persists across a tunnel reconnect: the name is excluded from every register batch the tunnel sends until a matching `e`, so a reconnect can't silently resurrect (and, for stdio, respawn) a service the user deliberately stopped. A dashboard-driven disable is unaffected — that name is still re-registered on reconnect so the server can answer `SERVER_DISABLED` and resume-on-enable keeps working.

**Degrade path:** stdout not a TTY, or `--no-tui` — skip bubbletea, use `internal/output`'s plain-line renderer (parity with Node's pino pretty-on-TTY / NDJSON-off-TTY, ✓/!/✗, NO_COLOR). Non-TTY fallback is plain lines on stdout plus JSON log lines on stderr (`slog`, matching Node's pino behaviour) — no `--json` mode (§2, §13).

Plain mode prints one line per `ServiceChanged`, its only place for a public URL: `✓ notes: active https://notes-<username>.<domain>/mcp`, `✗ notes: rejected (QUOTA_EXCEEDED): <message> · <hint>`, `✗ notes: taken_over (TAKEN_OVER): <reason> · <hint>`, `· notes: disabled (DISABLED): <reason> · <hint>`, `· notes: pending`, `· notes: pending (INTERNAL): <message>` (a transient retry starting); the URL, ` (CODE)`, `: message` and ` · hint` parts appear only when the event carries them. A reconnect isn't reprinted (`reconnectFlicker`): a code-less `pending` right after `active` is skipped, and so is a line identical to the last one printed for that name; a `pending` carrying a code (a transient retry starting) still prints, and a rejected or disabled name prints `pending` and its outcome again on each reconnect. The connection line carries ` reason=<text>` whenever `ConnStateChanged` has one.

**Logging while the TUI owns the screen:** `log/slog` to a file; the log-tail pane reads the same stream via the event bus, not the file. Non-interactive mode logs to stderr, as Node does. **`login`** uses `huh` (or a plain `fmt` prompt), not the full-screen TUI — device-code display and browser-open failure messaging port from Node.

## 10. Testing

- **Unit tests** with DI seams mirroring Node's (`fetchImpl`, `sleep`, `now`, `random`, `spawn`) — small Go interfaces (`Clock`, `Rand`, `execFn`) fakeable without a real clock/process.
- **In-process fake tunnel**: `wsmixer.AcceptConn` (exported, `wsmixer/accept.go:81`) behind a `net/http` test server — preferred over `cmd/conformance-adapter` (a stdin/stdout harness for cross-SDK runs), which means a subprocess per test.
- **Parity gate, a different job**: run Node's existing e2e suites (conformance-adapter-driven) against the Go binary — right tool for "does the binary behave like Node against a real tunnel."
- **E2E scenarios** mirroring Node, shipped: tunnel register/registered/unregister-on-shutdown (in-process fake tunnel, `internal/cli`'s `up_e2e_posix_test.go`), relay HTTP-over-stream, bridge initialize. **Follow-ups**: stateless transport, GET push, and crash-recovery *through the tunnel* end-to-end have no Go equivalent yet — today's bridge-level tests cover them directly against the bridge, not via a fake tunnel connection.
- **CI**: real (`.github/workflows/ci.yml`), two jobs — `test`: build/vet/gofmt(ubuntu)/`-race`, matrixed ubuntu/macos/windows (process-group kill, NDJSON framing are platform-sensitive); `goreleaser-check`: `check`, `build --snapshot --single-target`, smoke-asserts `--version` isn't `dev` (ldflags applied). `release.yml`: `v*` tag → `goreleaser release`, tap/bucket tokens.

## 11. Distribution

- **goreleaser** (`.goreleaser.yaml` v2, OSS): `goos: [linux, darwin, windows]` × `goarch: [amd64, arm64]` (`CGO_ENABLED=0`) — six targets, including `windows/arm64`.
- `nfpms` for `.deb`/`.rpm`/`.apk`; `homebrew_casks` (covers macOS and Linux Homebrew — `brews:` is hard-deprecated since goreleaser v2.16; nfpm covers non-Homebrew Linux) and `scoops` for Homebrew/Scoop; sha256 `checksum`. Homebrew 6+ requires `brew trust --tap mcpwarp/tap` once before installing from this tap.
- Version via `-ldflags "-X main.version=..."` from the git tag. The runtime update notice lives in `internal/update` (see §2), not here — `release.prerelease: auto` is what keeps a prerelease tag (e.g. `v1.0.0-rc.1`) off `/releases/latest`, and therefore out of the notice too, since `internal/update.Check` only ever compares against that endpoint.
- **License:** MIT (decided 2026-09-05). `nfpms`/`homebrew_casks`/`scoops` declare `MIT`, matching the repo's `LICENSE` file.
- **Signing/notarization deferred** — Pro feature or custom post-hook; not needed for v1.

## 12. Milestones

| Milestone | Scope | Status (2026-09-05) |
|---|---|---|
| M0 | Repo skeleton, `internal/config`, `mcpwarp status` (no network) | Done: config, `status`, Node-identical errors |
| M1 | `internal/auth` — `login`/`logout`/`whoami`, credential store, refresh lock, `MCPWARP_TOKEN` | Done: device flow+PKCE, creds, lock, provider, PAT |
| M2 | `internal/bridge` + `internal/supervisor`, e2e-tested | Done: stdio/HTTP bridge, supervisor backoff/states |
| M3 | `mcpwarp up` tunnel path, on ws-mixer-go v0.4.0 (§4) | Done: register/unregister, fake-tunnel e2e |
| M4 | `internal/tui` — `up` screen wired to event bus; plain-mode renderer | Done: bubbletea screen, keybindings, renderer |
| M5 | Release pipeline — goreleaser, brew/scoop, CI matrix | Done: goreleaser config, `goreleaser-check`, `release.yml` |

**Not yet run**: the parity gate (§10) — Node's e2e suites against the Go binary.

## 13. Decisions (2026-09-05)

| # | Topic | Outcome |
|---|---|---|
| 1 | SDK gap | Option A: `ws-mixer-go` v0.4.0 grows to spec (§4); hybrid dropped |
| 2 | Static token mode | `MCPWARP_TOKEN` only (no `--token`), carrying a dashboard-issued `mcpwarp_pat_` personal access token sent verbatim as Bearer; no CLI token-creation command (§5) |
| 3 | Credential storage | File for v1, identical to Node, shared `~/.mcpwarp`; keychain later (§5) |
| 4 | Repo/module path | `github.com/mcpwarp/cli`, binary `mcpwarp`; owner renames the local folder |
| 5 | `up` TUI default | On when stdin and stdout are both TTYs; `--no-tui` forces plain; non-TTY auto-falls back (§2, §9) |
| 6 | Windows Job Objects | In scope for v1 (§7) |
| 7 | `.cmd`/`.bat` via `cmd.exe /c` | Deferred; keep Node's behaviour and documented workaround (§7) |
| 8 | Exit codes 130/143 on SIGINT/SIGTERM | Kept, for parity |
| 9 | TUI library | bubbletea v2, pinned exact version, isolated in `internal/tui` behind the event bus (§9) |
| 10 | `--json` | Dropped from v1; non-TTY fallback plus JSON log lines on stderr cover scripting (§2, §9) |
| 11 | TUI keybindings incl. local disable/enable | Decided, in scope (§9) |
| 12 | License | MIT for the CLI |
| 13 | ServiceGroup (2026-10-24) | Adoption reversed: ws-mixer-go v0.11.0 removed `ServiceGroup` (ws-mixer-spec D-2026-10-24-01: MCPWARP.md §6A is informative guidance for multi-service hosts). The CLI keeps its own multi-service layer, `internal/tunnel` plus `appproto.FrameQueue`, following §6A (§8) |
| 14 | Forwarded paths (2026-10-24) | `/mcp` only, every service, no per-service path option (ws-mixer-spec D-2026-10-24-02); breaking for `http` servers reached on `/sse`, `/messages` or any other path (§8) |
| 15 | Whole-frame refusal on a first connection (2026-10-24) | Fatal: a `TOO_MANY_SERVICES` (or any G7 refusal) leaving every service refused while nothing was ever registered exits 1, G5 being judged on the overall state (§8) |
| 16 | Shutdown unregister (2026-10-24) | Waits for `unregistered` (or the connection's end) within the 2s budget, not only for the send; never for a taken-over name; no register and no fatal exit while closing (§8) |
| 17 | `SERVER_DISABLED` code (2026-10-24) | Reported as `disabled` with code `DISABLED`, the same as a dashboard `disable` op (MCPWARP.md §6A.4), not under the wire code (§9) |
| — | Language/TUI-library alternatives | Rust rewrite and `tview` both evaluated and rejected by the owner |
