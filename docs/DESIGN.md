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

## 3. Architecture

Package layout (Go modules, module path `github.com/mcpwarp/cli`, decided — §13):

```
cmd/mcpwarp/            main() is a dozen lines: cli.Execute + last-resort KillAllLiveChildren/os.Exit
internal/cli/           cobra wiring, exit-code mapping, signal relay (Execute/runRoot)
internal/shutdown/      the bounded signal/fatal-exit handler registry (Register/Run/FatalExit)
internal/config/        load/validate config.json, same schema as Node
internal/auth/          device flow + PKCE, credential store, refresh lock, token provider seam
internal/tunnel/        wraps ws-mixer-go Dial/Conn; register/unregister; reconnect (v0.4.0, §4)
internal/registry/      name/id/url table from `registered`; resolveByHost (Node: tunnel/registry.ts)
internal/relay/         raw-HTTP-over-stream relay to bridge's listener; never throws (Node: http/*, forward/*)
internal/bridge/        stdio child + local HTTP server per config entry; NDJSON framing; JSON-RPC routing
internal/supervisor/    child lifecycle: spawn, backoff, restart, disable/enable, replaceChild
internal/appproto/      the {"mcpwarp":{"v":1,"op":...}} envelope: encode/decode, op table, OVERLOADED queue
internal/eventbus/      typed events → TUI and/or plain renderer
internal/tui/           bubbletea v2 program(s): `up` screen, huh-based prompts for `login`
internal/output/        plain-mode renderer (tables, spinners) — --no-tui / non-TTY path
```

**Goroutine/ownership model.** One process per `mcpwarp up` invocation (same as Node — no daemonization). No `errgroup`: `runUp` (`internal/cli/up.go`) blocks on `deps.BlockForever(ctx)` (`<-ctx.Done()`) until a signal cancels ctx or a fatal disconnect calls `shutdown.FatalExit`.

- **main goroutine**: flag parsing (cobra, `internal/cli`), then `runUp` blocking as above until shutdown.
- **tunnel goroutine** (owns `*wsmixer.Conn`): the one shared callback goroutine ws-mixer-go promises — `OnStream`/`OnApp`/`OnDrain` fire here, in wire order (`research-sdk.md` item 5). Callbacks decode, push an event, return — never block on I/O/locks or call bridge/supervisor synchronously; anything blocking runs on a goroutine the callback only *launches*.
- **per-child supervisor goroutine** (one per server): owns spawn/restart/backoff. On replace it calls into the bridge **in-process**, mirroring Node's `supervisor.ts:427` calling `Bridge.replaceChild(child)` directly, not over HTTP. Loopback HTTP is a *different* boundary (`internal/relay`).
- **per-stream goroutines**: `OnStream` handoff, one per accepted stream. `internal/registry.resolveByHost` picks the target service from the `Host` header — `OPEN` carries no metadata (`src/forward/relay.ts:9-17`) — then `internal/relay` proxies via the `Forwarder` using `http.ReadRequest`/`(*http.Response).Write` over the stream (simpler than Node's hand-written parser/writer). Never-throws: a failure writes an HTTP-shaped error response; only unrecoverable ones fall back to `stream.Reset`.
- **event bus consumer goroutine(s)**: the TUI's `Program`, or the plain renderer for `--no-tui`/non-TTY — one per run, by TTY detection.

**Event bus is three channels, not one** — a single drop-oldest channel would silently drop `registered`/`disable`/`enable`/`error` frames, a correctness bug: **Control** (lossless, backpressures the producer) carries `ConnStateChanged`, `ServerStateChanged{name, state, restarts}`, `StreamOpened/Closed`, `AppError{code, message, service?}` (register errors `QUOTA_EXCEEDED`/`CONFLICT`/`SERVER_DISABLED`/`INVALID_NAME`/`USERNAME_REQUIRED`, `client.ts:351-364`, and tunnel `error`; feeds §9's last-error panel); **Telemetry** (`LogLine`, drop-oldest, same policy as the OVERLOADED queue, §8); **Metrics** (`MetricSample` — e.g. `ping_rtt`, `bytes_total` — drop-oldest). All three make the TUI purely a *consumer*: bridge/supervisor/tunnel code never imports `internal/tui`, only `internal/eventbus`.

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
- Token-provider contract: fresh → return; else lock → re-read → refresh-if-stale → persist. `markAccepted()` fires on `welcome`; asked again while unaccepted → force refresh. Refresh rotation mandatory. `invalid_grant` clears creds, surfaces "run `mcpwarp login`"; a proactive-refresh failure with a still-valid token warns and returns the old token.
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
`name` matches `^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`, 1-30 chars, unique. `http` entries require an `http(s)` URL with no query string. Unknown keys are errors (strict, matches Node's zod). Env vars: `MCPWARP_AUTH_URL`, `MCPWARP_AUTH_REALM`, `MCPWARP_AUTH_CLIENT_ID`, `MCPWARP_CONNECT_URL` (default `wss://connect.mcpwarp.io`), `MCPWARP_WEB_URL`, `MCPWARP_DEBUG`. Validation errors exit 2, one per line, `servers[N].field: msg`; JSON errors report line/col; missing file prints the example config.

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
| Supervisor states | `healthy/restarting/failed/disabled/stopped`; `enable` = respawn + re-register | Port as-is |
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

Envelope: `{"mcpwarp":{"v":1,"op":...}}` wrapping `SendApp`'s body on stream 0 (`mcpwarp` is the CLI's own to marshal — `research-sdk.md:73`).

| Direction | Op | Payload | Notes |
|---|---|---|---|
| out | `register` | `{services:[{name,kind}]}` | Sent on every `welcome` |
| out | `unregister` | `{services:[{name}]}` | On shutdown |
| in | `registered` | `{services:[{name,id,url,created}], errors:[{name,code,message}]}` | Authoritative on reconnect — resurrects locally-disabled servers |
| in | `unregistered` | `{services:[{name,id}]}` | |
| in | `disable` | `{id,reason}` | May arrive before id is known locally → pending-disables map keyed by id |
| in | `enable` | `{id,name}` | Resolved by name |
| in | `error` | `{code,message}` | `UNSUPPORTED_VERSION` (fatal, exit 1), `OVERLOADED` (pause 1s), `UNKNOWN_OP`/`BAD_REQUEST` (warn), `INTERNAL` |
| register errors | — | `QUOTA_EXCEEDED`, `CONFLICT`, `SERVER_DISABLED` (non-fatal, wait) | |

Go encode/decode: a flat struct can't express op-specific payloads — two-pass decode or `json.RawMessage` per-op in a switch. **Unknown op/version**: ignored-with-log, not fatal. **OVERLOADED**: pause sends 1s, queue up to 64, drop-oldest beyond — port the Node constants exactly.

## 9. TUI

**bubbletea v2, pinned exact version, decided (§13)** (`research-stack.md`) — v1 is frozen; v2 is young but the only forward-looking choice. Isolated in `internal/tui` behind the event bus, with no `--no-tui`-default fallback plan. `tview` was evaluated and rejected by the owner. Companion libs: `bubbles` + `lipgloss` v2, `huh` for prompts.

**`up` screen** (fed by the event bus, §3): connection state; per-server table — NAME, KIND, STATE, RESTARTS, URL (no streams column; stream events carry no server name) — with aggregate streams/bytes/latency in the header; latency from `MetricSample{Kind:"ping_rtt"}` (`busMetrics.PingRTT`, not a `LogLine`); bytes polled from `Stats()` once a second (`BytesTransferred` fires per DATA frame — too hot to publish per call); last error; scrollable log tail.

**Keybindings (decided):** `q` quit, `?` help, arrows or `j`/`k` move selection, `l` toggle log pane, `pgup`/`pgdown`/`ctrl+u`/`ctrl+d` page/half-page the log pane, `r` restart selected server, `d` disable selected server, `e` enable selected server.

**TTY requirement.** The TUI path requires *both* stdin and stdout to be a TTY (`defaultUpIsTTY`), not stdout alone — it reads keypresses too, so either being redirected degrades to the plain renderer. A `kill -INT` while the TUI owns the screen races bubbletea's own signal handling benignly: bubbletea restores the terminal either way.

`d` stops the child (supervisor state `disabled`, same as a remote disable) and sends `unregister` for that one service; `e` respawns it and sends `register` for it. Both reuse the existing ops (§8) — no new protocol. Consequence: while locally disabled, the dashboard shows the service as absent, not paused; an agent-paused dashboard state is a possible later SaaS addition, out of scope here.

**Degrade path:** stdout not a TTY, or `--no-tui` — skip bubbletea, use `internal/output`'s plain-line renderer (parity with Node's pino pretty-on-TTY / NDJSON-off-TTY, ✓/!/✗, NO_COLOR). Non-TTY fallback is plain lines on stdout plus JSON log lines on stderr (`slog`, matching Node's pino behaviour) — no `--json` mode (§2, §13).

**Logging while the TUI owns the screen:** `log/slog` to a file; the log-tail pane reads the same stream via the event bus, not the file. Non-interactive mode logs to stderr, as Node does. **`login`** uses `huh` (or a plain `fmt` prompt), not the full-screen TUI — device-code display and browser-open failure messaging port from Node.

## 10. Testing

- **Unit tests** with DI seams mirroring Node's (`fetchImpl`, `sleep`, `now`, `random`, `spawn`) — small Go interfaces (`Clock`, `Rand`, `execFn`) fakeable without a real clock/process.
- **In-process fake tunnel**: `wsmixer.AcceptConn` (exported, `wsmixer/accept.go:81`) behind a `net/http` test server — preferred over `cmd/conformance-adapter` (a stdin/stdout harness for cross-SDK runs), which means a subprocess per test.
- **Parity gate, a different job**: run Node's existing e2e suites (conformance-adapter-driven) against the Go binary — right tool for "does the binary behave like Node against a real tunnel."
- **E2E scenarios** mirroring Node, shipped: tunnel register/registered/unregister-on-shutdown (in-process fake tunnel, `internal/cli`'s `up_e2e_posix_test.go`), relay HTTP-over-stream, bridge initialize. **Follow-ups**: stateless transport, GET push, and crash-recovery *through the tunnel* end-to-end have no Go equivalent yet — today's bridge-level tests cover them directly against the bridge, not via a fake tunnel connection.
- **CI**: real (`.github/workflows/ci.yml`), two jobs — `test`: build/vet/gofmt(ubuntu)/`-race`, matrixed ubuntu/macos/windows (process-group kill, NDJSON framing are platform-sensitive); `goreleaser-check`: `check`, `build --snapshot --single-target`, smoke-asserts `--version` isn't `dev` (ldflags applied). `release.yml`: `v*` tag → `goreleaser release`, tap/bucket tokens.

## 11. Distribution

- **goreleaser** (`.goreleaser.yaml` v2, OSS): `goos: [linux, darwin, windows]` × `goarch: [amd64, arm64]` (`CGO_ENABLED=0`) — six targets, including `windows/arm64`.
- `nfpms` for `.deb`/`.rpm`/`.apk`; `homebrew_casks` (macOS only — `brews:` is hard-deprecated since goreleaser v2.16; Linux gets the nfpm packages instead) and `scoops` for Homebrew/Scoop; sha256 `checksum`.
- Version via `-ldflags "-X main.version=..."` from the git tag — no runtime update check (parity with Node). `release.prerelease: auto` keeps a prerelease tag (e.g. `v1.0.0-rc.1`) off the "latest" release.
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
| — | Language/TUI-library alternatives | Rust rewrite and `tview` both evaluated and rejected by the owner |
