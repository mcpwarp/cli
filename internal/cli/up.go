// Command `mcpwarp up` — M3B (docs/DESIGN.md §3, §5, §8, §9): loads config,
// resolves auth, spawns one bridge+supervisor per stdio server, opens the
// tunnel, registers every configured service, prints the post-connect
// table, and blocks in the foreground until a signal-driven shutdown
// (internal/shutdown) or a fatal disconnect (internal/tunnel's FatalExit
// seam) ends the process. Ported from mcpwarp-cli's
// src/cli/commands/up.ts.
package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/mcpwarp/cli/internal/appproto"
	"github.com/mcpwarp/cli/internal/auth"
	"github.com/mcpwarp/cli/internal/bridge"
	"github.com/mcpwarp/cli/internal/config"
	"github.com/mcpwarp/cli/internal/eventbus"
	"github.com/mcpwarp/cli/internal/output"
	"github.com/mcpwarp/cli/internal/registry"
	"github.com/mcpwarp/cli/internal/shutdown"
	"github.com/mcpwarp/cli/internal/supervisor"
	"github.com/mcpwarp/cli/internal/tui"
	"github.com/mcpwarp/cli/internal/tunnel"
	"github.com/mcpwarp/ws-mixer-go/wsmixer"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

// --- connect/web URL settings (ported from mcpwarp-cli's
// src/tunnel/settings.ts; internal/tunnel doesn't own this resolution, so
// it lives here, the way auth settings live in internal/auth but their
// resolution against --issuer already does too) -----------------------

const (
	defaultConnectURL = "wss://connect.mcpwarp.io"
	defaultWebURL     = "https://mcpwarp.io"
)

// connectSettingsError is a malformed --connect-url/MCPWARP_CONNECT_URL or
// MCPWARP_WEB_URL value — usage-shaped (exit 2), matching Node's
// ConnectSettingsError.
type connectSettingsError struct{ message string }

func (e *connectSettingsError) Error() string { return e.message }
func (e *connectSettingsError) ExitCode() int { return 2 }

// resolveConnectURL applies Node's precedence: --connect-url flag >
// MCPWARP_CONNECT_URL > defaultConnectURL.
func resolveConnectURL(flagValue string) (string, error) {
	raw := flagValue
	if raw == "" {
		raw = os.Getenv("MCPWARP_CONNECT_URL")
	}
	if raw == "" {
		raw = defaultConnectURL
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") {
		return "", &connectSettingsError{message: fmt.Sprintf("invalid connect URL: %s (must be ws:// or wss://)", raw)}
	}
	return strings.TrimRight(raw, "/"), nil
}

// resolveWebURL: MCPWARP_WEB_URL, or defaultWebURL — no CLI flag, matching
// Node's webUrl().
func resolveWebURL() (string, error) {
	raw := os.Getenv("MCPWARP_WEB_URL")
	if raw == "" {
		raw = defaultWebURL
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return "", &connectSettingsError{message: fmt.Sprintf("invalid web URL: %s (must be http:// or https://, check MCPWARP_WEB_URL)", raw)}
	}
	return strings.TrimRight(raw, "/"), nil
}

// --- token source adapter --------------------------------------------

// tunnelHandle is the subset of *tunnel.Tunnel runUp's shutdown handler,
// Controller and TUI wiring need — narrowed to an interface (rather than
// depending on the concrete type StartTunnel's default returns) purely so
// a test can fake it without dialing a real tunnel; *tunnel.Tunnel
// satisfies this as-is.
type tunnelHandle interface {
	Unregister(ctx context.Context) error
	Close(ctx context.Context) error
	// RegisterService/UnregisterService back a local d/e keybinding
	// (Controller.Disable/Enable) with the same register/unregister ops
	// §8 already defines, rather than driving only the local supervisor.
	RegisterService(name string)
	UnregisterService(name string) error
	// Registry/Stats feed the TUI's dashboard (DESIGN.md §9): public URLs
	// once "registered" lands, and a bytes-transferred figure.
	Registry() *registry.Registry
	Stats() wsmixer.Stats
}

// staticTokenAdapter widens auth.StaticTokenProvider's Token() (no ctx, no
// error) into tunnel.TokenSource's Token(ctx) (string, error) shape — the
// "one-line closure" tunnel.go's own doc comment calls for.
type staticTokenAdapter struct{ p *auth.StaticTokenProvider }

func (a staticTokenAdapter) Token(context.Context) (string, error) { return a.p.Token(), nil }

// --- Controller: local d/e/r actions (DESIGN.md §9) -------------------

// Controller implements the TUI's/plain-mode's restart/disable/enable
// keybindings (DESIGN.md §9): Disable/Enable drive both the local
// supervisor (stop/respawn the child) and the matching tunnel op
// (unregister/register that one service), reusing §8's existing wire
// behaviour rather than inventing new ops.
type Controller struct {
	mu          sync.Mutex
	supervisors map[string]*supervisor.Supervisor
	// httpNames is the set of configured http server names — they have no
	// supervisor, so Disable/Enable/Restart need this to tell "http" apart
	// from "unknown" once the supervisor lookup below comes up empty.
	// Built once in runUp and never mutated afterward, so reading it needs
	// no lock (unlike supervisors, looked up per-call through get()).
	httpNames map[string]struct{}
	t         tunnelHandle
}

func newController(supervisors map[string]*supervisor.Supervisor, httpNames map[string]struct{}, t tunnelHandle) *Controller {
	return &Controller{supervisors: supervisors, httpNames: httpNames, t: t}
}

func (c *Controller) get(name string) *supervisor.Supervisor {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.supervisors[name]
}

func (c *Controller) isHTTP(name string) bool {
	_, ok := c.httpNames[name]
	return ok
}

// State reports name's current supervisor state (e.g. "healthy"), used by
// newTUIRenderer to seed the TUI's initial STATE column for a stdio server
// before the first eventbus.ServerStateChanged arrives. ok is false for an
// http server, which has no supervisor.
func (c *Controller) State(name string) (state string, ok bool) {
	s := c.get(name)
	if s == nil {
		return "", false
	}
	return string(s.GetState()), true
}

// Restart stops and immediately respawns name's child (supervisor.Restart).
// An http server has no child to restart — that's a distinct error from
// "unknown server", not a silent no-op.
func (c *Controller) Restart(name string) error {
	s := c.get(name)
	if s == nil {
		if c.isHTTP(name) {
			return fmt.Errorf("http servers have no child to restart")
		}
		return fmt.Errorf("unknown server %q", name)
	}
	s.Restart()
	return nil
}

// Disable stops name's child (supervisor.Disable) and unregisters it from
// the tunnel (tunnelHandle.UnregisterService) — the local counterpart of a
// remote disable. An http server has no child to stop, so only the
// unregister half applies.
func (c *Controller) Disable(name string) error {
	if s := c.get(name); s != nil {
		s.Disable()
	} else if !c.isHTTP(name) {
		return fmt.Errorf("unknown server %q", name)
	}
	if c.t != nil {
		return c.t.UnregisterService(name)
	}
	return nil
}

// Enable respawns name's child (supervisor.Enable) and re-registers it with
// the tunnel (tunnelHandle.RegisterService) — the local counterpart of a
// remote enable. An http server has no child to respawn, so only the
// re-register half applies.
func (c *Controller) Enable(name string) error {
	if s := c.get(name); s != nil {
		s.Enable()
	} else if !c.isHTTP(name) {
		return fmt.Errorf("unknown server %q", name)
	}
	if c.t != nil {
		c.t.RegisterService(name)
	}
	return nil
}

// --- the renderer seam --------------------------------------------------

// upUIDeps is everything a renderer (the plain-line drain loop, or the
// bubbletea TUI) needs to drive `up`'s foreground loop.
type upUIDeps struct {
	Bus        *eventbus.Bus
	Log        *slog.Logger
	Controller *Controller
	Services   []output.TableRow
	// Registry is the tunnel's live name/id/url table — the TUI wiring
	// polls it for the public URL a service is assigned once "registered"
	// lands (Services/rows above are only the caller's initial, pre-tunnel
	// snapshot). Nil in a plain-mode upUIDeps, which has no use for it.
	Registry  *registry.Registry
	PlainMode bool

	// SetLogWriter, if non-nil, redirects ctx.Log's underlying writer —
	// the TUI's TTY path swaps it to tui.OpenLogFile's file for as long as
	// it owns the screen (DESIGN.md §9), restoring it afterward. A no-op
	// seam (nil) when the renderer never needs to (plain mode, or a
	// Context built without NewLoggerWithSwap).
	SetLogWriter func(w io.Writer)
}

// runUIFunc is the seam the renderer plugs into: reads deps.Bus for events
// and deps.Controller for keybindings. It must keep draining deps.Bus.Control
// (and, while it owns the screen, deps.Bus.Telemetry) until that channel
// closes — runUp only calls bus.Close() once the shutdown sequence's servers
// handler has finished, so returning early on ctx cancellation alone would
// miss whatever the tunnel/bridge shutdown steps still publish. quit is true
// only when the renderer itself asked to end the process (the TUI's `q`);
// runUp acts on it after closing uiDone (B-1: calling shutdown.FatalExit
// before that would deadlock the servers handler, which waits on uiDone).
type runUIFunc func(ctx context.Context, deps upUIDeps) (quit bool, err error)

// runPlainRenderer is the --no-tui/non-TTY degrade path (DESIGN.md §9);
// plain mode has no `q` keybinding, so quit is always false. Uses
// context.Background() rather than the ctx passed in (cancelled the instant
// a signal arrives) since the drain must outlive that and only stop once
// bus.Close() actually runs, per runUIFunc's doc comment.
func runPlainRenderer(_ context.Context, deps upUIDeps) (bool, error) {
	err := output.RunPlain(context.Background(), deps.Bus, output.Stdout)
	return false, err
}

// tuiRunFunc is tui.Run behind a var: a test seam so the TUI renderer path
// can be exercised without launching a real bubbletea Program.
var tuiRunFunc = tui.Run

// tuiControllerAdapter widens Controller's error-returning Restart/
// Disable/Enable into tui.Controller's no-return-value shape, logging a
// failure (unknown server name, tunnel op error) rather than dropping it
// silently.
type tuiControllerAdapter struct {
	c   *Controller
	log *slog.Logger
}

func (a tuiControllerAdapter) Restart(name string) { a.run(name, "restart", a.c.Restart) }
func (a tuiControllerAdapter) Disable(name string) { a.run(name, "disable", a.c.Disable) }
func (a tuiControllerAdapter) Enable(name string)  { a.run(name, "enable", a.c.Enable) }

func (a tuiControllerAdapter) run(name, action string, fn func(string) error) {
	if err := fn(name); err != nil && a.log != nil {
		a.log.Warn("controller action failed", "action", action, "name", name, "err", err)
	}
}

// newTUIRenderer builds the runUIFunc used on a TTY without --no-tui
// (DESIGN.md §9): redirects logging to tui.OpenLogFile's file for as long
// as the dashboard owns the terminal, polls the tunnel's registry/Stats
// once a second for the public-URL snapshot and bytes-transferred figure
// the bus can't otherwise produce (simplest option; ConnStateChanged/
// AppError alone can't tell the model a service's public URL — carried
// only on "registered" — has appeared), and reports tuiRunFunc's quit flag
// back to runUp (on `q`) rather than acting on it itself — see runUIFunc's
// doc comment (B-1).
func newTUIRenderer(home string, t tunnelHandle) runUIFunc {
	return func(ctx context.Context, deps upUIDeps) (bool, error) {
		if deps.SetLogWriter != nil {
			if f, err := tui.OpenLogFile(home); err == nil {
				deps.SetLogWriter(f)
				defer func() {
					deps.SetLogWriter(os.Stderr)
					f.Close()
				}()
			} else {
				deps.Log.Warn("could not open the TUI log file; logging to stderr instead", "err", err)
			}
		}

		// Seed each row's initial STATE: http starts at the registry's
		// active status (no supervisor); stdio reads its supervisor's.
		servers := make([]tui.Server, len(deps.Services))
		for i, r := range deps.Services {
			s := tui.Server{Name: r.Name, Kind: r.Kind}
			if r.Kind == config.KindHTTP {
				s.State = string(registry.StatusActive)
			} else if deps.Controller != nil {
				if st, ok := deps.Controller.State(r.Name); ok {
					s.State = st
				}
			}
			servers[i] = s
		}

		pollCtx, stopPoll := context.WithCancel(ctx)
		defer stopPoll()
		updates := make(chan tui.SnapshotMsg, 1)
		go pollTunnelForTUI(pollCtx, t, deps.Bus, updates, time.Second)

		return tuiRunFunc(ctx, deps.Bus, servers, tuiControllerAdapter{c: deps.Controller, log: deps.Log}, updates)
	}
}

// pollTunnelForTUI feeds the running dashboard what the bus alone can't:
// the registry's public URLs (pushed as a tui.SnapshotMsg once per
// interval) and a bytes-transferred metric derived from Stats() (DESIGN.md
// §9). Stops, closing updates, once ctx is done.
func pollTunnelForTUI(ctx context.Context, t tunnelHandle, bus *eventbus.Bus, updates chan<- tui.SnapshotMsg, interval time.Duration) {
	defer close(updates)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if reg := t.Registry(); reg != nil {
				rows := reg.Rows()
				snap := make([]tui.Server, len(rows))
				for i, r := range rows {
					s := tui.Server{Name: r.Name, Kind: r.Kind, URL: r.URL}
					// stdio's state is owned by eventbus.ServerStateChanged
					// (applySnapshot leaves an empty State untouched); only
					// http has a registry-derived state to report here.
					if r.Kind == config.KindHTTP {
						if r.Disabled {
							s.State = string(registry.StatusDisabled)
						} else {
							s.State = string(registry.StatusActive)
						}
					}
					snap[i] = s
				}
				select {
				case updates <- tui.SnapshotMsg{Servers: snap}:
				default:
					// A previous snapshot is still waiting to be sent —
					// the next tick's snapshot supersedes it anyway.
				}
			}
			stats := t.Stats()
			bus.PublishMetric(eventbus.MetricSample{
				Kind:  "bytes_total",
				Value: float64(stats.BytesIn + stats.BytesOut),
				At:    time.Now(),
			})
		}
	}
}

// --- wiring seams (DI for tests, mirroring Node's UpDeps) --------------

// defaultUpIsTTY requires both stdin and stdout to be a TTY — unlike
// login's defaultStdoutIsTTY (stdout alone, only used to decide whether a
// spinner can render), the TUI reads keypresses from stdin as well as
// drawing to stdout, so either one being redirected must degrade to the
// plain renderer.
func defaultUpIsTTY() bool {
	return term.IsTerminal(int(os.Stdin.Fd())) && term.IsTerminal(int(os.Stdout.Fd()))
}

// upDeps are the injectable seams runUp needs — defaults are the real
// implementations; tests override the ones they need to fake.
type upDeps struct {
	Issuer string
	Paths  *auth.Paths

	StartBridge   func(bridge.StartBridgeOptions) (*bridge.Bridge, error)
	NewSupervisor func(supervisor.Options, supervisor.Deps) *supervisor.Supervisor
	StartTunnel   func(context.Context, tunnel.Config) (tunnelHandle, error)

	RunUI        runUIFunc
	BlockForever func(ctx context.Context)

	// OnBridgeStarted, if set, is called synchronously right after each
	// stdio server's bridge is started (and before its supervisor is
	// constructed) — a test seam so an e2e test can observe the bridge
	// (its port, its current child's pid) without runUp exposing that
	// state through its return value.
	OnBridgeStarted func(name string, br *bridge.Bridge)

	Bus *eventbus.Bus

	IsTTY func() bool

	Version string

	// FatalExit is a DI seam over shutdown.FatalExit (default when nil):
	// invoked when RunUI's quit return value is true (the TUI's `q`), the
	// same bounded shutdown a SIGINT/SIGTERM would trigger, exit code 0.
	// Tests substitute a non-exiting stub — the real one calls os.Exit,
	// which would kill the test binary.
	FatalExit func(code int)
}

func (d upDeps) resolve() upDeps {
	if d.StartBridge == nil {
		d.StartBridge = bridge.StartBridge
	}
	if d.NewSupervisor == nil {
		d.NewSupervisor = supervisor.New
	}
	if d.StartTunnel == nil {
		d.StartTunnel = func(ctx context.Context, cfg tunnel.Config) (tunnelHandle, error) {
			return tunnel.Start(ctx, cfg)
		}
	}
	// d.RunUI is deliberately left nil here if unset: runUp picks
	// runPlainRenderer or newTUIRenderer once it knows plainMode, rather
	// than defaulting to one renderer before that's decided. A test that
	// sets RunUI itself always wins, same as any other seam here.
	if d.BlockForever == nil {
		d.BlockForever = func(ctx context.Context) { <-ctx.Done() }
	}
	if d.Bus == nil {
		d.Bus = eventbus.New(64)
	}
	if d.IsTTY == nil {
		d.IsTTY = defaultUpIsTTY
	}
	if d.Version == "" {
		d.Version = "dev"
	}
	if d.FatalExit == nil {
		d.FatalExit = shutdown.FatalExit
	}
	return d
}

func newUpCommand(ctxFor func(*cobra.Command) *Context) *cobra.Command {
	var noTUI bool
	cmd := &cobra.Command{
		Use:   "up",
		Short: "Expose the configured servers through the mcpwarp tunnel",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runUp(ctxFor(cmd), noTUI, upDeps{})
		},
	}
	cmd.Flags().BoolVar(&noTUI, "no-tui", false, "force the plain-line renderer even on a TTY")
	return cmd
}

// runUp mirrors Node's cli/commands/up.ts. See DESIGN.md §3 for the
// startup/shutdown ordering this implements.
func runUp(ctx *Context, noTUI bool, deps upDeps) error {
	ctx.Log.Debug("mcpwarp up invoked")
	deps = deps.resolve()

	cfg, err := ctx.LoadConfig()
	if err != nil {
		return reportConfigError(err)
	}

	// --- auth: MCPWARP_TOKEN -> StaticTokenProvider; else credentials file
	// -> TokenProvider; else "not logged in" (DESIGN.md §5, up.ts:50-54).
	var tokenSource tunnel.TokenSource
	var loggedInAs string
	if token := os.Getenv(auth.StaticTokenEnvVar); token != "" {
		stp := auth.NewStaticTokenProvider(token, func(m string) { output.Warn(m) })
		tokenSource = staticTokenAdapter{p: stp}
		loggedInAs = "personal access token"
	} else {
		// Node's up.ts: issuer ??= currentIssuer(...); paths ??=
		// credentialsPaths(issuer) — each defaulted independently, so a
		// caller (a test) can supply just one of deps.Issuer/deps.Paths
		// and still get the other resolved for real rather than silently
		// discarded.
		issuer := deps.Issuer
		if issuer == "" {
			var err error
			issuer, err = auth.CurrentIssuer(ctx.IssuerOverride)
			if err != nil {
				return reportConfigError(err)
			}
		}
		var paths auth.Paths
		if deps.Paths == nil {
			var err error
			paths, err = auth.CredentialsPaths(issuer, ctx.HomeDir)
			if err != nil {
				output.Error(err.Error())
				return RuntimeError()
			}
		} else {
			paths = *deps.Paths
		}

		creds := auth.Load(paths, nil)
		if creds == nil {
			output.Error("Not logged in. Run `mcpwarp login`.")
			return RuntimeError()
		}
		loggedInAs = creds.Email
		if loggedInAs == "" {
			loggedInAs = creds.PreferredUsername
		}
		if loggedInAs == "" {
			loggedInAs = creds.Sub
		}

		tp := auth.NewTokenProvider(auth.TokenProviderOptions{
			Issuer: issuer,
			Paths:  paths,
			Warn:   func(m string) { output.Warn(m) },
		})
		tokenSource = tp
	}

	connectURL, err := resolveConnectURL(ctx.ConnectURLOverride)
	if err != nil {
		return reportConfigError(err)
	}
	webURL, err := resolveWebURL()
	if err != nil {
		return reportConfigError(err)
	}

	bus := deps.Bus

	// Computed up front (DESIGN.md §9's degrade path) so sv.OnFailed below
	// can already tell whether to write straight to the terminal or go
	// through the bus like everything else does once a renderer owns the
	// screen.
	plainMode := noTUI || !deps.IsTTY()

	// Update notice exception (DESIGN.md §9): `up` never returns normally,
	// so root.go's printUpdateNotice (after root.ExecuteContext returns)
	// never runs for it. Once the TUI starts it also owns the whole
	// terminal, and the swap of ctx.Log's writer to the TUI log file
	// happens later, inside newTUIRenderer's own goroutine (up.go below) —
	// not here — so a plain ctx.Log.Warn/stderr write at this point would
	// race that swap: it either lands on raw stderr an instant before the
	// alt screen wipes it, or in the log file if the swap wins, depending
	// on how fast the background check happened to finish. Rather than
	// special-case "already finished", TUI mode always hands the notice to
	// the bus instead — the one thing the running TUI reliably renders
	// regardless of timing — as two LogLine telemetry events, the same
	// shape the log-tail pane already shows (internal/tui/view.go's
	// renderLogLines). Plain mode has no such race (no alt screen to wipe
	// anything), so it keeps printing straight to stderr: immediately if
	// the check already finished, before the renderer below writes
	// anything, or from a goroutine once it does.
	if plainMode {
		if n, ready := ctx.UpdateChecker.TryWait(); ready {
			if n != nil {
				n.Print()
			}
		} else {
			go func() {
				if n := ctx.UpdateChecker.Wait(updateCheckLogWait); n != nil {
					n.Print()
				}
			}()
		}
	} else {
		go func() {
			if n := ctx.UpdateChecker.Wait(updateCheckLogWait); n != nil {
				for _, line := range n.Lines() {
					bus.PublishTelemetry(eventbus.LogLine{Server: "update", Level: "warn", Text: line})
				}
			}
		}()
	}

	services := make([]appproto.RegisterService, len(cfg.Servers))
	for i, s := range cfg.Servers {
		services[i] = appproto.RegisterService{Name: s.Name, Kind: s.Kind}
	}
	// config.Validate already rejects a "kind":"http" server whose url
	// isn't a parseable http(s) URL (isHTTPOrHTTPSURL), so a parse failure
	// here can't happen for a config that made it through LoadConfig —
	// ignoring the error, like the bridge URL parse below, rather than
	// re-deriving an unreachable error path for it.
	targets := make(map[string]*url.URL, len(cfg.Servers))
	// httpNames backs Controller's d/e/r keybindings for a server with no
	// supervisor (below): it's how Disable/Enable/Restart tell "http" apart
	// from "unknown".
	httpNames := make(map[string]struct{}, len(cfg.Servers))
	for _, s := range cfg.Servers {
		if s.Kind != config.KindHTTP {
			continue
		}
		u, _ := url.Parse(s.URL)
		targets[s.Name] = u
		httpNames[s.Name] = struct{}{}
	}

	// --- startup order (DESIGN.md §3): stdio bridges bind before the
	// tunnel dials, so "registered" never routes to a dead listener.
	var bridges []*bridge.Bridge
	supervisors := make(map[string]*supervisor.Supervisor)

	stopEverythingStarted := func() {
		var wg sync.WaitGroup
		for _, sv := range supervisors {
			wg.Add(1)
			go func(sv *supervisor.Supervisor) { defer wg.Done(); sv.Stop() }(sv)
		}
		wg.Wait()
		for _, br := range bridges {
			_ = br.Close()
		}
	}

	for _, s := range cfg.Servers {
		if s.Kind != config.KindStdio {
			continue
		}
		spawnSpec := bridge.SpawnSpec{Command: s.Command, Args: s.Args, Env: s.Env, Cwd: s.Cwd}
		br, err := deps.StartBridge(bridge.StartBridgeOptions{
			Name:      s.Name,
			SpawnSpec: spawnSpec,
			Log:       ctx.Log,
			Bus:       bus,
		})
		if err != nil {
			// Node's up.ts:90-121: whatever already started must not leak
			// past this failure — stop it, then report and exit. Node's own
			// catch here returns exit 2 (a bad `command`/config is
			// usage-shaped), matched here rather than a generic runtime 1.
			stopEverythingStarted()
			output.Error(fmt.Sprintf("%s: %s", s.Name, err.Error()))
			return &exitError{code: 2}
		}
		bridges = append(bridges, br)
		u, _ := url.Parse(br.URL())
		targets[s.Name] = u
		if deps.OnBridgeStarted != nil {
			deps.OnBridgeStarted(s.Name, br)
		}

		sv := deps.NewSupervisor(supervisor.Options{
			Name:      s.Name,
			SpawnSpec: spawnSpec,
			Log:       ctx.Log,
			Child:     br.GetCurrentChild(),
			Bridge:    br,
			Bus:       bus,
		}, supervisor.Deps{
			// A restart spawn's stderr must reach the bus the same way the
			// bridge's own initial child's does (StartBridgeOptions.Bus
			// above) — otherwise a service's telemetry goes dark the
			// moment its first child is replaced.
			Spawn: func(spec bridge.SpawnSpec, log *slog.Logger, name string) (*bridge.StdioChild, error) {
				return bridge.NewStdioChildWithBus(spec, log, name, bus)
			},
		})
		name := s.Name
		sv.OnFailed(func(reason string) {
			if plainMode {
				output.Error(reason)
			} else {
				bus.Publish(eventbus.AppError{Code: "SERVER_FAILED", Message: reason, Service: name})
			}
		})
		supervisors[s.Name] = sv
	}

	onDisable := func(name, _id, _reason string) {
		if name == "" {
			return
		}
		if sv, ok := supervisors[name]; ok {
			sv.Disable()
		}
	}
	onEnable := func(name string) {
		if sv, ok := supervisors[name]; ok {
			sv.Enable()
		}
	}

	t, err := deps.StartTunnel(ctx.Context(), tunnel.Config{
		URL:         connectURL,
		WebURL:      webURL,
		TokenSource: tokenSource,
		Services:    services,
		Targets:     targets,
		Meta:        tunnel.Meta{Client: "mcpwarp-cli-go", Version: deps.Version, OS: runtime.GOOS},
		Bus:         bus,
		Log:         ctx.Log,
		OnDisable:   onDisable,
		OnEnable:    onEnable,
	})
	if err != nil {
		stopEverythingStarted()
		// Node up.ts's catch: NotLoggedInError/SessionExpiredError and any
		// other startTunnel failure both report the error and exit 1 — the
		// two cases only differ in Node by whether the error is rethrown
		// (an uncaught rejection, still exit 1) or returned, which this
		// port doesn't need to distinguish since both paths land on
		// RuntimeError() here. tunnel.Start wraps a raw connect failure as
		// "tunnel: connect: %w"; strip that wrapper so the message matches
		// Node's bare one.
		msg := strings.TrimPrefix(err.Error(), "tunnel: connect: ")
		output.Error(msg)
		return RuntimeError()
	}

	// uiDone is created before the shutdown handlers below are registered,
	// not merely before BlockForever, so the "servers" handler can wait on
	// it directly (B2): shutdown.Run calls osExit immediately after every
	// handler returns, with no other gap in which a renderer still
	// draining the bus after bus.Close() gets a chance to finish printing
	// whatever the "tunnel" handler just published (its own closing
	// ConnStateChanged included).
	uiDone := make(chan struct{})

	// --- shutdown ordering (DESIGN.md §3), registered in this order:
	// (1) tunnel: Unregister (2s) then Close (drain/close, bounded
	// internally); (2) servers: stop every supervisor concurrently, then
	// CloseContext every bridge, then close the bus and wait for the
	// renderer to notice.
	unregisterTunnel := shutdown.Register("tunnel", func(ctx context.Context) error {
		uctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		_ = t.Unregister(uctx)
		return t.Close(ctx)
	})
	stopServers := shutdown.Register("servers", func(ctx context.Context) error {
		var wg sync.WaitGroup
		for _, sv := range supervisors {
			wg.Add(1)
			go func(sv *supervisor.Supervisor) { defer wg.Done(); sv.StopContext(ctx) }(sv)
		}
		wg.Wait()
		for _, br := range bridges {
			wg.Add(1)
			go func(br *bridge.Bridge) { defer wg.Done(); _ = br.CloseContext(ctx) }(br)
		}
		wg.Wait()
		bridge.KillAllLiveChildren()
		bus.Close()
		select {
		case <-uiDone:
		case <-ctx.Done():
		}
		return nil
	})
	// Deregister (not run — Register's own return value only removes the
	// handler) on any return path that isn't a real signal/FatalExit: the
	// process is about to exit there anyway in production, but an
	// in-process caller (tests run several `up`s in one binary) would
	// otherwise leak these into shutdown's global handler list for a run
	// that's already over.
	defer unregisterTunnel()
	defer stopServers()

	// bridge.KillAllLiveChildren is also the last-resort net for the two
	// paths that skip the handler sequence above entirely: a second
	// signal/FatalExit racing an in-progress one (shutdown.Run/FatalExit's
	// own force-exit branch) and a handler sequence that runs past
	// shutdown.Deadline. Deliberately not unregistered: a second signal can
	// arrive after runUp has returned (ctx cancellation unblocks
	// BlockForever while the ordered sequence is still in the tunnel
	// handler) but before osExit, and that is exactly when the children
	// still need killing. The hook is idempotent and holds no per-run
	// state; tests clear it with shutdown.ResetForTests.
	shutdown.OnForceExit(bridge.KillAllLiveChildren)

	rows := make([]output.TableRow, 0, len(cfg.Servers))
	for _, s := range cfg.Servers {
		u := ""
		if tu, ok := targets[s.Name]; ok {
			u = tu.String()
		}
		rows = append(rows, output.TableRow{Name: s.Name, Kind: s.Kind, URL: u})
	}
	// The table and "connected as" line only matter to stdout scrollback in
	// plain mode — the TUI's alt screen hides them immediately, and printing
	// to stdout right before it takes over the terminal just clutters
	// whatever the user sees after quitting it.
	if plainMode {
		output.Info(output.FormatTable(rows, output.TableHeaders{}))
		if loggedInAs != "" {
			output.Success(fmt.Sprintf("connected as %s", loggedInAs))
		} else {
			output.Success("connected")
		}
	}

	controller := newController(supervisors, httpNames, t)

	// --- renderer selection (DESIGN.md §9's degrade path): a caller-
	// supplied deps.RunUI always wins (test seam); otherwise plainMode
	// picks runPlainRenderer or the TUI, home-dir-backed newTUIRenderer.
	runUI := deps.RunUI
	if runUI == nil {
		if plainMode {
			runUI = runPlainRenderer
		} else {
			home := ctx.HomeDir
			if home == "" {
				if h, err := os.UserHomeDir(); err == nil {
					home = h
				}
			}
			runUI = newTUIRenderer(home, t)
		}
	}

	var setLogWriter func(io.Writer)
	if ctx.LogWriter != nil {
		setLogWriter = ctx.LogWriter.Swap
	}

	go func() {
		quit, _ := runUI(ctx.Context(), upUIDeps{
			Bus:          bus,
			Log:          ctx.Log,
			Controller:   controller,
			Services:     rows,
			Registry:     t.Registry(),
			PlainMode:    plainMode,
			SetLogWriter: setLogWriter,
		})
		// B-1: uiDone must close before shutdown.FatalExit runs — FatalExit
		// runs the "servers" handler synchronously on this goroutine, and
		// that handler waits on uiDone; calling it first would deadlock for
		// up to shutdown.Deadline.
		close(uiDone)
		if quit {
			deps.FatalExit(0)
		}
	}()

	deps.BlockForever(ctx.Context())

	// Belt-and-suspenders: the real fix (B2) is the "servers" handler's own
	// wait on uiDone above, which runs before shutdown.Run's osExit; this
	// bounded wait just keeps a caller that drives runUp directly (a test,
	// or ctx cancelled without any shutdown sequence ever running) from
	// returning while the renderer goroutine is still mid-drain.
	select {
	case <-uiDone:
	case <-time.After(shutdown.Deadline + time.Second):
	}
	return nil
}
