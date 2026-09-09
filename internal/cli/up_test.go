package cli

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mcpwarp/cli/internal/bridge"
	"github.com/mcpwarp/cli/internal/eventbus"
	"github.com/mcpwarp/cli/internal/registry"
	"github.com/mcpwarp/cli/internal/shutdown"
	"github.com/mcpwarp/cli/internal/supervisor"
	"github.com/mcpwarp/cli/internal/tui"
	"github.com/mcpwarp/cli/internal/tunnel"
	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

func writeUpConfig(t *testing.T, dir, raw string) string {
	t.Helper()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// noopBlockForever returns immediately — every test here drives runUp to
// completion via a StartTunnel/StartBridge fake that errors out (or, for
// the shutdown-order test, by calling ctx's cancel itself) rather than
// actually blocking forever.
func noopBlockForever(context.Context) {}

func TestRunUp_ConfigErrorExits2(t *testing.T) {
	dir := t.TempDir()
	path := writeUpConfig(t, dir, `{"servers": not-json}`)
	stderr := withCapturedStderr(t)

	ctx := &Context{ConfigPath: path, HomeDir: dir, Log: NewLogger(false)}
	err := runUp(ctx, true, upDeps{BlockForever: noopBlockForever})
	if err == nil {
		t.Fatal("expected an error")
	}
	if code := err.(ExitCoder).ExitCode(); code != 2 {
		t.Fatalf("got exit code %d, want 2; stderr=%s", code, stderr.String())
	}
}

func TestRunUp_NotLoggedInExits1(t *testing.T) {
	dir := t.TempDir()
	path := writeUpConfig(t, dir, `{"servers": [{"name":"notes","kind":"http","url":"http://127.0.0.1:8765/mcp"}]}`)
	stderr := withCapturedStderr(t)

	os.Unsetenv("MCPWARP_TOKEN")
	ctx := &Context{ConfigPath: path, HomeDir: dir, Log: NewLogger(false)}
	err := runUp(ctx, true, upDeps{BlockForever: noopBlockForever})
	if err == nil {
		t.Fatal("expected an error")
	}
	if code := err.(ExitCoder).ExitCode(); code != 1 {
		t.Fatalf("got exit code %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "Not logged in. Run `mcpwarp login`.") {
		t.Fatalf("got %q", stderr.String())
	}
}

func TestRunUp_StaticTokenBuildsStaticSource(t *testing.T) {
	dir := t.TempDir()
	path := writeUpConfig(t, dir, `{"servers": [{"name":"notes","kind":"http","url":"http://127.0.0.1:8765/mcp"}]}`)
	withCapturedStdout(t)

	t.Setenv("MCPWARP_TOKEN", "mcpwarp_pat_abc123")

	var gotToken string
	sentinel := errors.New("stop after capturing config")
	ctx := &Context{ConfigPath: path, HomeDir: dir, Log: NewLogger(false)}
	err := runUp(ctx, true, upDeps{
		BlockForever: noopBlockForever,
		StartTunnel: func(_ context.Context, cfg tunnel.Config) (tunnelHandle, error) {
			gotToken, _ = cfg.TokenSource.Token(context.Background())
			return nil, sentinel
		},
	})
	if err == nil {
		t.Fatal("expected an error (from the StartTunnel stub)")
	}
	if gotToken != "mcpwarp_pat_abc123" {
		t.Fatalf("got token %q, want the static MCPWARP_TOKEN value verbatim", gotToken)
	}
}

func TestRunUp_StartupOrder_BridgesBeforeTunnel(t *testing.T) {
	dir := t.TempDir()
	path := writeUpConfig(t, dir, `{"servers": [
		{"name":"echo","kind":"stdio","command":"true","args":[]}
	]}`)
	withCapturedStdout(t)
	t.Setenv("MCPWARP_TOKEN", "mcpwarp_pat_abc123")

	var mu sync.Mutex
	var order []string

	sentinel := errors.New("stop after capturing order")
	ctx := &Context{ConfigPath: path, HomeDir: dir, Log: NewLogger(false)}
	err := runUp(ctx, true, upDeps{
		BlockForever: noopBlockForever,
		StartBridge: func(opts bridge.StartBridgeOptions) (*bridge.Bridge, error) {
			mu.Lock()
			order = append(order, "bridge:"+opts.Name)
			mu.Unlock()
			return bridge.StartBridge(bridge.StartBridgeOptions{
				Name:      opts.Name,
				SpawnSpec: bridge.SpawnSpec{Command: "true"},
				Log:       ctx.Log,
			})
		},
		NewSupervisor: func(opts supervisor.Options, deps supervisor.Deps) *supervisor.Supervisor {
			return supervisor.New(opts, deps)
		},
		StartTunnel: func(_ context.Context, cfg tunnel.Config) (tunnelHandle, error) {
			mu.Lock()
			order = append(order, "tunnel")
			mu.Unlock()
			if len(cfg.Services) != 1 || cfg.Services[0].Name != "echo" {
				t.Errorf("tunnel config missing the configured service: %+v", cfg.Services)
			}
			if _, ok := cfg.Targets["echo"]; !ok {
				t.Errorf("tunnel config missing a bridge target for echo: %+v", cfg.Targets)
			}
			return nil, sentinel
		},
	})
	if err == nil {
		t.Fatal("expected an error (from the StartTunnel stub)")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "bridge:echo" || order[1] != "tunnel" {
		t.Fatalf("got order %v, want [bridge:echo tunnel]", order)
	}
}

// TestRunUp_SpawnFailureClosesAlreadyStartedBridges ports Node up.test.ts's
// "spawn failure closes already-started bridges" case: when a later
// server's bridge fails to start, every bridge already started for an
// earlier server must be closed (not leaked), the error reported as
// "<name>: <message>", and the exit code 2 (a bad command/config is
// usage-shaped, matching Node's up.ts:90-121).
func TestRunUp_SpawnFailureClosesAlreadyStartedBridges(t *testing.T) {
	dir := t.TempDir()
	path := writeUpConfig(t, dir, `{"servers": [
		{"name":"first","kind":"stdio","command":"true","args":[]},
		{"name":"second","kind":"stdio","command":"true","args":[]}
	]}`)
	stderr := withCapturedStderr(t)
	t.Setenv("MCPWARP_TOKEN", "mcpwarp_pat_abc123")
	t.Cleanup(bridge.KillAllLiveChildren)

	var firstBridge *bridge.Bridge
	ctx := &Context{ConfigPath: path, HomeDir: dir, Log: NewLogger(false)}
	err := runUp(ctx, true, upDeps{
		BlockForever: noopBlockForever,
		StartBridge: func(opts bridge.StartBridgeOptions) (*bridge.Bridge, error) {
			if opts.Name == "second" {
				return nil, errors.New("boom")
			}
			br, err := bridge.StartBridge(bridge.StartBridgeOptions{
				Name: opts.Name, SpawnSpec: bridge.SpawnSpec{Command: "true"}, Log: ctx.Log,
			})
			firstBridge = br
			return br, err
		},
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	if code := err.(ExitCoder).ExitCode(); code != 2 {
		t.Fatalf("got exit code %d, want 2", code)
	}
	if want := "second: boom"; !strings.Contains(stderr.String(), want) {
		t.Fatalf("got stderr %q, want it to contain %q", stderr.String(), want)
	}
	if firstBridge == nil {
		t.Fatal("the first bridge was never started")
	}
	if _, err := http.Get(firstBridge.URL()); err == nil {
		t.Fatal("expected the first bridge's listener to be closed after the second server's spawn failure")
	}
}

// TestRunUp_StartTunnelFailureClosesBridges ports Node up.test.ts's
// "startTunnel failure closes bridges" case: every bridge already started
// must be closed when dialing the tunnel itself fails, not just when a
// later bridge's own spawn fails (the case above).
func TestRunUp_StartTunnelFailureClosesBridges(t *testing.T) {
	dir := t.TempDir()
	path := writeUpConfig(t, dir, `{"servers": [
		{"name":"echo","kind":"stdio","command":"true","args":[]}
	]}`)
	withCapturedStdout(t)
	withCapturedStderr(t)
	t.Setenv("MCPWARP_TOKEN", "mcpwarp_pat_abc123")
	t.Cleanup(bridge.KillAllLiveChildren)

	var startedBridge *bridge.Bridge
	ctx := &Context{ConfigPath: path, HomeDir: dir, Log: NewLogger(false)}
	err := runUp(ctx, true, upDeps{
		BlockForever:    noopBlockForever,
		OnBridgeStarted: func(_ string, br *bridge.Bridge) { startedBridge = br },
		StartTunnel: func(context.Context, tunnel.Config) (tunnelHandle, error) {
			return nil, errors.New("tunnel dial failed")
		},
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	if code := err.(ExitCoder).ExitCode(); code != 1 {
		t.Fatalf("got exit code %d, want 1", code)
	}
	if startedBridge == nil {
		t.Fatal("the bridge was never started")
	}
	if _, err := http.Get(startedBridge.URL()); err == nil {
		t.Fatal("expected the bridge's listener to be closed after startTunnel failed")
	}
}

// timeline records ordered string events under a mutex — shared by
// fakeTunnelHandle and a test's own RunUI stub so the exact shutdown
// sequence DESIGN.md §3 promises can be asserted directly, not just each
// step's individual side effect.
type timeline struct {
	mu     sync.Mutex
	events []string
}

func (tl *timeline) add(event string) {
	tl.mu.Lock()
	tl.events = append(tl.events, event)
	tl.mu.Unlock()
}

func (tl *timeline) snapshot() []string {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	return append([]string{}, tl.events...)
}

// fakeTunnelHandle lets the shutdown-order test control Unregister/Close
// without dialing a real tunnel.
type fakeTunnelHandle struct {
	mu               sync.Mutex
	unregisterCalled bool
	closeCalled      bool
	unregisterErr    error
	// tl, if set, gets a "tunnel:unregister"/"tunnel:close" entry appended
	// (order preserved by the mutex-guarded append) as each is called.
	tl *timeline

	registeredNames   []string
	unregisteredNames []string
}

func (f *fakeTunnelHandle) Unregister(context.Context) error {
	f.mu.Lock()
	f.unregisterCalled = true
	f.mu.Unlock()
	if f.tl != nil {
		f.tl.add("tunnel:unregister")
	}
	return f.unregisterErr
}

func (f *fakeTunnelHandle) Close(context.Context) error {
	f.mu.Lock()
	f.closeCalled = true
	f.mu.Unlock()
	if f.tl != nil {
		f.tl.add("tunnel:close")
	}
	return nil
}

// RegisterService/UnregisterService record the name they were called with
// (for TestControllerDisableEnableReachTunnel) in addition to being no-ops
// otherwise; Registry/Stats are zero values — no test drives the TUI
// renderer, the only other caller of those two.
func (f *fakeTunnelHandle) RegisterService(name string) {
	f.mu.Lock()
	f.registeredNames = append(f.registeredNames, name)
	f.mu.Unlock()
}

func (f *fakeTunnelHandle) UnregisterService(name string) error {
	f.mu.Lock()
	f.unregisteredNames = append(f.unregisteredNames, name)
	f.mu.Unlock()
	return nil
}
func (f *fakeTunnelHandle) Registry() *registry.Registry { return registry.New() }
func (f *fakeTunnelHandle) Stats() wsmixer.Stats         { return wsmixer.Stats{} }

// TestControllerDisableEnableReachTunnel is S-6: Controller.Disable/Enable
// (the `d`/`e` keybindings' backing logic) must reach both the local
// supervisor and the matching tunnelHandle op, not just one of the two.
func TestControllerDisableEnableReachTunnel(t *testing.T) {
	t.Cleanup(bridge.KillAllLiveChildren)

	br, err := bridge.StartBridge(bridge.StartBridgeOptions{
		Name:      "echo",
		SpawnSpec: bridge.SpawnSpec{Command: "true"},
		Log:       NewLogger(false),
	})
	if err != nil {
		t.Fatalf("StartBridge: %v", err)
	}
	sv := supervisor.New(supervisor.Options{
		Name:      "echo",
		SpawnSpec: bridge.SpawnSpec{Command: "true"},
		Log:       NewLogger(false),
		Child:     br.GetCurrentChild(),
		Bridge:    br,
	}, supervisor.Deps{})

	fake := &fakeTunnelHandle{}
	ctrl := newController(map[string]*supervisor.Supervisor{"echo": sv}, fake)

	if err := ctrl.Disable("echo"); err != nil {
		t.Fatalf("Disable: %v", err)
	}
	if sv.GetState() != supervisor.Disabled {
		t.Fatalf("supervisor state = %v, want %v", sv.GetState(), supervisor.Disabled)
	}
	fake.mu.Lock()
	unregistered := append([]string{}, fake.unregisteredNames...)
	fake.mu.Unlock()
	if !slices.Contains(unregistered, "echo") {
		t.Fatalf("expected UnregisterService(\"echo\"), got %v", unregistered)
	}

	if err := ctrl.Enable("echo"); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if sv.GetState() == supervisor.Disabled {
		t.Fatalf("expected the supervisor to leave Disabled after Enable, got %v", sv.GetState())
	}
	fake.mu.Lock()
	registered := append([]string{}, fake.registeredNames...)
	fake.mu.Unlock()
	if !slices.Contains(registered, "echo") {
		t.Fatalf("expected RegisterService(\"echo\"), got %v", registered)
	}

	if err := ctrl.Disable("does-not-exist"); err == nil {
		t.Fatal("expected an error for an unknown server name")
	}
}

func TestRunUp_ShutdownHandlerOrderAndSecondRunsAfterFirstErrors(t *testing.T) {
	dir := t.TempDir()
	// A stdio server (not just an http one) so a real supervisor exists to
	// publish the eventbus.ServerStateChanged{"stopped"} the timeline below
	// uses as its "servers:stop" marker.
	path := writeUpConfig(t, dir, `{"servers": [
		{"name":"echo","kind":"stdio","command":"true","args":[]}
	]}`)
	withCapturedStdout(t)
	t.Setenv("MCPWARP_TOKEN", "mcpwarp_pat_abc123")

	shutdown.ResetForTests()
	t.Cleanup(shutdown.ResetForTests)

	tl := &timeline{}
	fake := &fakeTunnelHandle{unregisterErr: errors.New("unregister failed"), tl: tl}
	upCtx, cancelUp := context.WithCancel(context.Background())

	ctx := &Context{ConfigPath: path, HomeDir: dir, Log: NewLogger(false), Ctx: upCtx}
	done := make(chan error, 1)
	go func() {
		done <- runUp(ctx, true, upDeps{
			BlockForever: func(c context.Context) { <-c.Done() },
			StartTunnel: func(context.Context, tunnel.Config) (tunnelHandle, error) {
				return fake, nil
			},
			// A minimal renderer: watches Control for the supervisor's
			// terminal "stopped" state, then records the bus/UI's own two
			// timeline entries once bus.Close() actually reaches it.
			RunUI: func(_ context.Context, deps upUIDeps) (bool, error) {
				for evt := range deps.Bus.Control {
					if sc, ok := evt.(eventbus.ServerStateChanged); ok && sc.State == string(supervisor.Stopped) {
						tl.add("servers:stop")
					}
				}
				tl.add("bus:close")
				for range deps.Bus.Telemetry {
				}
				tl.add("ui:done")
				return false, nil
			},
		})
	}()

	// runUp is now blocked in BlockForever; the shutdown handlers it
	// registered are live. Run them the way shutdown.Run would, without
	// exiting the test binary. Give it a moment to actually reach
	// BlockForever first — a config/auth error returning early (and never
	// registering the handlers) would otherwise make RunHandlersForTests a
	// silent no-op instead of a visible failure.
	select {
	case err := <-done:
		t.Fatalf("runUp returned before blocking: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	shutdown.RunHandlersForTests(context.Background())

	fake.mu.Lock()
	unregisterCalled, closeCalled := fake.unregisterCalled, fake.closeCalled
	fake.mu.Unlock()
	if !unregisterCalled {
		t.Error("expected Unregister to have been called")
	}
	if !closeCalled {
		t.Error("expected Close to have been called even though Unregister errored")
	}

	cancelUp()
	<-done

	want := []string{"tunnel:unregister", "tunnel:close", "servers:stop", "bus:close", "ui:done"}
	if got := tl.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("shutdown timeline = %v, want %v", got, want)
	}
}

// TestRunUp_QuitClosesUIDoneBeforeFatalExit is B-1: the renderer goroutine
// must close uiDone before invoking the FatalExit seam, not after —
// otherwise the "servers" shutdown handler (which waits on uiDone) races
// FatalExit's own handler sequence and blocks for the shared
// shutdown.Deadline. The FatalExit stub below runs the real registered
// handlers synchronously (mirroring what shutdown.FatalExit itself does,
// minus the actual os.Exit) so this reproduces the deadlock exactly if
// up.go's goroutine calls it before closing uiDone.
func TestRunUp_QuitClosesUIDoneBeforeFatalExit(t *testing.T) {
	dir := t.TempDir()
	path := writeUpConfig(t, dir, `{"servers": [{"name":"notes","kind":"http","url":"http://127.0.0.1:8765/mcp"}]}`)
	withCapturedStdout(t)
	t.Setenv("MCPWARP_TOKEN", "mcpwarp_pat_abc123")

	shutdown.ResetForTests()
	t.Cleanup(shutdown.ResetForTests)

	upCtx, cancelUp := context.WithCancel(context.Background())
	t.Cleanup(cancelUp)
	ctx := &Context{ConfigPath: path, HomeDir: dir, Log: NewLogger(false), Ctx: upCtx}

	var elapsed time.Duration
	fatalExitDone := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- runUp(ctx, true, upDeps{
			BlockForever: func(c context.Context) { <-c.Done() },
			StartTunnel: func(context.Context, tunnel.Config) (tunnelHandle, error) {
				return &fakeTunnelHandle{}, nil
			},
			RunUI: func(_ context.Context, deps upUIDeps) (bool, error) {
				return true, nil // simulate the TUI's `q`
			},
			FatalExit: func(int) {
				start := time.Now()
				shutdown.RunHandlersForTests(context.Background())
				elapsed = time.Since(start)
				close(fatalExitDone)
			},
		})
	}()

	select {
	case <-fatalExitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the FatalExit seam to be invoked")
	}
	if elapsed > time.Second {
		t.Fatalf("FatalExit's handler sequence took %v, want under 1s — uiDone was likely still open when FatalExit ran", elapsed)
	}

	cancelUp()
	<-done
}

// TestRunUp_RendererSelection asserts PlainMode via a caller-supplied
// RunUI override, which always wins regardless of plainMode (a seam, not
// the auto-selection logic) — see TestRunUp_RendererSelection_NoOverride
// for the actual runPlainRenderer/newTUIRenderer pick.
func TestRunUp_RendererSelection(t *testing.T) {
	dir := t.TempDir()
	path := writeUpConfig(t, dir, `{"servers": [{"name":"notes","kind":"http","url":"http://127.0.0.1:8765/mcp"}]}`)
	withCapturedStdout(t)
	t.Setenv("MCPWARP_TOKEN", "mcpwarp_pat_abc123")

	cases := []struct {
		name      string
		noTUI     bool
		isTTY     bool
		wantPlain bool
	}{
		{"no-tui flag forces plain", true, true, true},
		{"non-TTY auto-degrades to plain", false, false, true},
		{"TTY without --no-tui", false, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shutdown.ResetForTests()
			t.Cleanup(shutdown.ResetForTests)

			var gotPlain bool
			uiInvoked := make(chan struct{})
			upCtx, cancelUp := context.WithCancel(context.Background())
			ctx := &Context{ConfigPath: path, HomeDir: dir, Log: NewLogger(false), Ctx: upCtx}

			done := make(chan error, 1)
			go func() {
				done <- runUp(ctx, tc.noTUI, upDeps{
					BlockForever: func(c context.Context) { <-c.Done() },
					IsTTY:        func() bool { return tc.isTTY },
					StartTunnel: func(context.Context, tunnel.Config) (tunnelHandle, error) {
						return &fakeTunnelHandle{}, nil
					},
					RunUI: func(c context.Context, deps upUIDeps) (bool, error) {
						gotPlain = deps.PlainMode
						close(uiInvoked)
						<-c.Done()
						return false, nil
					},
				})
			}()

			select {
			case <-uiInvoked:
			case err := <-done:
				t.Fatalf("runUp returned before invoking RunUI: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for RunUI to be invoked")
			}
			cancelUp()
			shutdown.RunHandlersForTests(context.Background())
			<-done

			if gotPlain != tc.wantPlain {
				t.Fatalf("got PlainMode=%v, want %v", gotPlain, tc.wantPlain)
			}
		})
	}
}

// TestRunUp_TUIModeSkipsStdoutTable is a nit fix: the pre-connect table and
// "connected as" line belong to the plain renderer's stdout scrollback only
// — the TUI's alt screen hides them immediately, so printing them first just
// clutters what the user sees after quitting the TUI.
func TestRunUp_TUIModeSkipsStdoutTable(t *testing.T) {
	dir := t.TempDir()
	path := writeUpConfig(t, dir, `{"servers": [{"name":"notes","kind":"http","url":"http://127.0.0.1:8765/mcp"}]}`)
	t.Setenv("MCPWARP_TOKEN", "mcpwarp_pat_abc123")

	shutdown.ResetForTests()
	t.Cleanup(shutdown.ResetForTests)

	origTUIRun := tuiRunFunc
	t.Cleanup(func() { tuiRunFunc = origTUIRun })
	tuiRunFunc = func(ctx context.Context, _ *eventbus.Bus, _ []tui.Server, _ tui.Controller, _ <-chan tui.SnapshotMsg) (bool, error) {
		<-ctx.Done()
		return false, nil
	}

	stdout := withCapturedStdout(t)
	upCtx, cancelUp := context.WithCancel(context.Background())
	ctx := &Context{ConfigPath: path, HomeDir: dir, Log: NewLogger(false), Ctx: upCtx}

	done := make(chan error, 1)
	go func() {
		done <- runUp(ctx, false, upDeps{
			BlockForever: func(c context.Context) { <-c.Done() },
			IsTTY:        func() bool { return true },
			StartTunnel: func(context.Context, tunnel.Config) (tunnelHandle, error) {
				return &fakeTunnelHandle{}, nil
			},
		})
	}()

	waitDeadline := time.Now().Add(2 * time.Second)
	for shutdown.HandlerCount() < 2 {
		if time.Now().After(waitDeadline) {
			t.Fatal("timed out waiting for runUp to register its shutdown handlers")
		}
		time.Sleep(time.Millisecond)
	}
	cancelUp()
	shutdown.RunHandlersForTests(context.Background())
	<-done

	if got := stdout.String(); strings.Contains(got, "connected") || strings.Contains(got, "NAME") {
		t.Fatalf("expected no table/\"connected as\" output in TUI mode, got stdout %q", got)
	}
}

// TestNewTUIRenderer_SetsAndRestoresLogWriter is S-6: newTUIRenderer must
// redirect deps.SetLogWriter to tui.OpenLogFile's file for the duration it
// owns the screen, then restore os.Stderr once tuiRunFunc returns.
func TestNewTUIRenderer_SetsAndRestoresLogWriter(t *testing.T) {
	origTUIRun := tuiRunFunc
	t.Cleanup(func() { tuiRunFunc = origTUIRun })
	tuiRunFunc = func(ctx context.Context, _ *eventbus.Bus, _ []tui.Server, _ tui.Controller, _ <-chan tui.SnapshotMsg) (bool, error) {
		return false, nil
	}

	dir := t.TempDir()
	renderer := newTUIRenderer(dir, &fakeTunnelHandle{})

	var writers []io.Writer
	quit, err := renderer(context.Background(), upUIDeps{
		Bus:          eventbus.New(1),
		Log:          NewLogger(false),
		SetLogWriter: func(w io.Writer) { writers = append(writers, w) },
	})
	if err != nil || quit {
		t.Fatalf("got quit=%v err=%v, want false/nil", quit, err)
	}
	if len(writers) != 2 {
		t.Fatalf("expected SetLogWriter called twice (open, then restore), got %d: %v", len(writers), writers)
	}
	if _, ok := writers[0].(*os.File); !ok {
		t.Fatalf("expected the first SetLogWriter call to carry the opened log file, got %T", writers[0])
	}
	if writers[1] != os.Stderr {
		t.Fatalf("expected the second SetLogWriter call to restore os.Stderr, got %v", writers[1])
	}
	if _, err := os.Stat(tui.LogFilePath(dir)); err != nil {
		t.Fatalf("expected the TUI log file to have been created: %v", err)
	}
}

// TestRunUp_RendererSelection_NoOverride exercises the actual auto-pick
// (no deps.RunUI supplied) between runPlainRenderer and newTUIRenderer:
// TTY+no --no-tui must reach tuiRunFunc, the other two must not — all
// without ever launching a real bubbletea Program, by substituting
// tuiRunFunc itself (the seam newTUIRenderer calls through).
func TestRunUp_RendererSelection_NoOverride(t *testing.T) {
	dir := t.TempDir()
	path := writeUpConfig(t, dir, `{"servers": [{"name":"notes","kind":"http","url":"http://127.0.0.1:8765/mcp"}]}`)
	withCapturedStdout(t)
	t.Setenv("MCPWARP_TOKEN", "mcpwarp_pat_abc123")

	cases := []struct {
		name    string
		noTUI   bool
		isTTY   bool
		wantTUI bool
	}{
		{"no-tui flag forces plain", true, true, false},
		{"non-TTY auto-degrades to plain", false, false, false},
		{"TTY without --no-tui reaches the TUI seam", false, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shutdown.ResetForTests()
			t.Cleanup(shutdown.ResetForTests)

			origTUIRun := tuiRunFunc
			t.Cleanup(func() { tuiRunFunc = origTUIRun })
			var tuiCalled atomic.Bool
			tuiRunFunc = func(ctx context.Context, _ *eventbus.Bus, _ []tui.Server, _ tui.Controller, _ <-chan tui.SnapshotMsg) (bool, error) {
				tuiCalled.Store(true)
				<-ctx.Done()
				return false, nil
			}

			upCtx, cancelUp := context.WithCancel(context.Background())
			ctx := &Context{ConfigPath: path, HomeDir: dir, Log: NewLogger(false), Ctx: upCtx}

			done := make(chan error, 1)
			go func() {
				done <- runUp(ctx, tc.noTUI, upDeps{
					BlockForever: func(c context.Context) { <-c.Done() },
					IsTTY:        func() bool { return tc.isTTY },
					StartTunnel: func(context.Context, tunnel.Config) (tunnelHandle, error) {
						return &fakeTunnelHandle{}, nil
					},
				})
			}()

			// No explicit "reached RunUI" signal here (unlike the override
			// test above) since the plain path's own runUI has none — wait
			// instead for runUp to have registered its two shutdown
			// handlers (done just before the renderer goroutine is
			// launched), a real condition rather than a fixed sleep that
			// RunHandlersForTests below would otherwise race ahead of.
			waitDeadline := time.Now().Add(2 * time.Second)
			for shutdown.HandlerCount() < 2 {
				if time.Now().After(waitDeadline) {
					t.Fatal("timed out waiting for runUp to register its shutdown handlers")
				}
				time.Sleep(time.Millisecond)
			}
			cancelUp()
			shutdown.RunHandlersForTests(context.Background())
			<-done

			if got := tuiCalled.Load(); got != tc.wantTUI {
				t.Fatalf("tuiRunFunc invoked = %v, want %v", got, tc.wantTUI)
			}
		})
	}
}
