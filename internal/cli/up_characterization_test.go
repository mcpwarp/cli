package cli

// Characterization tests for `mcpwarp up` against internal/fakeedge. They pin
// today's user-visible behaviour at the level that has to survive replacing
// internal/tunnel/registry/relay/appproto with ws-mixer-go's
// mcpwarp.ServiceGroup: what the edge sees on the wire, the eventbus events
// a renderer consumes (DESIGN.md §3), the rows the TUI is handed (tuiRunFunc's
// seeded servers and SnapshotMsg updates, §9), the plain renderer's lines for
// that same event stream, relayed responses, and the process exit code.
// Nothing here reads the tunnel's registry or other internals. The seams
// used are runUp's own: upDeps.FatalExit (which up.go hands to the tunnel,
// so a fatal disconnect is recorded instead of exiting the test binary),
// upDeps.Reconnect (a short backoff cap, so a redial ws-mixer-go schedules at
// random(0, cap) lands in well under a second), and tuiRunFunc, the TUI's
// real input.

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mcpwarp/cli/internal/bridge"
	"github.com/mcpwarp/cli/internal/eventbus"
	"github.com/mcpwarp/cli/internal/fakeedge"
	"github.com/mcpwarp/cli/internal/output"
	"github.com/mcpwarp/cli/internal/shutdown"
	"github.com/mcpwarp/cli/internal/tui"
	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

const (
	charToken  = "mcpwarp_pat_characterization"
	charWebURL = "https://web.example.test"

	// charWait bounds every wait on a bus event or TUI row. Rows reach the
	// TUI through a once-a-second registry poll (DESIGN.md §9), so a row
	// change can lag its cause by up to a second.
	charWait = 5 * time.Second
	// charReconnectWait covers one reconnect under charReconnect.
	charReconnectWait = 5 * time.Second
	// charNeverWindow is how long a "this never happens" check watches.
	charNeverWindow = 300 * time.Millisecond

	// charSentinelLog is what the tunnel logs for fakeedge.Conn.Sentinel's
	// unknown op (DESIGN.md §8's ignored-with-log), the fence that every
	// app message sent before it has been handled.
	charSentinelLog = "ignoring unrecognized mcpwarp app message"

	charHTTPServers = `{"name":"notes","kind":"http","url":"http://127.0.0.1:1/mcp"},
		{"name":"docs","kind":"http","url":"http://127.0.0.1:2/mcp"}`
)

// charReconnect keeps every redial, including a 4014's at random(0, Cap),
// under a second.
var charReconnect = wsmixer.ReconnectOptions{Base: 50 * time.Millisecond, Cap: 500 * time.Millisecond}

// charBuffer is a goroutine-safe io.Writer whose writes wake the harness's
// waiters.
type charBuffer struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	onWrite func()
}

func (b *charBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	n, err := b.buf.Write(p)
	b.mu.Unlock()
	if b.onWrite != nil {
		b.onWrite()
	}
	return n, err
}

func (b *charBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type charSetup struct {
	// servers is the body of the config's "servers" array.
	servers string
	// onApp answers the edge's app messages; nil replies "registered"
	// accepting every name of each register while autoReply is set.
	onApp func(h *charHarness, c *fakeedge.Conn, f fakeedge.Frame)
	// authenticate is fakeedge.Options.Authenticate.
	authenticate func(attempt int, hello *wsmixer.Hello) error
	// plainMode runs the real plain renderer (`--no-tui`) instead of the
	// TUI seam; the harness then sees stdout only, no events or rows.
	plainMode bool
	// onBridgeStarted is upDeps.OnBridgeStarted.
	onBridgeStarted func(name string, br *bridge.Bridge)
}

// charHarness runs one in-process `mcpwarp up` against a fake edge and
// records everything a user could see.
type charHarness struct {
	t    *testing.T
	edge *fakeedge.Server

	// autoReply gates charSetup's default reply, so a test can hold a
	// register's reply and observe the row pending in between.
	autoReply atomic.Bool

	mu        sync.Mutex
	changed   chan struct{}
	control   []any
	telemetry []eventbus.LogLine
	metrics   []eventbus.MetricSample
	seeded    []tui.Server
	ctrl      tui.Controller
	uiStarted bool
	snapSeq   int
	rows      map[string]tui.Server
	fatal     []int

	plain  *charBuffer // RunPlain's lines for the recorded Control stream
	stdout *charBuffer
	stderr *charBuffer
	logs   *charBuffer

	upDone       chan error
	shutdownOnce sync.Once
}

func startCharacterization(t *testing.T, setup charSetup) *charHarness {
	t.Helper()
	t.Setenv("MCPWARP_TOKEN", charToken)
	t.Setenv("MCPWARP_WEB_URL", charWebURL)
	shutdown.ResetForTests()
	t.Cleanup(shutdown.ResetForTests)
	t.Cleanup(bridge.KillAllLiveChildren)

	h := &charHarness{
		t:       t,
		changed: make(chan struct{}),
		rows:    make(map[string]tui.Server),
		upDone:  make(chan error, 1),
	}
	h.autoReply.Store(true)
	h.plain = &charBuffer{onWrite: h.notify}
	h.stdout = &charBuffer{onWrite: h.notify}
	h.stderr = &charBuffer{onWrite: h.notify}
	h.logs = &charBuffer{onWrite: h.notify}

	onApp := setup.onApp
	if onApp == nil {
		onApp = charReplyAll
	}
	h.edge = fakeedge.New(t, fakeedge.Options{
		OnApp:        func(c *fakeedge.Conn, f fakeedge.Frame) { onApp(h, c, f) },
		Authenticate: setup.authenticate,
	})

	origStdout, origStderr := output.Stdout, output.Stderr
	output.Stdout, output.Stderr = h.stdout, h.stderr
	t.Cleanup(func() { output.Stdout, output.Stderr = origStdout, origStderr })
	if !setup.plainMode {
		origTUIRun := tuiRunFunc
		tuiRunFunc = h.runTUI
		t.Cleanup(func() { tuiRunFunc = origTUIRun })
	}

	dir := t.TempDir()
	path := writeUpConfig(t, dir, `{"servers": [`+setup.servers+`]}`)
	upCtx, cancelUp := context.WithCancel(context.Background())
	ctx := &Context{
		ConfigPath:         path,
		HomeDir:            dir,
		ConnectURLOverride: h.edge.URL,
		// NewLogger's handler and level, written where the test can read it.
		Log: slog.New(slog.NewJSONHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
		Ctx: upCtx,
	}
	go func() {
		h.upDone <- runUp(ctx, setup.plainMode, upDeps{
			BlockForever:    func(c context.Context) { <-c.Done() },
			IsTTY:           func() bool { return !setup.plainMode },
			FatalExit:       h.fatalExit,
			Reconnect:       charReconnect,
			OnBridgeStarted: setup.onBridgeStarted,
		})
		h.notify()
	}()
	t.Cleanup(func() {
		h.shutdown()
		cancelUp()
		select {
		case <-h.upDone:
		case <-time.After(charWait):
			t.Error("runUp did not return after shutdown and cancellation")
		}
	})

	// runUp registers its shutdown handlers before starting the renderer,
	// so once the renderer runs, h.shutdown has something to run.
	if setup.plainMode {
		h.waitStdout("connected as personal access token")
	} else {
		h.waitFor("the TUI to start", func() bool {
			h.mu.Lock()
			defer h.mu.Unlock()
			return h.uiStarted
		})
	}
	return h
}

// charReplyAll is charSetup's default edge: every register is accepted
// whole (fakeedge.Accepted) while autoReply is set.
func charReplyAll(h *charHarness, c *fakeedge.Conn, f fakeedge.Frame) {
	if f.Op == "register" && h.autoReply.Load() {
		_ = c.Registered(fakeedge.Accepted(f.Names...), nil)
	}
}

// runTUI stands in for tui.Run: it records the seeded rows, the
// Controller, every SnapshotMsg and every bus event, and feeds the same
// Control/Telemetry stream to output.RunPlain so a test can read the plain
// renderer's lines for it. Like the real renderer it drains Control until
// the bus closes (runUIFunc's contract).
func (h *charHarness) runTUI(_ context.Context, bus *eventbus.Bus, servers []tui.Server, ctrl tui.Controller, updates <-chan tui.SnapshotMsg) (bool, error) {
	h.record(func() {
		h.seeded = append([]tui.Server(nil), servers...)
		h.ctrl = ctrl
		h.uiStarted = true
	})

	plainBus := eventbus.New(64)
	plainDone := make(chan struct{})
	go func() {
		defer close(plainDone)
		_ = output.RunPlain(context.Background(), plainBus, h.plain)
	}()
	defer func() {
		plainBus.Close()
		<-plainDone
	}()

	control, telemetry, metrics := bus.Control, bus.Telemetry, bus.Metrics
	for control != nil {
		select {
		case evt, ok := <-control:
			if !ok {
				control = nil
				continue
			}
			h.record(func() { h.control = append(h.control, evt) })
			plainBus.Publish(evt)
		case line, ok := <-telemetry:
			if !ok {
				telemetry = nil
				continue
			}
			h.record(func() { h.telemetry = append(h.telemetry, line) })
			plainBus.PublishTelemetry(line)
		case m, ok := <-metrics:
			if !ok {
				metrics = nil
				continue
			}
			h.record(func() { h.metrics = append(h.metrics, m) })
		case snap, ok := <-updates:
			if !ok {
				updates = nil
				continue
			}
			h.record(func() {
				h.snapSeq++
				h.rows = make(map[string]tui.Server, len(snap.Servers))
				for _, s := range snap.Servers {
					h.rows[s.Name] = s
				}
			})
		}
	}
	return false, nil
}

func (h *charHarness) fatalExit(code int) {
	h.record(func() { h.fatal = append(h.fatal, code) })
}

func (h *charHarness) record(fn func()) {
	h.mu.Lock()
	fn()
	h.mu.Unlock()
	h.notify()
}

func (h *charHarness) notify() {
	h.mu.Lock()
	close(h.changed)
	h.changed = make(chan struct{})
	h.mu.Unlock()
}

// shutdown runs the handlers runUp registered — what a SIGINT or FatalExit
// runs (DESIGN.md §3) — once. When it returns the renderer has drained the
// closed bus, so every event the run produced is recorded.
func (h *charHarness) shutdown() {
	h.shutdownOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), shutdown.Deadline)
		defer cancel()
		shutdown.RunHandlersForTests(ctx)
	})
}

func (h *charHarness) waitFor(what string, cond func() bool) {
	h.t.Helper()
	h.waitForWithin(what, charWait, cond)
}

func (h *charHarness) waitForWithin(what string, d time.Duration, cond func() bool) {
	h.t.Helper()
	deadline := time.NewTimer(d)
	defer deadline.Stop()
	for {
		h.mu.Lock()
		changed := h.changed
		h.mu.Unlock()
		if cond() {
			return
		}
		select {
		case <-changed:
		case <-deadline.C:
			h.t.Fatalf("timed out after %v waiting for %s\n%s", d, what, h.describe())
		}
	}
}

func (h *charHarness) describe() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var b strings.Builder
	fmt.Fprintf(&b, "control events:")
	for _, e := range h.control {
		fmt.Fprintf(&b, "\n  %#v", e)
	}
	fmt.Fprintf(&b, "\nseeded rows: %+v\nlatest rows (snapshot %d): %+v\nfatal exits: %v", h.seeded, h.snapSeq, h.rows, h.fatal)
	fmt.Fprintf(&b, "\nplain:\n%s\nstdout:\n%s\nstderr:\n%s\nlogs:\n%s", h.plain.String(), h.stdout.String(), h.stderr.String(), h.logs.String())
	return b.String()
}

// row is the TUI's view of name: the latest SnapshotMsg's row, or the
// seeded one until a snapshot carries it.
func (h *charHarness) row(name string) (tui.Server, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if r, ok := h.rows[name]; ok {
		return r, true
	}
	for _, r := range h.seeded {
		if r.Name == name {
			return r, true
		}
	}
	return tui.Server{}, false
}

func (h *charHarness) snapshotSeq() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.snapSeq
}

// waitRow waits until name's row satisfies ok.
func (h *charHarness) waitRow(name, what string, ok func(tui.Server) bool) tui.Server {
	h.t.Helper()
	return h.waitRowAfter(-1, name, what, ok)
}

// waitRowAfter is waitRow counting only snapshots newer than seq — for a
// row that must still read the same after something happened.
func (h *charHarness) waitRowAfter(seq int, name, what string, ok func(tui.Server) bool) tui.Server {
	h.t.Helper()
	var got tui.Server
	h.waitFor(what, func() bool {
		if h.snapshotSeq() <= seq {
			return false
		}
		r, found := h.row(name)
		got = r
		return found && ok(r)
	})
	return got
}

func charActive(r tui.Server) bool {
	return r.Registration == tui.RegistrationActive && r.URL == "https://"+r.Name+".example/mcp"
}

func (h *charHarness) appErrors(code string) []eventbus.AppError {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []eventbus.AppError
	for _, e := range h.control {
		if ae, ok := e.(eventbus.AppError); ok && (code == "" || ae.Code == code) {
			out = append(out, ae)
		}
	}
	return out
}

func (h *charHarness) waitAppError(code, service string) eventbus.AppError {
	h.t.Helper()
	var got eventbus.AppError
	h.waitFor("an AppError "+code+" for "+service, func() bool {
		for _, ae := range h.appErrors(code) {
			if ae.Service == service {
				got = ae
				return true
			}
		}
		return false
	})
	return got
}

func (h *charHarness) connStates() []eventbus.ConnStateChanged {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []eventbus.ConnStateChanged
	for _, e := range h.control {
		if cs, ok := e.(eventbus.ConnStateChanged); ok {
			out = append(out, cs)
		}
	}
	return out
}

// waitConnState waits for the n'th ConnStateChanged with state (1-based).
func (h *charHarness) waitConnState(state string, n int) eventbus.ConnStateChanged {
	h.t.Helper()
	return h.waitConnStateWithin(state, n, charWait)
}

func (h *charHarness) waitConnStateWithin(state string, n int, d time.Duration) eventbus.ConnStateChanged {
	h.t.Helper()
	var got eventbus.ConnStateChanged
	h.waitForWithin(fmt.Sprintf("ConnStateChanged %q #%d", state, n), d, func() bool {
		seen := 0
		for _, cs := range h.connStates() {
			if cs.State == state {
				seen++
				if seen == n {
					got = cs
					return true
				}
			}
		}
		return false
	})
	return got
}

func (h *charHarness) serverStates(name string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, e := range h.control {
		if sc, ok := e.(eventbus.ServerStateChanged); ok && sc.Name == name {
			out = append(out, sc.State)
		}
	}
	return out
}

func (h *charHarness) fatalCodes() []int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]int(nil), h.fatal...)
}

func (h *charHarness) waitFatal() int {
	h.t.Helper()
	h.waitForWithin("a fatal exit", charReconnectWait, func() bool { return len(h.fatalCodes()) > 0 })
	return h.fatalCodes()[0]
}

// expectNoFatal watches for a fatal exit for charNeverWindow: the tunnel
// fires it on its own goroutine, so there is no event to fence on.
func (h *charHarness) expectNoFatal(why string) {
	h.t.Helper()
	deadline := time.Now().Add(charNeverWindow)
	for time.Now().Before(deadline) {
		if codes := h.fatalCodes(); len(codes) > 0 {
			h.t.Fatalf("unexpected fatal exit %v: %s", codes, why)
		}
		time.Sleep(time.Millisecond)
	}
}

func (h *charHarness) waitPlain(substr string) {
	h.t.Helper()
	h.waitFor("plain line containing "+substr, func() bool { return strings.Contains(h.plain.String(), substr) })
}

func (h *charHarness) waitStdout(substr string) {
	h.t.Helper()
	h.waitFor("stdout containing "+substr, func() bool { return strings.Contains(h.stdout.String(), substr) })
}

func (h *charHarness) waitLog(substr string) {
	h.t.Helper()
	h.waitFor("a log line containing "+substr, func() bool { return strings.Contains(h.logs.String(), substr) })
}

func (h *charHarness) controller() tui.Controller {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ctrl
}

// forceReconnect ends the live connection with a plain INTERNAL_ERROR
// close, which the client redials after normal backoff.
func (h *charHarness) forceReconnect() {
	h.t.Helper()
	c := h.edge.Latest()
	if c == nil {
		h.t.Fatal("no connection to close")
	}
	_ = c.Close(uint32(wsmixer.InternalErrorCode), "forcing a reconnect for test")
}

func TestUpCharacterization_RegisterSuccessActiveWithURL(t *testing.T) {
	h := startCharacterization(t, charSetup{servers: charHTTPServers})

	for _, name := range []string{"notes", "docs"} {
		r, ok := h.row(name)
		if !ok || r.Registration != tui.RegistrationPending || r.State != tui.RegistrationPending || r.Kind != "http" || r.URL != "" {
			t.Fatalf("seeded %s row = %+v (found %v), want an http row pending with no URL", name, r, ok)
		}
	}
	if got := h.edge.WaitRegister(t, 1); strings.Join(got, ",") != "notes,docs" {
		t.Fatalf("first register = %v, want every configured server in config order", got)
	}

	connected := h.waitConnState("connected", 1)
	if connected.Session == "" {
		t.Fatalf("ConnStateChanged = %+v, want the welcome's session", connected)
	}
	for _, name := range []string{"notes", "docs"} {
		r := h.waitRow(name, name+" active", charActive)
		if r.State != tui.RegistrationActive || r.Kind != "http" {
			t.Fatalf("%s row = %+v, want an http row whose State is its registration", name, r)
		}
	}
	h.waitPlain("✓ connection connected session=" + connected.Session)
	h.waitFor("a bytes_total metric", func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		for _, m := range h.metrics {
			if m.Kind == "bytes_total" {
				return true
			}
		}
		return false
	})
	if errs := h.appErrors(""); len(errs) != 0 {
		t.Fatalf("AppErrors for a clean registration: %+v", errs)
	}
	h.expectNoFatal("a clean registration")
}

func TestUpCharacterization_PlainModeRegisterSuccess(t *testing.T) {
	h := startCharacterization(t, charSetup{servers: charHTTPServers, plainMode: true})

	h.edge.WaitRegister(t, 1)
	h.waitStdout("✓ connection connected session=")
	out := h.stdout.String()
	for _, want := range []string{"NAME", "notes", "http://127.0.0.1:1/mcp", "docs", "http://127.0.0.1:2/mcp", "✓ connected as personal access token"} {
		if !strings.Contains(out, want) {
			t.Fatalf("plain stdout missing %q:\n%s", want, out)
		}
	}
	if strings.Index(out, "connected as") > strings.Index(out, "connection connected") {
		t.Fatalf("plain stdout prints the connection line before \"connected as\":\n%s", out)
	}
}

func TestUpCharacterization_RegisterQuotaExceededRejected(t *testing.T) {
	h := startCharacterization(t, charSetup{
		servers: charHTTPServers,
		onApp: func(h *charHarness, c *fakeedge.Conn, f fakeedge.Frame) {
			if f.Op == "register" {
				_ = c.Registered(fakeedge.Accepted("notes"), []fakeedge.RegisterError{
					{Name: "docs", Code: "QUOTA_EXCEEDED", Message: "server limit reached"},
				})
			}
		},
	})

	ae := h.waitAppError("QUOTA_EXCEEDED", "docs")
	wantHint := "upgrade your plan at " + charWebURL + "/settings to add more servers"
	if ae.Message != "server limit reached" || ae.Hint != wantHint {
		t.Fatalf("AppError = %+v, want the edge's message and hint %q", ae, wantHint)
	}
	h.waitRow("docs", "docs rejected", func(r tui.Server) bool {
		return r.Registration == tui.RegistrationRejected && r.State == tui.RegistrationRejected && r.URL == "" && r.Kind == "http"
	})
	h.waitRow("notes", "notes active", charActive)
	h.waitPlain("✗ QUOTA_EXCEEDED: server limit reached service=docs · " + wantHint)
	h.expectNoFatal("a per-service refusal alongside a success")
}

func TestUpCharacterization_RegisterConflictRejected(t *testing.T) {
	h := startCharacterization(t, charSetup{
		servers: charHTTPServers,
		onApp: func(h *charHarness, c *fakeedge.Conn, f fakeedge.Frame) {
			if f.Op == "register" {
				_ = c.Registered(fakeedge.Accepted("notes"), []fakeedge.RegisterError{
					{Name: "docs", Code: "CONFLICT", Message: "name in use"},
				})
			}
		},
	})

	ae := h.waitAppError("CONFLICT", "docs")
	const wantHint = "a server with this name is already registered — rename it, or check its kind"
	if ae.Message != "name in use" || ae.Hint != wantHint {
		t.Fatalf("AppError = %+v, want the edge's message and hint %q", ae, wantHint)
	}
	h.waitRow("docs", "docs rejected", func(r tui.Server) bool {
		return r.Registration == tui.RegistrationRejected && r.URL == ""
	})
	h.waitRow("notes", "notes active", charActive)
	h.waitPlain("✗ CONFLICT: name in use service=docs · " + wantHint)
	h.expectNoFatal("a per-service refusal alongside a success")
}

func TestUpCharacterization_DashboardEnablePendingThenActive(t *testing.T) {
	h := startCharacterization(t, charSetup{servers: charHTTPServers})
	h.waitRow("notes", "notes active", charActive)
	c := h.edge.Latest()

	_ = c.Disable("id-notes", "paused in the dashboard")
	h.waitRow("notes", "notes disabled", func(r tui.Server) bool {
		return r.Registration == tui.RegistrationDisabled && r.State == tui.RegistrationDisabled
	})

	h.autoReply.Store(false)
	_ = c.Enable("id-notes", "notes")
	if got := h.edge.WaitRegister(t, 2); strings.Join(got, ",") != "notes" {
		t.Fatalf("register after the dashboard enable = %v, want [notes]", got)
	}
	h.waitRow("notes", "notes pending until the re-register's reply", func(r tui.Server) bool {
		return r.Registration == tui.RegistrationPending && r.State == tui.RegistrationPending
	})
	_ = c.Registered(fakeedge.Accepted("notes"), nil)
	h.waitRow("notes", "notes active again", charActive)
	h.waitRow("docs", "docs untouched", charActive)
}

func TestUpCharacterization_LocalDisableEnableViaController(t *testing.T) {
	h := startCharacterization(t, charSetup{servers: charHTTPServers})
	h.waitRow("docs", "docs active", charActive)

	h.controller().Disable("docs")
	unregister := h.edge.WaitEvent(t, "an unregister", func(e fakeedge.Event) bool {
		return e.Kind == fakeedge.EventApp && e.Frame.Op == "unregister"
	})
	if strings.Join(unregister.Frame.Names, ",") != "docs" {
		t.Fatalf("unregister after d = %v, want [docs]", unregister.Frame.Names)
	}
	h.waitRow("docs", "docs disabled", func(r tui.Server) bool {
		return r.Registration == tui.RegistrationDisabled && r.State == tui.RegistrationDisabled
	})

	h.forceReconnect()
	h.edge.WaitUntilFor(t, "the reconnect's register", charReconnectWait, func() bool {
		return len(h.edge.RegisterBatches()) >= 2
	})
	if got := h.edge.RegisterBatches()[1]; strings.Join(got, ",") != "notes" {
		t.Fatalf("register after the reconnect = %v, want docs left out: [notes]", got)
	}
	seq := h.snapshotSeq()
	h.waitConnState("connected", 2)
	h.waitRowAfter(seq, "docs", "docs still disabled after the reconnect", func(r tui.Server) bool {
		return r.Registration == tui.RegistrationDisabled
	})

	h.autoReply.Store(false)
	h.controller().Enable("docs")
	if got := h.edge.WaitRegister(t, 3); strings.Join(got, ",") != "docs" {
		t.Fatalf("register after e = %v, want [docs]", got)
	}
	h.waitRow("docs", "docs pending until e's reply", func(r tui.Server) bool {
		return r.Registration == tui.RegistrationPending && r.State == tui.RegistrationPending
	})
	_ = h.edge.Latest().Registered(fakeedge.Accepted("docs"), nil)
	h.waitRow("docs", "docs active after e", charActive)
}

func TestUpCharacterization_ConflictOpStickyUntilLocalEnable(t *testing.T) {
	const takenOverHint = "another `mcpwarp up` on this account now serves it; stop that one or rename this server, then press e (or restart this `mcpwarp up`)"
	h := startCharacterization(t, charSetup{servers: charHTTPServers})
	h.waitRow("docs", "docs active", charActive)
	c := h.edge.Latest()

	_ = c.Conflict("id-docs", "docs", "taken_over")
	ae := h.waitAppError("CONFLICT", "docs")
	if !strings.Contains(ae.Message, "taken_over") || ae.Hint != takenOverHint {
		t.Fatalf("AppError = %+v, want the reason in its message and the take-over hint", ae)
	}
	h.waitRow("docs", "docs rejected", func(r tui.Server) bool {
		return r.Registration == tui.RegistrationRejected && r.URL == ""
	})
	h.waitPlain("✗ CONFLICT: " + ae.Message + " service=docs · " + takenOverHint)

	// A dashboard enable for a taken-over name is ignored: the sentinel
	// behind it is logged only once the enable has been handled, and no
	// register went out for it.
	_ = c.Enable("id-docs", "docs")
	_ = c.Sentinel()
	h.waitLog(charSentinelLog)
	if n := len(h.edge.RegisterBatches()); n != 1 {
		t.Fatalf("a dashboard enable re-registered a taken-over name: %v", h.edge.RegisterBatches())
	}

	h.forceReconnect()
	h.edge.WaitUntilFor(t, "the reconnect's register", charReconnectWait, func() bool {
		return len(h.edge.RegisterBatches()) >= 2
	})
	if got := h.edge.RegisterBatches()[1]; strings.Join(got, ",") != "notes" {
		t.Fatalf("register after the reconnect = %v, want the taken-over docs left out: [notes]", got)
	}
	h.waitConnState("connected", 2)
	seq := h.snapshotSeq()
	h.waitRowAfter(seq, "docs", "docs still rejected after the reconnect", func(r tui.Server) bool {
		return r.Registration == tui.RegistrationRejected && r.URL == ""
	})

	h.controller().Enable("docs")
	if got := h.edge.WaitRegister(t, 3); strings.Join(got, ",") != "docs" {
		t.Fatalf("register after e = %v, want [docs]", got)
	}
	h.waitRow("docs", "docs claimed back by e", charActive)
	h.expectNoFatal("a take-over")
}

func TestUpCharacterization_ConnectionLimitNotFatalAndReconnects(t *testing.T) {
	const reason = "CONNECTION_LIMIT: too many agent connections for this account (limit 10)"
	h := startCharacterization(t, charSetup{
		servers: charHTTPServers,
		onApp: func(h *charHarness, c *fakeedge.Conn, f fakeedge.Frame) {
			if f.Op != "register" {
				return
			}
			if c.Index > 1 {
				_ = c.Registered(fakeedge.Accepted(f.Names...), nil)
				return
			}
			// The tunnel's cap: an error op, then a 4014 close carrying the
			// same text. Off the delivery goroutine, in that order.
			go func() {
				_ = c.SendError("CONNECTION_LIMIT", reason)
				_ = c.Close(uint32(wsmixer.ApplicationCloseCode), reason)
			}()
		},
	})

	const wantHint = "close another `mcpwarp up` and this one will reconnect"
	ae := h.waitAppError("CONNECTION_LIMIT", "")
	if ae.Message != "too many agent connections for this account (limit 10)" || ae.Hint != wantHint {
		t.Fatalf("AppError = %+v, want the reason without its prefix and hint %q", ae, wantHint)
	}
	h.waitConnState("disconnected", 1)
	h.waitPlain("✗ CONNECTION_LIMIT: too many agent connections for this account (limit 10) · " + wantHint)

	// ws-mixer-go schedules a 4014 redial at random(0, cap): 30s in
	// production (DESIGN.md §8), charReconnect's 500ms here.
	h.waitConnStateWithin("connected", 2, charReconnectWait)
	for _, name := range []string{"notes", "docs"} {
		h.waitRow(name, name+" active after the reconnect", charActive)
	}
	if n := len(h.appErrors("CONNECTION_LIMIT")); n != 1 {
		t.Fatalf("got %d CONNECTION_LIMIT AppErrors for one error op plus its close, want 1", n)
	}
	if codes := h.fatalCodes(); len(codes) != 0 {
		t.Fatalf("CONNECTION_LIMIT exited %v, want it non-fatal", codes)
	}
}

func TestUpCharacterization_TooManyServicesRejectsEveryRow(t *testing.T) {
	const message = "register carries 2 services, max 1 per message"
	h := startCharacterization(t, charSetup{
		servers: charHTTPServers,
		onApp: func(h *charHarness, c *fakeedge.Conn, f fakeedge.Frame) {
			if f.Op == "register" {
				_ = c.SendError("TOO_MANY_SERVICES", message)
			}
		},
	})

	const wantHint = "the tunnel refused this many servers in one register; remove some from the config"
	ae := h.waitAppError("TOO_MANY_SERVICES", "")
	if ae.Message != message || ae.Hint != wantHint {
		t.Fatalf("AppError = %+v, want the edge's message and hint %q", ae, wantHint)
	}
	for _, name := range []string{"notes", "docs"} {
		h.waitRow(name, name+" rejected", func(r tui.Server) bool {
			return r.Registration == tui.RegistrationRejected && r.URL == "" && r.Kind == "http"
		})
	}
	h.waitPlain("✗ TOO_MANY_SERVICES: " + message + " · " + wantHint)
	h.expectNoFatal("TOO_MANY_SERVICES, even on the first connection")
	if n := len(h.appErrors("")); n != 1 {
		t.Fatalf("got %d AppErrors, want one for the whole batch: %+v", n, h.appErrors(""))
	}
}

func TestUpCharacterization_FirstReplyAllRefusedIsFatal(t *testing.T) {
	h := startCharacterization(t, charSetup{
		servers: charHTTPServers,
		onApp: func(h *charHarness, c *fakeedge.Conn, f fakeedge.Frame) {
			if f.Op == "register" {
				_ = c.Registered(nil, []fakeedge.RegisterError{
					{Name: "notes", Code: "QUOTA_EXCEEDED", Message: "server limit reached"},
					{Name: "docs", Code: "CONFLICT", Message: "name in use"},
				})
			}
		},
	})

	if code := h.waitFatal(); code != 1 {
		t.Fatalf("fatal exit code = %d, want 1", code)
	}
	h.waitAppError("QUOTA_EXCEEDED", "notes")
	h.waitAppError("CONFLICT", "docs")
	for _, name := range []string{"notes", "docs"} {
		h.waitRow(name, name+" rejected", func(r tui.Server) bool {
			return r.Registration == tui.RegistrationRejected && r.URL == ""
		})
	}
}

func TestUpCharacterization_FirstReplyDisabledAndQuotaNotFatal(t *testing.T) {
	h := startCharacterization(t, charSetup{
		servers: charHTTPServers,
		onApp: func(h *charHarness, c *fakeedge.Conn, f fakeedge.Frame) {
			if f.Op == "register" {
				_ = c.Registered(nil, []fakeedge.RegisterError{
					{Name: "notes", Code: "SERVER_DISABLED", Message: "disabled in dashboard"},
					{Name: "docs", Code: "QUOTA_EXCEEDED", Message: "server limit reached"},
				})
			}
		},
	})

	disabled := h.waitAppError("SERVER_DISABLED", "notes")
	if disabled.Hint != "this server was disabled in the dashboard — waiting, toggle it on to resume" {
		t.Fatalf("SERVER_DISABLED AppError = %+v", disabled)
	}
	h.waitAppError("QUOTA_EXCEEDED", "docs")
	h.expectNoFatal("a first reply mixing SERVER_DISABLED with QUOTA_EXCEEDED")
	h.waitRow("notes", "notes disabled", func(r tui.Server) bool {
		return r.Registration == tui.RegistrationDisabled && r.URL == ""
	})
	h.waitRow("docs", "docs rejected", func(r tui.Server) bool {
		return r.Registration == tui.RegistrationRejected && r.URL == ""
	})
}

func TestUpCharacterization_UnauthorizedAfterRefreshRetryIsFatal(t *testing.T) {
	h := startCharacterization(t, charSetup{
		servers: charHTTPServers,
		authenticate: func(attempt int, _ *wsmixer.Hello) error {
			if attempt == 1 {
				return nil
			}
			return wsmixer.Unauthorized("token no longer valid")
		},
	})
	h.waitRow("notes", "notes active", charActive)

	h.forceReconnect()
	if code := h.waitFatal(); code != 1 {
		t.Fatalf("fatal exit code = %d, want 1", code)
	}
	refused := 0
	for _, e := range h.edge.Events() {
		if e.Kind == fakeedge.EventRefused {
			refused++
		}
	}
	if refused != 2 {
		t.Fatalf("edge refused %d handshakes before the fatal exit, want 2 (the redial and its one token-refresh retry)", refused)
	}
	h.waitLog("session expired, run `mcpwarp login`")
	h.waitConnState("disconnected", 1)
}

func TestUpCharacterization_UnsupportedVersionIsFatal(t *testing.T) {
	h := startCharacterization(t, charSetup{
		servers: charHTTPServers,
		onApp: func(h *charHarness, c *fakeedge.Conn, f fakeedge.Frame) {
			if f.Op == "register" {
				_ = c.SendError("UNSUPPORTED_VERSION", "upgrade")
			}
		},
	})

	if code := h.waitFatal(); code != 1 {
		t.Fatalf("fatal exit code = %d, want 1", code)
	}
	h.waitLog("this mcpwarp CLI speaks a protocol version the tunnel no longer supports; upgrade mcpwarp")
	h.shutdown()
	if errs := h.appErrors(""); len(errs) != 0 {
		t.Fatalf("UNSUPPORTED_VERSION published AppErrors %+v; today it reaches only the log", errs)
	}
	if r, _ := h.row("notes"); r.Registration != tui.RegistrationPending {
		t.Fatalf("notes row = %+v, want still pending (no reply ever came)", r)
	}
}

// relay sends a GET for host's /mcp over a stream the edge opens on its
// latest connection, as it routes a public request (DESIGN.md §3: OPEN
// carries no metadata, routed by Host), and returns the response.
func (h *charHarness) relay(host string) (status int, body string) {
	h.t.Helper()
	c := h.edge.Latest()
	if c == nil {
		h.t.Fatal("no connection to open a stream on")
	}
	ctx, cancel := context.WithTimeout(context.Background(), charWait)
	defer cancel()
	stream, err := c.OpenStream(ctx)
	if err != nil {
		h.t.Fatalf("OpenStream: %v", err)
	}
	fmt.Fprintf(stream, "GET /mcp HTTP/1.1\r\nHost: %s\r\n\r\n", host)
	_ = stream.CloseWrite()
	resp, err := http.ReadResponse(bufio.NewReader(stream), nil)
	if err != nil {
		h.t.Fatalf("reading the response for %s: %v", host, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		h.t.Fatalf("reading the body for %s: %v", host, err)
	}
	return resp.StatusCode, string(b)
}

func TestUpCharacterization_RelayStatuses(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "backend saw "+r.URL.Path)
	}))
	t.Cleanup(backend.Close)
	h := startCharacterization(t, charSetup{servers: `{"name":"api","kind":"http","url":"` + backend.URL + `/mcp"},
		{"name":"notes","kind":"http","url":"http://127.0.0.1:1/mcp"},
		{"name":"docs","kind":"http","url":"http://127.0.0.1:2/mcp"}`})
	for _, name := range []string{"api", "notes", "docs"} {
		h.waitRow(name, name+" active", charActive)
	}
	_ = h.edge.Latest().Disable("id-docs", "paused in the dashboard")
	h.waitRow("docs", "docs disabled", func(r tui.Server) bool { return r.Registration == tui.RegistrationDisabled })

	cases := []struct {
		host       string
		wantStatus int
		wantBody   string
	}{
		{"api.example", http.StatusOK, "backend saw /mcp"},
		{"nobody.example", http.StatusNotFound, `{"error":"unknown service"}`},
		{"docs.example", http.StatusServiceUnavailable, `{"error":"service disabled"}`},
		{"notes.example", http.StatusBadGateway, `{"error":"local server unreachable"}`},
	}
	for _, tc := range cases {
		if status, body := h.relay(tc.host); status != tc.wantStatus || body != tc.wantBody {
			t.Errorf("GET %s/mcp = %d %q, want %d %q", tc.host, status, body, tc.wantStatus, tc.wantBody)
		}
	}
}

func TestUpCharacterization_InvalidNameAndUsernameRequiredHints(t *testing.T) {
	const (
		invalidMsg  = "server name must match [a-z0-9]([a-z0-9-]*[a-z0-9])? (max 30 chars)"
		usernameMsg = "sign in to the dashboard once to choose a username"
	)
	h := startCharacterization(t, charSetup{
		servers: charHTTPServers + `,{"name":"extra","kind":"http","url":"http://127.0.0.1:3/mcp"}`,
		onApp: func(h *charHarness, c *fakeedge.Conn, f fakeedge.Frame) {
			if f.Op == "register" {
				_ = c.Registered(fakeedge.Accepted("notes"), []fakeedge.RegisterError{
					{Name: "docs", Code: "INVALID_NAME", Message: invalidMsg},
					{Name: "extra", Code: "USERNAME_REQUIRED", Message: usernameMsg},
				})
			}
		},
	})

	const (
		invalidHint  = "fix the name in your config — it is the public URL slug and must match [a-z0-9]([a-z0-9-]*[a-z0-9])? (max 30 chars)"
		usernameHint = "sign in once at " + charWebURL + " to choose a username, then run `mcpwarp up` again"
	)
	if ae := h.waitAppError("INVALID_NAME", "docs"); ae.Message != invalidMsg || ae.Hint != invalidHint {
		t.Fatalf("INVALID_NAME AppError = %+v, want hint %q", ae, invalidHint)
	}
	if ae := h.waitAppError("USERNAME_REQUIRED", "extra"); ae.Message != usernameMsg || ae.Hint != usernameHint {
		t.Fatalf("USERNAME_REQUIRED AppError = %+v, want hint %q", ae, usernameHint)
	}
	h.waitPlain("✗ INVALID_NAME: " + invalidMsg + " service=docs · " + invalidHint)
	h.waitPlain("✗ USERNAME_REQUIRED: " + usernameMsg + " service=extra · " + usernameHint)
	for _, name := range []string{"docs", "extra"} {
		h.waitRow(name, name+" rejected", func(r tui.Server) bool {
			return r.Registration == tui.RegistrationRejected && r.URL == ""
		})
	}
	h.waitRow("notes", "notes active", charActive)
	h.expectNoFatal("per-service refusals alongside a success")
}

func TestUpCharacterization_ShutdownUnregistersThenDrains(t *testing.T) {
	h := startCharacterization(t, charSetup{servers: charHTTPServers})
	for _, name := range []string{"notes", "docs"} {
		h.waitRow(name, name+" active", charActive)
	}

	start := time.Now()
	h.shutdown()
	// A healthy edge acknowledges the drain at once, so the sequence must
	// not lean on shutdown.Deadline's bound.
	if took := time.Since(start); took > shutdown.Deadline/2 {
		t.Fatalf("shutdown took %v, want well under shutdown.Deadline (%v)", took, shutdown.Deadline)
	}

	// The edge delivers app messages and drains in wire order, but may do
	// so after the client has already finished closing.
	h.edge.WaitEvent(t, "the client's drain", func(e fakeedge.Event) bool { return e.Kind == fakeedge.EventDrain })
	events := h.edge.Events()
	unregisterAt, drainAt := -1, -1
	for i, e := range events {
		switch {
		case e.Kind == fakeedge.EventApp && e.Frame.Op == "unregister" && unregisterAt < 0:
			unregisterAt = i
			if strings.Join(e.Frame.Names, ",") != "notes,docs" {
				t.Fatalf("shutdown unregister = %v, want every configured server", e.Frame.Names)
			}
		case e.Kind == fakeedge.EventDrain && drainAt < 0:
			drainAt = i
			if e.DrainReason != "client_requested" {
				t.Fatalf("shutdown drain reason = %q, want client_requested", e.DrainReason)
			}
		}
	}
	if unregisterAt < 0 || drainAt < 0 || unregisterAt > drainAt {
		t.Fatalf("edge events: unregister at %d, client drain at %d; want an unregister, then the drain", unregisterAt, drainAt)
	}
	states := h.connStates()
	if last := states[len(states)-1]; last.State != "closed" {
		t.Fatalf("last ConnStateChanged = %+v, want closed", last)
	}
	if !strings.Contains(h.plain.String(), "✗ connection closed session=") {
		t.Fatalf("plain output missing the closed line:\n%s", h.plain.String())
	}
	if codes := h.fatalCodes(); len(codes) != 0 {
		t.Fatalf("a clean shutdown exited %v", codes)
	}
}
