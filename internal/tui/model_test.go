package tui

import (
	"context"
	"sync"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/mcpwarp/cli/internal/eventbus"
)

type fakeController struct {
	mu                           sync.Mutex
	restarted, disabled, enabled []string
}

func (f *fakeController) Restart(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restarted = append(f.restarted, name)
}
func (f *fakeController) Disable(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.disabled = append(f.disabled, name)
}
func (f *fakeController) Enable(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enabled = append(f.enabled, name)
}

// slowController sleeps before recording the call, standing in for a real
// Controller whose Disable does SIGTERM + a 2s grace wait.
type slowController struct {
	delay  time.Duration
	called chan string
}

func (s *slowController) Restart(name string) {
	time.Sleep(s.delay)
	s.called <- name
}
func (s *slowController) Disable(name string) {}
func (s *slowController) Enable(name string)  {}

func namedKey(t *testing.T, name string) tea.KeyPressMsg {
	t.Helper()
	switch name {
	case "up":
		return tea.KeyPressMsg{Code: tea.KeyUp}
	case "down":
		return tea.KeyPressMsg{Code: tea.KeyDown}
	case "pgup":
		return tea.KeyPressMsg{Code: tea.KeyPgUp}
	case "pgdown":
		return tea.KeyPressMsg{Code: tea.KeyPgDown}
	case "ctrl+c":
		return tea.KeyPressMsg{Code: 'c', Mod: tea.ModCtrl}
	case "ctrl+u":
		return tea.KeyPressMsg{Code: 'u', Mod: tea.ModCtrl}
	case "ctrl+d":
		return tea.KeyPressMsg{Code: 'd', Mod: tea.ModCtrl}
	default:
		if len([]rune(name)) != 1 {
			t.Fatalf("namedKey: unsupported name %q", name)
		}
		return tea.KeyPressMsg{Text: name, Code: []rune(name)[0]}
	}
}

func newTestModel(servers []Server, ctrl Controller) Model {
	bus := eventbus.New(4)
	m := New(bus, servers, ctrl)
	m.width, m.height = 80, 24
	return m
}

func TestSelectionBounds(t *testing.T) {
	m := newTestModel([]Server{{Name: "a"}, {Name: "b"}, {Name: "c"}}, nil)

	// Moving up from the first row stays put.
	next, _ := m.Update(namedKey(t, "up"))
	m = next.(Model)
	if m.cursor != 0 {
		t.Fatalf("cursor = %d, want 0", m.cursor)
	}

	// j/down moves forward, clamped at the last row.
	for i := 0; i < 5; i++ {
		next, _ = m.Update(namedKey(t, "j"))
		m = next.(Model)
	}
	if m.cursor != 2 {
		t.Fatalf("cursor = %d, want 2 (clamped)", m.cursor)
	}

	next, _ = m.Update(namedKey(t, "k"))
	m = next.(Model)
	if m.cursor != 1 {
		t.Fatalf("cursor = %d, want 1", m.cursor)
	}
}

func TestSelectionBoundsEmpty(t *testing.T) {
	m := newTestModel(nil, nil)
	next, _ := m.Update(namedKey(t, "j"))
	m = next.(Model)
	if _, ok := m.selectedName(); ok {
		t.Fatalf("selectedName should report false with no servers")
	}
}

func TestRestartDisableEnableActOnSelection(t *testing.T) {
	ctrl := &fakeController{}
	m := newTestModel([]Server{{Name: "a"}, {Name: "b"}}, ctrl)

	next, _ := m.Update(namedKey(t, "j")) // select "b"
	m = next.(Model)

	// r/d/e return a Cmd rather than calling the Controller inline (blocker
	// 1: a slow Disable must not freeze Update) — run each returned Cmd to
	// exercise the actual controller call.
	for _, key := range []string{"r", "d", "e"} {
		_, cmd := m.Update(namedKey(t, key))
		if cmd == nil {
			t.Fatalf("Update(%q) returned a nil Cmd, want the controller action", key)
		}
		cmd()
	}

	if got := ctrl.restarted; len(got) != 1 || got[0] != "b" {
		t.Fatalf("restarted = %v, want [b]", got)
	}
	if got := ctrl.disabled; len(got) != 1 || got[0] != "b" {
		t.Fatalf("disabled = %v, want [b]", got)
	}
	if got := ctrl.enabled; len(got) != 1 || got[0] != "b" {
		t.Fatalf("enabled = %v, want [b]", got)
	}
}

// TestControllerActionDoesNotBlockUpdate is blocker 1's named test: a
// Controller call that sleeps (standing in for Disable's SIGTERM + 2s grace
// wait) must not be run inline inside Update, or it would freeze the UI and
// stall the lossless Control drain (producers block in Publish).
func TestControllerActionDoesNotBlockUpdate(t *testing.T) {
	ctrl := &slowController{delay: 2 * time.Second, called: make(chan string, 1)}
	m := newTestModel([]Server{{Name: "a"}}, ctrl)

	start := time.Now()
	_, cmd := m.Update(namedKey(t, "r"))
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("Update blocked for %v, want a prompt return", elapsed)
	}
	if cmd == nil {
		t.Fatalf("expected Update to hand back a Cmd for the controller action")
	}

	go cmd()
	select {
	case name := <-ctrl.called:
		if name != "a" {
			t.Fatalf("Restart called with %q, want \"a\"", name)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("cmd() never invoked Restart")
	}
}

func TestToggleLogPane(t *testing.T) {
	m := newTestModel(nil, nil)
	if m.logsVisible {
		t.Fatalf("logsVisible should start false")
	}
	next, _ := m.Update(namedKey(t, "l"))
	m = next.(Model)
	if !m.logsVisible {
		t.Fatalf("logsVisible should be true after l")
	}
	next, _ = m.Update(namedKey(t, "l"))
	m = next.(Model)
	if m.logsVisible {
		t.Fatalf("logsVisible should be false after second l")
	}
}

func TestHelpToggle(t *testing.T) {
	m := newTestModel(nil, nil)
	next, _ := m.Update(namedKey(t, "?"))
	m = next.(Model)
	if !m.showHelp {
		t.Fatalf("showHelp should be true after ?")
	}
	// Any other key dismisses it.
	next, _ = m.Update(namedKey(t, "x"))
	m = next.(Model)
	if m.showHelp {
		t.Fatalf("showHelp should be false after a dismiss key")
	}
}

func TestQuitSetsQuitting(t *testing.T) {
	m := newTestModel(nil, nil)
	next, cmd := m.Update(namedKey(t, "q"))
	m = next.(Model)
	if !m.quitting {
		t.Fatalf("quitting should be true after q")
	}
	if cmd == nil {
		t.Fatalf("expected a tea.Quit cmd")
	}
	if msg := cmd(); msg == nil {
		t.Fatalf("expected tea.Quit's message, got nil")
	}
}

func TestQuitFromHelpOverlay(t *testing.T) {
	m := newTestModel(nil, nil)
	next, _ := m.Update(namedKey(t, "?"))
	m = next.(Model)
	next, cmd := m.Update(namedKey(t, "ctrl+c"))
	m = next.(Model)
	if !m.quitting || cmd == nil {
		t.Fatalf("ctrl+c from help overlay should quit")
	}
}

func TestApplyControlEvents(t *testing.T) {
	m := newTestModel([]Server{{Name: "svc"}}, nil)

	next, _ := m.Update(controlEventMsg{evt: eventbus.ConnStateChanged{State: "connected", Session: "sess-1", Reason: ""}})
	m = next.(Model)
	if m.connState != "connected" || m.connSession != "sess-1" {
		t.Fatalf("conn state not applied: %+v", m)
	}

	next, _ = m.Update(controlEventMsg{evt: eventbus.ServerStateChanged{Name: "svc", State: "restarting", Restarts: 3}})
	m = next.(Model)
	if m.servers[0].State != "restarting" || m.servers[0].Restarts != 3 {
		t.Fatalf("server state not applied: %+v", m.servers[0])
	}

	// A ServerStateChanged for a name outside the initial snapshot appends
	// a new row.
	next, _ = m.Update(controlEventMsg{evt: eventbus.ServerStateChanged{Name: "new-svc", State: "healthy", Restarts: 0}})
	m = next.(Model)
	if len(m.servers) != 2 || m.servers[1].Name != "new-svc" {
		t.Fatalf("expected new-svc appended, got %+v", m.servers)
	}

	next, _ = m.Update(controlEventMsg{evt: eventbus.StreamOpened{ID: 1}})
	m = next.(Model)
	next, _ = m.Update(controlEventMsg{evt: eventbus.StreamOpened{ID: 2}})
	m = next.(Model)
	next, _ = m.Update(controlEventMsg{evt: eventbus.StreamClosed{ID: 1}})
	m = next.(Model)
	if m.streamsOpened != 2 || m.streamsClosed != 1 {
		t.Fatalf("stream counters wrong: opened=%d closed=%d", m.streamsOpened, m.streamsClosed)
	}

	next, _ = m.Update(controlEventMsg{evt: eventbus.AppError{Code: "QUOTA_EXCEEDED", Message: "too many", Service: "svc"}})
	m = next.(Model)
	if m.lastErr == "" {
		t.Fatalf("expected lastErr to be set")
	}
}

func TestApplyMetricSample(t *testing.T) {
	m := newTestModel(nil, nil)
	next, _ := m.Update(metricEventMsg{sample: eventbus.MetricSample{Kind: "ping_rtt", Value: 42}})
	m = next.(Model)
	if !m.haveLatency || m.latencyMs != 42 {
		t.Fatalf("latency not applied: %+v", m)
	}
}

func TestApplyMetricSampleBytesTotal(t *testing.T) {
	m := newTestModel(nil, nil)
	next, _ := m.Update(metricEventMsg{sample: eventbus.MetricSample{Kind: "bytes_total", Value: 1024}})
	m = next.(Model)
	if !m.haveBytes || m.bytesTotal != 1024 {
		t.Fatalf("bytes not applied: %+v", m)
	}
}

// TestApplyTelemetryRecordsLogLine asserts a plain telemetry LogLine (ping
// RTT now arrives only as a MetricSample, see TestApplyMetricSample) is
// still appended to the log tail.
func TestApplyTelemetryRecordsLogLine(t *testing.T) {
	m := newTestModel(nil, nil)
	next, _ := m.Update(telemetryEventMsg{line: eventbus.LogLine{Server: "tunnel", Level: "info", Text: "hello"}})
	m = next.(Model)
	if len(m.logs) != 1 {
		t.Fatalf("expected the LogLine to be recorded, got %d", len(m.logs))
	}
}

func TestApplySnapshotMergesKindAndURL(t *testing.T) {
	m := newTestModel([]Server{{Name: "svc", State: "healthy", Restarts: 2}}, nil)
	next, _ := m.Update(SnapshotMsg{Servers: []Server{{Name: "svc", Kind: "stdio", URL: "http://x"}, {Name: "other", Kind: "http", URL: "http://y"}}})
	m = next.(Model)
	if m.servers[0].Kind != "stdio" || m.servers[0].URL != "http://x" {
		t.Fatalf("existing server not merged: %+v", m.servers[0])
	}
	if m.servers[0].State != "healthy" || m.servers[0].Restarts != 2 {
		t.Fatalf("snapshot must not clobber live state: %+v", m.servers[0])
	}
	if len(m.servers) != 2 || m.servers[1].Name != "other" {
		t.Fatalf("expected snapshot to append unseen server, got %+v", m.servers)
	}
}

// TestApplySnapshotUpdatesNonEmptyState covers the http side of types.go's
// SnapshotMsg rule: a snapshot row with a State (e.g. a registry poll's
// disabled/active toggle) overwrites the model's current value.
func TestApplySnapshotUpdatesNonEmptyState(t *testing.T) {
	m := newTestModel([]Server{{Name: "svc", State: "active"}}, nil)
	next, _ := m.Update(SnapshotMsg{Servers: []Server{{Name: "svc", State: "disabled"}}})
	m = next.(Model)
	if m.servers[0].State != "disabled" {
		t.Fatalf("expected non-empty snapshot State to update the row, got %+v", m.servers[0])
	}
}

func TestClosedTelemetryAndMetricsStopListening(t *testing.T) {
	bus := eventbus.New(1)
	m := New(bus, nil, nil)
	bus.Close()

	_, cmd := m.Update(telemetryClosedMsg{})
	if cmd != nil {
		t.Fatalf("telemetryClosedMsg must not re-arm a listen Cmd")
	}
	_, cmd = m.Update(metricClosedMsg{})
	if cmd != nil {
		t.Fatalf("metricClosedMsg must not re-arm a listen Cmd")
	}
}

// TestControlClosedQuits asserts controlClosedMsg tells bubbletea to quit
// (so the Program returns and the terminal gets restored) without treating
// it as a user-requested quit: quitting must stay false, since runUp uses
// that flag to decide whether to drive its own FatalExit(0) shutdown, and a
// closed bus already means a shutdown is under way elsewhere.
func TestControlClosedQuits(t *testing.T) {
	bus := eventbus.New(1)
	m := New(bus, nil, nil)

	next, cmd := m.Update(controlClosedMsg{})
	m = next.(Model)
	if m.quitting {
		t.Fatalf("controlClosedMsg must not set quitting")
	}
	if cmd == nil {
		t.Fatalf("expected a tea.Quit cmd")
	}
	msg := cmd()
	if _, ok := msg.(tea.QuitMsg); !ok {
		t.Fatalf("expected tea.QuitMsg, got %T", msg)
	}
}

// TestListenersStopOnContextCancellation is blocker 3's named test: once
// Run's ctx is canceled, a listen Cmd's blocking channel receive must not
// leak forever — bubbletea can't cancel a running Cmd goroutine, so a leaked
// receive keeps competing for events on bus with whatever reads it next
// (e.g. a subsequent plain consumer), stealing them intermittently.
func TestListenersStopOnContextCancellation(t *testing.T) {
	bus := eventbus.New(0) // unbuffered: nothing but the listener/test reads it
	ctx, cancel := context.WithCancel(context.Background())
	cmd := listenControl(ctx, bus)
	cancel()

	done := make(chan tea.Msg, 1)
	go func() { done <- cmd() }()
	select {
	case msg := <-done:
		if _, ok := msg.(controlClosedMsg); !ok {
			t.Fatalf("listenControl() after cancel = %T, want controlClosedMsg", msg)
		}
	case <-time.After(time.Second):
		t.Fatalf("listenControl's Cmd did not return after ctx cancellation")
	}

	// The canceled Cmd goroutine above has now returned, not parked on
	// bus.Control — a later Publish must reach this consumer, not it.
	got := make(chan any, 1)
	go func() { got <- <-bus.Control }()
	bus.Publish(eventbus.StreamOpened{ID: 7})
	select {
	case evt := <-got:
		if so, ok := evt.(eventbus.StreamOpened); !ok || so.ID != 7 {
			t.Fatalf("subsequent consumer got %#v, want StreamOpened{ID:7}", evt)
		}
	case <-time.After(time.Second):
		t.Fatalf("event was lost/stolen by the canceled listener")
	}
}

func TestWindowSizeMsg(t *testing.T) {
	m := newTestModel(nil, nil)
	next, _ := m.Update(tea.WindowSizeMsg{Width: 40, Height: 10})
	m = next.(Model)
	if m.width != 40 || m.height != 10 {
		t.Fatalf("window size not applied: %+v", m)
	}
}

func TestInitReturnsNilWithoutBus(t *testing.T) {
	m := New(nil, nil, nil)
	if cmd := m.Init(); cmd != nil {
		t.Fatalf("Init() with nil bus should return nil, not a listen Cmd that will panic")
	}
}

// TestLogPaneScrollKeysMoveViewport is item 9's test: once the log pane is
// open, PgUp/PgDn/ctrl+u/ctrl+d must scroll it rather than being dropped or
// misinterpreted as a table/selection key.
func TestLogPaneScrollKeysMoveViewport(t *testing.T) {
	m := newTestModel(nil, nil)
	var logs []eventbus.LogLine
	for i := 0; i < 100; i++ {
		logs = append(logs, eventbus.LogLine{Server: "fs", Text: "line"})
	}
	next, _ := m.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	m = next.(Model)
	next, _ = m.Update(namedKey(t, "l"))
	m = next.(Model)
	for _, l := range logs {
		next, _ = m.Update(telemetryEventMsg{line: l})
		m = next.(Model)
	}
	if !m.logsVP.AtBottom() {
		t.Fatalf("expected the log viewport to start at the bottom")
	}

	next, _ = m.Update(namedKey(t, "pgup"))
	m = next.(Model)
	if m.logsVP.AtBottom() {
		t.Fatalf("pgup should have scrolled the log pane away from the bottom")
	}

	next, _ = m.Update(namedKey(t, "pgdown"))
	m = next.(Model)
	if !m.logsVP.AtBottom() {
		t.Fatalf("pgdown should have scrolled the log pane back to the bottom")
	}

	next, _ = m.Update(namedKey(t, "ctrl+u"))
	m = next.(Model)
	if m.logsVP.AtBottom() {
		t.Fatalf("ctrl+u should have scrolled the log pane away from the bottom")
	}
	next, _ = m.Update(namedKey(t, "ctrl+d"))
	m = next.(Model)
	if !m.logsVP.AtBottom() {
		t.Fatalf("ctrl+d should have scrolled the log pane back to the bottom")
	}
}

func TestInitSubscribesAllThreeChannels(t *testing.T) {
	bus := eventbus.New(1)
	m := New(bus, nil, nil)
	cmd := m.Init()
	if cmd == nil {
		t.Fatalf("Init() with a bus should return a Cmd")
	}
	bus.Publish(eventbus.ConnStateChanged{State: "connected"})
	msg := cmd()
	batch, ok := msg.(tea.BatchMsg)
	if !ok {
		t.Fatalf("expected tea.BatchMsg, got %T", msg)
	}
	if len(batch) != 3 {
		t.Fatalf("expected 3 batched listen commands, got %d", len(batch))
	}
}
