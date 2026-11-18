package bridge

import (
	"runtime"
	"testing"
	"time"

	"github.com/mcpwarp/cli/internal/eventbus"
)

// What the bridge reports on its bus about the version it serves: one
// ServerVersionChanged per change, "" once the process ends, nothing once the
// bridge is closing.

// startReporting is startEraBridgeCold with a bus whose Control channel the
// test reads.
func startReporting(t *testing.T, mutate func(*StartBridgeOptions), args ...string) (*Bridge, *eventbus.Bus) {
	t.Helper()
	bus := eventbus.New(64)
	b := startEraBridgeCold(t, func(o *StartBridgeOptions) {
		o.Bus = bus
		if mutate != nil {
			mutate(o)
		}
	}, args...)
	return b, bus
}

// nextVersion waits for the next event on bus and returns its version.
func nextVersion(t *testing.T, bus *eventbus.Bus) string {
	t.Helper()
	select {
	case evt := <-bus.Control:
		e, ok := evt.(eventbus.ServerVersionChanged)
		if !ok || e.Name != "fake-era" {
			t.Fatalf("unexpected event %#v", evt)
		}
		return e.Version
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for a version report")
	}
	return ""
}

// settle waits until whatever the current process has queued has run: a
// flush in progress holds flushMu until it is done, publishing included.
func settle(b *Bridge) {
	_, st, _ := b.snapshotEra()
	b.flushEra(st)
}

// exitHandled returns a channel closed once c's exit listeners registered
// before this call, the bridge's among them, have all returned: c runs its
// exit listeners one after another, in the order they were registered. Call
// it before c can exit.
func exitHandled(c *StdioChild) <-chan struct{} {
	done := make(chan struct{})
	c.OnExit(func(ExitInfo) { close(done) })
	return done
}

func waitExitHandled(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the bridge's exit listener")
	}
}

// noMoreVersions fails if bus holds an event.
func noMoreVersions(t *testing.T, bus *eventbus.Bus) {
	t.Helper()
	select {
	case evt := <-bus.Control:
		t.Fatalf("unexpected event %#v", evt)
	default:
	}
}

func TestEraReport_VerdictIsReportedOnce(t *testing.T) {
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"--era=legacy"}, eventbus.VersionV1},
		{[]string{"--era=modern"}, eventbus.VersionV2},
	} {
		b, bus := startReporting(t, nil, c.args...)
		if got := nextVersion(t, bus); got != c.want {
			t.Errorf("%v: reported %q, want %q", c.args, got, c.want)
		}
		settle(b)
		noMoreVersions(t, bus)
	}
}

// A child silent past the probe timeout is reported v1, then v2 once on the
// one flip, here a forwarded initialize it rejects naming only 2026 versions.
func TestEraReport_TimeoutThenFlip(t *testing.T) {
	b, bus := startReporting(t, shortProbe, "--era=modern", "--discover-hold")
	if got := nextVersion(t, bus); got != eventbus.VersionV1 {
		t.Fatalf("at the timeout: reported %q", got)
	}
	if r := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil); r.status != 400 {
		t.Fatalf("forwarded initialize: %d %s", r.status, r.body)
	}
	if got := nextVersion(t, bus); got != eventbus.VersionV2 {
		t.Fatalf("after the rejection: reported %q", got)
	}
	settle(b)
	noMoreVersions(t, bus)
}

// The same flip, caused by a modern answer to server/discover that comes
// after the probe timeout.
func TestEraReport_TimeoutThenLateModernAnswer(t *testing.T) {
	b, bus := startReporting(t, shortProbe, "--era=modern", "--discover-hold")
	if got := nextVersion(t, bus); got != eventbus.VersionV1 {
		t.Fatalf("at the timeout: reported %q", got)
	}
	releaseProbe(t, b)
	if got := nextVersion(t, bus); got != eventbus.VersionV2 {
		t.Fatalf("after the late answer: reported %q", got)
	}
	settle(b)
	noMoreVersions(t, bus)
}

// A late answer that is not modern keeps the version, so it reports nothing.
func TestEraReport_LateV1AnswerReportsNothing(t *testing.T) {
	b, bus := startReporting(t, shortProbe, "--era=legacy", "--discover-delay-ms=2000")
	if got := nextVersion(t, bus); got != eventbus.VersionV1 {
		t.Fatalf("at the timeout: reported %q", got)
	}
	if got := phaseOf(b); got != phaseTimeout {
		t.Fatalf("setup: the late answer came before the timeout: %s", describe(mach(got)))
	}
	waitFor(t, "the late answer", func() bool { return phaseOf(b) == phaseV1 })
	settle(b)
	noMoreVersions(t, bus)
}

func TestEraReport_ChildExitClearsTheVersion(t *testing.T) {
	b, bus := startReporting(t, nil, "--era=legacy")
	if got := nextVersion(t, bus); got != eventbus.VersionV1 {
		t.Fatalf("verdict: reported %q", got)
	}
	_ = b.GetCurrentChild().Close()
	if got := nextVersion(t, bus); got != "" {
		t.Fatalf("after the exit: reported %q", got)
	}
	settle(b)
	noMoreVersions(t, bus)
}

// A failed write to stdin clears the version itself, before the bridge closes
// the child: the clear is on the bus by the time the failed Send returns.
func TestEraReport_FailedWriteClearsTheVersion(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("needs /bin/sh")
	}
	// The child reads the probe, never answers it, then closes its stdin.
	bus := eventbus.New(64)
	b, err := StartBridge(StartBridgeOptions{
		Name:         "fake-era",
		SpawnSpec:    SpawnSpec{Command: "/bin/sh", Args: []string{"-c", "IFS= read -r line; exec 0<&-; sleep 30"}},
		Log:          testLogger(),
		ProbeTimeout: 100 * time.Millisecond,
		Bus:          bus,
	})
	if err != nil {
		t.Fatalf("StartBridge: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if got := nextVersion(t, bus); got != eventbus.VersionV1 {
		t.Fatalf("at the timeout: reported %q", got)
	}
	child := b.GetCurrentChild()
	handled := exitHandled(child)
	ping := map[string]any{"jsonrpc": "2.0", "id": 1, "method": "ping"}
	waitFor(t, "a failed write", func() bool { return child.Send(ping) != nil })
	select {
	case evt := <-bus.Control:
		if e, ok := evt.(eventbus.ServerVersionChanged); !ok || e.Version != "" {
			t.Fatalf("after the failed write: %#v, want the clear", evt)
		}
	default:
		t.Fatal("the failed write returned before the clear was published")
	}
	waitExitHandled(t, handled)
	settle(b)
	noMoreVersions(t, bus)
}

// The replaced process's clear comes before the new process's verdict.
func TestEraReport_ReplaceChildClearsThenReportsTheNewProcess(t *testing.T) {
	b, bus := startReporting(t, nil, "--era=legacy")
	old := b.GetCurrentChild()
	t.Cleanup(func() { _ = old.Close() })
	handled := exitHandled(old)
	if got := nextVersion(t, bus); got != eventbus.VersionV1 {
		t.Fatalf("first verdict: reported %q", got)
	}
	b.ReplaceChild(newEraChild(t, "--era=modern"))
	if got := nextVersion(t, bus); got != "" {
		t.Fatalf("after ReplaceChild: reported %q, want the clear first", got)
	}
	if got := nextVersion(t, bus); got != eventbus.VersionV2 {
		t.Fatalf("new process: reported %q", got)
	}
	_ = old.Close()
	waitExitHandled(t, handled)
	settle(b)
	noMoreVersions(t, bus)
}

// Close ends the process like an exit does, but its clear is not published,
// nor is anything the exit that follows queues.
func TestEraReport_CloseReportsNothing(t *testing.T) {
	b, bus := startReporting(t, nil, "--era=legacy")
	if got := nextVersion(t, bus); got != eventbus.VersionV1 {
		t.Fatalf("verdict: reported %q", got)
	}
	handled := exitHandled(b.GetCurrentChild())
	_ = b.Close()
	waitExitHandled(t, handled)
	settle(b)
	noMoreVersions(t, bus)
}

// A process that exits before answering never had a version to report or
// clear.
func TestEraReport_ChildGoneAtSpawnReportsNothing(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("needs the true command")
	}
	bus := eventbus.New(64)
	b, err := StartBridge(StartBridgeOptions{Name: "true", SpawnSpec: SpawnSpec{Command: "true"}, Log: testLogger(), Bus: bus})
	if err != nil {
		t.Fatalf("StartBridge: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	waitFor(t, "the process to end", func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.era.m.ended
	})
	settle(b)
	noMoreVersions(t, bus)
}
