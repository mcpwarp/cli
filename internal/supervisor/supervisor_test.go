package supervisor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/mcpwarp/cli/internal/bridge"
	"github.com/mcpwarp/cli/internal/eventbus"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// exitSpec spawns a process that sleeps ~delay then exits with code —
// cross-platform via a shell (this is the test's own deliberate choice of
// command, not a shim-resolution concern StdioChild itself has to solve).
func exitSpec(code int, delay time.Duration) bridge.SpawnSpec {
	if runtime.GOOS == "windows" {
		return bridge.SpawnSpec{
			Command: "cmd.exe",
			Args:    []string{"/C", fmt.Sprintf("ping -n %d 127.0.0.1 >NUL & exit %d", int(delay.Seconds())+1, code)},
		}
	}
	return bridge.SpawnSpec{
		Command: "/bin/sh",
		Args:    []string{"-c", fmt.Sprintf("sleep %f; exit %d", delay.Seconds(), code)},
	}
}

// fakeBridge is a SupervisedBridge stub for tests.
type fakeBridge struct {
	mu            sync.Mutex
	replaced      []*bridge.StdioChild
	failedReasons []string
}

func (f *fakeBridge) ReplaceChild(c *bridge.StdioChild) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replaced = append(f.replaced, c)
}
func (f *fakeBridge) MarkFailed(reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failedReasons = append(f.failedReasons, reason)
}
func (f *fakeBridge) failedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.failedReasons)
}
func (f *fakeBridge) replacedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.replaced)
}

// spawnTestChild spawns a real, trivially-controllable child (via a
// shell's sleep+exit) so a crash can be triggered deterministically
// without a full MCP fixture.
func spawnTestChild(t *testing.T, exitCode int, delay time.Duration) *bridge.StdioChild {
	t.Helper()
	c, err := bridge.NewStdioChild(exitSpec(exitCode, delay), testLogger(), "test")
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	return c
}

func waitForState(t *testing.T, s *Supervisor, want State, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.GetState() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for state %q, currently %q", want, s.GetState())
}

// waitForFailedCount polls fb, rather than asserting immediately after
// waitForState(..., Failed, ...): handleExit flips state to Failed before
// calling MarkFailed, so a check made the instant GetState() reports
// Failed can still race MarkFailed itself.
func waitForFailedCount(t *testing.T, fb *fakeBridge, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fb.failedCount() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for MarkFailed call count %d, currently %d", want, fb.failedCount())
}

func TestSupervisor_BackoffDelay_FullJitterExponential(t *testing.T) {
	s := &Supervisor{random: func() float64 { return 1.0 }}
	cases := []struct {
		failures int
		wantMS   float64
	}{
		{1, 1000},
		{2, 2000},
		{3, 4000},
		{4, 8000},
		{5, 16000},
		{6, 30000}, // capped
		{20, 30000},
	}
	for _, c := range cases {
		s.consecutiveFailures = c.failures
		got := s.backoffDelay()
		if got.Milliseconds() != int64(c.wantMS) {
			t.Errorf("failures=%d: got %v, want %vms", c.failures, got, c.wantMS)
		}
	}
}

func TestSupervisor_BackoffDelay_JitterIsScaled(t *testing.T) {
	s := &Supervisor{random: func() float64 { return 0.5 }}
	s.consecutiveFailures = 1
	got := s.backoffDelay()
	if got.Milliseconds() != 500 {
		t.Fatalf("want 500ms at 0.5 jitter of a 1000ms base, got %v", got)
	}
}

func TestSupervisor_RestartsOnCrashAndCallsReplaceChild(t *testing.T) {
	child := spawnTestChild(t, 1, 0)
	fb := &fakeBridge{}
	var spawns int
	var mu sync.Mutex
	s := New(Options{
		Name: "svc", Log: testLogger(), Child: child, Bridge: fb,
	}, Deps{
		Sleep: func(time.Duration) {},
		Now:   time.Now,
		Spawn: func(spec bridge.SpawnSpec, log *slog.Logger, name string) (*bridge.StdioChild, error) {
			mu.Lock()
			spawns++
			mu.Unlock()
			return spawnTestChild(t, 0, time.Hour), nil // stays alive
		},
	})
	t.Cleanup(func() { s.Stop() })

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && fb.replacedCount() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if fb.replacedCount() != 1 {
		t.Fatalf("want 1 ReplaceChild call, got %d", fb.replacedCount())
	}
	if s.GetState() != Healthy {
		t.Fatalf("want healthy after the restart lands, got %v", s.GetState())
	}
}

func TestSupervisor_TenConsecutiveCrashesGivesUp(t *testing.T) {
	child := spawnTestChild(t, 1, 0)
	fb := &fakeBridge{}
	// now() never advances, so every restart counts toward the same
	// consecutive-failure budget (no 60s healthy-uptime reset).
	fixedNow := time.Unix(0, 0)
	s := New(Options{
		Name: "svc", Log: testLogger(), Child: child, Bridge: fb,
	}, Deps{
		Sleep: func(time.Duration) {},
		Now:   func() time.Time { return fixedNow },
		Spawn: func(spec bridge.SpawnSpec, log *slog.Logger, name string) (*bridge.StdioChild, error) {
			return spawnTestChild(t, 1, 0), nil // crashes immediately again
		},
	})
	t.Cleanup(func() { s.Stop() })

	waitForState(t, s, Failed, 5*time.Second)
	waitForFailedCount(t, fb, 1, time.Second)
}

// TestSupervisor_RapidCrashLoopNeverParksHealthyWithDeadChild stresses the
// wire()/current-assignment race activate() used to have (a replacement
// child exiting in the gap between wire() and the state flip to Healthy
// was misattributed to the stale previous child by handleExit's
// staleness guard and silently dropped, parking the supervisor Healthy
// with a dead child). Repeated back-to-back crash loops give that window
// many chances to be hit; every trial must still reach Failed with
// MarkFailed called exactly once — never fewer (a dropped crash extending
// the loop past the failure cap unnoticed) and never more (double
// counting the same exit).
func TestSupervisor_RapidCrashLoopNeverParksHealthyWithDeadChild(t *testing.T) {
	for trial := 0; trial < 20; trial++ {
		child := spawnTestChild(t, 1, 0)
		fb := &fakeBridge{}
		fixedNow := time.Unix(0, 0)
		s := New(Options{
			Name: "svc", Log: testLogger(), Child: child, Bridge: fb,
		}, Deps{
			Sleep: func(time.Duration) {},
			Now:   func() time.Time { return fixedNow },
			Spawn: func(spec bridge.SpawnSpec, log *slog.Logger, name string) (*bridge.StdioChild, error) {
				return spawnTestChild(t, 1, 0), nil // crashes immediately again
			},
		})

		waitForState(t, s, Failed, 5*time.Second)
		waitForFailedCount(t, fb, 1, time.Second)
		if s.GetState() == Healthy {
			t.Fatalf("trial %d: supervisor must never report healthy while its current child has already exited", trial)
		}
		s.Stop()
	}
}

func TestSupervisor_EnableAfterFailedRespawns(t *testing.T) {
	child := spawnTestChild(t, 1, 0)
	fb := &fakeBridge{}
	fixedNow := time.Unix(0, 0)
	crashing := true
	var mu sync.Mutex
	s := New(Options{
		Name: "svc", Log: testLogger(), Child: child, Bridge: fb,
	}, Deps{
		Sleep: func(time.Duration) {},
		Now:   func() time.Time { return fixedNow },
		Spawn: func(spec bridge.SpawnSpec, log *slog.Logger, name string) (*bridge.StdioChild, error) {
			mu.Lock()
			c := crashing
			mu.Unlock()
			if c {
				return spawnTestChild(t, 1, 0), nil
			}
			return spawnTestChild(t, 0, time.Hour), nil
		},
	})
	t.Cleanup(func() { s.Stop() })

	waitForState(t, s, Failed, 5*time.Second)

	mu.Lock()
	crashing = false
	mu.Unlock()
	s.Enable()

	waitForState(t, s, Healthy, 3*time.Second)
}

func TestSupervisor_DisableThenEnable(t *testing.T) {
	child := spawnTestChild(t, 0, time.Hour)
	fb := &fakeBridge{}
	s := New(Options{
		Name: "svc", Log: testLogger(), Child: child, Bridge: fb,
	}, Deps{
		Sleep: func(time.Duration) {},
		Now:   time.Now,
		Spawn: func(spec bridge.SpawnSpec, log *slog.Logger, name string) (*bridge.StdioChild, error) {
			return spawnTestChild(t, 0, time.Hour), nil
		},
	})
	t.Cleanup(func() { s.Stop() })

	s.Disable()
	if s.GetState() != Disabled {
		t.Fatalf("want disabled, got %v", s.GetState())
	}
	if !child.HasExited() {
		waitForChildExit(t, child, time.Second)
	}

	s.Enable()
	waitForState(t, s, Healthy, 3*time.Second)
	if fb.replacedCount() != 1 {
		t.Fatalf("want 1 ReplaceChild after enable, got %d", fb.replacedCount())
	}
}

// TestSupervisor_StopCutsBackoffSleepShort proves Stop()/Disable() don't
// block for the full (up to 30s) backoff sleep. With the real time.Sleep
// in play (no Deps.Sleep override), a crashing child puts the supervisor
// into a real backoff wait; Stop() called mid-wait must still return
// quickly rather than blocking for whatever's left of that sleep.
func TestSupervisor_StopCutsBackoffSleepShort(t *testing.T) {
	child := spawnTestChild(t, 1, 0)
	fb := &fakeBridge{}
	s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: fb}, Deps{
		Spawn: func(spec bridge.SpawnSpec, log *slog.Logger, name string) (*bridge.StdioChild, error) {
			return spawnTestChild(t, 1, 0), nil
		},
	})
	t.Cleanup(func() { s.Stop() })

	// The child above crashes almost immediately, putting the supervisor
	// into a real (uncapped Sleep) backoff wait — failures=1 averages
	// ~500ms, so by 300ms in it's very likely mid-sleep.
	time.Sleep(300 * time.Millisecond)

	start := time.Now()
	s.Stop()
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("Stop() took %v — backoff sleep was not cut short", elapsed)
	}
	if s.GetState() != Stopped {
		t.Fatalf("want stopped, got %v", s.GetState())
	}
}

// TestSupervisor_StopBeforeBackoffSleepStartsReturnsPromptly lands Stop()
// after handleExit has scheduled a crash restart but before restartAfter's
// goroutine exists: an unbuffered bus parks handleExit in its
// publish(Restarting), which sits between that lock hold and the go
// statement. The cancel channel must already be registered by then, so
// Stop() cuts a backoff that hasn't even started yet instead of waiting
// out the (here gated, so endless) sleep.
func TestSupervisor_StopBeforeBackoffSleepStartsReturnsPromptly(t *testing.T) {
	// The delay keeps the crash after New() has wired OnExit: an already
	// exited child runs handleExit synchronously inside New(), parking this
	// goroutine on the unbuffered bus before the drain below exists.
	child := spawnTestChild(t, 1, 200*time.Millisecond)
	fb := &fakeBridge{}
	bus := eventbus.New(0)
	sleepGate := make(chan struct{})
	var mu sync.Mutex
	spawns := 0
	s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: fb, Bus: bus}, Deps{
		Sleep: func(time.Duration) { <-sleepGate },
		Spawn: func(bridge.SpawnSpec, *slog.Logger, string) (*bridge.StdioChild, error) {
			mu.Lock()
			spawns++
			mu.Unlock()
			return spawnTestChild(t, 0, time.Hour), nil
		},
	})
	drainDone := make(chan struct{})
	t.Cleanup(func() {
		close(sleepGate) // frees the abandoned Sleep goroutine
		bus.Close()      // ends the drain below
		<-drainDone
	})

	deadline := time.Now().Add(3 * time.Second)
	for {
		s.mu.Lock()
		backingOff := s.backingOff
		s.mu.Unlock()
		if backingOff {
			break // handleExit is parked in publish: restartAfter hasn't started
		}
		if time.Now().After(deadline) {
			t.Fatal("handleExit never scheduled a backoff")
		}
		time.Sleep(5 * time.Millisecond)
	}

	stopDone := make(chan struct{})
	go func() {
		s.Stop()
		close(stopDone)
	}()
	waitForState(t, s, Stopped, time.Second)

	go func() {
		defer close(drainDone)
		for range bus.Control {
		}
	}()

	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() waited on a backoff it should have cut short before it began")
	}
	if got := s.GetState(); got != Stopped {
		t.Fatalf("want stopped, got %v", got)
	}
	mu.Lock()
	got := spawns
	mu.Unlock()
	if got != 0 {
		t.Fatalf("want no spawn after Stop, got %d", got)
	}
}

func TestSupervisor_StopIsTerminal(t *testing.T) {
	child := spawnTestChild(t, 0, time.Hour)
	fb := &fakeBridge{}
	s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: fb}, Deps{Sleep: func(time.Duration) {}})

	s.Stop()
	if s.GetState() != Stopped {
		t.Fatalf("want stopped, got %v", s.GetState())
	}
	s.Enable()
	if s.GetState() != Stopped {
		t.Fatalf("enable() on a stopped supervisor must stay a no-op, got %v", s.GetState())
	}
}

// blockingReplaceBridge is a SupervisedBridge stub whose ReplaceChild call
// blocks until released, letting a test land Stop() squarely inside
// activate()'s window between spawning a replacement and flipping state to
// Healthy.
type blockingReplaceBridge struct {
	*fakeBridge
	entered chan struct{}
	release chan struct{}
}

func (b *blockingReplaceBridge) ReplaceChild(c *bridge.StdioChild) {
	close(b.entered)
	<-b.release
	b.fakeBridge.ReplaceChild(c)
}

// TestSupervisor_StopRacingActivateReplaceChild reproduces F3: Stop()
// landing while activate() is blocked inside ReplaceChild (replacement
// already spawned) must not have its Stopped transition overwritten by
// activate()'s tail unconditionally writing state=Healthy once
// ReplaceChild unblocks — the supervisor must not resurrect itself and
// keep spawning after Stop() has returned.
func TestSupervisor_StopRacingActivateReplaceChild(t *testing.T) {
	child := spawnTestChild(t, 1, 0) // crashes almost immediately
	entered := make(chan struct{})
	release := make(chan struct{})
	bb := &blockingReplaceBridge{fakeBridge: &fakeBridge{}, entered: entered, release: release}
	var spawns int
	var mu sync.Mutex
	s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: bb}, Deps{
		Sleep: func(time.Duration) {},
		Spawn: func(spec bridge.SpawnSpec, log *slog.Logger, name string) (*bridge.StdioChild, error) {
			mu.Lock()
			spawns++
			mu.Unlock()
			return spawnTestChild(t, 0, time.Hour), nil // stays alive once spawned
		},
	})
	t.Cleanup(func() { s.Stop() })

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("activate() never reached ReplaceChild")
	}

	stopDone := make(chan struct{})
	go func() {
		s.Stop()
		close(stopDone)
	}()

	time.Sleep(50 * time.Millisecond) // give Stop() time to land state=Stopped and start waiting
	close(release)                    // let activate() finish

	select {
	case <-stopDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop() never returned")
	}
	if s.GetState() != Stopped {
		t.Fatalf("want stopped after Stop() returns, got %v", s.GetState())
	}

	time.Sleep(100 * time.Millisecond)
	if s.GetState() != Stopped {
		t.Fatalf("state must not flip away from stopped once Stop() has returned, got %v", s.GetState())
	}
	mu.Lock()
	got := spawns
	mu.Unlock()
	if got != 1 {
		t.Fatalf("want exactly 1 spawn (the one raced against Stop), got %d", got)
	}
}

// TestSupervisor_SpawnReturnsAlreadyExitedChild_NeverParksHealthy is the
// deterministic version of TestSupervisor_RapidCrashLoopNeverParksHealthyWithDeadChild:
// that test relies on a real subprocess's exit racing activate() rather
// than reliably hitting the already-exited window (spawnTestChild's exit(0)
// still takes real wall-clock time to land). Here Spawn blocks until the
// child has actually exited before returning it to activate(), so every
// run hits the exact window: the old current-before-wire ordering
// misattributed the exit to the stale previous child and parked Healthy
// with a dead current child; the fixed ordering always reaches Failed.
func TestSupervisor_SpawnReturnsAlreadyExitedChild_NeverParksHealthy(t *testing.T) {
	child := spawnTestChild(t, 1, 0)
	fb := &fakeBridge{}
	fixedNow := time.Unix(0, 0)
	s := New(Options{
		Name: "svc", Log: testLogger(), Child: child, Bridge: fb,
	}, Deps{
		Sleep: func(time.Duration) {},
		Now:   func() time.Time { return fixedNow },
		Spawn: func(spec bridge.SpawnSpec, log *slog.Logger, name string) (*bridge.StdioChild, error) {
			c := spawnTestChild(t, 1, 0)
			waitForChildExit(t, c, 2*time.Second) // deterministic: already exited before activate() sees it
			return c, nil
		},
	})
	t.Cleanup(func() { s.Stop() })

	waitForState(t, s, Failed, 5*time.Second)
	if s.GetState() == Healthy {
		t.Fatal("supervisor parked healthy with an already-exited current child")
	}
}

func TestSupervisor_Name(t *testing.T) {
	child := spawnTestChild(t, 0, time.Hour)
	fb := &fakeBridge{}
	s := New(Options{Name: "svc-name", Log: testLogger(), Child: child, Bridge: fb}, Deps{Sleep: func(time.Duration) {}})
	t.Cleanup(func() { s.Stop() })

	if s.Name() != "svc-name" {
		t.Fatalf("want %q, got %q", "svc-name", s.Name())
	}
}

// TestSupervisor_Restart_RespawnsWithoutResettingFailures proves Restart()
// respawns immediately (unlike a crash's backoff wait) without forgiving
// an ongoing crash loop the way Enable's fresh-counters semantics do: it
// works from Healthy (Enable would refuse — a no-op unless
// Disabled/Failed) and preserves consecutiveFailures across the call.
func TestSupervisor_Restart_RespawnsWithoutResettingFailures(t *testing.T) {
	child := spawnTestChild(t, 0, time.Hour)
	fb := &fakeBridge{}
	s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: fb}, Deps{
		Sleep: func(time.Duration) {},
		Spawn: func(spec bridge.SpawnSpec, log *slog.Logger, name string) (*bridge.StdioChild, error) {
			return spawnTestChild(t, 0, time.Hour), nil
		},
	})
	t.Cleanup(func() { s.Stop() })

	s.mu.Lock()
	s.consecutiveFailures = 3
	s.mu.Unlock()

	s.Restart()

	waitForState(t, s, Healthy, 3*time.Second)
	if fb.replacedCount() != 1 {
		t.Fatalf("want 1 ReplaceChild call after Restart, got %d", fb.replacedCount())
	}
	s.mu.Lock()
	failures := s.consecutiveFailures
	s.mu.Unlock()
	if failures != 3 {
		t.Fatalf("want consecutiveFailures untouched by Restart (still 3), got %d", failures)
	}
}

// TestSupervisor_Restart_NoOpOnceStopped proves Restart() respects a prior
// terminal Stop() rather than resurrecting the supervisor.
func TestSupervisor_Restart_NoOpOnceStopped(t *testing.T) {
	child := spawnTestChild(t, 0, time.Hour)
	fb := &fakeBridge{}
	s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: fb}, Deps{Sleep: func(time.Duration) {}})

	s.Stop()
	s.Restart()

	if s.GetState() != Stopped {
		t.Fatalf("want stopped after Restart() on an already-stopped supervisor, got %v", s.GetState())
	}
}

// TestSupervisor_StopContext_ReturnsEarlyOnCancellation proves
// StopContext returns as soon as ctx is done rather than waiting out an
// in-flight restart, per the §3 5s shutdown-deadline requirement — the
// old no-arg Stop() (context.Background(), never done) still waits it
// out, as covered by TestSupervisor_StopCutsBackoffSleepShort.
func TestSupervisor_StopContext_ReturnsEarlyOnCancellation(t *testing.T) {
	child := spawnTestChild(t, 1, 0) // crashes almost immediately
	entered := make(chan struct{})
	release := make(chan struct{})
	bb := &blockingReplaceBridge{fakeBridge: &fakeBridge{}, entered: entered, release: release}
	s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: bb}, Deps{
		Sleep: func(time.Duration) {},
		Spawn: func(spec bridge.SpawnSpec, log *slog.Logger, name string) (*bridge.StdioChild, error) {
			return spawnTestChild(t, 0, time.Hour), nil
		},
	})
	t.Cleanup(func() {
		close(release) // let the still-blocked activate()/current.Close() finish in the background
		s.Stop()
	})

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("activate() never reached ReplaceChild")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	s.StopContext(ctx)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("StopContext took %v — did not return early on ctx.Done()", elapsed)
	}
}

// gatedSpawn is a Deps.Spawn that counts calls, signals entered on each
// one, and holds every spawn until release is closed — so a test can pile
// callers up behind one in-flight spawn, or land Disable() inside it. The
// children it returns stay alive until closed.
type gatedSpawn struct {
	t       *testing.T
	entered chan struct{}
	release chan struct{}

	mu       sync.Mutex
	spawns   int
	children []*bridge.StdioChild
}

func newGatedSpawn(t *testing.T) *gatedSpawn {
	return &gatedSpawn{t: t, entered: make(chan struct{}, 64), release: make(chan struct{})}
}

func (g *gatedSpawn) spawn(bridge.SpawnSpec, *slog.Logger, string) (*bridge.StdioChild, error) {
	g.mu.Lock()
	g.spawns++
	g.mu.Unlock()
	g.entered <- struct{}{}
	<-g.release
	c := spawnTestChild(g.t, 0, time.Hour)
	g.mu.Lock()
	g.children = append(g.children, c)
	g.mu.Unlock()
	return c, nil
}

func (g *gatedSpawn) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.spawns
}

func (g *gatedSpawn) lastChild() *bridge.StdioChild {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.children) == 0 {
		return nil
	}
	return g.children[len(g.children)-1]
}

func waitEntered(t *testing.T, g *gatedSpawn) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("spawn was never called")
	}
}

// TestSupervisor_EnsureRunning_DisabledStartsChildAndWaitsUntilHealthy:
// from Disabled, EnsureRunning spawns through the Enable path and only
// returns once the new child is the bridge's live child and the state is
// Healthy — the spawn is held open first to prove it actually waits.
func TestSupervisor_EnsureRunning_DisabledStartsChildAndWaitsUntilHealthy(t *testing.T) {
	child := spawnTestChild(t, 0, time.Hour)
	fb := &fakeBridge{}
	g := newGatedSpawn(t)
	s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: fb}, Deps{
		Sleep: func(time.Duration) {},
		Spawn: g.spawn,
	})
	t.Cleanup(func() { s.Stop() })

	s.Disable()

	errc := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		errc <- s.EnsureRunning(ctx)
	}()

	waitEntered(t, g)
	select {
	case err := <-errc:
		t.Fatalf("EnsureRunning returned (%v) before the spawn finished", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(g.release)

	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("EnsureRunning: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("EnsureRunning never returned")
	}
	if got := s.GetState(); got != Healthy {
		t.Fatalf("want healthy as soon as EnsureRunning returns, got %v", got)
	}
	if fb.replacedCount() != 1 {
		t.Fatalf("want the new child handed to the bridge once, got %d", fb.replacedCount())
	}
	if g.count() != 1 {
		t.Fatalf("want 1 spawn, got %d", g.count())
	}
}

// TestSupervisor_EnsureRunning_ConcurrentCallersSpawnOnce piles several
// EnsureRunning callers and a dashboard-style Enable() onto one disabled
// supervisor while the first spawn is held open: all of them must share
// that one spawn, and every EnsureRunning caller must see it succeed.
func TestSupervisor_EnsureRunning_ConcurrentCallersSpawnOnce(t *testing.T) {
	child := spawnTestChild(t, 0, time.Hour)
	fb := &fakeBridge{}
	g := newGatedSpawn(t)
	s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: fb}, Deps{
		Sleep: func(time.Duration) {},
		Spawn: g.spawn,
	})
	t.Cleanup(func() { s.Stop() })

	s.Disable()

	const callers = 8
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- s.EnsureRunning(ctx)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Enable()
	}()

	waitEntered(t, g)
	time.Sleep(50 * time.Millisecond) // let the rest reach the in-flight spawn
	close(g.release)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("EnsureRunning: %v", err)
		}
	}
	if g.count() != 1 {
		t.Fatalf("want exactly 1 spawn across all callers, got %d", g.count())
	}
	if fb.replacedCount() != 1 {
		t.Fatalf("want 1 ReplaceChild, got %d", fb.replacedCount())
	}
	if got := s.GetState(); got != Healthy {
		t.Fatalf("want healthy, got %v", got)
	}
}

// TestSupervisor_EnsureRunning_HealthyReturnsImmediately: an already
// cancelled ctx proves the Healthy path never waits on anything, and no
// spawn happens.
func TestSupervisor_EnsureRunning_HealthyReturnsImmediately(t *testing.T) {
	child := spawnTestChild(t, 0, time.Hour)
	fb := &fakeBridge{}
	g := newGatedSpawn(t)
	s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: fb}, Deps{
		Sleep: func(time.Duration) {},
		Spawn: g.spawn,
	})
	t.Cleanup(func() { s.Stop() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.EnsureRunning(ctx); err != nil {
		t.Fatalf("want nil for a healthy child, got %v", err)
	}
	if g.count() != 0 {
		t.Fatalf("want no spawn for a healthy child, got %d", g.count())
	}
}

// TestSupervisor_EnsureRunning_DisableDuringWaitWins lands Disable() while
// EnsureRunning's spawn is in flight: EnsureRunning must report
// ErrDisabled, the supervisor must end Disabled, and the child spawned for
// it must be closed rather than left running.
func TestSupervisor_EnsureRunning_DisableDuringWaitWins(t *testing.T) {
	child := spawnTestChild(t, 0, time.Hour)
	fb := &fakeBridge{}
	g := newGatedSpawn(t)
	s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: fb}, Deps{
		Sleep: func(time.Duration) {},
		Spawn: g.spawn,
	})
	t.Cleanup(func() { s.Stop() })

	s.Disable()

	errc := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		errc <- s.EnsureRunning(ctx)
	}()
	waitEntered(t, g)

	disableDone := make(chan struct{})
	go func() {
		s.Disable()
		close(disableDone)
	}()
	waitForState(t, s, Disabled, time.Second) // terminate() has flipped the state and is waiting on the spawn
	close(g.release)

	select {
	case err := <-errc:
		if !errors.Is(err, ErrDisabled) {
			t.Fatalf("want ErrDisabled, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("EnsureRunning never returned")
	}
	select {
	case <-disableDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Disable never returned")
	}
	if got := s.GetState(); got != Disabled {
		t.Fatalf("want disabled, got %v", got)
	}
	spawned := g.lastChild()
	if spawned == nil {
		t.Fatal("spawn never produced a child")
	}
	waitForChildExit(t, spawned, 3*time.Second)
}

// TestSupervisor_EnsureRunning_DisableInFlightRefuses: EnsureRunning
// called while Disable() is still closing the child refuses outright
// rather than spawning behind it.
func TestSupervisor_EnsureRunning_DisableInFlightRefuses(t *testing.T) {
	child := spawnTestChild(t, 0, time.Hour)
	fb := &fakeBridge{}
	g := newGatedSpawn(t)
	s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: fb}, Deps{
		Sleep: func(time.Duration) {},
		Spawn: g.spawn,
	})
	t.Cleanup(func() { s.Stop() })

	s.mu.Lock()
	s.state = Disabled
	s.terminating = make(chan struct{}) // a Disable() mid-close
	s.mu.Unlock()

	if err := s.EnsureRunning(context.Background()); !errors.Is(err, ErrDisabled) {
		t.Fatalf("want ErrDisabled while a Disable is in flight, got %v", err)
	}
	if g.count() != 0 {
		t.Fatalf("want no spawn, got %d", g.count())
	}
	s.mu.Lock()
	s.terminating = nil
	s.mu.Unlock()
}

// TestSupervisor_EnsureRunning_FailedRefusesWithoutSpawning: a supervisor
// that gave up stays Failed with its failure count intact — only an
// explicit Enable forgives a crash loop.
func TestSupervisor_EnsureRunning_FailedRefusesWithoutSpawning(t *testing.T) {
	child := spawnTestChild(t, 1, 0)
	fb := &fakeBridge{}
	fixedNow := time.Unix(0, 0)
	var mu sync.Mutex
	spawns := 0
	s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: fb}, Deps{
		Sleep: func(time.Duration) {},
		Now:   func() time.Time { return fixedNow },
		Spawn: func(bridge.SpawnSpec, *slog.Logger, string) (*bridge.StdioChild, error) {
			mu.Lock()
			spawns++
			mu.Unlock()
			return spawnTestChild(t, 1, 0), nil
		},
	})
	t.Cleanup(func() { s.Stop() })

	waitForState(t, s, Failed, 5*time.Second)
	mu.Lock()
	before := spawns
	mu.Unlock()

	if err := s.EnsureRunning(context.Background()); !errors.Is(err, ErrFailed) {
		t.Fatalf("want ErrFailed, got %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	after := spawns
	mu.Unlock()
	if after != before {
		t.Fatalf("EnsureRunning on a failed supervisor spawned (%d -> %d)", before, after)
	}
	if got := s.GetState(); got != Failed {
		t.Fatalf("want failed, got %v", got)
	}
	s.mu.Lock()
	failures := s.consecutiveFailures
	s.mu.Unlock()
	if failures != maxConsecutiveFailures {
		t.Fatalf("want consecutiveFailures left at %d, got %d", maxConsecutiveFailures, failures)
	}
}

// TestSupervisor_EnsureRunning_BackoffRefusesWithoutShortcutting holds a
// crash restart inside its backoff sleep: EnsureRunning must refuse with
// ErrRestarting and leave the sleep, the failure count and the spawn
// count alone.
func TestSupervisor_EnsureRunning_BackoffRefusesWithoutShortcutting(t *testing.T) {
	child := spawnTestChild(t, 1, 0)
	fb := &fakeBridge{}
	sleepGate := make(chan struct{})
	var mu sync.Mutex
	spawns := 0
	s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: fb}, Deps{
		Sleep: func(time.Duration) { <-sleepGate },
		Spawn: func(bridge.SpawnSpec, *slog.Logger, string) (*bridge.StdioChild, error) {
			mu.Lock()
			spawns++
			mu.Unlock()
			return spawnTestChild(t, 0, time.Hour), nil
		},
	})
	t.Cleanup(func() {
		s.Stop()         // cuts the backoff short via sleepCancel
		close(sleepGate) // then frees the abandoned Sleep goroutine
	})

	deadline := time.Now().Add(3 * time.Second)
	for {
		s.mu.Lock()
		backingOff := s.backingOff
		s.mu.Unlock()
		if backingOff {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("supervisor never entered its backoff sleep")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if err := s.EnsureRunning(context.Background()); !errors.Is(err, ErrRestarting) {
		t.Fatalf("want ErrRestarting during backoff, got %v", err)
	}
	if got := s.GetState(); got != Restarting {
		t.Fatalf("want still restarting, got %v", got)
	}
	s.mu.Lock()
	failures, backingOff := s.consecutiveFailures, s.backingOff
	s.mu.Unlock()
	if failures != 1 || !backingOff {
		t.Fatalf("backoff altered: consecutiveFailures=%d backingOff=%v", failures, backingOff)
	}
	mu.Lock()
	got := spawns
	mu.Unlock()
	if got != 0 {
		t.Fatalf("want no spawn while backing off, got %d", got)
	}
}

// TestSupervisor_EnsureRunning_CtxBoundsWaitButSpawnCompletes: a ctx that
// runs out mid-spawn returns its error to the caller, but the spawn it
// started still lands Healthy for the next request.
func TestSupervisor_EnsureRunning_CtxBoundsWaitButSpawnCompletes(t *testing.T) {
	child := spawnTestChild(t, 0, time.Hour)
	fb := &fakeBridge{}
	g := newGatedSpawn(t)
	s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: fb}, Deps{
		Sleep: func(time.Duration) {},
		Spawn: g.spawn,
	})
	t.Cleanup(func() { s.Stop() })

	s.Disable()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.EnsureRunning(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	waitEntered(t, g)
	close(g.release)
	waitForState(t, s, Healthy, 3*time.Second)
	if g.count() != 1 {
		t.Fatalf("want 1 spawn, got %d", g.count())
	}
}

// TestSupervisor_EnsureRunning_SpawnFailureFromDisabledReportsBackoff: a
// spawn that fails outright hands off to handleExit's crash backoff, which
// replaces s.restarting with its own channel; the waiter on the enable's
// channel must still be released and report ErrRestarting, with the
// backoff left running.
func TestSupervisor_EnsureRunning_SpawnFailureFromDisabledReportsBackoff(t *testing.T) {
	child := spawnTestChild(t, 0, time.Hour)
	fb := &fakeBridge{}
	sleepGate := make(chan struct{})
	s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: fb}, Deps{
		Sleep: func(time.Duration) { <-sleepGate },
		Spawn: func(bridge.SpawnSpec, *slog.Logger, string) (*bridge.StdioChild, error) {
			return nil, errors.New("exec: no such file")
		},
	})
	t.Cleanup(func() {
		s.Stop()         // cuts the backoff short via sleepCancel
		close(sleepGate) // then frees the abandoned Sleep goroutine
	})

	s.Disable()

	errc := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		errc <- s.EnsureRunning(ctx)
	}()
	select {
	case err := <-errc:
		if !errors.Is(err, ErrRestarting) {
			t.Fatalf("want ErrRestarting after a failed spawn, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("EnsureRunning stranded after a failed spawn")
	}
	s.mu.Lock()
	state, backingOff := s.state, s.backingOff
	s.mu.Unlock()
	if state != Restarting || !backingOff {
		t.Fatalf("want restarting with a backoff in progress, got state=%v backingOff=%v", state, backingOff)
	}
}

// TestSupervisor_EnsureRunning_JoinsInFlightRestart: EnsureRunning called
// while a Restart() spawn is held open waits on that spawn rather than
// refusing or starting its own, and succeeds once it lands.
func TestSupervisor_EnsureRunning_JoinsInFlightRestart(t *testing.T) {
	child := spawnTestChild(t, 0, time.Hour)
	fb := &fakeBridge{}
	g := newGatedSpawn(t)
	s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: fb}, Deps{
		Sleep: func(time.Duration) {},
		Spawn: g.spawn,
	})
	t.Cleanup(func() { s.Stop() })

	restartDone := make(chan struct{})
	go func() {
		s.Restart()
		close(restartDone)
	}()
	waitEntered(t, g)

	errc := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		errc <- s.EnsureRunning(ctx)
	}()
	select {
	case err := <-errc:
		t.Fatalf("EnsureRunning returned (%v) before the restart's spawn finished", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(g.release)

	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("EnsureRunning: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("EnsureRunning never returned")
	}
	<-restartDone
	if g.count() != 1 {
		t.Fatalf("want the restart's spawn only, got %d", g.count())
	}
}

// TestSupervisor_Restart_JoinsInFlightEnsureRunningSpawn is the mirror of
// TestSupervisor_EnsureRunning_JoinsInFlightRestart: Restart() called while
// an EnsureRunning respawn is held open must join that spawn instead of
// running a second activate() alongside it, which would overwrite
// s.current without closing the first child and leak it.
func TestSupervisor_Restart_JoinsInFlightEnsureRunningSpawn(t *testing.T) {
	child := spawnTestChild(t, 0, time.Hour)
	fb := &fakeBridge{}
	g := newGatedSpawn(t)
	s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: fb}, Deps{
		Sleep: func(time.Duration) {},
		Spawn: g.spawn,
	})
	t.Cleanup(func() { s.Stop() })

	s.Disable()

	errc := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		errc <- s.EnsureRunning(ctx)
	}()
	waitEntered(t, g)

	restartDone := make(chan struct{})
	go func() {
		s.Restart()
		close(restartDone)
	}()
	select {
	case <-restartDone:
		t.Fatal("Restart returned before the in-flight spawn finished")
	case <-time.After(50 * time.Millisecond):
	}
	close(g.release)

	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("EnsureRunning: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("EnsureRunning never returned")
	}
	select {
	case <-restartDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Restart never returned")
	}

	if g.count() != 1 {
		t.Fatalf("want exactly 1 spawn shared by EnsureRunning and Restart, got %d", g.count())
	}
	if fb.replacedCount() != 1 {
		t.Fatalf("want 1 ReplaceChild, got %d", fb.replacedCount())
	}
	if got := s.GetState(); got != Healthy {
		t.Fatalf("want healthy, got %v", got)
	}
	spawned := g.lastChild()
	s.mu.Lock()
	current := s.current
	s.mu.Unlock()
	if spawned == nil || current != spawned {
		t.Fatalf("want the one spawned child tracked as current, got current=%p spawned=%p", current, spawned)
	}
	if spawned.HasExited() {
		t.Fatal("the tracked child must still be running")
	}
	if got := s.Restarts(); got != 1 {
		t.Fatalf("want 1 restart counted, got %d", got)
	}
}

// TestSupervisor_Restart_JoinsCrashBackoffRestart holds a crash restart
// inside its backoff sleep: Restart() must wait that backoff out (not
// shortcut it, not spawn alongside it) and treat the backoff's own spawn as
// the restart, so exactly one child is spawned and tracked.
func TestSupervisor_Restart_JoinsCrashBackoffRestart(t *testing.T) {
	child := spawnTestChild(t, 1, 0)
	fb := &fakeBridge{}
	g := newGatedSpawn(t)
	close(g.release) // only the backoff sleep is gated here
	sleepGate := make(chan struct{})
	s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: fb}, Deps{
		Sleep: func(time.Duration) { <-sleepGate },
		Spawn: g.spawn,
	})
	t.Cleanup(func() { s.Stop() })

	deadline := time.Now().Add(3 * time.Second)
	for {
		s.mu.Lock()
		backingOff := s.backingOff
		s.mu.Unlock()
		if backingOff {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("supervisor never entered its backoff sleep")
		}
		time.Sleep(5 * time.Millisecond)
	}

	restartDone := make(chan struct{})
	go func() {
		s.Restart()
		close(restartDone)
	}()
	select {
	case <-restartDone:
		t.Fatal("Restart returned while the crash restart was still backing off")
	case <-time.After(50 * time.Millisecond):
	}
	if g.count() != 0 {
		t.Fatalf("want no spawn while backing off, got %d", g.count())
	}
	close(sleepGate)

	select {
	case <-restartDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Restart never returned")
	}

	if g.count() != 1 {
		t.Fatalf("want exactly 1 spawn (the backoff's), got %d", g.count())
	}
	if fb.replacedCount() != 1 {
		t.Fatalf("want 1 ReplaceChild, got %d", fb.replacedCount())
	}
	if got := s.GetState(); got != Healthy {
		t.Fatalf("want healthy, got %v", got)
	}
	spawned := g.lastChild()
	s.mu.Lock()
	current, failures := s.current, s.consecutiveFailures
	s.mu.Unlock()
	if spawned == nil || current != spawned {
		t.Fatalf("want the one spawned child tracked as current, got current=%p spawned=%p", current, spawned)
	}
	if spawned.HasExited() {
		t.Fatal("the tracked child must still be running")
	}
	if failures != 1 {
		t.Fatalf("want consecutiveFailures untouched by Restart (still 1), got %d", failures)
	}
	if got := s.Restarts(); got != 1 {
		t.Fatalf("want 1 restart counted, got %d", got)
	}
}

// TestSupervisor_Restart_DuringDisableReturnsWithoutSpawning holds a
// Disable() in flight (terminate() waiting on a gated EnsureRunning spawn)
// and calls Restart(): it must return at once without spawning, and the
// supervisor must end Disabled with the gated spawn's child closed.
func TestSupervisor_Restart_DuringDisableReturnsWithoutSpawning(t *testing.T) {
	child := spawnTestChild(t, 0, time.Hour)
	fb := &fakeBridge{}
	g := newGatedSpawn(t)
	s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: fb}, Deps{
		Sleep: func(time.Duration) {},
		Spawn: g.spawn,
	})
	t.Cleanup(func() { s.Stop() })

	s.Disable()

	errc := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		errc <- s.EnsureRunning(ctx)
	}()
	waitEntered(t, g)

	disableDone := make(chan struct{})
	go func() {
		s.Disable()
		close(disableDone)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		terminating := s.terminating != nil
		s.mu.Unlock()
		if terminating {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Disable never started")
		}
		time.Sleep(5 * time.Millisecond)
	}

	restartDone := make(chan struct{})
	go func() {
		s.Restart()
		close(restartDone)
	}()
	select {
	case <-restartDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Restart blocked behind the in-flight Disable")
	}
	close(g.release)

	select {
	case err := <-errc:
		if !errors.Is(err, ErrDisabled) {
			t.Fatalf("want ErrDisabled, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("EnsureRunning never returned")
	}
	select {
	case <-disableDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Disable never returned")
	}

	if g.count() != 1 {
		t.Fatalf("want only EnsureRunning's spawn, got %d", g.count())
	}
	if got := s.GetState(); got != Disabled {
		t.Fatalf("want disabled, got %v", got)
	}
	spawned := g.lastChild()
	if spawned == nil {
		t.Fatal("spawn never produced a child")
	}
	waitForChildExit(t, spawned, 3*time.Second)
}

// TestSupervisor_TerminateDuringRestartSpawnHasNoChildToClose lands
// Disable()/Stop() while a Restart() spawn is held open: Restart has
// already cleared s.current and closed the old child, and activate()
// closes the new one without tracking it once it sees the terminal state,
// so terminate() finds no child at all. It must finish cleanly (a nil
// Close would panic the whole process) with the new child closed.
func TestSupervisor_TerminateDuringRestartSpawnHasNoChildToClose(t *testing.T) {
	for _, tc := range []struct {
		name   string
		target State
		call   func(*Supervisor)
	}{
		{"Disable", Disabled, (*Supervisor).Disable},
		{"Stop", Stopped, (*Supervisor).Stop},
	} {
		t.Run(tc.name, func(t *testing.T) {
			child := spawnTestChild(t, 0, time.Hour)
			fb := &fakeBridge{}
			g := newGatedSpawn(t)
			s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: fb}, Deps{
				Sleep: func(time.Duration) {},
				Spawn: g.spawn,
			})
			t.Cleanup(func() { s.Stop() })

			restartDone := make(chan struct{})
			go func() {
				s.Restart()
				close(restartDone)
			}()
			waitEntered(t, g)

			termDone := make(chan struct{})
			go func() {
				tc.call(s)
				close(termDone)
			}()
			waitForState(t, s, tc.target, time.Second) // terminate() is waiting on the spawn
			close(g.release)

			select {
			case <-termDone:
			case <-time.After(3 * time.Second):
				t.Fatalf("%s never returned", tc.name)
			}
			select {
			case <-restartDone:
			case <-time.After(3 * time.Second):
				t.Fatal("Restart never returned")
			}

			if got := s.GetState(); got != tc.target {
				t.Fatalf("want %v, got %v", tc.target, got)
			}
			if fb.replacedCount() != 0 {
				t.Fatalf("want the new child never handed to the bridge, got %d ReplaceChild", fb.replacedCount())
			}
			spawned := g.lastChild()
			if spawned == nil {
				t.Fatal("spawn never produced a child")
			}
			waitForChildExit(t, spawned, 3*time.Second)
			waitForChildExit(t, child, 3*time.Second)
		})
	}
}

func TestSupervisor_EnsureRunning_StoppedRefuses(t *testing.T) {
	child := spawnTestChild(t, 0, time.Hour)
	s := New(Options{Name: "svc", Log: testLogger(), Child: child, Bridge: &fakeBridge{}}, Deps{Sleep: func(time.Duration) {}})
	s.Stop()
	if err := s.EnsureRunning(context.Background()); !errors.Is(err, ErrStopped) {
		t.Fatalf("want ErrStopped, got %v", err)
	}
}

func waitForChildExit(t *testing.T, c *bridge.StdioChild, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.HasExited() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for child exit")
}
