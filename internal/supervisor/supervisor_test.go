package supervisor

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/mcpwarp/cli/internal/bridge"
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
