package shutdown

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestRegisterRunsInOrderAndUnregisters(t *testing.T) {
	ResetForTests()
	t.Cleanup(ResetForTests)

	var order []string
	var mu sync.Mutex
	record := func(name string) Fn {
		return func(context.Context) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return nil
		}
	}
	Register("first", record("first"))
	unregisterSecond := Register("second", record("second"))
	Register("third", record("third"))
	unregisterSecond()
	unregisterSecond() // must be a no-op the second time

	runHandlers(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "first" || order[1] != "third" {
		t.Fatalf("got %v", order)
	}
}

func TestRunHandlesPanicAndSlowHandler(t *testing.T) {
	ResetForTests()
	t.Cleanup(ResetForTests)
	origExit := osExit
	origDeadline := Deadline
	t.Cleanup(func() { osExit = origExit; Deadline = origDeadline })
	Deadline = 20 * time.Millisecond

	Register("panics", func(context.Context) error { panic("boom") })
	Register("slow", func(context.Context) error {
		time.Sleep(time.Second)
		return nil
	})

	var gotCode int
	done := make(chan struct{})
	osExit = func(c int) {
		gotCode = c
		close(done)
	}

	go Run(os.Interrupt)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run never reached the deadline and exited")
	}
	if gotCode != 130 {
		t.Fatalf("got exit code %d", gotCode)
	}
}

// TestDeadlineOverrunRunsForceExitHooks is S-3: a handler sequence that
// doesn't finish within Deadline must still run the OnForceExit hooks
// before exiting — the same last-resort net a racing second signal gets
// (TestASecondSignalForceExitsImmediately) — since whatever handler is
// still running past Deadline never got the chance to clean up itself.
func TestDeadlineOverrunRunsForceExitHooks(t *testing.T) {
	ResetForTests()
	t.Cleanup(ResetForTests)
	origExit := osExit
	origDeadline := Deadline
	t.Cleanup(func() { osExit = origExit; Deadline = origDeadline })
	Deadline = 20 * time.Millisecond

	Register("slow", func(context.Context) error {
		time.Sleep(time.Second)
		return nil
	})

	var hookRan atomic.Bool
	OnForceExit(func() { hookRan.Store(true) })

	done := make(chan struct{})
	osExit = func(int) { close(done) }

	go Run(os.Interrupt)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run never reached the deadline and exited")
	}
	if !hookRan.Load() {
		t.Fatal("expected the OnForceExit hook to have run before osExit")
	}
}

func TestASecondSignalForceExitsImmediately(t *testing.T) {
	ResetForTests()
	t.Cleanup(ResetForTests)
	origExit := osExit
	origDeadline := Deadline
	t.Cleanup(func() { osExit = origExit; Deadline = origDeadline })
	Deadline = time.Hour // the first Run must never reach this on its own

	blockFirst := make(chan struct{})
	Register("blocks", func(context.Context) error {
		<-blockFirst
		return nil
	})

	// The first (SIGINT) sequence stays parked in its "blocks" handler for
	// the whole test, so exit(130) is only expected once blockFirst closes
	// at the very end; exit(143) is expected immediately, from the second
	// (SIGTERM) signal force-exiting without waiting for the first.
	exited130 := make(chan struct{})
	exited143 := make(chan struct{})
	osExit = func(c int) {
		switch c {
		case 130:
			close(exited130)
		case 143:
			close(exited143)
		}
	}

	go Run(os.Interrupt)
	for { // wait for the first Run to actually set shuttingDown (package's own mu, not a test-local one)
		mu.Lock()
		s := shuttingDown
		mu.Unlock()
		if s {
			break
		}
		time.Sleep(time.Millisecond)
	}
	Run(syscall.SIGTERM)

	select {
	case <-exited143:
	case <-time.After(time.Second):
		t.Fatal("the second signal never force-exited")
	}
	select {
	case <-exited130:
		t.Fatal("the first (blocked) sequence should not have exited yet")
	default:
	}

	close(blockFirst)
	select {
	case <-exited130:
	case <-time.After(time.Second):
		t.Fatal("the first sequence never finished once unblocked")
	}
}

// TestSecondSignalRunsForceExitHooks is S-6: the force-exit path a racing
// second signal takes (Run's shuttingDown branch) must run the registered
// OnForceExit hooks, not just osExit — this is the "second signal/fatal
// exit" half of OnForceExit's own doc comment.
func TestSecondSignalRunsForceExitHooks(t *testing.T) {
	ResetForTests()
	t.Cleanup(ResetForTests)
	origExit := osExit
	origDeadline := Deadline
	t.Cleanup(func() { osExit = origExit; Deadline = origDeadline })
	Deadline = time.Hour // the first Run must never reach this on its own

	blockFirst := make(chan struct{})
	Register("blocks", func(context.Context) error {
		<-blockFirst
		return nil
	})

	var hookRan atomic.Bool
	OnForceExit(func() { hookRan.Store(true) })

	exited130 := make(chan struct{})
	exited143 := make(chan struct{})
	osExit = func(c int) {
		switch c {
		case 130:
			close(exited130)
		case 143:
			close(exited143)
		}
	}

	go Run(os.Interrupt)
	for { // wait for the first Run to actually set shuttingDown
		mu.Lock()
		s := shuttingDown
		mu.Unlock()
		if s {
			break
		}
		time.Sleep(time.Millisecond)
	}
	Run(syscall.SIGTERM)

	select {
	case <-exited143:
	case <-time.After(time.Second):
		t.Fatal("the second signal never force-exited")
	}
	if !hookRan.Load() {
		t.Fatal("expected the OnForceExit hook to have run on the racing second signal")
	}

	// Let the first (blocked) sequence finish and call osExit(130) — like
	// TestASecondSignalForceExitsImmediately, this must happen before the
	// t.Cleanup above restores osExit/Deadline, or that write races this
	// goroutine's read of them.
	close(blockFirst)
	select {
	case <-exited130:
	case <-time.After(time.Second):
		t.Fatal("the first sequence never finished once unblocked")
	}
}

// TestForceExitHooksAreBounded is the ForceExitDeadline regression: a
// force-exit hook that hangs forever must not stop RunForceExitHooksForTests
// (and, in production, osExit right after it) from returning.
func TestForceExitHooksAreBounded(t *testing.T) {
	ResetForTests()
	t.Cleanup(ResetForTests)
	origDeadline := ForceExitDeadline
	t.Cleanup(func() { ForceExitDeadline = origDeadline })
	ForceExitDeadline = 50 * time.Millisecond

	OnForceExit(func() { <-make(chan struct{}) }) // blocks forever

	done := make(chan struct{})
	go func() {
		RunForceExitHooksForTests()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(ForceExitDeadline + time.Second):
		t.Fatal("RunForceExitHooksForTests did not return within ForceExitDeadline + margin")
	}
}

func TestFatalExitRunsTheSequence(t *testing.T) {
	ResetForTests()
	t.Cleanup(ResetForTests)
	origExit := osExit
	t.Cleanup(func() { osExit = origExit })

	ran := false
	Register("cleanup", func(context.Context) error { ran = true; return nil })

	done := make(chan struct{})
	var gotCode int
	osExit = func(c int) { gotCode = c; close(done) }

	FatalExit(1)
	<-done
	if gotCode != 1 {
		t.Fatalf("got %d", gotCode)
	}
	if !ran {
		t.Fatal("expected the registered handler to have run")
	}
}
