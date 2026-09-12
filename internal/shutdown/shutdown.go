// Package shutdown is the Go analogue of Node's cli/shutdown.ts
// (docs/DESIGN.md §3): a small ordered registry that every SIGINT/SIGTERM
// and any fatal-disconnect path runs through, instead of a bare os.Exit.
//
// Handlers registered via Register run in registration order on Run or
// FatalExit, racing one shared deadline — a hung handler gets whatever it
// managed to do, it never wedges the process past that bound. A second
// signal (or fatal exit) that arrives while a sequence is already running
// force-exits immediately rather than running the sequence twice.
//
// The handler loop itself is sequential, not parallel: each handler is
// awaited before the next one starts. A handler that hangs past Deadline
// therefore doesn't just run out of time itself — it consumes the whole
// budget, and every handler registered after it never runs at all. Per-step
// budgets inside each handler (a context.WithTimeout around its own work)
// are the real guard against that; the shared Deadline is a last-resort
// backstop on the total, not a per-handler one.
package shutdown

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"syscall"
	"time"
)

// Fn is a shutdown step. Its error is logged nowhere yet (M1 has nothing to
// log it to) but is not allowed to stop the rest of the sequence from
// running.
type Fn func(ctx context.Context) error

// Deadline bounds the whole sequence, regardless of how many handlers are
// registered or how long any one of them hangs. A var, not a const, so
// tests can shrink it.
var Deadline = 5 * time.Second

// ForceExitDeadline bounds the OnForceExit hook set, run just before
// osExit on any of the three call sites below — a hung hook can never
// prevent the process from actually exiting. That budget is shared by all
// hooks, run in registration order, so a hook that blocks starves every
// hook registered after it; register the important ones first. A var, not
// a const, so tests can shrink it.
var ForceExitDeadline = 2 * time.Second

// osExit is os.Exit behind a var so tests can observe the code instead of
// actually killing the test binary.
var osExit = os.Exit

type entry struct {
	id int
	fn Fn
}

type forceExitEntry struct {
	id int
	fn func()
}

var (
	mu              sync.Mutex
	handlers        []entry
	nextID          int
	shuttingDown    bool
	forceExitFns    []forceExitEntry
	nextForceExitID int
)

// Register appends fn to the shutdown sequence, run in registration order
// by Run/FatalExit. name identifies the step for future diagnostics; it is
// not otherwise interpreted. The returned func removes fn again; safe to
// call more than once.
func Register(name string, fn Fn) func() {
	mu.Lock()
	id := nextID
	nextID++
	handlers = append(handlers, entry{id: id, fn: fn})
	mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			mu.Lock()
			defer mu.Unlock()
			for i, e := range handlers {
				if e.id == id {
					handlers = append(handlers[:i], handlers[i+1:]...)
					return
				}
			}
		})
	}
}

// OnForceExit registers fn to run (with its own recover, so one hook
// panicking never stops the rest) on an immediate-exit path that skips the
// ordered handler sequence entirely: a second signal/fatal exit arriving
// while a sequence is already running. Register/Run's own handler sequence
// already gets a bounded, ordered chance to clean up; this is the
// last-resort net for state that sequence never reaches on that path (e.g.
// bridge.KillAllLiveChildren, restoring a TUI's terminal mode) — called
// just before osExit, bounded by ForceExitDeadline. The returned func
// removes fn again, mirroring Register; safe to call more than once.
// Cleared by ResetForTests.
func OnForceExit(fn func()) func() {
	mu.Lock()
	id := nextForceExitID
	nextForceExitID++
	forceExitFns = append(forceExitFns, forceExitEntry{id: id, fn: fn})
	mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			mu.Lock()
			defer mu.Unlock()
			for i, e := range forceExitFns {
				if e.id == id {
					forceExitFns = append(forceExitFns[:i], forceExitFns[i+1:]...)
					return
				}
			}
		})
	}
}

func runForceExitHooks() {
	mu.Lock()
	fns := append([]forceExitEntry{}, forceExitFns...)
	mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, e := range fns {
			func() {
				defer func() {
					if r := recover(); r != nil {
						slog.Error("shutdown force-exit hook panicked", "recovered", r)
					}
				}()
				e.fn()
			}()
		}
	}()
	select {
	case <-done:
	case <-time.After(ForceExitDeadline):
		// A hung hook must never block osExit — it just misses out on
		// anything registered after it in the same call.
	}
}

func snapshot() []entry {
	mu.Lock()
	defer mu.Unlock()
	out := make([]entry, len(handlers))
	copy(out, handlers)
	return out
}

func runHandlers(ctx context.Context) {
	for _, e := range snapshot() {
		func() {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("shutdown handler panicked", "handler", e.id, "recovered", r)
				}
			}() // one handler misbehaving must not block the rest
			_ = e.fn(ctx)
		}()
	}
}

func runSequenceAndExit(exitCode int) {
	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	done := make(chan struct{})
	go func() {
		runHandlers(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(Deadline):
		// The ordered sequence didn't finish in time — run the same
		// last-resort hooks a racing second signal/fatal exit would (e.g.
		// bridge.KillAllLiveChildren), since whatever handler is still
		// running past Deadline never got the chance to.
		runForceExitHooks()
	}
	osExit(exitCode)
}

// ExitCodeFor maps a signal to the conventional 128+signal exit code —
// exported so callers outside this package (cli.Execute, N1) that must wait
// on the same in-flight signal can compute the exit code Run/FatalExit will
// use without duplicating the mapping.
func ExitCodeFor(sig os.Signal) int {
	if sig == syscall.SIGTERM {
		return 143
	}
	return 130 // SIGINT/os.Interrupt, and the default for anything else
}

// Run runs the shutdown sequence for sig and exits the process with
// 128+signal. Call it once per received signal — a call that arrives while
// an earlier one is still running force-exits immediately instead of
// running the sequence a second time.
func Run(sig os.Signal) {
	code := ExitCodeFor(sig)
	mu.Lock()
	if shuttingDown {
		mu.Unlock()
		runForceExitHooks()
		osExit(code)
		return
	}
	shuttingDown = true
	mu.Unlock()
	runSequenceAndExit(code)
}

// FatalExit runs the same bounded shutdown sequence as Run, then exits with
// code — for a fatal disconnect/protocol error that must still unregister
// and close cleanly rather than skipping cleanup with a bare os.Exit. Races
// an in-progress signal shutdown the same way Run races a second signal.
func FatalExit(code int) {
	mu.Lock()
	if shuttingDown {
		mu.Unlock()
		runForceExitHooks()
		osExit(code)
		return
	}
	shuttingDown = true
	mu.Unlock()
	runSequenceAndExit(code)
}

// ResetForTests clears registered handlers, force-exit hooks and the
// in-progress flag.
func ResetForTests() {
	mu.Lock()
	defer mu.Unlock()
	handlers = nil
	nextID = 0
	shuttingDown = false
	forceExitFns = nil
	nextForceExitID = 0
}

// HandlerCount reports how many handlers are currently registered — a test
// seam for other packages (e.g. output's spinner) that register through
// this package and want to assert they did so exactly once.
func HandlerCount() int {
	mu.Lock()
	defer mu.Unlock()
	return len(handlers)
}

// RunHandlersForTests runs the registered handlers once, in registration
// order, without exiting the process — the non-fatal counterpart to
// Run/FatalExit for a test that needs to observe a handler's side effect
// (e.g. that a spinner's cursor-restore handler actually restores the
// cursor) without racing a real os.Exit.
func RunHandlersForTests(ctx context.Context) {
	runHandlers(ctx)
}

// RunForceExitHooksForTests runs the registered OnForceExit hooks once,
// without exiting the process — the force-exit counterpart to
// RunHandlersForTests.
func RunForceExitHooksForTests() {
	runForceExitHooks()
}

// ForceExitHookCountForTests reports how many OnForceExit hooks are
// currently registered — HandlerCount's counterpart for force-exit hooks.
func ForceExitHookCountForTests() int {
	mu.Lock()
	defer mu.Unlock()
	return len(forceExitFns)
}
