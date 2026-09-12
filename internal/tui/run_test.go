package tui

import (
	"context"
	"io"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/mcpwarp/cli/internal/eventbus"
	"github.com/mcpwarp/cli/internal/shutdown"
)

// TestRunReturnsOnClosedBus is the run.go regression for the controlClosedMsg
// fix: closing the bus while a headless Program is running must make Run
// return (false, nil) promptly instead of hanging until ctx cancellation.
func TestRunReturnsOnClosedBus(t *testing.T) {
	old := programOptions
	programOptions = []tea.ProgramOption{tea.WithInput(nil), tea.WithOutput(io.Discard)}
	defer func() { programOptions = old }()

	bus := eventbus.New(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type result struct {
		quit bool
		err  error
	}
	done := make(chan result, 1)
	go func() {
		quit, err := Run(ctx, bus, nil, nil, nil)
		done <- result{quit, err}
	}()

	bus.Close()

	select {
	case r := <-done:
		if r.quit {
			t.Fatalf("Run() quit = true, want false (closed bus is not a user quit)")
		}
		if r.err != nil {
			t.Fatalf("Run() err = %v, want nil", r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s of the bus closing")
	}
}

// TestRunForceExitKillsProgram is the run.go regression for OnForceExit:
// a force-exit hook firing while the TUI is up (a second signal racing an
// in-progress shutdown, or the ordered handlers blowing shutdown.Deadline)
// must kill the Program and restore the terminal, and Run must report that
// as another "shutdown already under way elsewhere" case rather than a
// real error.
func TestRunForceExitKillsProgram(t *testing.T) {
	t.Cleanup(shutdown.ResetForTests)
	shutdown.ResetForTests()

	old := programOptions
	programOptions = []tea.ProgramOption{tea.WithInput(nil), tea.WithOutput(io.Discard)}
	defer func() { programOptions = old }()

	bus := eventbus.New(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type result struct {
		quit bool
		err  error
	}
	done := make(chan result, 1)
	go func() {
		quit, err := Run(ctx, bus, nil, nil, nil)
		done <- result{quit, err}
	}()

	// Run registers its OnForceExit hook itself once its goroutine gets to
	// run, so poll rather than assume it's there after a single call.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(2 * time.Second)
	for {
		select {
		case r := <-done:
			if r.quit {
				t.Fatalf("Run() quit = true, want false (force-exit is not a user quit)")
			}
			if r.err != nil {
				t.Fatalf("Run() err = %v, want nil", r.err)
			}
			goto ranOnce
		case <-ticker.C:
			shutdown.RunForceExitHooksForTests()
		case <-timeout:
			t.Fatal("Run did not return within 2s of the force-exit hook running")
		}
	}
ranOnce:

	if n := shutdown.ForceExitHookCountForTests(); n != 0 {
		t.Fatalf("force-exit hook count = %d after Run returned, want 0 (Run should have unregistered it)", n)
	}

	// A force-exit hook may fire again later (a second signal, or a leftover
	// hook from an earlier Run in the same process) after this Run has
	// already returned; it must stay a no-op rather than panic.
	shutdown.RunForceExitHooksForTests()
}
