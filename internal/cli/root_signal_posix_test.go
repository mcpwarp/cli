//go:build !windows

package cli

import (
	"bytes"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mcpwarp/cli/internal/output"
	"github.com/mcpwarp/cli/internal/shutdown"
	"github.com/spf13/cobra"
)

// TestSignalDuringCommandOwnsExit reproduces N1: a signal arriving during a
// command must decide the exit code even when the command finishes
// successfully (or returns ctx.Err()) in that same instant — as logout does
// after completing its last write right as SIGINT lands. runRoot is handed
// a bare command whose RunE self-signals and then returns, rather than
// going through Execute/Root, so the race is exercised deterministically
// in-process without depending on any real subcommand's timing.
//
// POSIX-only: self-signalling via syscall.Kill has no Windows equivalent.
func TestSignalDuringCommandOwnsExit(t *testing.T) {
	cases := []struct {
		name     string
		sig      syscall.Signal
		wantCode int
	}{
		{"SIGINT", syscall.SIGINT, 130},
		{"SIGTERM", syscall.SIGTERM, 143},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			origRunShutdown := runShutdown
			t.Cleanup(func() { runShutdown = origRunShutdown })
			var calls int32
			var gotSig os.Signal
			runShutdown = func(sig os.Signal) {
				atomic.AddInt32(&calls, 1)
				gotSig = sig
			}

			origStderr := output.Stderr
			var stderr bytes.Buffer
			output.Stderr = &stderr
			t.Cleanup(func() { output.Stderr = origStderr })

			cmd := &cobra.Command{
				Use:           "race",
				SilenceUsage:  true,
				SilenceErrors: true,
				RunE: func(cmd *cobra.Command, args []string) error {
					// Send ourselves the signal, then give the relay
					// goroutine time to observe it before this RunE
					// returns — mirrors logout finishing (successfully, or
					// by observing ctx cancellation) at the same instant
					// the signal lands.
					_ = syscall.Kill(os.Getpid(), tc.sig)
					time.Sleep(100 * time.Millisecond)
					return cmd.Context().Err() // context.Canceled once ctx is cancelled
				},
			}

			code := runRoot(cmd)

			if code != tc.wantCode {
				t.Fatalf("got exit code %d, want %d", code, tc.wantCode)
			}
			if atomic.LoadInt32(&calls) != 1 {
				t.Fatalf("expected runShutdown to run exactly once, got %d", calls)
			}
			if gotSig != tc.sig {
				t.Fatalf("runShutdown called with %v, want %v", gotSig, tc.sig)
			}
			if stderr.Len() != 0 {
				t.Fatalf("expected the context.Canceled error to be suppressed, got stderr %q", stderr.String())
			}
		})
	}
}

// TestSignalShutdownTimeoutForcesExit covers the backstop: if runShutdown
// never returns (a wedged sequence past shutdown.Deadline), runRoot must
// still force-exit with 128+sig itself rather than hanging or falling
// through to the command's own result.
//
// POSIX-only: self-signalling via syscall.Kill has no Windows equivalent.
func TestSignalShutdownTimeoutForcesExit(t *testing.T) {
	origRunShutdown := runShutdown
	origExecuteOSExit := executeOSExit
	origDeadline := shutdown.Deadline
	t.Cleanup(func() {
		runShutdown = origRunShutdown
		executeOSExit = origExecuteOSExit
		shutdown.Deadline = origDeadline
	})
	shutdown.Deadline = 20 * time.Millisecond

	started := make(chan struct{})
	block := make(chan struct{})
	t.Cleanup(func() { close(block) }) // let the leaked goroutine finish once the test is done
	runShutdown = func(os.Signal) {
		close(started)
		<-block
	}

	var forcedCode int
	var forced int32
	executeOSExit = func(code int) {
		atomic.AddInt32(&forced, 1)
		forcedCode = code
	}

	cmd := &cobra.Command{
		Use:           "race",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			_ = syscall.Kill(os.Getpid(), syscall.SIGINT)
			time.Sleep(50 * time.Millisecond)
			return nil
		},
	}

	code := runRoot(cmd)

	// Ensures runShutdown's dereference of the package var (which happened
	// once, at its call site, well before this point) is safely ordered
	// before t.Cleanup restores that var — otherwise the leaked goroutine's
	// read and the cleanup's write are unsynchronized concurrent accesses
	// to the same var, even though they don't overlap in wall-clock time.
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("runShutdown was never invoked")
	}

	if code != 130 {
		t.Fatalf("got exit code %d, want 130", code)
	}
	if atomic.LoadInt32(&forced) != 1 || forcedCode != 130 {
		t.Fatalf("expected executeOSExit(130) exactly once, got count=%d code=%d", forced, forcedCode)
	}
}
