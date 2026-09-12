package cli

import (
	"context"
	"log/slog"
	"time"

	"github.com/mcpwarp/cli/internal/update"
)

// updateCheckGrace is how long a command other than `up` waits, at the end
// of its run, for the background update.Check goroutine before giving up
// and dropping the notice — a slow or hung GitHub request must never delay
// the process exiting.
const updateCheckGrace = 200 * time.Millisecond

// updateCheckLogWait bounds how long `up`'s own deferred-log goroutine
// (up.go) waits for a background check that hadn't finished yet by the
// time `up` started rendering — generous next to update.Check's own 2s
// HTTP timeout, since `up` runs indefinitely anyway and losing the notice
// to a slightly slow DNS lookup would be a shame.
const updateCheckLogWait = 5 * time.Second

// updateCheck is update.Check behind a var, so a test can fake a fast,
// canned Notice without touching the network or the filesystem.
var updateCheck = update.Check

// lastCommandContext is the most recently built *Context (Root's ctxFor),
// read back by runRoot right after root.ExecuteContext returns to print
// the update notice. It has to live here rather than as a local closed
// over by ctxFor and PersistentPostRun: cobra skips PersistentPostRun
// entirely when RunE returns an error (command.go's execute() returns
// early), which is exactly the exit-1/exit-2 paths a real invocation most
// often takes — so the notice is printed unconditionally in runRoot
// instead, after ExecuteContext returns either way. Root() resets this to
// nil on every call so one invocation's leftover value can't leak into a
// later one (in this process, or a test) that never reaches ctxFor at all
// (a bad flag, an unknown command).
var lastCommandContext *Context

// printUpdateNotice waits (briefly — updateCheckGrace) for ctx's
// background check and prints it, except for `up`: it owns the terminal
// and never returns to runRoot in normal operation, and handles its own
// notice by logging instead (DESIGN.md §9's `up` exception; see up.go).
func printUpdateNotice(ctx *Context) {
	if ctx == nil || ctx.CommandName == "up" {
		return
	}
	if n := ctx.UpdateChecker.Wait(updateCheckGrace); n != nil {
		n.Print()
	}
}

// updateChecker runs update.Check in the background from the moment a
// Context is built (ctxFor, "the beginning of every command") and hands
// its result to whoever asks — most commands via PersistentPostRun below,
// `up` via its own exception path (up.go) since it never returns normally
// and owns the terminal.
type updateChecker struct {
	result chan *update.Notice
}

// startUpdateChecker launches the background check. version is the
// running binary's version (main's ldflags value, "dev" in a dev build);
// homeDir is Context.HomeDir, empty in production (update.Check falls back
// to os.UserHomeDir() itself, same as auth.CredentialsPathFor does).
func startUpdateChecker(ctx context.Context, version, homeDir string, log *slog.Logger) *updateChecker {
	uc := &updateChecker{result: make(chan *update.Notice, 1)}
	go func() {
		uc.result <- updateCheck(ctx, version, update.Options{HomeDir: homeDir, Log: log})
	}()
	return uc
}

// Wait blocks up to timeout for the background check to finish, returning
// its Notice (nil on timeout, on a nil receiver, or if there's nothing to
// report) — never blocks longer than that, so a slow network call can't
// delay a command's exit.
func (uc *updateChecker) Wait(timeout time.Duration) *update.Notice {
	if uc == nil {
		return nil
	}
	select {
	case n := <-uc.result:
		return n
	case <-time.After(timeout):
		return nil
	}
}

// TryWait is Wait(0) done without the race of a zero-duration timer: it
// returns immediately, reporting ok=false (rather than a false nil Notice)
// when the check hasn't finished yet — `up` uses this to decide whether to
// print/log the notice right away or hand off to a goroutine that waits
// for it (DESIGN.md §9's `up` exception).
func (uc *updateChecker) TryWait() (n *update.Notice, ok bool) {
	if uc == nil {
		return nil, false
	}
	select {
	case n := <-uc.result:
		return n, true
	default:
		return nil, false
	}
}
