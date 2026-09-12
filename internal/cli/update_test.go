package cli

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mcpwarp/cli/internal/output"
	"github.com/mcpwarp/cli/internal/update"
)

// TestUpdateNoticePrintedAfterCommand exercises the real wiring (Execute,
// not runWhoami directly): a fake, instant updateCheck seam stands in for
// the network, and whoami runs against an empty $HOME so it deterministically
// isn't logged in (exit 1) — the point is that the notice still appears,
// after the command's own output, and the exit code is unaffected by it.
func TestUpdateNoticePrintedAfterCommand(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	orig := updateCheck
	updateCheck = func(ctx context.Context, current string, opts update.Options) *update.Notice {
		return &update.Notice{Current: "0.1.0", Latest: "0.2.0"}
	}
	t.Cleanup(func() { updateCheck = orig })

	var buf strings.Builder
	origStderr := output.Stderr
	output.Stderr = &buf
	t.Cleanup(func() { output.Stderr = origStderr })

	code := Execute("0.1.0", []string{"whoami"})
	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}

	out := buf.String()
	idxNormal := strings.Index(out, "Not logged in")
	idxNotice := strings.Index(out, "A new release of mcpwarp is available: 0.1.0 → 0.2.0")
	if idxNormal == -1 {
		t.Fatalf("expected the command's own output, got %q", out)
	}
	if idxNotice == -1 {
		t.Fatalf("expected the update notice, got %q", out)
	}
	if idxNotice < idxNormal {
		t.Errorf("notice printed before the command's own output: %q", out)
	}
	if !strings.Contains(out, "To upgrade, run:") {
		t.Errorf("expected the upgrade-hint line, got %q", out)
	}
}

// TestUpdateNoticeSkippedForUp confirms the `up` exception: runRoot's
// printUpdateNotice must not fire for it (up.go handles its own notice via
// ctx.Log/output directly, never through this path) — checked here via
// lastCommandContext/printUpdateNotice rather than running the real `up`
// command, which blocks.
func TestUpdateNoticeSkippedForUp(t *testing.T) {
	orig := updateCheck
	updateCheck = func(ctx context.Context, current string, opts update.Options) *update.Notice {
		return &update.Notice{Current: "0.1.0", Latest: "0.2.0"}
	}
	t.Cleanup(func() { updateCheck = orig })

	ctx := &Context{CommandName: "up", UpdateChecker: startUpdateChecker(context.Background(), "0.1.0", t.TempDir(), NewLogger(false))}

	var buf strings.Builder
	origStderr := output.Stderr
	output.Stderr = &buf
	t.Cleanup(func() { output.Stderr = origStderr })

	printUpdateNotice(ctx)

	if buf.Len() != 0 {
		t.Errorf("expected no output for the `up` exception, got %q", buf.String())
	}

	// printUpdateNotice returns immediately for CommandName "up" without
	// ever waiting on ctx.UpdateChecker — drain it ourselves so
	// startUpdateChecker's goroutine (which reads the updateCheck var)
	// has definitely finished before the t.Cleanup above restores it.
	ctx.UpdateChecker.Wait(time.Second)
}
