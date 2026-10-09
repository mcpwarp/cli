package cli

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
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
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("USERPROFILE", dir)
	t.Setenv("MCPWARP_TOKEN", "")

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

// updateCacheFile is update.cacheData's on-disk shape
// (~/.mcpwarp/update-check.json), restated here so these tests can seed
// and inspect the cache the real update.Check reads and writes without
// reaching into that package's unexported internals.
type updateCacheFile struct {
	LastCheck time.Time `json:"last_check"`
	LastTag   string    `json:"last_tag"`
}

func updateCachePath(home string) string {
	return filepath.Join(home, ".mcpwarp", "update-check.json")
}

func writeUpdateCache(t *testing.T, home string, c updateCacheFile) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(updateCachePath(home)), 0o700); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(updateCachePath(home), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func readUpdateCache(t *testing.T, home string) updateCacheFile {
	t.Helper()
	b, err := os.ReadFile(updateCachePath(home))
	if err != nil {
		t.Fatalf("reading the update cache: %v", err)
	}
	var c updateCacheFile
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

// useRealUpdateCheck routes updateCheck through the real update.Check —
// OnFetch hook, cache and all — pointed at endpoint with the given fetch
// timeout and a stderr that counts as a terminal, under a fresh $HOME it
// returns. It clears the opt-out env vars TestMain (and a CI runner) set,
// since this is the one place in the package that wants Check to run.
func useRealUpdateCheck(t *testing.T, endpoint string, timeout time.Duration) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("MCPWARP_NO_UPDATE_NOTIFIER", "")
	t.Setenv("CI", "")
	t.Setenv("MCPWARP_TOKEN", "")

	orig := updateCheck
	updateCheck = func(ctx context.Context, current string, opts update.Options) *update.Notice {
		opts.Endpoint = endpoint
		opts.Timeout = timeout
		opts.IsTerminal = func() bool { return true }
		return update.Check(ctx, current, opts)
	}
	t.Cleanup(func() { updateCheck = orig })
	return home
}

// releaseServerAfter answers GitHub's latest-release call with tag after
// delay, or not at all if the client gives up first (delay < 0 waits for
// exactly that) — counting every request it receives.
func releaseServerAfter(t *testing.T, tag string, delay time.Duration) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var after <-chan time.Time
		if delay >= 0 {
			after = time.After(delay)
		}
		select {
		case <-after:
		case <-r.Context().Done():
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"tag_name": tag})
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

// captureOutputStderr swaps output.Stderr (where Notice.Print writes) for
// a buffer until t ends.
func captureOutputStderr(t *testing.T) *strings.Builder {
	t.Helper()
	var buf strings.Builder
	origStderr := output.Stderr
	output.Stderr = &buf
	t.Cleanup(func() { output.Stderr = origStderr })
	return &buf
}

// TestUpdateNoticeWaitsOutSlowFetch is the regression this whole wait
// exists for: a stale cache sends Check to the network, and the fetch
// answers well after updateCheckGrace (as a cold api.github.com request
// routinely does) but inside its timeout. The command must still print the
// notice and — the part that used to be lost when the process exited first
// — leave the fresh result in the cache by the time Execute returns.
func TestUpdateNoticeWaitsOutSlowFetch(t *testing.T) {
	srv, calls := releaseServerAfter(t, "v0.2.0", updateCheckGrace+300*time.Millisecond)
	home := useRealUpdateCheck(t, srv.URL, 2*time.Second)
	start := time.Now()
	writeUpdateCache(t, home, updateCacheFile{LastCheck: start.Add(-25 * time.Hour), LastTag: "v0.1.0"})
	buf := captureOutputStderr(t)

	if code := Execute("0.1.0", []string{"whoami"}); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}

	if !strings.Contains(buf.String(), "A new release of mcpwarp is available: 0.1.0 → 0.2.0") {
		t.Errorf("expected the update notice, got %q", buf.String())
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("expected 1 HTTP call (stale cache), got %d", got)
	}
	c := readUpdateCache(t, home)
	if c.LastTag != "v0.2.0" {
		t.Errorf("cache LastTag = %q, want v0.2.0", c.LastTag)
	}
	if c.LastCheck.Before(start) {
		t.Errorf("cache LastCheck = %v, want this run's check (after %v)", c.LastCheck, start)
	}
}

// TestUpdateNoticeFreshCacheSkipsFetchWait: a fresh cache answers without
// the network, so nothing extends the wait — against an endpoint that
// would hang for the whole (deliberately long) fetch timeout had Check
// called it.
func TestUpdateNoticeFreshCacheSkipsFetchWait(t *testing.T) {
	srv, calls := releaseServerAfter(t, "v0.3.0", -1)
	home := useRealUpdateCheck(t, srv.URL, 5*time.Second)
	writeUpdateCache(t, home, updateCacheFile{LastCheck: time.Now().Add(-time.Hour), LastTag: "v0.2.0"})
	buf := captureOutputStderr(t)

	start := time.Now()
	if code := Execute("0.1.0", []string{"whoami"}); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	elapsed := time.Since(start)

	if !strings.Contains(buf.String(), "A new release of mcpwarp is available: 0.1.0 → 0.2.0") {
		t.Errorf("expected the cached notice, got %q", buf.String())
	}
	if got := calls.Load(); got != 0 {
		t.Errorf("expected no HTTP calls (cache fresh), got %d", got)
	}
	if elapsed >= time.Second {
		t.Errorf("Execute took %v with a fresh cache, want well under the 5s fetch timeout", elapsed)
	}
}

// TestUpdateNoticeHungFetchRecordsAttempt: an endpoint that never answers
// costs the command the fetch timeout, no more, and Check's failed-fetch
// path still records the attempt (keeping the last known tag) before
// Execute returns — so the next run is debounced instead of hanging again.
func TestUpdateNoticeHungFetchRecordsAttempt(t *testing.T) {
	const timeout = 300 * time.Millisecond
	srv, calls := releaseServerAfter(t, "v0.3.0", -1)
	home := useRealUpdateCheck(t, srv.URL, timeout)
	start := time.Now()
	writeUpdateCache(t, home, updateCacheFile{LastCheck: start.Add(-25 * time.Hour), LastTag: "v0.2.0"})
	buf := captureOutputStderr(t)

	if code := Execute("0.1.0", []string{"whoami"}); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	elapsed := time.Since(start)

	if strings.Contains(buf.String(), "A new release") {
		t.Errorf("expected no notice from a failed fetch, got %q", buf.String())
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("expected 1 HTTP call (stale cache), got %d", got)
	}
	if elapsed < timeout || elapsed > timeout+updateFetchMargin+time.Second {
		t.Errorf("Execute took %v, want about the %v fetch timeout", elapsed, timeout)
	}
	c := readUpdateCache(t, home)
	if c.LastCheck.Before(start) {
		t.Errorf("cache LastCheck = %v, want the failed attempt recorded (after %v)", c.LastCheck, start)
	}
	if c.LastTag != "v0.2.0" {
		t.Errorf("cache LastTag = %q, want the last known v0.2.0 kept", c.LastTag)
	}
}

// fakeBlockingCheck swaps updateCheck for a fake that runs before (e.g.
// reports a fetch via opts.OnFetch, or sleeps) and then blocks until the
// returned release func is called, after which it returns nil. Meant for
// a synctest bubble, where the fake's blocking and the waits under test
// all run on the bubble's fake clock.
func fakeBlockingCheck(t *testing.T, before func(opts update.Options)) (release func()) {
	t.Helper()
	unblock := make(chan struct{})
	orig := updateCheck
	updateCheck = func(ctx context.Context, current string, opts update.Options) *update.Notice {
		before(opts)
		<-unblock
		return nil
	}
	t.Cleanup(func() { updateCheck = orig })
	return func() { close(unblock) }
}

// TestWaitBeforeExitNoFetchStopsAtGrace: a check that is slow without ever
// starting a fetch (a stuck cache read, say) gets updateCheckGrace and not
// a moment more — the extension is only for a fetch actually in flight.
func TestWaitBeforeExitNoFetchStopsAtGrace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := fakeBlockingCheck(t, func(update.Options) {})
		ctx := &Context{CommandName: "status", UpdateChecker: startUpdateChecker(context.Background(), "0.1.0", t.TempDir(), NewLogger(false))}
		buf := captureOutputStderr(t)

		start := time.Now()
		printUpdateNotice(ctx)
		if elapsed := time.Since(start); elapsed != updateCheckGrace {
			t.Errorf("waited %v, want exactly updateCheckGrace (%v)", elapsed, updateCheckGrace)
		}
		if buf.Len() != 0 {
			t.Errorf("expected no output, got %q", buf.String())
		}

		release()
		ctx.UpdateChecker.Wait(time.Second)
	})
}

// TestWaitBeforeExitFetchAfterGraceStopsAtGrace: a check that only decides
// to fetch after updateCheckGrace has already run out is too late for the
// extension — the wait stops at the grace, same as a check that never
// fetches.
func TestWaitBeforeExitFetchAfterGraceStopsAtGrace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := fakeBlockingCheck(t, func(opts update.Options) {
			time.Sleep(300 * time.Millisecond)
			opts.OnFetch(2 * time.Second)
		})
		ctx := &Context{CommandName: "status", UpdateChecker: startUpdateChecker(context.Background(), "0.1.0", t.TempDir(), NewLogger(false))}

		start := time.Now()
		if n := ctx.UpdateChecker.WaitBeforeExit(ctx.Context().Done()); n != nil {
			t.Errorf("WaitBeforeExit = %+v, want nil", n)
		}
		if elapsed := time.Since(start); elapsed != updateCheckGrace {
			t.Errorf("waited %v, want exactly updateCheckGrace (%v)", elapsed, updateCheckGrace)
		}

		release()
		ctx.UpdateChecker.Wait(time.Second)
	})
}

// TestWaitBeforeExitWaitsOutSlowFetch is TestUpdateNoticeWaitsOutSlowFetch
// on the fake clock: a fetch that reports itself and then takes longer than
// updateCheckGrace still has its notice printed, as soon as it lands.
func TestWaitBeforeExitWaitsOutSlowFetch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const fetchTook = updateCheckGrace + 500*time.Millisecond
		orig := updateCheck
		updateCheck = func(ctx context.Context, current string, opts update.Options) *update.Notice {
			opts.OnFetch(2 * time.Second)
			time.Sleep(fetchTook)
			return &update.Notice{Current: "0.1.0", Latest: "0.2.0"}
		}
		t.Cleanup(func() { updateCheck = orig })
		ctx := &Context{CommandName: "status", UpdateChecker: startUpdateChecker(context.Background(), "0.1.0", t.TempDir(), NewLogger(false))}
		buf := captureOutputStderr(t)

		start := time.Now()
		printUpdateNotice(ctx)
		if elapsed := time.Since(start); elapsed != fetchTook {
			t.Errorf("waited %v, want the fetch's own %v", elapsed, fetchTook)
		}
		if !strings.Contains(buf.String(), "A new release of mcpwarp is available: 0.1.0 → 0.2.0") {
			t.Errorf("expected the update notice, got %q", buf.String())
		}
	})
}

// TestWaitBeforeExitBoundedByFetchTimeout: a check that reported a fetch
// and then never returns (past its own timeout, which update.Check's
// context.WithTimeout normally rules out) still releases the command at
// the fetch timeout plus updateFetchMargin — never forever.
func TestWaitBeforeExitBoundedByFetchTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const timeout = time.Second
		release := fakeBlockingCheck(t, func(opts update.Options) { opts.OnFetch(timeout) })
		ctx := &Context{CommandName: "status", UpdateChecker: startUpdateChecker(context.Background(), "0.1.0", t.TempDir(), NewLogger(false))}
		buf := captureOutputStderr(t)

		start := time.Now()
		printUpdateNotice(ctx)
		if elapsed, want := time.Since(start), timeout+updateFetchMargin; elapsed != want {
			t.Errorf("waited %v, want the fetch timeout plus margin (%v)", elapsed, want)
		}
		if buf.Len() != 0 {
			t.Errorf("expected no output, got %q", buf.String())
		}

		release()
		ctx.UpdateChecker.Wait(time.Second)
	})
}

// TestWaitBeforeExitCutShortBySignal: runRoot calls printUpdateNotice
// before its own signal branch, so a Ctrl-C landing during the extended
// wait (the command's ctx cancelled by signal.NotifyContext) must end the
// wait right then rather than holding the exit for the rest of the fetch.
func TestWaitBeforeExitCutShortBySignal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const signalAt = updateCheckGrace + 100*time.Millisecond
		release := fakeBlockingCheck(t, func(opts update.Options) { opts.OnFetch(10 * time.Second) })
		cmdCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ctx := &Context{Ctx: cmdCtx, CommandName: "status", UpdateChecker: startUpdateChecker(cmdCtx, "0.1.0", t.TempDir(), NewLogger(false))}
		buf := captureOutputStderr(t)

		time.AfterFunc(signalAt, cancel)
		start := time.Now()
		printUpdateNotice(ctx)
		if elapsed := time.Since(start); elapsed != signalAt {
			t.Errorf("waited %v, want to stop at the signal (%v)", elapsed, signalAt)
		}
		if buf.Len() != 0 {
			t.Errorf("expected no output, got %q", buf.String())
		}

		release()
		ctx.UpdateChecker.Wait(time.Second)
	})
}

// TestWaitBeforeExitSignalledBeforeEntry: a signal that has already landed
// by the time WaitBeforeExit runs wins even over a finished check — the
// notice must not print during shutdown just because select happened to
// pick the ready result over the closed done. select picks at random among
// ready cases, so this repeats 64 times: a WaitBeforeExit that only checked
// done alongside result would get past all of them with probability 2^-64.
func TestWaitBeforeExitSignalledBeforeEntry(t *testing.T) {
	done := make(chan struct{})
	close(done)
	for range 64 {
		uc := &updateChecker{result: make(chan *update.Notice, 1), fetching: make(chan struct{})}
		uc.result <- &update.Notice{Current: "0.1.0", Latest: "0.2.0"}
		if n := uc.WaitBeforeExit(done); n != nil {
			t.Fatalf("WaitBeforeExit = %+v, want nil", n)
		}
	}
}
