package bridge

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Process-level checks of the era rules whose effect a transition table
// cannot show: real writers, timers and children, and what the client sees.

// An initialize answered after its request timed out still counts: the
// process is marked initialized, which clears the cached verdict as the first
// successful initialize does. The client already had its 504 and gets no
// session.
func TestEra_AbandonedInitializeAnswerStillCounts(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) {
		o.ProbeTimeout = 200 * time.Millisecond
		o.RequestTimeout = 500 * time.Millisecond
	}, "--era=dual", "--discover-hold", "--initialize-hold")

	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 400 || eraName(b) != "legacy" {
		t.Fatalf("setup: %d %s era=%q", r.status, r.body, eraName(b))
	}
	if r := eraPost(t, b, acceptJSON, eraInitialize(2).body, nil); r.status != 504 {
		t.Fatalf("held initialize: %d %s", r.status, r.body)
	}
	// The held discover answer, then the initialize's, reach the bridge.
	releaseUntil(t, b, 2)
	waitFor(t, "the abandoned initialize's answer to count", func() bool { return eraName(b) == "legacy-capable" })
	if got := outstandingProbes(b); len(got) != 0 {
		t.Fatalf("outstanding after the initialize: %q", got)
	}
	if sessionMinted(b) {
		t.Fatal("an abandoned initialize minted a session")
	}
}

// A modern-looking rejection whose client left still starts the background
// probe a rejection starts.
func TestEra_AbandonedInitializeRejectionStillProbes(t *testing.T) {
	b := startEraBridge(t, nil, "--era=legacy", "--initialize-hold", "--reject-initialize=1", "--reject-code=-32601")

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		resp, _, err := eraSend(ctx, b.URL(), acceptJSON, eraInitialize(1).body, nil)
		if err == nil {
			resp.Body.Close()
		}
		errc <- err
	}()
	firstPendingID(t, b)
	cancel()
	<-errc
	drained := make(chan struct{})
	go func() { b.wg.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		t.Fatal("handler did not return after the client left")
	}
	if eraName(b) != "unknown" || probeInFlight(b) {
		t.Fatalf("setup: era=%q probe=%v", eraName(b), probeInFlight(b))
	}

	releaseUntil(t, b, 1)
	waitFor(t, "the background probe's legacy answer", func() bool { return eraName(b) == "legacy" })
	if n := discoverCount(t, b); n != 1 {
		t.Fatalf("child saw %d probes, want 1", n)
	}
}

// A modern-shaped request to a dead, unreplaced child is "not running",
// whatever the verdict, and no probe is written to it.
func TestEra_ModernRequestToDeadChildIsNotRunning(t *testing.T) {
	for _, explicit := range []bool{true, false} {
		b := startEraBridge(t, nil, "--era=legacy")
		if explicit {
			if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 400 || verdictSourceOf(b) != srcProbe {
				t.Fatalf("setup: %d %s", r.status, r.body)
			}
		}
		child := b.GetCurrentChild()
		_ = child.Close()
		waitFor(t, "the child to exit", func() bool { return child.HasExited() })
		b.mu.Lock()
		seqBefore := b.era.m.seq
		b.mu.Unlock()

		r := eraPost(t, b, acceptJSON, modernCall(2), nil)
		if r.status != 502 || !strings.Contains(r.body, "not running") || strings.Contains(r.body, "initialize is required") {
			t.Errorf("explicit legacy %v: %d %s", explicit, r.status, r.body)
		}
		b.mu.Lock()
		seqAfter := b.era.m.seq
		b.mu.Unlock()
		if seqAfter != seqBefore || probeInFlight(b) {
			t.Errorf("explicit legacy %v: a probe was started for a dead child", explicit)
		}
	}
}

// With a real timer: the re-probe interval counts from the timeout, not from
// when the probe was sent. A request right after a timeout does not re-probe,
// even though the timed-out probe went out a full probe timeout before it;
// one after the interval does.
func TestEraProbe_ReprobeIntervalCountsFromTheTimeout(t *testing.T) {
	const probe = time.Second
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.ProbeTimeout = probe }, "--era=modern", "--discover-hold")

	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 400 || r.elapsed < probe*3/4 {
		t.Fatalf("first probe should time out: %d after %s", r.status, r.elapsed)
	}
	answered := time.Now()
	b.mu.Lock()
	q := b.era.m.quietUntil
	b.mu.Unlock()
	// The probe was sent about a probe timeout before its answer: an interval
	// counted from the send (or never set) would already be over by now.
	if q.IsZero() || q.Before(answered.Add(-probe/4).Add(probe)) {
		t.Fatalf("quiet until %v, want at least a probe timeout after the timeout (answered %v)", q, answered)
	}
	sent := time.Now()
	if r := eraPost(t, b, acceptJSON, modernCall(2), nil); r.status != 400 {
		t.Fatalf("right after the timeout: %d %s", r.status, r.body)
	}
	// Judged on the machine's own clock: inside the interval no probe may
	// start; a slow run that got past it must see one.
	if inside := sent.Before(q); inside == probeInFlight(b) {
		t.Fatalf("request at %v with the interval ending %v: probe in flight %v", sent, q, probeInFlight(b))
	}
	waitFor(t, "no probe out", func() bool { return !probeInFlight(b) })
	waitReprobeDue(t, b)
	if r := eraPost(t, b, acceptJSON, modernCall(3), nil); r.status != 400 || !probeInFlight(b) {
		t.Fatalf("after the interval: %d, probe in flight %v", r.status, probeInFlight(b))
	}
}

// A flood of bare initialize rejections against a child that never
// answers a probe starts each probe as soon as the last one timed out (the
// child has only been silent), but never two at once: about one probe per
// probe timeout, not one per rejection.
func TestEraProbe_RejectionFloodIsRateLimited(t *testing.T) {
	const probe = 100 * time.Millisecond
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.ProbeTimeout = probe }, "--era=modern", "--discover-drop=1000000")
	_, st := b.currentChildEra()
	bare := initRejection(-32601)

	start := time.Now()
	rejections := 0
	for time.Since(start) < time.Second {
		b.noteInitializeAnswer(st, bare)
		rejections++
		time.Sleep(200 * time.Microsecond)
	}
	elapsed := time.Since(start)
	b.mu.Lock()
	probes := b.era.m.seq
	b.mu.Unlock()
	limit := int(elapsed/probe) + 2
	if probes > limit || probes < 3 {
		t.Fatalf("%d probes for %d rejections over %s, want 3..%d", probes, rejections, elapsed, limit)
	}
}

// Backoff runs below use a 200ms probe timeout and an 800ms cap over 4s: the
// ideal schedule probes at 0, 200, 600, 1400, 2200, 3000 and 3800ms, seven
// in all, against twenty for one per probe timeout. A stall long enough to
// turn one soft answer into a timeout restarts the schedule from the base,
// which costs about three probes more; the bound allows for one restart.
const (
	backoffProbe = 200 * time.Millisecond
	backoffCap   = 800 * time.Millisecond
	backoffRun   = 4 * time.Second
	backoffMax   = 12
)

func backoffOpts(o *StartBridgeOptions) {
	o.ProbeTimeout = backoffProbe
	o.MaxReprobeInterval = backoffCap
}

// Once the child has answered soft-legacy, a rejection flood follows the
// backoff schedule like modern traffic does: a handful of probes.
func TestEraProbe_RejectionFloodAfterSoftAnswersBacksOff(t *testing.T) {
	t.Parallel()
	b := startEraBridge(t, backoffOpts, "--era=modern", "--discover-error=-32602:1000000")
	_, st := b.currentChildEra()
	bare := initRejection(-32601)

	start := time.Now()
	rejections := 0
	for time.Since(start) < backoffRun {
		b.noteInitializeAnswer(st, bare)
		rejections++
		time.Sleep(200 * time.Microsecond)
	}
	b.mu.Lock()
	probes := b.era.m.seq
	b.mu.Unlock()
	if probes > backoffMax || probes < 3 {
		t.Fatalf("%d probes for %d rejections over %s, want 3..%d", probes, rejections, backoffRun, backoffMax)
	}
}

// A child that rejects initialize with a modern-only list and answers every
// probe with -32602: the rejection verdict stands, and its re-checks follow
// the backoff schedule under a burst of modern requests or of refused 2025
// requests alike, never one probe per request.
func TestEraProbe_SoftRecheckAnswersOnARejectionVerdictBackOff(t *testing.T) {
	t.Parallel()
	for _, via := range []string{"modern requests", "refused 2025 requests"} {
		t.Run(via, func(t *testing.T) {
			t.Parallel()
			b := startEraBridge(t, backoffOpts, "--era=modern", "--discover-error=-32602:1000000")
			if r := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil); r.status != 400 || !b.eraModernOnly() || verdictSourceOf(b) != srcRejection {
				t.Fatalf("setup: %d %s src=%v", r.status, r.body, verdictSourceOf(b))
			}
			start := time.Now()
			requests := 0
			for i := 2; time.Since(start) < backoffRun; i++ {
				if via == "modern requests" {
					if r := eraPost(t, b, acceptJSON, modernCall(i), nil); r.status != 200 {
						t.Fatalf("modern request %d: %d %s", i, r.status, r.body)
					}
				} else if r := eraPost(t, b, acceptJSON, eraInitialize(i).body, nil); r.status != 400 || !strings.Contains(r.body, "only speaks") {
					t.Fatalf("2025 request %d: %d %s", i, r.status, r.body)
				}
				requests++
				time.Sleep(2 * time.Millisecond)
			}
			if verdictSourceOf(b) != srcRejection {
				t.Fatalf("src=%v: a soft answer replaced the rejection verdict", verdictSourceOf(b))
			}
			if n := discoverCount(t, b); n > backoffMax || n < 3 || n*5 > requests {
				t.Fatalf("child saw %d probes for %d requests over %s, want 3..%d", n, requests, backoffRun, backoffMax)
			}
		})
	}
}

// A modern-only verdict taken from one odd initialize rejection does not
// lock 2025 traffic out of a dual-era child for good. Its first check probe
// goes unanswered; later traffic re-checks it at the interval, modern
// requests and refused 2025 requests alike, and the dual-era answer restores
// 2025 service.
func TestEra_RejectionVerdictIsRecheckedByLaterTraffic(t *testing.T) {
	t.Parallel()
	for _, via := range []string{"modern request", "refused 2025 request"} {
		t.Run(via, func(t *testing.T) {
			t.Parallel()
			// Long enough that the held re-check is surely still out when the
			// response is checked.
			b := startEraBridge(t, func(o *StartBridgeOptions) { o.ProbeTimeout = time.Second },
				"--era=dual", "--discover-drop=2", "--discover-hold", "--reject-initialize=1", "--reject-modern-evidence")
			// The first probe is dropped: a timeout verdict.
			if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 400 || verdictSourceOf(b) != srcTimeout {
				t.Fatalf("setup: %d %s src=%v", r.status, r.body, verdictSourceOf(b))
			}
			// The odd rejection lists only 2026-07-28; its check probe is dropped too.
			if r := eraPost(t, b, acceptJSON, eraInitialize(2).body, nil); r.status != 400 || !b.eraModernOnly() {
				t.Fatalf("rejection: %d %s modernOnly=%v", r.status, r.body, b.eraModernOnly())
			}
			waitFor(t, "the check probe to time out", func() bool { return !probeInFlight(b) && len(outstandingProbes(b)) == 2 })
			if !b.eraModernOnly() || verdictSourceOf(b) != srcRejection {
				t.Fatalf("after the unanswered check: modernOnly=%v src=%v", b.eraModernOnly(), verdictSourceOf(b))
			}

			waitReprobeDue(t, b)
			// The re-check's answer is held by the child, so a response that
			// arrives while the probe is still out did not wait for it.
			if via == "modern request" {
				if r := eraPost(t, b, acceptJSON, modernCall(4), nil); r.status != 200 {
					t.Fatalf("modern request: %d %s", r.status, r.body)
				}
			} else if r := eraPost(t, b, acceptJSON, eraInitialize(4).body, nil); r.status != 400 || !strings.Contains(r.body, "initialize is not supported") {
				t.Fatalf("2025 initialize: %d %s", r.status, r.body)
			}
			if !probeInFlight(b) {
				t.Fatal("no re-check was started, or the response waited for its answer")
			}
			releaseDirect(t, b)
			waitFor(t, "the re-check's dual-era answer", func() bool { return verdictSourceOf(b) == srcProbe })
			if b.eraModernOnly() {
				t.Fatal("the dual-era answer left the child modern-only")
			}
			if r := eraPost(t, b, acceptJSON, eraInitialize(5).body, nil); r.status != 200 || r.header.Get("Mcp-Session-Id") == "" {
				t.Fatalf("initialize after the re-check: %d %s", r.status, r.body)
			}
		})
	}
}

// A modern request that joins a probe sent before the first initialize
// does not take that probe's outcome: once it settles, a fresh probe asks the
// initialized child.
func TestEraProbe_RequestJoiningAPreInitializeProbeProbesAfresh(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.ProbeTimeout = time.Second }, "--era=legacy-silent")

	first := make(chan eraResponse, 1)
	go func() { first <- eraPost(t, b, acceptJSON, modernCall(1), nil) }()
	waitProbeInFlight(t, b)
	if r := eraPost(t, b, acceptJSON, eraInitialize(2).body, nil); r.status != 200 {
		t.Fatalf("initialize: %d %s", r.status, r.body)
	}
	// Joins the pre-initialize probe, which the silent child never answers.
	r := eraPost(t, b, acceptJSON, modernCall(3), nil)
	if r.status != 400 || !strings.Contains(r.body, "2025-11-25") {
		t.Fatalf("joined request: %d %s", r.status, r.body)
	}
	if rf := <-first; rf.status != 400 {
		t.Fatalf("first waiter: %d %s", rf.status, rf.body)
	}
	// It asked again: a fresh probe, which the initialized child answers. (A
	// request that took the old probe's outcome would leave no verdict.)
	waitFor(t, "the fresh probe's answer", func() bool { return verdictSourceOf(b) == srcProbe })
	if r.elapsed > 2*time.Second {
		t.Fatalf("waited %s, more than a probe timeout plus slack", r.elapsed)
	}
}

// A client that left before its initialize was answered: the later
// successful answer still marks the process initialized.
func TestEra_DisconnectedInitializeSuccessStillCounts(t *testing.T) {
	b := startEraBridge(t, nil, "--era=legacy", "--initialize-hold")

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		resp, _, err := eraSend(ctx, b.URL(), acceptJSON, eraInitialize(1).body, nil)
		if err == nil {
			resp.Body.Close()
		}
		errc <- err
	}()
	firstPendingID(t, b)
	cancel()
	<-errc
	drained := make(chan struct{})
	go func() { b.wg.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		t.Fatal("handler did not return after the client left")
	}
	if eraName(b) != "unknown" {
		t.Fatalf("setup: era=%q", eraName(b))
	}
	releaseUntil(t, b, 1)
	waitFor(t, "the late successful answer to count", func() bool { return eraName(b) == "legacy-capable" })
	if sessionMinted(b) {
		t.Fatal("an abandoned initialize minted a session")
	}
}

// A probe answered -32603 (a booting child) is legacy at once, without
// waiting for the timeout, and a later modern answer upgrades it.
func TestEraProbe_SoftLegacyAnswerThenModernUpgrades(t *testing.T) {
	b := startEraBridge(t, nil, "--era=modern", "--discover-error=-32603:1") // the default 15s probe timeout
	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 400 || r.elapsed > 5*time.Second || verdictSourceOf(b) != srcSoft {
		t.Fatalf("soft answer: %d after %s src=%v: %s", r.status, r.elapsed, verdictSourceOf(b), r.body)
	}
	makeReprobeDue(b)
	if r := eraPost(t, b, acceptJSON, modernCall(2), nil); r.status != 400 || r.elapsed > 5*time.Second {
		t.Fatalf("re-probing request: %d after %s", r.status, r.elapsed)
	}
	waitFor(t, "the re-probe's modern answer", func() bool { return eraName(b) == "modern" })
	if r := eraPost(t, b, acceptJSON, modernCall(3), nil); r.status != 200 {
		t.Fatalf("after the upgrade: %d %s", r.status, r.body)
	}
	if n := discoverCount(t, b); n != 2 {
		t.Fatalf("child saw %d probes, want 2", n)
	}
}

// A child answering every probe with -32602 is served at once each
// time, stays re-probeable, and backs off.
func TestEraProbe_SoftLegacyAnswerStaysReprobeable(t *testing.T) {
	b := startEraBridge(t, nil, "--era=modern", "--discover-error=-32602:100")
	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 400 || r.elapsed > 5*time.Second || verdictSourceOf(b) != srcSoft {
		t.Fatalf("soft answer: %d after %s src=%v", r.status, r.elapsed, verdictSourceOf(b))
	}
	if r := eraPost(t, b, acceptJSON, modernCall(2), nil); r.status != 400 || probeInFlight(b) || discoverCount(t, b) != 1 {
		t.Fatalf("inside the interval: %d, probe %v", r.status, probeInFlight(b))
	}
	makeReprobeDue(b)
	if r := eraPost(t, b, acceptJSON, modernCall(3), nil); r.status != 400 || r.elapsed > 5*time.Second {
		t.Fatalf("re-probing request: %d after %s", r.status, r.elapsed)
	}
	waitFor(t, "the second soft answer", func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.era.m.softStreak == 2
	})
	b.mu.Lock()
	gap := time.Until(b.era.m.quietUntil)
	b.mu.Unlock()
	if verdictSourceOf(b) != srcSoft || gap < b.probeTimeout {
		t.Fatalf("src=%v, next re-probe in %s (want the doubled interval)", verdictSourceOf(b), gap)
	}
}

// A DiscoverResult without supportedVersions is explicit legacy, never
// re-probed.
func TestEraProbe_ResultWithoutVersionsIsExplicitLegacy(t *testing.T) {
	b := startEraBridge(t, nil, "--era=modern", "--discover-no-versions")
	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 400 || eraName(b) != "legacy" || verdictSourceOf(b) != srcProbe {
		t.Fatalf("%d %s era=%q src=%v", r.status, r.body, eraName(b), verdictSourceOf(b))
	}
	makeReprobeDue(b)
	if r := eraPost(t, b, acceptJSON, modernCall(2), nil); r.status != 400 || probeInFlight(b) {
		t.Fatalf("second request: %d, probe %v", r.status, probeInFlight(b))
	}
	if n := discoverCount(t, b); n != 1 {
		t.Fatalf("child saw %d probes, want 1", n)
	}
}

// Under steady modern traffic, a child answering -32602 to every probe gets a
// handful of probes with growing gaps, then one per cap, far fewer than one
// per probe timeout.
func TestEraProbe_SoftLegacyBackoffUnderSteadyTraffic(t *testing.T) {
	t.Parallel()
	b := startEraBridge(t, backoffOpts, "--era=modern", "--discover-error=-32602:1000000")

	start := time.Now()
	for i := 1; time.Since(start) < backoffRun; i++ {
		if r := eraPost(t, b, acceptJSON, modernCall(i), nil); r.status != 400 {
			t.Fatalf("request %d: %d %s", i, r.status, r.body)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if n := discoverCount(t, b); n > backoffMax || n < 3 {
		t.Fatalf("child saw %d probes in %s, want 3..%d", n, backoffRun, backoffMax)
	}
}
