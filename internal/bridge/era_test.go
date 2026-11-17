package bridge

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Era-detection behaviour beyond the matrix: state transitions, the lazy
// discover probe, head holding, and the races around them. They run against
// fakemcp --era=<mode>; its debug/* methods answer under every mode and
// shape, so a test can poke a child the bridge considers legacy.

const eraTestVersion = "2026-07-28"

func startEraBridge(t *testing.T, mutate func(*StartBridgeOptions), args ...string) *Bridge {
	t.Helper()
	b := startEraBridgeCold(t, mutate, args...)
	warmUpFakeMCP(t, b)
	return b
}

// startEraBridgeCold is startEraBridge without the warm-up round trip, for a
// child whose slow start is the point.
func startEraBridgeCold(t *testing.T, mutate func(*StartBridgeOptions), args ...string) *Bridge {
	t.Helper()
	opts := StartBridgeOptions{
		Name:      "fake-era",
		SpawnSpec: SpawnSpec{Command: buildFakeMCP(t), Args: args},
		Log:       testLogger(),
	}
	if mutate != nil {
		mutate(&opts)
	}
	b, err := StartBridge(opts)
	if err != nil {
		t.Fatalf("StartBridge: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// warmUpFakeMCP round-trips a 2025-style debug request, which changes no
// era state, so the child is known to be reading stdin before a test starts
// timing probes.
func warmUpFakeMCP(t *testing.T, b *Bridge) {
	t.Helper()
	resp := postJSON(t, b.URL(), map[string]any{"jsonrpc": "2.0", "id": 0, "method": "debug/discoverCount"}, nil)
	decodeBody(t, resp)
}

func eraName(b *Bridge) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	m := b.era.m
	switch {
	case m.verdict.kind == verdictModern || m.verdict.kind == verdictRejection:
		return "modern"
	case m.verdict.kind != verdictNone:
		return "legacy"
	case m.initOK:
		return "legacy-capable"
	}
	return "unknown"
}

func outstandingProbes(b *Bridge) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.era.m.outstanding...)
}

func probeInFlight(b *Bridge) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.era.m.probe.live()
}

// probeWriteRunning reports whether st's probe write has not returned yet
// (as far as the machine knows). b.mu must be held.
func probeWriteRunning(st *eraState) bool {
	return st.m.probe.phase == probeWriting || st.m.probe.phase == probeUnsent
}

// makeReprobeDue moves the current process's re-probe interval into the past,
// as if a probe timeout had elapsed since the last timeout.
func makeReprobeDue(b *Bridge) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.era.m.quietUntil = time.Now().Add(-time.Millisecond)
}

// waitReprobeDue waits until the current process's re-probe interval has
// passed on its own.
func waitReprobeDue(t *testing.T, b *Bridge) {
	t.Helper()
	waitFor(t, "the re-probe interval to pass", func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return !time.Now().Before(b.era.m.quietUntil)
	})
}

func waitProbeInFlight(t *testing.T, b *Bridge) {
	t.Helper()
	waitFor(t, "probe in flight", func() bool { return probeInFlight(b) })
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// modernBody is a modern-shaped request; progressToken, if non-nil, rides
// in _meta.
func modernBody(id any, method string, params map[string]any, progressToken any) map[string]any {
	meta := eraMeta(eraTestVersion)
	if progressToken != nil {
		meta["progressToken"] = progressToken
	}
	p := map[string]any{"_meta": meta}
	for k, v := range params {
		p[k] = v
	}
	return map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": p}
}

func modernCall(id any) map[string]any {
	return modernBody(id, "tools/call", map[string]any{"name": "echo", "arguments": map[string]any{}}, nil)
}

type eraResponse struct {
	status  int
	header  http.Header
	body    string
	elapsed time.Duration
}

// eraSend sends one POST and returns as soon as the response head is in,
// leaving the body to the caller.
func eraSend(ctx context.Context, url, accept string, body map[string]any, hdr map[string]string) (*http.Response, time.Duration, error) {
	raw, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	// A modern-shaped body gets its standard headers unless hdr names them;
	// an empty value in hdr leaves that header out.
	if isModernShaped(body) {
		params, _ := body["params"].(map[string]any)
		meta, _ := params["_meta"].(map[string]any)
		method, _ := body["method"].(string)
		std := map[string]string{"MCP-Protocol-Version": fmt.Sprint(meta[protocolVersionMetaKey]), "Mcp-Method": method}
		switch method {
		case "tools/call", "prompts/get":
			std["Mcp-Name"] = fmt.Sprint(params["name"])
		case "resources/read":
			std["Mcp-Name"] = fmt.Sprint(params["uri"])
		}
		for k, v := range std {
			given := false
			for name := range hdr {
				given = given || strings.EqualFold(name, k)
			}
			if !given {
				req.Header.Set(k, v)
			}
		}
	}
	for k, v := range hdr {
		switch {
		case v == "":
		case http.CanonicalHeaderKey(k) != k:
			// A name spelled otherwise than canonically goes out as given.
			req.Header[k] = []string{v}
		default:
			req.Header.Set(k, v)
		}
	}
	start := time.Now()
	tr := &http.Transport{DisableKeepAlives: true}
	defer tr.CloseIdleConnections()
	resp, err := (&http.Client{Transport: tr}).Do(req)
	return resp, time.Since(start), err
}

func eraPost(t *testing.T, b *Bridge, accept string, body map[string]any, hdr map[string]string) eraResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, headAt, err := eraSend(ctx, b.URL(), accept, body, hdr)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return eraResponse{resp.StatusCode, resp.Header, string(buf), headAt}
}

const (
	acceptSSE  = "application/json, text/event-stream"
	acceptJSON = "application/json"
)

// debugCall round-trips one of the fake's debug/* methods in whatever shape
// the bridge will currently take: modern-shaped once the bridge refuses 2025
// traffic for the child, otherwise 2025-style, carrying the session id if one
// exists.
// It returns the answer's result.
func debugCall(t *testing.T, b *Bridge, method string, params map[string]any) map[string]any {
	t.Helper()
	var body map[string]any
	hdr := map[string]string{}
	if b.eraRefusesLegacy() {
		body = modernBody(1, method, params, nil)
	} else {
		body = map[string]any{"jsonrpc": "2.0", "id": 1, "method": method}
		if params != nil {
			body["params"] = params
		}
		b.session.mu.Lock()
		if b.session.hasID {
			hdr["Mcp-Session-Id"] = b.session.currentID
		}
		b.session.mu.Unlock()
	}
	r := eraPost(t, b, acceptJSON, body, hdr)
	var m map[string]any
	if err := json.Unmarshal([]byte(r.body), &m); err != nil || r.status != 200 {
		t.Fatalf("%s: %d %s", method, r.status, r.body)
	}
	result, _ := m["result"].(map[string]any)
	return result
}

func debugCount(t *testing.T, b *Bridge, method string) int {
	t.Helper()
	n, _ := debugCall(t, b, method, nil)["count"].(float64)
	return int(n)
}

func discoverCount(t *testing.T, b *Bridge) int { return debugCount(t, b, "debug/discoverCount") }

// forwardedCount is how many requests (other than discover probes and debug
// calls) the child has been sent.
func forwardedCount(t *testing.T, b *Bridge) int { return debugCount(t, b, "debug/requestCount") }

// releaseHeld makes the fake send every answer it is holding; they are on
// the wire, and so handled by the bridge, before this returns.
func releaseHeld(t *testing.T, b *Bridge) {
	t.Helper()
	debugCall(t, b, "debug/release", nil)
}

func childWasCancelled(t *testing.T, b *Bridge, requestID any) bool {
	t.Helper()
	return debugCall(t, b, "debug/wasCancelled", map[string]any{"requestId": requestID})["cancelled"] == true
}

func newEraChild(t *testing.T, args ...string) *StdioChild {
	t.Helper()
	c, err := NewStdioChild(SpawnSpec{Command: buildFakeMCP(t), Args: args}, testLogger(), "fake-era")
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestEraState_Transitions(t *testing.T) {
	t.Run("dual", func(t *testing.T) {
		b := startEraBridge(t, nil, "--era=dual")
		expect := func(want string) {
			t.Helper()
			if got := eraName(b); got != want {
				t.Fatalf("era = %q, want %q", got, want)
			}
		}
		expect("unknown")
		if r := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil); r.status != 200 || r.header.Get("Mcp-Session-Id") == "" {
			t.Fatalf("initialize: %d %v", r.status, r.header)
		}
		expect("legacy-capable")
		if r := eraPost(t, b, acceptJSON, modernCall(2), nil); r.status != 200 {
			t.Fatalf("modern call: %d %s", r.status, r.body)
		}
		expect("modern")
		only := b.eraRefusesLegacy()
		if only {
			t.Fatal("a dual-era child must not be modern-only")
		}
	})
	t.Run("legacy", func(t *testing.T) {
		b := startEraBridge(t, nil, "--era=legacy")
		eraPost(t, b, acceptJSON, eraInitialize(1).body, nil)
		if got := eraName(b); got != "legacy-capable" {
			t.Fatalf("era = %q, want legacy-capable", got)
		}
		if r := eraPost(t, b, acceptJSON, modernCall(2), nil); r.status != 400 {
			t.Fatalf("modern call on legacy child: %d %s", r.status, r.body)
		}
		if got := eraName(b); got != "legacy" {
			t.Fatalf("era = %q, want legacy", got)
		}
	})
	t.Run("modern-only", func(t *testing.T) {
		b := startEraBridge(t, nil, "--era=modern")
		if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 200 {
			t.Fatalf("modern call: %d %s", r.status, r.body)
		}
		if got := eraName(b); got != "modern" {
			t.Fatalf("era = %q, want modern", got)
		}
		// The probe answer alone does not make the bridge refuse 2025 traffic;
		// the child's own rejection of a forwarded initialize does.
		if b.eraRefusesLegacy() {
			t.Fatal("refusing 2025 traffic on a probe answer alone")
		}
		if r := eraPost(t, b, acceptJSON, eraInitialize(2).body, nil); r.status != 400 || strings.Contains(r.body, "only speaks") {
			t.Fatalf("initialize: %d %s", r.status, r.body)
		}
		if !b.eraRefusesLegacy() {
			t.Fatal("not refusing 2025 traffic after the child rejected initialize")
		}
	})
}

// makeRefusing has b's modern child reject a forwarded initialize, so the
// bridge refuses later 2025 traffic itself.
func makeRefusing(t *testing.T, b *Bridge) {
	t.Helper()
	if r := eraPost(t, b, acceptJSON, eraInitialize(900).body, nil); r.status != 400 || !b.eraRefusesLegacy() {
		t.Fatalf("setup: initialize %d %s, refuses %v", r.status, r.body, b.eraRefusesLegacy())
	}
}

func shortProbe(o *StartBridgeOptions) { o.ProbeTimeout = 200 * time.Millisecond }

// probeTimedOutVerdict reports whether the verdict is only a timeout, and the
// newest outstanding probe id ("" if none).
func probeTimedOutVerdict(b *Bridge) (timedOut bool, lateID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n := len(b.era.m.outstanding); n > 0 {
		lateID = b.era.m.outstanding[n-1]
	}
	return b.era.m.verdict.kind == verdictTimeout, lateID
}

// verdictSource ranks what a verdict rests on, for assertions that do not
// care which way the child answered.
type verdictSource int

const (
	srcNone      verdictSource = iota
	srcTimeout                 // a probe timed out
	srcSoft                    // a probe got a soft-legacy error
	srcRejection               // an initialize rejection's version list
	srcProbe                   // the child's modern or explicit legacy answer
)

func verdictSourceOf(b *Bridge) verdictSource {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.era.m.verdict.kind {
	case verdictTimeout:
		return srcTimeout
	case verdictSoft:
		return srcSoft
	case verdictRejection:
		return srcRejection
	case verdictModern, verdictLegacy:
		return srcProbe
	}
	return srcNone
}

func TestEraProbe_TimeoutSendsNoCancelAndLateModernAnswerUpgrades(t *testing.T) {
	b := startEraBridge(t, shortProbe, "--era=modern", "--discover-hold")

	r := eraPost(t, b, acceptJSON, modernCall(1), nil)
	if r.status != 400 || !strings.Contains(r.body, `"code":-32600`) {
		t.Fatalf("timed-out probe should read as legacy: %d %s", r.status, r.body)
	}
	if r.elapsed < 150*time.Millisecond {
		t.Fatalf("answered after %s, before the probe could have timed out", r.elapsed)
	}
	if timedOut, late := probeTimedOutVerdict(b); !timedOut || late != probeIDPrefix+"1" || eraName(b) != "legacy" {
		t.Fatalf("after the timeout: timedOut=%v lateID=%q era=%q", timedOut, late, eraName(b))
	}

	// The fake answers a cancelled request never, so the late answer below
	// can only arrive if the bridge sent no notifications/cancelled.
	releaseHeld(t, b)
	if got := eraName(b); got != "modern" {
		t.Fatalf("late DiscoverResult did not upgrade the verdict: era = %q", got)
	}
	if childWasCancelled(t, b, probeIDPrefix+"1") {
		t.Fatal("the bridge cancelled the probe it gave up on")
	}
	if r := eraPost(t, b, acceptJSON, modernCall(2), nil); r.status != 200 {
		t.Fatalf("after the upgrade: %d %s", r.status, r.body)
	}
	if n := discoverCount(t, b); n != 1 {
		t.Fatalf("child saw %d discover requests, want 1 (no second probe)", n)
	}
	if _, late := probeTimedOutVerdict(b); late != "" {
		t.Fatalf("outstanding probe id %q kept after its answer", late)
	}
}

// A modern child that starts slowly reads its backlog, cancellations first,
// only after the probe gave up: a cancel behind the discover would leave it
// legacy for good.
func TestEraProbe_SlowStartModernChildIsUpgraded(t *testing.T) {
	b := startEraBridgeCold(t, shortProbe, "--era=modern", "--startup-delay-ms=800")

	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 400 {
		t.Fatalf("before the child is up: %d %s", r.status, r.body)
	}
	waitFor(t, "slow-starting child's answer to upgrade the verdict", func() bool { return eraName(b) == "modern" })
	if r := eraPost(t, b, acceptJSON, modernCall(2), nil); r.status != 200 {
		t.Fatalf("after the upgrade: %d %s", r.status, r.body)
	}
	if childWasCancelled(t, b, probeIDPrefix+"1") {
		t.Fatal("the bridge cancelled the probe it gave up on")
	}
}

func TestEraProbe_LateLegacyAnswerMakesTheVerdictExplicit(t *testing.T) {
	b := startEraBridge(t, shortProbe, "--era=legacy", "--discover-delay-ms=600")

	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 400 {
		t.Fatalf("legacy child: %d %s", r.status, r.body)
	}
	waitFor(t, "the child's late -32601 to be handled", func() bool {
		_, late := probeTimedOutVerdict(b)
		return late == ""
	})
	if got := eraName(b); got != "legacy" {
		t.Fatalf("era = %q", got)
	}
	// The child's own answer, however late, makes the verdict explicit.
	if timedOut, _ := probeTimedOutVerdict(b); timedOut {
		t.Fatal("verdict still marked as a timeout after the child answered legacy")
	}
}

func TestEraProbe_ReprobeAfterFirstSuccessfulInitialize(t *testing.T) {
	// legacy-silent says nothing before initialize, so the first probe times
	// out and reads as legacy; after initialize it answers (-32601) and the
	// verdict is taken afresh, from the child's own answer.
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.ProbeTimeout = time.Second }, "--era=legacy-silent")
	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 400 {
		t.Fatalf("before initialize: %d %s", r.status, r.body)
	}
	if got := eraName(b); got != "legacy" {
		t.Fatalf("era = %q", got)
	}
	if timedOut, _ := probeTimedOutVerdict(b); !timedOut {
		t.Fatal("first verdict should be a timeout")
	}
	eraPost(t, b, acceptJSON, eraInitialize(2).body, nil)
	if got := eraName(b); got != "legacy-capable" {
		t.Fatalf("a successful initialize must drop the cached verdict, era = %q", got)
	}
	if r := eraPost(t, b, acceptJSON, modernCall(3), nil); r.status != 400 || !strings.Contains(r.body, "2025-11-25") {
		t.Fatalf("after initialize: %d %s", r.status, r.body)
	}
	if got := eraName(b); got != "legacy" {
		t.Fatalf("era = %q", got)
	}
	if timedOut, _ := probeTimedOutVerdict(b); timedOut {
		t.Fatal("the re-probe should have been answered by the initialized child, not timed out")
	}
}

// The probe that timed out before initialize is still outstanding in the
// child; its answer after initialize is a pre-initialize answer and must not
// stand in for the re-probe.
func TestEraProbe_LatePreInitializeAnswerDoesNotSkipReprobe(t *testing.T) {
	b := startEraBridge(t, shortProbe, "--era=dual", "--discover-hold")

	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 400 {
		t.Fatalf("probe should time out: %d %s", r.status, r.body)
	}
	if r := eraPost(t, b, acceptJSON, eraInitialize(2).body, nil); r.status != 200 {
		t.Fatalf("initialize: %d %s", r.status, r.body)
	}
	if _, late := probeTimedOutVerdict(b); late != "" {
		t.Fatalf("initialize left the timed-out probe %q outstanding", late)
	}
	releaseHeld(t, b) // the old DiscoverResult, modern, arrives now
	if got := eraName(b); got != "legacy-capable" {
		t.Fatalf("a pre-initialize answer set the verdict: era = %q", got)
	}

	if r := eraPost(t, b, acceptJSON, modernCall(3), nil); r.status != 400 {
		t.Fatalf("re-probe should time out again (held): %d %s", r.status, r.body)
	}
	if n := discoverCount(t, b); n != 2 {
		t.Fatalf("child saw %d discover requests, want a fresh one after initialize", n)
	}
	releaseHeld(t, b)
	if got := eraName(b); got != "modern" {
		t.Fatalf("the post-initialize answer should upgrade: era = %q", got)
	}
}

// The probe the bridge gave up on is the one thing it leaves outstanding: it
// is a single id, not a request slot, and is dropped with the process.
func TestEraProbe_OutstandingProbeIsBoundedAndDropped(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.ProbeTimeout = 200 * time.Millisecond; o.MaxPending = 1 },
		"--era=modern", "--discover-hold")
	b.mu.Lock()
	st := b.era
	b.mu.Unlock()
	orig := b.GetCurrentChild()
	t.Cleanup(func() { _ = orig.Close() })

	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 400 {
		t.Fatalf("probe should time out: %d %s", r.status, r.body)
	}
	b.mu.Lock()
	pending := len(b.pending)
	b.mu.Unlock()
	if _, late := probeTimedOutVerdict(b); late == "" || pending != 0 {
		t.Fatalf("lateID=%q pending=%d", late, pending)
	}
	// With maxPending 1, a slot taken by the outstanding probe would 503 this.
	if r := eraPost(t, b, acceptJSON, map[string]any{"jsonrpc": "2.0", "id": 5, "method": "debug/requestCount"}, nil); r.status != 200 {
		t.Fatalf("outstanding probe is holding a request slot: %d %s", r.status, r.body)
	}

	b.ReplaceChild(newEraChild(t, "--era=modern", "--discover-hold"))
	b.mu.Lock()
	dropped := len(st.m.outstanding) == 0
	b.mu.Unlock()
	if !dropped {
		t.Fatal("ReplaceChild left the old process's outstanding probe")
	}

	// And on exit.
	if r := eraPost(t, b, acceptJSON, modernCall(2), nil); r.status != 400 {
		t.Fatalf("replacement child's probe should time out: %d %s", r.status, r.body)
	}
	if _, late := probeTimedOutVerdict(b); late == "" {
		t.Fatal("no outstanding probe to drop")
	}
	_ = b.GetCurrentChild().Close()
	waitFor(t, "exit to drop the outstanding probe", func() bool {
		_, late := probeTimedOutVerdict(b)
		return late == ""
	})
}

// A probe whose write to the child's stdin never completes (the pipe is
// full) can never be answered, so its timeout must not leave a legacy
// verdict behind; the next modern request probes again.
func TestEraProbe_NeverWrittenProbeDoesNotStick(t *testing.T) {
	release := filepath.Join(t.TempDir(), "release")
	t.Cleanup(func() { _ = os.WriteFile(release, nil, 0o600) })
	b := startEraBridgeCold(t, shortProbe, "--era=modern", "--stdin-stall-until-file="+release)
	child := b.GetCurrentChild()

	// An oversized request fills the pipe and holds the child's write lock.
	big := map[string]any{"jsonrpc": "2.0", "id": 1, "method": "debug/requestCount", "params": map[string]any{"pad": strings.Repeat("x", 1<<20)}}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if resp, _, err := eraSend(ctx, b.URL(), acceptJSON, big, nil); err == nil {
			resp.Body.Close()
		}
	}()
	waitFor(t, "a blocked write holding the child's stdin", func() bool {
		if child.writeMu.TryLock() {
			child.writeMu.Unlock()
			return false
		}
		return true
	})

	if r := eraPost(t, b, acceptJSON, modernCall(2), nil); r.status != 400 {
		t.Fatalf("probe that cannot be written reads as legacy for now: %d %s", r.status, r.body)
	}
	b.mu.Lock()
	stuck := b.era.m.verdict.kind != verdictNone
	late := b.era.m.outstanding
	sending := probeWriteRunning(b.era)
	b.mu.Unlock()
	if stuck || len(late) != 0 || !sending {
		t.Fatalf("verdict stuck=%v outstanding=%q sending=%v after a probe that was never written", stuck, late, sending)
	}
	// While that write is outstanding no second one is queued behind it.
	if r := eraPost(t, b, acceptJSON, modernCall(3), nil); r.status != 400 || probeInFlight(b) {
		t.Fatalf("second request: %d, probe in flight %v", r.status, probeInFlight(b))
	}

	// The write completes after the timer fired: the probe is in the child
	// now, so it is tracked, its answer upgrades the verdict, and no second
	// probe is written.
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the late-written probe's answer to upgrade the verdict", func() bool { return eraName(b) == "modern" })
	if r := eraPost(t, b, acceptJSON, modernCall(4), nil); r.status != 200 {
		t.Fatalf("after the upgrade: %d %s", r.status, r.body)
	}
	if n := discoverCount(t, b); n != 1 {
		t.Fatalf("child saw %d discover requests, want 1", n)
	}
	if _, late := probeTimedOutVerdict(b); late != "" {
		t.Fatalf("outstanding probe id %q kept after its answer", late)
	}
}

// A probe whose write returned just after its timer fired is outstanding in
// the child and must be tracked as the one late id, whichever of the write
// goroutine and the child's answer reaches the lock first.
func TestEraProbe_LateWrittenProbeIsAdopted(t *testing.T) {
	setup := func(t *testing.T) (*Bridge, *eraState, string) {
		b := startEraBridge(t, nil, "--era=modern")
		st := b.era
		id := startProbeByHand(t, b, st)
		b.probeTimedOut(st, id) // the timer, before the write returned
		b.mu.Lock()
		defer b.mu.Unlock()
		if st.m.probe.phase != probeUnsent || len(st.m.outstanding) != 0 || st.m.verdict.kind != verdictNone {
			t.Fatalf("setup: phase=%v outstanding=%q verdict=%v", st.m.probe.phase, st.m.outstanding, st.m.verdict.kind)
		}
		return b, st, id
	}
	t.Run("write returns first", func(t *testing.T) {
		b, st, id := setup(t)
		b.writeProbe(st, id)
		waitFor(t, "the late write to be adopted and answered", func() bool { return eraName(b) == "modern" })
		b.mu.Lock()
		defer b.mu.Unlock()
		if len(st.m.outstanding) != 0 || st.m.probe.phase != probeIdle || st.m.verdict.kind != verdictModern {
			t.Fatalf("outstanding=%q phase=%v verdict=%v", st.m.outstanding, st.m.probe.phase, st.m.verdict.kind)
		}
	})
	t.Run("answer first", func(t *testing.T) {
		b, st, id := setup(t)
		b.handleProbeResponse(st, id, map[string]any{"result": map[string]any{"supportedVersions": []any{eraTestVersion}}})
		if eraName(b) != "modern" || verdictSourceOf(b) != srcProbe {
			t.Fatalf("era=%q src=%v", eraName(b), verdictSourceOf(b))
		}
		// The write goroutine's report, arriving now, changes nothing.
		b.mu.Lock()
		b.applyEraLocked(st, eraEvent{kind: evWriteDone, id: id, now: time.Now()})
		out := len(st.m.outstanding)
		b.mu.Unlock()
		if out != 0 || eraName(b) != "modern" {
			t.Fatalf("the stale write report was adopted: outstanding=%d era=%q", out, eraName(b))
		}
	})
	t.Run("initialize in between", func(t *testing.T) {
		b, st, id := setup(t)
		b.noteInitializeAnswer(st, map[string]any{"result": map[string]any{}})
		b.writeProbe(st, id)
		if _, late := probeTimedOutVerdict(b); late != "" || eraName(b) != "legacy-capable" {
			t.Fatalf("a probe from before initialize was adopted: lateID=%q era=%q", late, eraName(b))
		}
	})
}

// startProbeByHand starts a probe on st as a modern request with no verdict
// would, registering its waiters and timer but leaving its write to the test.
func startProbeByHand(t *testing.T, b *Bridge, st *eraState) string {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var eff eraEffects
	st.m, eff = st.m.apply(eraEvent{kind: evModernRequest, now: time.Now()})
	if eff.start == "" {
		t.Fatalf("no probe started: %+v", eff)
	}
	b.startProbeLocked(st, eff.start)
	return eff.start
}

// A probe in flight when initialize succeeds is a pre-initialize one: its
// waiters get its outcome, but it caches nothing and leaves no late id, so
// the next modern request probes afresh.
func TestEraProbe_InFlightAcrossInitializeCachesNothing(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.ProbeTimeout = 400 * time.Millisecond }, "--era=legacy-silent")

	res := make(chan eraResponse, 1)
	go func() { res <- eraPost(t, b, acceptJSON, modernCall(1), nil) }()
	waitProbeInFlight(t, b)
	if r := eraPost(t, b, acceptJSON, eraInitialize(2).body, nil); r.status != 200 {
		t.Fatalf("initialize: %d %s", r.status, r.body)
	}
	if r := <-res; r.status != 400 {
		t.Fatalf("waiter: %d %s", r.status, r.body)
	}
	if timedOut, late := probeTimedOutVerdict(b); timedOut || late != "" || eraName(b) != "legacy-capable" {
		t.Fatalf("pre-initialize probe left state: timedOut=%v lateID=%q era=%q", timedOut, late, eraName(b))
	}
	// The next request probes afresh, and the initialized child answers it.
	if r := eraPost(t, b, acceptJSON, modernCall(3), nil); r.status != 400 || !strings.Contains(r.body, "2025-11-25") {
		t.Fatalf("re-probe: %d %s", r.status, r.body)
	}
	if timedOut, _ := probeTimedOutVerdict(b); timedOut || verdictSourceOf(b) != srcProbe {
		t.Fatalf("re-probe did not settle from the child's own answer: src=%v", verdictSourceOf(b))
	}
}

// The probe's wait is the request's own: it starts after the respawn
// EnsureChild does, on its own timer, not inside EnsureChild's bound.
func TestEraProbe_WaitFollowsEnsureChildOnItsOwnTimer(t *testing.T) {
	var bp atomic.Pointer[Bridge]
	b := startEraBridge(t, func(o *StartBridgeOptions) {
		o.ProbeTimeout = 20 * time.Second
		o.EnsureChild = func(ctx context.Context) error {
			bp.Load().ReplaceChild(newEraChild(t, "--era=modern", "--discover-delay-ms=300"))
			return nil
		}
	}, "--era=modern")
	bp.Store(b)
	_ = b.GetCurrentChild().Close()
	waitFor(t, "the child to exit", func() bool { return b.GetCurrentChild().HasExited() })

	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 200 || r.elapsed < 250*time.Millisecond {
		t.Fatalf("%d after %s: %s", r.status, r.elapsed, r.body)
	}
}

// A request whose first probe wait ends in a ReplaceChild retries against the
// new process, but within the same one probe timeout, not a second one.
func TestEraProbe_RetryAfterReplaceChildSharesOneDeadline(t *testing.T) {
	const probe = 2 * time.Second
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.ProbeTimeout = probe }, "--era=modern", "--discover-hold")
	// Spawned up front, so the replacement itself costs the timed wait nothing.
	repl := newEraChild(t, "--era=modern", "--discover-hold")

	res := make(chan eraResponse, 1)
	go func() { res <- eraPost(t, b, acceptJSON, modernCall(1), nil) }()
	waitProbeInFlight(t, b)
	time.Sleep(probe / 2)
	b.ReplaceChild(repl)

	r := <-res
	if r.status != 400 || !strings.Contains(r.body, `"code":-32600`) {
		t.Fatalf("a retry that runs out of time reads as legacy for now: %d %s", r.status, r.body)
	}
	// A second full timeout on the replacement would end at 1.5x the probe.
	if r.elapsed < probe*3/4 || r.elapsed > probe+probe/4 {
		t.Fatalf("waited %s in total, want about one probe timeout (%s), not %s", r.elapsed, probe, probe*3/2)
	}
	// The new process's own flight outlives this request's wait and keeps its timer.
	if !probeInFlight(b) {
		t.Fatal("the retry's flight was dropped when its request stopped waiting")
	}
}

func TestEraProbe_DefaultTimeoutFitsTheRelaysHeaderTimeout(t *testing.T) {
	if defaultProbeTimeout != 15*time.Second {
		t.Fatalf("defaultProbeTimeout = %s", defaultProbeTimeout)
	}
	// ensureChild (when a respawn is needed), the probe wait, then the head
	// hold. The retry after a respawn shares the probe wait's one deadline
	// (admitModern), so it adds nothing: 5s + 15s + 2s.
	if total := ensureChildTimeout + defaultProbeTimeout + defaultHeadHold; total >= 30*time.Second {
		t.Fatalf("%s leaves no room under the relay's 30s response-header timeout", total)
	}
	b := startTestBridge(t)
	if b.probeTimeout != defaultProbeTimeout {
		t.Fatalf("bridge probe timeout %s", b.probeTimeout)
	}
	// With the real default in force, a retry whose deadline has mostly gone
	// leaves after what is left, not after a fresh 15s.
	hb := startEraBridge(t, nil, "--era=modern", "--discover-hold")
	if hb.probeTimeout != defaultProbeTimeout {
		t.Fatalf("bridge probe timeout %s", hb.probeTimeout)
	}
	_, st := hb.currentChildEra()
	start := time.Now()
	if v := hb.discover(context.Background(), st, start.Add(150*time.Millisecond)); v != answerLegacy {
		t.Fatalf("verdict %v, want legacy for now", v)
	}
	if waited := time.Since(start); waited > 5*time.Second {
		t.Fatalf("waited %s on a 150ms deadline", waited)
	}
	b2 := startEraBridge(t, shortProbe, "--era=modern")
	if b2.probeTimeout != 200*time.Millisecond {
		t.Fatalf("override ignored: %s", b2.probeTimeout)
	}
}

// The initialize head lapsed with a provisional session and the child then
// died: the committed stream ends with the child-stopped error and the
// session of the handshake that never completed is gone.
func TestEra_ChildDiesAfterProvisionalSessionCommitted(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.InitializeHoldTimeout = 100 * time.Millisecond },
		"--era=legacy", "--initialize-hold")

	resp := sendHeadOnly(t, b, acceptSSE, eraInitialize(1).body)
	if resp.StatusCode != 200 || resp.Header.Get("Mcp-Session-Id") == "" || !sessionMinted(b) {
		t.Fatalf("setup: %d, session %q, minted %v", resp.StatusCode, resp.Header.Get("Mcp-Session-Id"), sessionMinted(b))
	}
	_ = b.GetCurrentChild().Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "local server restarted") || !strings.Contains(string(body), `"code":-32000`) {
		t.Fatalf("stream: %s", body)
	}
	if sessionMinted(b) {
		t.Fatal("the provisional session survived a handshake that never completed")
	}
}

func TestEraProbe_ReprobeUpgradesDualChildAfterInitialize(t *testing.T) {
	b := startEraBridge(t, nil, "--era=dual", "--discover-delay-ms=0")
	eraPost(t, b, acceptJSON, modernCall(1), nil)
	eraPost(t, b, acceptJSON, eraInitialize(2).body, nil)
	eraPost(t, b, acceptJSON, modernCall(3), nil)
	// Asked statelessly: the initialized bridge now gates 2025-style
	// requests on the session header.
	r := eraPost(t, b, acceptJSON, modernBody(4, "debug/discoverCount", nil, nil), nil)
	if !strings.Contains(r.body, `"count":2`) {
		t.Fatalf("want 2 discover requests (one before, one after initialize): %s", r.body)
	}
}

func TestEraMissingClientCapabilitiesIs400(t *testing.T) {
	b := startEraBridge(t, nil, "--era=modern")
	body := modernCall(1)
	meta := body["params"].(map[string]any)["_meta"].(map[string]any)
	delete(meta, clientCapabilitiesMetaKey)
	before := forwardedCount(t, b)
	for _, accept := range []string{acceptJSON, acceptSSE} {
		r := eraPost(t, b, accept, body, nil)
		if r.status != 400 || !strings.Contains(r.body, `"code":-32602`) {
			t.Errorf("%s: %d %s", accept, r.status, r.body)
		}
	}
	if n := forwardedCount(t, b); n != before {
		t.Errorf("a request without clientCapabilities reached the child (%d -> %d)", before, n)
	}
	// Any other -32602 the child sends stays in a 200.
	r := eraPost(t, b, acceptSSE, modernBody(2, "debug/delayedError", map[string]any{"code": -32602}, nil), nil)
	if r.status != 200 {
		t.Errorf("child's own -32602: %d %s", r.status, r.body)
	}
}

func TestEraSend_KeepsHeaderNameCaseOnTheWire(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	head := make(chan string, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
		var sb strings.Builder
		buf := make([]byte, 4096)
		for !strings.Contains(sb.String(), "\r\n\r\n") {
			n, err := c.Read(buf)
			sb.Write(buf[:n])
			if err != nil {
				break
			}
		}
		head <- sb.String()
		_, _ = io.WriteString(c, "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
	}()
	resp, _, err := eraSend(context.Background(), "http://"+ln.Addr().String(), acceptJSON, modernCall(1),
		map[string]string{"mcp-protocol-version": eraTestVersion, "MCP-METHOD": "tools/call", "mcp-name": "echo"})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	h := <-head
	for _, want := range []string{"\r\nmcp-protocol-version: ", "\r\nMCP-METHOD: ", "\r\nmcp-name: "} {
		if !strings.Contains(h, want) {
			t.Errorf("request head lacks %q:\n%s", want, h)
		}
	}
	if strings.Contains(h, "\r\nMcp-Method: ") || strings.Contains(h, "\r\nMcp-Name: ") {
		t.Errorf("canonical duplicates went out as well:\n%s", h)
	}
}

func TestEraModernHeaderValidation(t *testing.T) {
	b := startEraBridge(t, nil, "--era=modern")
	b64 := func(s string) string { return "=?base64?" + base64.StdEncoding.EncodeToString([]byte(s)) + "?=" }
	cases := []struct {
		name string
		body map[string]any
		hdr  map[string]string
		ok   bool
	}{
		{"all matching", modernCall(1), nil, true},
		{"names in other case", modernCall(1), map[string]string{"mcp-protocol-version": eraTestVersion, "MCP-METHOD": "tools/call", "mcp-name": "echo"}, true},
		{"protocol version missing", modernCall(1), map[string]string{"MCP-Protocol-Version": ""}, false},
		{"protocol version mismatch", modernCall(1), map[string]string{"MCP-Protocol-Version": "2025-11-25"}, false},
		{"method missing", modernCall(1), map[string]string{"Mcp-Method": ""}, false},
		{"method mismatch", modernCall(1), map[string]string{"Mcp-Method": "tools/list"}, false},
		{"method value is case-sensitive", modernCall(1), map[string]string{"Mcp-Method": "Tools/Call"}, false},
		{"name missing", modernCall(1), map[string]string{"Mcp-Name": ""}, false},
		{"name mismatch", modernCall(1), map[string]string{"Mcp-Name": "other"}, false},
		{"name value is case-sensitive", modernCall(1), map[string]string{"Mcp-Name": "Echo"}, false},
		{"name base64 matching", modernBody(1, "tools/call", map[string]any{"name": "héllo"}, nil), map[string]string{"Mcp-Name": b64("héllo")}, true},
		{"name base64 mismatching", modernBody(1, "tools/call", map[string]any{"name": "héllo"}, nil), map[string]string{"Mcp-Name": b64("hello")}, false},
		{"name base64 malformed", modernCall(1), map[string]string{"Mcp-Name": "=?base64?***?="}, false},
		{"name base64 without padding", modernCall(1), map[string]string{"Mcp-Name": "=?base64?" + base64.RawStdEncoding.EncodeToString([]byte("echoo")) + "?="}, false},
		{"name base64 of invalid UTF-8", modernCall(1), map[string]string{"Mcp-Name": "=?base64?" + base64.StdEncoding.EncodeToString([]byte{0xff, 0xfe}) + "?="}, false},
		{"name with a non-ASCII byte", modernBody(1, "tools/call", map[string]any{"name": "héllo"}, nil), map[string]string{"Mcp-Name": "héllo"}, false},
		{"name base64 of a plain name", modernCall(1), map[string]string{"Mcp-Name": b64("echo")}, true},
		{"sentinel marker is case-sensitive", modernCall(1), map[string]string{"Mcp-Name": "=?BASE64?" + base64.StdEncoding.EncodeToString([]byte("echo")) + "?="}, false},
		{"resources/read uses uri", modernBody(1, "resources/read", map[string]any{"uri": "file:///a"}, nil), map[string]string{"Mcp-Name": "file:///a"}, true},
		{"resources/read uri mismatch", modernBody(1, "resources/read", map[string]any{"uri": "file:///a"}, nil), map[string]string{"Mcp-Name": "file:///b"}, false},
		{"prompts/get uses name", modernBody(1, "prompts/get", map[string]any{"name": "p"}, nil), map[string]string{"Mcp-Name": "x"}, false},
		{"tools/list needs no name", modernBody(1, "tools/list", nil, nil), nil, true},
		{"tools/list ignores a stray name", modernBody(1, "tools/list", nil, nil), map[string]string{"Mcp-Name": "whatever"}, true},
	}
	for _, c := range cases {
		for _, accept := range []string{acceptJSON, acceptSSE} {
			before := forwardedCount(t, b)
			r := eraPost(t, b, accept, c.body, c.hdr)
			forwarded := forwardedCount(t, b) - before
			if c.ok {
				if r.status == 400 && strings.Contains(r.body, "-32020") {
					t.Errorf("%s (%s): rejected: %s", c.name, accept, r.body)
				}
				if forwarded != 1 {
					t.Errorf("%s (%s): child saw %d requests, want 1", c.name, accept, forwarded)
				}
				continue
			}
			if r.status != 400 || !strings.Contains(r.body, `"code":-32020`) {
				t.Errorf("%s (%s): %d %s", c.name, accept, r.status, r.body)
			}
			if forwarded != 0 {
				t.Errorf("%s (%s): the rejected request reached the child", c.name, accept)
			}
		}
	}
	// One probe for the whole table.
	if n := discoverCount(t, b); n != 1 {
		t.Errorf("discover count %d", n)
	}
}

func TestEraLegacyChildWinsOverBadHeaders(t *testing.T) {
	b := startEraBridge(t, nil, "--era=legacy")
	r := eraPost(t, b, acceptJSON, modernCall(1), map[string]string{"MCP-Protocol-Version": "1900-01-01", "Mcp-Method": "nope", "Mcp-Name": ""})
	if r.status != 400 || !strings.Contains(r.body, `"code":-32600`) || strings.Contains(r.body, "-32020") {
		t.Fatalf("a legacy child must answer with the non-modern -32600 whatever the headers: %d %s", r.status, r.body)
	}
	if n := forwardedCount(t, b); n != 0 {
		t.Fatalf("the refused request reached the legacy child (%d)", n)
	}
}

func TestEraDefaultFakeAnswersDiscover(t *testing.T) {
	b := startTestBridge(t)
	r := eraPost(t, b, acceptJSON, modernBody(1, "ping", nil, nil), nil)
	if r.status != 200 || !strings.Contains(r.body, `"pong"`) {
		t.Fatalf("modern-shaped ping on the default fake: %d %s", r.status, r.body)
	}
}

func TestEraLegacyChildRejectingOneInitializeIsNotLockedOut(t *testing.T) {
	for _, accept := range []string{acceptJSON, acceptSSE} {
		for _, code := range []string{"-32602", "-32601", "-32022"} {
			b := startEraBridge(t, nil, "--era=legacy", "--reject-initialize=1", "--reject-code="+code)
			bad := eraPost(t, b, accept, eraInitialize(1).body, nil)
			if bad.status != 400 || !strings.Contains(bad.body, `"code":`+code) || !strings.Contains(bad.body, "bad initialize") {
				t.Fatalf("%s %s: want the child's own error as a 400, got %d %s", accept, code, bad.status, bad.body)
			}
			if bad.header.Get("Mcp-Session-Id") != "" || sessionMinted(b) {
				t.Fatalf("%s %s: session from a rejected initialize", accept, code)
			}
			if b.eraRefusesLegacy() {
				t.Fatalf("%s %s: a single rejection with no version list must not read as modern-only", accept, code)
			}
			if code != "-32602" {
				// Modern-looking, so a background probe starts; the legacy
				// child's own answer makes the verdict explicit.
				waitFor(t, "the background probe's legacy answer", func() bool { return eraName(b) == "legacy" })
				if b.eraRefusesLegacy() || verdictSourceOf(b) != srcProbe {
					t.Fatalf("%s %s: refuses2025=%v src=%v after the probe", accept, code, b.eraRefusesLegacy(), verdictSourceOf(b))
				}
			} else if probeInFlight(b) || eraName(b) != "unknown" {
				t.Fatalf("%s %s: a bad-params rejection started a probe or set a verdict: era=%q", accept, code, eraName(b))
			}
			good := eraPost(t, b, accept, eraInitialize(2).body, nil)
			if good.status != 200 || good.header.Get("Mcp-Session-Id") == "" {
				t.Fatalf("%s %s: good initialize after the bad one: %d %v", accept, code, good.status, good.header)
			}
		}
	}
}

func TestEraInitializeRejectionWithModernEvidenceIsCached(t *testing.T) {
	if !onlyModernVersions([]string{"2026-07-28"}) || onlyModernVersions([]string{"2026-07-28", "2025-11-25"}) || onlyModernVersions(nil) {
		t.Fatal("only a non-empty list of 2026-era versions is modern evidence")
	}
	// legacy-silent leaves the background probe a rejection starts unanswered,
	// so only the rejection itself can have set what is seen here.
	b := startEraBridge(t, nil, "--era=legacy-silent")
	completeRejected(b, initRejection(-32601, "2026-07-28"))
	if got := eraName(b); got != "modern" || !b.eraRefusesLegacy() {
		t.Fatalf("era = %q", got)
	}
	// An explicit legacy answer is not overwritten by a rejection's say-so.
	b2 := startEraBridge(t, nil, "--era=legacy")
	eraPost(t, b2, acceptJSON, modernCall(1), nil)
	completeRejected(b2, initRejection(-32601, "2026-07-28"))
	if got := eraName(b2); got != "legacy" {
		t.Fatalf("era = %q", got)
	}
	// Nothing is cached from no list at all: the bridge never invents one.
	b3 := startEraBridge(t, nil, "--era=legacy-silent")
	completeRejected(b3, initRejection(-32601))
	if got := eraName(b3); got != "unknown" {
		t.Fatalf("era = %q", got)
	}
}

// A verdict that is only the probe giving up yields to an initialize
// rejection that carries modern evidence; one the child answered does not.
func TestEraInitializeRejectionOverwritesTimeoutVerdictOnly(t *testing.T) {
	for _, accept := range []string{acceptJSON, acceptSSE} {
		t.Run("timeout verdict/"+accept, func(t *testing.T) {
			b := startEraBridge(t, shortProbe, "--era=legacy-silent", "--reject-initialize=1", "--reject-modern-evidence")
			if r := eraPost(t, b, accept, modernCall(1), nil); r.status != 400 {
				t.Fatalf("probe should time out: %d %s", r.status, r.body)
			}
			if timedOut, _ := probeTimedOutVerdict(b); !timedOut {
				t.Fatal("setup: verdict should be a timeout")
			}
			r := eraPost(t, b, accept, eraInitialize(2).body, nil)
			if r.status != 400 || !strings.Contains(r.body, `"supported":["2026-07-28"]`) {
				t.Fatalf("initialize: %d %s", r.status, r.body)
			}
			if got := eraName(b); got != "modern" || !b.eraRefusesLegacy() {
				t.Fatalf("modern evidence did not overwrite the timeout verdict: era = %q", got)
			}
		})
		t.Run("explicit legacy verdict/"+accept, func(t *testing.T) {
			b := startEraBridge(t, nil, "--era=legacy", "--reject-initialize=1", "--reject-modern-evidence")
			if r := eraPost(t, b, accept, modernCall(1), nil); r.status != 400 {
				t.Fatalf("legacy child: %d %s", r.status, r.body)
			}
			r := eraPost(t, b, accept, eraInitialize(2).body, nil)
			if r.status != 400 {
				t.Fatalf("initialize: %d %s", r.status, r.body)
			}
			if got := eraName(b); got != "legacy" || b.eraRefusesLegacy() {
				t.Fatalf("an explicit legacy verdict was overwritten: era = %q", got)
			}
		})
	}
}

// A modern-looking rejected 2025 initialize (here a bare -32601) is answered
// at once with the child's own error; the probe it starts runs in the
// background, never waited for, so pure 2025 traffic pays nothing for era
// detection.
func TestEraInitializeRejectionStartsProbeWithoutWaiting(t *testing.T) {
	for _, accept := range []string{acceptJSON, acceptSSE} {
		// The held discover answer proves the 400 did not wait for it; the
		// default (15s) probe timeout means a probe wait could not hide.
		b := startEraBridge(t, nil, "--era=dual", "--reject-initialize=1", "--reject-code=-32601", "--discover-hold")
		r := eraPost(t, b, accept, eraInitialize(1).body, nil)
		if r.status != 400 || !strings.Contains(r.body, "bad initialize") || r.header.Get("Mcp-Session-Id") != "" {
			t.Fatalf("%s: %d %v %s", accept, r.status, r.header, r.body)
		}
		if !probeInFlight(b) {
			t.Fatalf("%s: no probe was started", accept)
		}
		if r.elapsed > 5*time.Second || sessionMinted(b) {
			t.Fatalf("%s: answered after %s, session minted %v", accept, r.elapsed, sessionMinted(b))
		}
		if got := eraName(b); got != "unknown" {
			t.Fatalf("%s: era = %q before the probe is answered", accept, got)
		}
		// Its answer applies with the usual precedence (dual lists both).
		releaseDirect(t, b)
		waitFor(t, "the probe's answer", func() bool { return eraName(b) == "modern" })
		if b.eraRefusesLegacy() || verdictSourceOf(b) != srcProbe {
			t.Fatalf("%s: refuses2025=%v src=%v", accept, b.eraRefusesLegacy(), verdictSourceOf(b))
		}
	}

	t.Run("no second probe", func(t *testing.T) {
		b := startEraBridge(t, nil, "--era=dual", "--reject-initialize=2", "--reject-code=-32601", "--discover-hold")
		eraPost(t, b, acceptJSON, eraInitialize(1).body, nil)
		eraPost(t, b, acceptJSON, eraInitialize(2).body, nil)
		waitFor(t, "the child to receive the probe", func() bool { return discoverCount(t, b) >= 1 })
		if n := discoverCount(t, b); n != 1 {
			t.Fatalf("child saw %d probes for two rejections, want 1", n)
		}
	})
	t.Run("silent child", func(t *testing.T) {
		b := startEraBridge(t, nil, "--era=legacy-silent", "--reject-initialize=1", "--reject-code=-32601")
		r := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil)
		if r.status != 400 || r.elapsed > 10*time.Second {
			t.Fatalf("silent child: %d after %s", r.status, r.elapsed)
		}
		if !probeInFlight(b) {
			t.Fatal("no probe was started")
		}
	})
	t.Run("released by Close", func(t *testing.T) {
		b := startEraBridge(t, nil, "--era=dual", "--reject-initialize=1", "--reject-code=-32601", "--discover-hold")
		eraPost(t, b, acceptJSON, eraInitialize(1).body, nil)
		if !probeInFlight(b) {
			t.Fatal("no probe was started")
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if probeInFlight(b) {
			t.Fatal("probe still registered after Close")
		}
	})
}

// releaseDirect makes the fake send its held answers without an HTTP round
// trip, which a bridge in the middle of a held probe could not give.
func releaseDirect(t *testing.T, b *Bridge) {
	t.Helper()
	// Behind a probe's own write, which runs on its own goroutine.
	waitFor(t, "the probe's write to finish", func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return !probeWriteRunning(b.era)
	})
	if err := b.GetCurrentChild().Send(map[string]any{"jsonrpc": "2.0", "id": 9001, "method": "debug/release"}); err != nil {
		t.Fatal(err)
	}
}

// Evidence ranks probe answer > initialize rejection with modern-only
// evidence > timeout. The rejection's verdict stands until a probe says
// otherwise: the background probe the rejection starts checks it, and a late
// answer to that probe corrects it too.
func TestEraVerdictPrecedence(t *testing.T) {
	t.Run("probe answer corrects a rejection-derived modern-only verdict", func(t *testing.T) {
		// A dual-era child whose one odd initialize rejection named only the
		// modern version.
		// The probe the rejection starts is held, so the verdict it leaves
		// can be looked at before the probe answers.
		b := startEraBridge(t, nil, "--era=dual", "--reject-initialize=1", "--reject-modern-evidence", "--discover-hold")
		if r := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil); r.status != 400 {
			t.Fatalf("initialize: %d %s", r.status, r.body)
		}
		if !b.eraRefusesLegacy() || verdictSourceOf(b) != srcRejection {
			t.Fatalf("setup: era=%q src=%v", eraName(b), verdictSourceOf(b))
		}
		if r := eraPost(t, b, acceptJSON, eraInitialize(2).body, nil); r.status != 400 || !strings.Contains(r.body, "initialize is not supported") || !strings.Contains(r.body, `"code":-32600`) {
			t.Fatalf("2025 initialize while the rejection verdict stands: %d %s", r.status, r.body)
		}
		// The probe's DiscoverResult lists both versions.
		releaseDirect(t, b)
		waitFor(t, "the probe's answer", func() bool { return verdictSourceOf(b) == srcProbe })
		if r := eraPost(t, b, acceptJSON, modernCall(3), nil); r.status != 200 {
			t.Fatalf("modern call: %d %s", r.status, r.body)
		}
		if b.eraRefusesLegacy() || verdictSourceOf(b) != srcProbe {
			t.Fatalf("probe did not outrank the rejection: refuses2025=%v src=%v", b.eraRefusesLegacy(), verdictSourceOf(b))
		}
		if r := eraPost(t, b, acceptJSON, eraInitialize(4).body, nil); r.status != 200 || r.header.Get("Mcp-Session-Id") == "" {
			t.Fatalf("2025 traffic still locked out of a dual-era child: %d %s", r.status, r.body)
		}
	})
	t.Run("a cached rejection verdict is never waited on; a late probe answer corrects it", func(t *testing.T) {
		// The default probe timeout: a wait on the held probe could not hide.
		b := startEraBridge(t, nil, "--era=dual", "--reject-initialize=1", "--reject-modern-evidence", "--discover-hold")
		eraPost(t, b, acceptJSON, eraInitialize(1).body, nil)
		// The rejection's background probe is held; the request is answered
		// from the rejection's verdict at once, as for a modern child.
		r := eraPost(t, b, acceptJSON, modernCall(2), nil)
		if r.status != 200 || r.elapsed > 5*time.Second || !probeInFlight(b) {
			t.Fatalf("modern call while the probe is held: %d after %s, probe in flight %v: %s", r.status, r.elapsed, probeInFlight(b), r.body)
		}
		// The probe times out: a timeout never overrides the rejection.
		timeOutProbeByHand(t, b)
		if !b.eraRefusesLegacy() || verdictSourceOf(b) != srcRejection {
			t.Fatalf("a timeout overwrote the rejection verdict: era=%q src=%v", eraName(b), verdictSourceOf(b))
		}
		releaseHeld(t, b)
		if b.eraRefusesLegacy() || verdictSourceOf(b) != srcProbe {
			t.Fatalf("late answer did not outrank the rejection: refuses2025=%v src=%v", b.eraRefusesLegacy(), verdictSourceOf(b))
		}
	})
	t.Run("probe answer overwrites a timeout, rejection overwrites a timeout only", func(t *testing.T) {
		b := startEraBridge(t, shortProbe, "--era=modern", "--discover-hold")
		eraPost(t, b, acceptJSON, modernCall(1), nil)
		if verdictSourceOf(b) != srcTimeout {
			t.Fatalf("src=%v", verdictSourceOf(b))
		}
		completeRejected(b, initRejection(-32601, eraTestVersion))
		if verdictSourceOf(b) != srcRejection {
			t.Fatalf("rejection did not overwrite a timeout: %v", verdictSourceOf(b))
		}
		releaseHeld(t, b)
		if verdictSourceOf(b) != srcProbe {
			t.Fatalf("probe did not overwrite the rejection: %v", verdictSourceOf(b))
		}
		completeRejected(b, initRejection(-32601, "2026-07-28"))
		if verdictSourceOf(b) != srcProbe {
			t.Fatal("a rejection overwrote a probe answer")
		}
	})
}

func TestEraProbe_SharedByConcurrentRequests(t *testing.T) {
	b := startEraBridge(t, nil, "--era=modern", "--discover-hold")

	const n = 8
	statuses := make(chan int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			statuses <- eraPost(t, b, acceptSSE, modernCall(i), nil).status
		}(i)
	}
	waitProbeInFlight(t, b)

	// A 2025-style request is not held behind the probe.
	r := eraPost(t, b, acceptJSON, map[string]any{"jsonrpc": "2.0", "id": 99, "method": "debug/wasCancelled", "params": map[string]any{"requestId": "x"}}, nil)
	if r.status != 200 || !probeInFlight(b) {
		t.Fatalf("2025-style request status %d, probe still in flight: %v", r.status, probeInFlight(b))
	}

	waitFor(t, "the child to receive the probe", func() bool { return discoverCount(t, b) >= 1 })
	releaseHeld(t, b)
	wg.Wait()
	close(statuses)
	for s := range statuses {
		if s != 200 {
			t.Errorf("modern request got %d", s)
		}
	}
	if c := discoverCount(t, b); c != 1 {
		t.Fatalf("child saw %d discover requests, want 1", c)
	}
}

func TestEraProbe_RespawnResetsResult(t *testing.T) {
	b := startEraBridge(t, nil, "--era=legacy")
	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 400 {
		t.Fatalf("legacy child: %d %s", r.status, r.body)
	}
	if got := eraName(b); got != "legacy" {
		t.Fatalf("era = %q", got)
	}

	b.ReplaceChild(newEraChild(t, "--era=modern"))
	if got := eraName(b); got != "unknown" {
		t.Fatalf("era after ReplaceChild = %q, want unknown", got)
	}
	if r := eraPost(t, b, acceptJSON, modernCall(2), nil); r.status != 200 {
		t.Fatalf("modern replacement: %d %s", r.status, r.body)
	}
	if got := eraName(b); got != "modern" {
		t.Fatalf("era = %q", got)
	}
}

// logCapture is a debug-level logger whose output a test can wait on.
type logCapture struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (l *logCapture) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logCapture) count(sub string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Count(l.buf.String(), sub)
}

func (l *logCapture) logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(l, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// A respawn that lands while a waiter is on the old process's probe sends the
// waiter to the new process rather than failing it, and the old process's
// answer, when it comes, is dropped without touching the new one's state.
func TestEraProbe_ReplaceChildRetargetsWaitersAndDropsStaleAnswer(t *testing.T) {
	logs := &logCapture{}
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.Log = logs.logger() }, "--era=modern", "--discover-hold")
	old := b.GetCurrentChild()
	t.Cleanup(func() { _ = old.Close() })

	res := make(chan eraResponse, 1)
	go func() { res <- eraPost(t, b, acceptJSON, modernCall(1), nil) }()
	waitProbeInFlight(t, b)
	waitFor(t, "the old child to receive the probe", func() bool { return discoverCount(t, b) >= 1 })

	b.ReplaceChild(newEraChild(t, "--era=legacy"))
	if r := <-res; r.status != 400 || !strings.Contains(r.body, `"code":-32600`) {
		t.Fatalf("waiter on a replaced child should get the new (legacy) child's verdict: %d %s", r.status, r.body)
	}
	if got := eraName(b); got != "legacy" {
		t.Fatalf("era = %q", got)
	}

	// The old process answers its probe now: it must be seen and dropped.
	dropped := logs.count("late or unknown server/discover answer")
	if err := old.Send(map[string]any{"jsonrpc": "2.0", "id": 9001, "method": "debug/release"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the stale probe answer to be delivered", func() bool {
		return logs.count("late or unknown server/discover answer") > dropped
	})
	if got := eraName(b); got != "legacy" {
		t.Fatalf("stale probe answer reached the new process: era = %q", got)
	}
}

func TestEraProbe_ReplaceChildLandingBeforeDiscoverRetargets(t *testing.T) {
	b := startEraBridge(t, nil, "--era=legacy")
	staleChild, staleState := b.currentChildEra()
	b.ReplaceChild(newEraChild(t, "--era=modern"))

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	child, st, ok := b.admitModern(httptest.NewRecorder(), req, modernCall(1), staleChild, staleState)
	curChild, curState := b.currentChildEra()
	if !ok || child != curChild || st != curState {
		t.Fatalf("admitModern ok=%v, child current=%v state current=%v", ok, child == curChild, st == curState)
	}
}

func TestEraProbe_ChildExitReleasesWaiters(t *testing.T) {
	b := startEraBridge(t, nil, "--era=modern", "--discover-hold")

	res := make(chan eraResponse, 1)
	go func() { res <- eraPost(t, b, acceptSSE, modernCall(1), nil) }()
	waitProbeInFlight(t, b)

	_ = b.GetCurrentChild().Close()
	if r := <-res; r.status != 502 {
		t.Fatalf("waiter on an exited child: %d %s", r.status, r.body)
	}
	if probeInFlight(b) {
		t.Fatal("probe still registered after the child exited")
	}
}

func TestEraProbe_ClientDisconnectDuringProbe(t *testing.T) {
	b := startEraBridge(t, nil, "--era=modern", "--discover-hold")

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		resp, _, err := eraSend(ctx, b.URL(), acceptSSE, modernCall(1), nil)
		if err == nil {
			resp.Body.Close()
		}
		errc <- err
	}()
	waitProbeInFlight(t, b)
	cancel()
	<-errc

	// The handler must leave while the probe is still running.
	drained := make(chan struct{})
	go func() { b.wg.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		t.Fatal("handler of a disconnected waiter did not return")
	}
	if !probeInFlight(b) {
		t.Fatal("probe ended with its first waiter's client")
	}

	// The probe still lands, and the result is cached for the next request.
	waitFor(t, "the child to receive the probe", func() bool { return discoverCount(t, b) >= 1 })
	releaseHeld(t, b)
	waitFor(t, "probe result", func() bool { return eraName(b) == "modern" })
	if r := eraPost(t, b, acceptJSON, modernCall(2), nil); r.status != 200 {
		t.Fatalf("after probe: %d %s", r.status, r.body)
	}
}

func TestEra_GetAndDeleteAre405OnceRefusing2025Traffic(t *testing.T) {
	b := startEraBridge(t, nil, "--era=modern")
	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 200 {
		t.Fatalf("modern call: %d %s", r.status, r.body)
	}
	statuses := func() (int, int, string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		get, _ := http.NewRequestWithContext(ctx, http.MethodGet, b.URL(), nil)
		get.Header.Set("Accept", "text/event-stream")
		del, _ := http.NewRequestWithContext(ctx, http.MethodDelete, b.URL(), nil)
		var codes [2]int
		allow := ""
		for i, req := range []*http.Request{get, del} {
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			codes[i] = resp.StatusCode
			if i == 0 {
				allow = resp.Header.Get("Allow")
			}
		}
		return codes[0], codes[1], allow
	}
	// A probe answer alone does not rule out a dual-era child.
	if g, d, _ := statuses(); g == 405 || d == 405 {
		t.Fatalf("405 on a probe answer alone: GET %d, DELETE %d", g, d)
	}
	makeRefusing(t, b)
	if g, d, allow := statuses(); g != 405 || d != 405 || allow != "POST" {
		t.Fatalf("after the child rejected initialize: GET %d, DELETE %d, Allow %q", g, d, allow)
	}
}

func TestEra_GetAndDeleteUnchangedOnDualChild(t *testing.T) {
	b := startEraBridge(t, nil, "--era=dual")
	init := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil)
	sid := init.header.Get("Mcp-Session-Id")
	if r := eraPost(t, b, acceptJSON, modernCall(2), nil); r.status != 200 {
		t.Fatalf("modern call: %d %s", r.status, r.body)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	get, _ := http.NewRequestWithContext(ctx, http.MethodGet, b.URL(), nil)
	get.Header.Set("Accept", "text/event-stream")
	get.Header.Set("Mcp-Session-Id", sid)
	resp, err := http.DefaultClient.Do(get)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("GET on a dual child: %d", resp.StatusCode)
	}

	del, _ := http.NewRequest(http.MethodDelete, b.URL(), nil)
	del.Header.Set("Mcp-Session-Id", sid)
	dresp, err := http.DefaultClient.Do(del)
	if err != nil {
		t.Fatal(err)
	}
	dresp.Body.Close()
	if dresp.StatusCode != 204 {
		t.Errorf("DELETE on a dual child: %d", dresp.StatusCode)
	}
}

func sessionMinted(b *Bridge) bool {
	b.session.mu.Lock()
	defer b.session.mu.Unlock()
	return b.session.hasID
}

func TestEra_NoSessionAfterFailedInitialize(t *testing.T) {
	for _, accept := range []string{acceptJSON, acceptSSE} {
		b := startEraBridge(t, nil, "--era=modern")
		for i := 0; i < 2; i++ {
			r := eraPost(t, b, accept, eraInitialize(i+1).body, nil)
			if r.status != 400 || r.header.Get("Mcp-Session-Id") != "" {
				t.Fatalf("%s: rejected initialize: %d %v", accept, r.status, r.header)
			}
			if !strings.Contains(r.body, `"supported":["2026-07-28"]`) {
				t.Fatalf("%s: body does not name the supported versions: %s", accept, r.body)
			}
		}
		if sessionMinted(b) {
			t.Fatalf("%s: a session was minted for a failed initialize", accept)
		}
		// 2025-style traffic is therefore not gated on a session header: it
		// gets the modern-only refusal, not "session header required".
		r := eraPost(t, b, accept, map[string]any{"jsonrpc": "2.0", "id": 5, "method": "ping"}, nil)
		if r.status != 400 || !strings.Contains(r.body, `"code":-32600`) || !strings.Contains(r.body, `"supported":["2026-07-28"]`) {
			t.Fatalf("%s: header-less 2025-style request: %d %s", accept, r.status, r.body)
		}
	}
}

func TestEra_LegacyShapedTrafficIsRefusedOnceRefusing2025Traffic(t *testing.T) {
	b := startEraBridge(t, nil, "--era=modern")
	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 200 {
		t.Fatalf("modern call: %d %s", r.status, r.body)
	}
	makeRefusing(t, b)
	before := forwardedCount(t, b)

	call := func(id int) map[string]any {
		return map[string]any{"jsonrpc": "2.0", "id": id, "method": "tools/call", "params": map[string]any{"name": "echo"}}
	}
	cases := []struct {
		name string
		body map[string]any
		hdr  map[string]string
	}{
		{"tools/call", call(11), nil},
		{"tools/call with a session header", call(12), map[string]string{"Mcp-Session-Id": "whatever"}},
		{"ping", map[string]any{"jsonrpc": "2.0", "id": 13, "method": "ping"}, nil},
		{"subscriptions/listen", map[string]any{"jsonrpc": "2.0", "id": 14, "method": "subscriptions/listen", "params": map[string]any{}}, nil},
		{"initialize", eraInitialize(15).body, nil},
		{"notification", map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"}, nil},
		{"client response", map[string]any{"jsonrpc": "2.0", "id": 16, "result": map[string]any{}}, nil},
	}
	for _, c := range cases {
		for _, accept := range []string{acceptJSON, acceptSSE} {
			r := eraPost(t, b, accept, c.body, c.hdr)
			if r.status != 400 || !strings.Contains(r.body, `"code":-32600`) || !strings.Contains(r.body, `"supported":["2026-07-28"]`) || r.header.Get("Mcp-Session-Id") != "" {
				t.Errorf("%s (%s): %d %v %s", c.name, accept, r.status, r.header, r.body)
			}
			// A refused request echoes its own id; a client response's id is
			// the server's and a notification has none, so those get null.
			wantID := `"id":null`
			if _, isMethod := c.body["method"]; isMethod {
				if id, ok := c.body["id"]; ok {
					wantID = fmt.Sprintf(`"id":%v`, id)
				}
			}
			if !strings.Contains(r.body, wantID) {
				t.Errorf("%s (%s): want %s in the refusal: %s", c.name, accept, wantID, r.body)
			}
		}
	}
	if n := forwardedCount(t, b); n != before {
		t.Fatalf("a refused request reached the child (%d -> %d)", before, n)
	}
	if sessionMinted(b) {
		t.Fatal("a refused request minted a session")
	}
	// A modern-shaped notification is not 2025-style traffic.
	note := map[string]any{"jsonrpc": "2.0", "method": "notifications/whatever", "params": map[string]any{"_meta": eraMeta(eraTestVersion)}}
	if r := eraPost(t, b, acceptJSON, note, nil); r.status != 202 {
		t.Fatalf("modern-shaped notification: %d %s", r.status, r.body)
	}
}

// Until the child has rejected a forwarded initialize with modern evidence,
// 2025-style requests go through: with no verdict, with a verdict that is
// only a probe timeout, and with a probe answer listing only the modern
// version.
func TestEra_LegacyShapedRequestsForwardUntilRefusing2025Traffic(t *testing.T) {
	b := startEraBridge(t, shortProbe, "--era=modern", "--discover-hold")
	ping := map[string]any{"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": map[string]any{"name": "echo"}}
	forwarded := func(what string) {
		t.Helper()
		before := forwardedCount(t, b)
		if r := eraPost(t, b, acceptJSON, ping, nil); r.status != 200 {
			t.Fatalf("%s: %d %s", what, r.status, r.body)
		}
		if forwardedCount(t, b) != before+1 {
			t.Fatalf("%s: not forwarded", what)
		}
	}
	forwarded("verdict unknown")

	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 400 {
		t.Fatalf("probe should time out: %d %s", r.status, r.body)
	}
	forwarded("verdict is a probe timeout")

	releaseHeld(t, b)
	if eraName(b) != "modern" || b.eraRefusesLegacy() {
		t.Fatalf("after the late answer: era=%q refuses=%v", eraName(b), b.eraRefusesLegacy())
	}
	forwarded("verdict is a probe answer listing only the modern version")

	// The child's own rejection of a forwarded initialize completes the
	// evidence.
	makeRefusing(t, b)
	if r := eraPost(t, b, acceptJSON, ping, nil); r.status != 400 || !strings.Contains(r.body, `"supported":["2026-07-28"]`) {
		t.Fatalf("once refusing: %d %s", r.status, r.body)
	}
}

// handleChildResponse feeds every initialize answer to the era state on the
// child's pump, before any handler reads it, so it counts even if nobody
// reads it (a client that left just as it arrived).
func TestEra_HandleChildResponseFeedsInitializeAnswers(t *testing.T) {
	b := startEraBridge(t, nil, "--era=legacy")
	st := b.era
	result := map[string]any{"jsonrpc": "2.0", "id": 1, "result": map[string]any{"protocolVersion": "2025-11-25"}}
	pend := func(id int64, e *pendingEntry) {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.pending[id] = e
	}

	// An unknown id, a non-initialize request, or a rejection that does not
	// look modern: nothing recorded.
	b.handleChildResponse(101, result)
	pend(102, &pendingEntry{originalID: 1, plain: httptest.NewRecorder(), timer: time.NewTimer(time.Hour), done: make(chan struct{})})
	b.handleChildResponse(102, result)
	pend(103, &pendingEntry{raw: make(chan map[string]any, 1), era: st})
	b.handleChildResponse(103, map[string]any{"jsonrpc": "2.0", "id": 103, "error": map[string]any{"code": float64(-32602)}})
	if got := eraName(b); got != "unknown" {
		t.Fatalf("era = %q", got)
	}

	// The answer lands in raw and nobody reads it: still recorded.
	pend(104, &pendingEntry{raw: make(chan map[string]any, 1), era: st})
	b.handleChildResponse(104, result)
	if got := eraName(b); got != "legacy-capable" {
		t.Fatalf("era = %q, want legacy-capable", got)
	}
}

func TestEra_ConcurrentInitializeMintsOneSession(t *testing.T) {
	b := startEraBridge(t, nil, "--era=legacy")
	const n = 6
	ids := make(chan string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			accept := acceptJSON
			if i%2 == 0 {
				accept = acceptSSE
			}
			r := eraPost(t, b, accept, eraInitialize(i).body, nil)
			ids <- r.header.Get("Mcp-Session-Id")
		}(i)
	}
	wg.Wait()
	close(ids)
	first := ""
	for id := range ids {
		if id == "" || (first != "" && id != first) {
			t.Fatalf("session ids differ or are missing: %q vs %q", id, first)
		}
		first = id
	}
}

// releaseUntil makes the fake send its held answers until n have gone out in
// total: the call that asked for them may be written before the child has
// read it.
func releaseUntil(t *testing.T, b *Bridge, n int) {
	t.Helper()
	released := 0
	waitFor(t, fmt.Sprintf("%d held answers to be released", n), func() bool {
		r, _ := debugCall(t, b, "debug/release", nil)["released"].(float64)
		released += int(r)
		return released >= n
	})
}

func sendHeadOnly(t *testing.T, b *Bridge, accept string, body map[string]any) *http.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	resp, _, err := eraSend(ctx, b.URL(), accept, body, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestEra_InitializeHeadHeldForSessionThenReleased(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.InitializeHoldTimeout = 200 * time.Millisecond },
		"--era=legacy", "--initialize-hold")

	// The child holds its answer, so a head can only be the hold deadline's.
	resp := sendHeadOnly(t, b, acceptSSE, eraInitialize(1).body)
	if resp.StatusCode != 200 {
		t.Fatalf("head: %d, want 200 at the hold deadline", resp.StatusCode)
	}
	sid := resp.Header.Get("Mcp-Session-Id")
	if sid == "" || !sessionMinted(b) {
		t.Fatalf("session %q / minted %v after a held head expired", sid, sessionMinted(b))
	}
	releaseUntil(t, b, 1)
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"protocolVersion"`) {
		t.Fatalf("result missing from the stream: %s", body)
	}
	// The head left with a provisional session id, which the successful
	// answer confirmed.
	if b.session.check("ping", sid, true).kind != sessionOK {
		t.Fatal("the confirmed session id was refused")
	}
}

func TestEra_InitializeLateRejectionRemovesProvisionalSession(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.InitializeHoldTimeout = 150 * time.Millisecond },
		"--era=legacy", "--initialize-hold", "--reject-initialize=1")

	resp := sendHeadOnly(t, b, acceptSSE, eraInitialize(1).body)
	if resp.StatusCode != 200 || resp.Header.Get("Mcp-Session-Id") == "" {
		t.Fatalf("head: %d, session %q; want a 200 with a provisional session at the deadline", resp.StatusCode, resp.Header.Get("Mcp-Session-Id"))
	}
	releaseUntil(t, b, 1)
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "bad initialize") {
		t.Fatalf("rejection missing from the stream: %s", body)
	}
	if sessionMinted(b) {
		t.Fatal("the provisional session outlived the rejection")
	}
	// Nothing is gated on the revoked session any more.
	if r := eraPost(t, b, acceptJSON, map[string]any{"jsonrpc": "2.0", "id": 2, "method": "ping"}, nil); r.status == 400 {
		t.Fatalf("header-less request after the revoke: %d %s", r.status, r.body)
	}
}

// A progress frame is another way for the head of a held initialize to go
// out; it must carry the provisional session exactly as the deadline's does.
func TestEra_ProgressFrameOnHeldInitializeCarriesProvisionalSession(t *testing.T) {
	init := eraInitialize(1).body
	init["params"].(map[string]any)["_meta"] = map[string]any{"progressToken": "p1"}

	t.Run("accepted", func(t *testing.T) {
		b := startEraBridge(t, nil, "--era=legacy", "--initialize-hold")
		resp := sendHeadOnly(t, b, acceptSSE, init)
		sid := resp.Header.Get("Mcp-Session-Id")
		if resp.StatusCode != 200 || sid == "" || !sessionMinted(b) {
			t.Fatalf("head committed by progress: %d, session %q, minted %v", resp.StatusCode, sid, sessionMinted(b))
		}
		releaseUntil(t, b, 1)
		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), `"progressToken":"p1"`) || !strings.Contains(string(body), `"protocolVersion"`) {
			t.Fatalf("stream: %s", body)
		}
		if b.session.check("ping", sid, true).kind != sessionOK {
			t.Fatal("the session the client holds is not the one the bridge confirmed")
		}
	})
	t.Run("rejected", func(t *testing.T) {
		b := startEraBridge(t, nil, "--era=legacy", "--initialize-hold", "--reject-initialize=1")
		resp := sendHeadOnly(t, b, acceptSSE, init)
		if resp.StatusCode != 200 || resp.Header.Get("Mcp-Session-Id") == "" {
			t.Fatalf("head committed by progress: %d, session %q", resp.StatusCode, resp.Header.Get("Mcp-Session-Id"))
		}
		releaseUntil(t, b, 1)
		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), "bad initialize") {
			t.Fatalf("stream: %s", body)
		}
		if sessionMinted(b) {
			t.Fatal("the provisional session outlived the rejection")
		}
	})
}

func TestEra_ClientDisconnectWithProvisionalSessionRevokesIt(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.InitializeHoldTimeout = 100 * time.Millisecond },
		"--era=legacy", "--initialize-hold")

	ctx, cancel := context.WithCancel(context.Background())
	resp, _, err := eraSend(ctx, b.URL(), acceptSSE, eraInitialize(1).body, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Header.Get("Mcp-Session-Id") == "" || !sessionMinted(b) {
		t.Fatalf("setup: no provisional session (%q, minted %v)", resp.Header.Get("Mcp-Session-Id"), sessionMinted(b))
	}
	cancel()
	resp.Body.Close()
	waitFor(t, "the handler to leave and revoke the provisional session", func() bool { return !sessionMinted(b) })
}

// Two initializes at once, one rejected and one accepted: whichever the
// child rejects gets its 400 and no session of its own, the other a 200, and
// exactly the accepted one's session stands.
func TestEra_ConcurrentInitializesWithDifferentOutcomes(t *testing.T) {
	for _, accept := range []string{acceptJSON, acceptSSE} {
		t.Run(accept, func(t *testing.T) {
			b := startEraBridge(t, nil, "--era=legacy", "--initialize-hold", "--reject-initialize=1")
			type out struct {
				status int
				sid    string
			}
			res := make(chan out, 2)
			for i := 1; i <= 2; i++ {
				go func(i int) {
					r := eraPost(t, b, accept, eraInitialize(i).body, nil)
					res <- out{r.status, r.header.Get("Mcp-Session-Id")}
				}(i)
			}
			waitFor(t, "both initializes pending", func() bool {
				b.mu.Lock()
				defer b.mu.Unlock()
				return len(b.pending) == 2
			})
			waitFor(t, "the child to hold both", func() bool { return forwardedCount(t, b) >= 2 })
			releaseUntil(t, b, 2)

			a, c := <-res, <-res
			if a.status > c.status {
				a, c = c, a
			}
			if a.status != 200 || a.sid == "" || c.status != 400 || c.sid != "" {
				t.Fatalf("outcomes %+v and %+v", a, c)
			}
			if b.session.check("ping", a.sid, true).kind != sessionOK {
				t.Fatal("the accepted initialize's session is not the bridge's")
			}
			// The rejection's background probe may land before or after the
			// accepted answer: either the initialized child's own legacy
			// answer or a pre-initialize one that cached nothing.
			if got := eraName(b); (got != "legacy-capable" && got != "legacy") || b.eraRefusesLegacy() {
				t.Fatalf("era = %q", got)
			}
		})
	}
}

func TestEra_ProvisionalSessionSurvivesWhileAnotherHolderIsPending(t *testing.T) {
	s := newSession()
	a, trackedA := s.provisionalMint()
	b, trackedB := s.provisionalMint()
	if a != b || !trackedA || !trackedB {
		t.Fatalf("provisional mints: %q/%v %q/%v", a, trackedA, b, trackedB)
	}
	s.revoke()
	if !s.hasID {
		t.Fatal("revoked while another provisional holder was pending")
	}
	s.revoke()
	if s.hasID {
		t.Fatal("not revoked once every holder released")
	}
	s.provisionalMint()
	s.check("initialize", "", false)
	s.revoke()
	if !s.hasID {
		t.Fatal("a confirmed session was revoked")
	}
}

func TestEra_HoldMapsFastErrorToStatus(t *testing.T) {
	b := startEraBridge(t, nil, "--era=modern")
	for code, want := range map[int]int{-32601: 404, -32022: 400, -32020: 400, -32021: 400, -32602: 200, -32000: 200} {
		r := eraPost(t, b, acceptSSE, modernBody(1, "debug/delayedError", map[string]any{"code": code}, nil), nil)
		if r.status != want {
			t.Errorf("code %d: status %d, want %d (%s)", code, r.status, want, r.body)
		}
		if want != 200 && !strings.HasPrefix(r.header.Get("Content-Type"), "application/json") {
			t.Errorf("code %d: content type %q", code, r.header.Get("Content-Type"))
		}
	}
}

func TestEra_HoldReleasedAtDeadlineLaterErrorStaysInStream(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.HeadHoldTimeout = 150 * time.Millisecond }, "--era=modern")

	// The child holds its error, so the head can only be the deadline's.
	resp := sendHeadOnly(t, b, acceptSSE, modernBody(1, "debug/heldError", map[string]any{"code": -32601}, nil))
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("head: %d %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	releaseUntil(t, b, 1)
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"code":-32601`) {
		t.Fatalf("late error missing from the 200 stream: %s", body)
	}
}

func TestEra_NonFinalMessageCommitsHeadAtOnce(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.HeadHoldTimeout = time.Hour }, "--era=modern")

	// The hold never expires and the error is held: only the progress
	// notification can have committed the head.
	resp := sendHeadOnly(t, b, acceptSSE, modernBody(1, "debug/progressThenError", map[string]any{"hold": true, "code": -32601}, "tok"))
	if resp.StatusCode != 200 {
		t.Fatalf("head: %d", resp.StatusCode)
	}
	releaseUntil(t, b, 1)
	body, _ := io.ReadAll(resp.Body)
	s := string(body)
	if !strings.Contains(s, `"progressToken":"tok"`) || !strings.Contains(s, `"code":-32601`) {
		t.Fatalf("stream missing progress or the final error: %s", s)
	}
}

func TestEra_ClientDisconnectDuringHold(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.HeadHoldTimeout = time.Hour }, "--era=modern")
	// Learn the era first so the request below goes straight to the hold.
	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 200 {
		t.Fatalf("modern call: %d %s", r.status, r.body)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		resp, _, err := eraSend(ctx, b.URL(), acceptSSE, modernBody(7, "debug/heldError", map[string]any{"code": -32601}, nil), nil)
		if err == nil {
			resp.Body.Close()
		}
		errc <- err
	}()
	id := firstPendingID(t, b)
	cancel()
	<-errc

	drained := make(chan struct{})
	go func() { b.wg.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		t.Fatal("handler did not return after the client left during the hold")
	}
	waitFor(t, "notifications/cancelled", func() bool { return childWasCancelled(t, b, float64(id)) })

	// The child's error arrives after the handler is gone: it must be
	// dropped without writing to the finished response (-race watches).
	releaseUntil(t, b, 1)
	if r := eraPost(t, b, acceptJSON, modernCall(8), nil); r.status != 200 {
		t.Fatalf("bridge unhealthy afterwards: %d %s", r.status, r.body)
	}
}

// The request timer is the other thing that can end a held head: it picks
// the status (504) like any other final error.
func TestEra_RequestTimeoutOnHeldHeadIs504(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) {
		o.RequestTimeout = 300 * time.Millisecond
		o.HeadHoldTimeout = time.Hour
	}, "--era=modern")

	r := eraPost(t, b, acceptSSE, modernBody(1, "debug/heldError", map[string]any{"code": -32601}, nil), nil)
	if r.status != 504 || !strings.HasPrefix(r.header.Get("Content-Type"), "application/json") || !strings.Contains(r.body, "upstream timeout") {
		t.Fatalf("%d %q %s", r.status, r.header.Get("Content-Type"), r.body)
	}
	// The child's answer, when it comes, finds nobody.
	releaseUntil(t, b, 1)
	if r := eraPost(t, b, acceptJSON, modernCall(2), nil); r.status != 200 {
		t.Fatalf("bridge unhealthy afterwards: %d %s", r.status, r.body)
	}
}

// A panic in the handler while the head is held: the held head is released
// without a write racing the recovery, which answers 502, and no session is
// left behind for an initialize.
func TestEra_PanicWhileHeadIsHeld(t *testing.T) {
	cases := []struct {
		name string
		body map[string]any
		prep func(b *Bridge)
	}{
		{"modern", modernCall(1), func(b *Bridge) {
			b.mu.Lock()
			b.era.m.verdict = eraVerdict{kind: verdictModern, versions: []string{eraTestVersion}}
			b.mu.Unlock()
		}},
		{"initialize", eraInitialize(1).body, func(*Bridge) {}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := startEraBridge(t, func(o *StartBridgeOptions) {
				o.HeadHoldTimeout = time.Hour
				o.InitializeHoldTimeout = time.Hour
			}, "--era=modern")
			real := b.GetCurrentChild()
			// A child with no stdin: Send panics after the head is held.
			b.ReplaceChild(newBareChild())
			t.Cleanup(func() { b.ReplaceChild(real) })
			c.prep(b)

			r := eraPost(t, b, acceptSSE, c.body, nil)
			if r.status != 502 || !strings.Contains(r.body, "internal error") {
				t.Fatalf("%d %s", r.status, r.body)
			}
			if sessionMinted(b) {
				t.Fatal("a session outlived the panic")
			}
			b.mu.Lock()
			pending := len(b.pending)
			b.mu.Unlock()
			if pending != 0 {
				t.Fatalf("%d entries left pending", pending)
			}
		})
	}
}

// postStatus returns the response status, or the transport error.
func postStatus(b *Bridge, accept string, body map[string]any) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp, _, err := eraSend(ctx, b.URL(), accept, body, nil)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

// Close answers what it releases with a 502 that is flushed before the
// handler returns, so the listener's hard close cannot cut it off.
func TestEra_CloseReleasesHeldHeadAndProbe(t *testing.T) {
	type result struct {
		status int
		err    error
	}
	t.Run("held head", func(t *testing.T) {
		b := startEraBridge(t, func(o *StartBridgeOptions) { o.HeadHoldTimeout = time.Hour }, "--era=modern")
		if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 200 {
			t.Fatalf("modern call: %d %s", r.status, r.body)
		}
		res := make(chan result, 1)
		go func() {
			s, err := postStatus(b, acceptSSE, modernBody(2, "debug/heldError", map[string]any{"code": -32601}, nil))
			res <- result{s, err}
		}()
		firstPendingID(t, b)
		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		select {
		case r := <-res:
			if r.err != nil || r.status != 502 {
				t.Fatalf("held request after Close: %d, %v", r.status, r.err)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("held request not released by Close")
		}
	})
	t.Run("probe", func(t *testing.T) {
		b := startEraBridge(t, nil, "--era=modern", "--discover-hold")
		res := make(chan result, 1)
		go func() {
			s, err := postStatus(b, acceptJSON, modernCall(1))
			res <- result{s, err}
		}()
		waitProbeInFlight(t, b)
		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		select {
		case r := <-res:
			if r.err != nil || r.status != 502 {
				t.Fatalf("waiter after Close: %d, %v", r.status, r.err)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("probe waiter not released by Close")
		}
		if probeInFlight(b) {
			t.Fatal("probe still registered after Close")
		}
	})
}

// The spec's "Value Encoding": the =?base64?...?= sentinel (case-sensitive
// markers) wraps standard-alphabet Base64 of UTF-8, and a raw header value is
// visible ASCII, space or tab. It names no other alphabet or padding rule, so
// only the standard padded form its examples use is accepted.
func TestDecodeMcpName(t *testing.T) {
	enc := func(b []byte) string { return "=?base64?" + base64.StdEncoding.EncodeToString(b) + "?=" }
	cases := []struct {
		name string
		in   string
		want string
		ok   bool
	}{
		{"plain", "get_weather", "get_weather", true},
		{"plain with space and tab", "a b\tc", "a b\tc", true},
		{"plain visible ASCII extremes", "!~", "!~", true},
		{"empty", "", "", true},
		{"URI", "file:///projects/myapp/config.json", "file:///projects/myapp/config.json", true},
		{"spec example, non-ASCII", "=?base64?SGVsbG8sIOS4lueVjA==?=", "Hello, 世界", true},
		{"spec example, padded value", "=?base64?IHBhZGRlZCA=?=", " padded ", true},
		{"spec example, newline", "=?base64?bGluZTEKbGluZTI=?=", "line1\nline2", true},
		{"spec example, sentinel-shaped value", "=?base64?PT9iYXNlNjQ/bGl0ZXJhbD89?=", "=?base64?literal?=", true},
		{"empty payload", "=?base64??=", "", true},
		{"standard alphabet with + and /", enc([]byte("a?>>b")), "a?>>b", true},
		{"URL-safe alphabet", "=?base64?" + base64.URLEncoding.EncodeToString([]byte("a?>>b")) + "?=", "", false},
		{"unpadded", "=?base64?" + base64.RawStdEncoding.EncodeToString([]byte("echoo")) + "?=", "", false},
		{"not base64", "=?base64?***?=", "", false},
		{"invalid UTF-8", enc([]byte{0xff, 0xfe}), "", false},
		{"truncated UTF-8", enc([]byte{0xe4, 0xb8}), "", false},
		{"marker case", "=?BASE64?ZWNobw==?=", "=?BASE64?ZWNobw==?=", true}, // not a sentinel: taken literally
		{"prefix only", "=?base64?ZWNobw==", "=?base64?ZWNobw==", true},
		{"suffix only", "ZWNobw==?=", "ZWNobw==?=", true},
		{"non-ASCII byte", "héllo", "", false},
		{"DEL", "a\x7fb", "", false},
		{"NUL", "a\x00b", "", false},
		{"CR", "a\rb", "", false},
		{"LF", "a\nb", "", false},
		{"ESC", "a\x1bb", "", false},
		{"invalid raw character inside a sentinel value", "=?base64?ZWNobw==\x01?=", "", false},
	}
	for _, c := range cases {
		got, err := decodeMcpName(c.in)
		if (err == nil) != c.ok || (c.ok && got != c.want) {
			t.Errorf("%s: decodeMcpName(%q) = %q, %v", c.name, c.in, got, err)
		}
	}
}

// A rejected initialize looks modern with -32601, -32022 or any
// data.supported, but is modern-only evidence only when data.supported lists
// 2026-era versions alone; the bridge never makes up a list.
func TestEraInitializeRejectionEvidence(t *testing.T) {
	rej := func(code int, supported ...any) map[string]any {
		e := map[string]any{"code": float64(code), "message": "x"}
		if supported != nil {
			e["data"] = map[string]any{"supported": supported}
		}
		return map[string]any{"jsonrpc": "2.0", "id": 1, "error": e}
	}
	cases := []struct {
		name         string
		msg          map[string]any
		looksModern  bool
		wantEvidence []string
	}{
		{"-32601 without data", rej(-32601), true, nil},
		{"-32601 with its own modern list", rej(-32601, "2026-07-28"), true, []string{"2026-07-28"}},
		{"-32601 listing a legacy version too", rej(-32601, "2026-07-28", "2025-11-25"), true, nil},
		{"-32022 without data", rej(-32022), true, nil},
		{"-32022 with its own modern list", rej(-32022, "2026-07-28", "2027-01-01"), true, []string{"2026-07-28", "2027-01-01"}},
		{"-32602 listing only modern versions", rej(-32602, "2026-07-28"), true, []string{"2026-07-28"}},
		{"-32602 without data", rej(-32602), false, nil},
		{"-32602 listing a legacy version too", rej(-32602, "2026-07-28", "2025-11-25"), true, nil},
		{"-32602 with an empty list", rej(-32602, []any{}...), true, nil},
		{"-32000 without data", rej(-32000), false, nil},
		// A list with an entry that is not a version string is no evidence.
		{"-32601 with a non-string entry", rej(-32601, "2026-07-28", 2027.0), true, nil},
		{"-32601 with a version that is no date", rej(-32601, "latest"), true, nil},
		{"-32601 with a malformed date", rej(-32601, "2026-7-28"), true, nil},
		// Codes compare as numbers; a fractional or non-numeric one does not look modern.
		{"-32601.0", map[string]any{"error": map[string]any{"code": -32601.0}}, true, nil},
		{"-32022.0", map[string]any{"error": map[string]any{"code": -32022.0}}, true, nil},
		{"-32601.5", map[string]any{"error": map[string]any{"code": -32601.5}}, false, nil},
		{"code as a string", map[string]any{"error": map[string]any{"code": "-32601"}}, false, nil},
		// Only an array is a list; null or anything else leaves the code to decide.
		{"-32602 with data.supported null", map[string]any{"error": map[string]any{"code": -32602.0, "data": map[string]any{"supported": nil}}}, false, nil},
		{"-32601 with data.supported null", map[string]any{"error": map[string]any{"code": -32601.0, "data": map[string]any{"supported": nil}}}, true, nil},
		{"-32602 with data.supported a string", map[string]any{"error": map[string]any{"code": -32602.0, "data": map[string]any{"supported": "2026-07-28"}}}, false, nil},
	}
	for _, c := range cases {
		cls, got := classifyInitializeRejection(c.msg)
		looks := cls != rejNotModern
		if looks != c.looksModern || strings.Join(got, ",") != strings.Join(c.wantEvidence, ",") || (cls == rejModernOnly) != (got != nil) {
			t.Errorf("%s: got %v, %v; want %v, %v", c.name, cls, got, c.looksModern, c.wantEvidence)
		}
	}
	// A valid list naming a 2025-era version says the child takes initialize,
	// whatever the code; an empty or invalid list does not.
	for _, c := range []struct {
		name string
		msg  map[string]any
		want rejectionClass
	}{
		{"-32602 listing a legacy version", rej(-32602, "2025-06-18", "2026-07-28"), rejLegacyListed},
		{"-32601 listing a legacy version too", rej(-32601, "2026-07-28", "2025-11-25"), rejLegacyListed},
		{"-32022 listing only a legacy version", rej(-32022, "2025-11-25"), rejLegacyListed},
		{"-32601 with an empty list", rej(-32601, []any{}...), rejMixed},
		{"-32601 with a non-string entry", rej(-32601, "2025-11-25", 7.0), rejMixed},
		{"-32601 with a modern-only list", rej(-32601, "2026-07-28"), rejModernOnly},
	} {
		if cls, _ := classifyInitializeRejection(c.msg); cls != c.want {
			t.Errorf("%s: class %v, want %v", c.name, cls, c.want)
		}
	}

	// A bare -32601 goes back to the client unchanged and caches nothing, so
	// 2025 traffic is still forwarded; the probe it starts is what decides.
	b := startEraBridge(t, nil, "--era=dual", "--reject-initialize=1", "--reject-code=-32601", "--discover-hold")
	r := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil)
	if r.status != 400 || !strings.Contains(r.body, `"code":-32601`) || !strings.Contains(r.body, "bad initialize") || strings.Contains(r.body, "supported") {
		t.Fatalf("rejection: %d %s", r.status, r.body)
	}
	if b.eraRefusesLegacy() || eraName(b) != "unknown" {
		t.Fatalf("a bare -32601 cached a verdict: era=%q src=%v", eraName(b), verdictSourceOf(b))
	}
	before := forwardedCount(t, b)
	if r := eraPost(t, b, acceptJSON, map[string]any{"jsonrpc": "2.0", "id": 2, "method": "ping"}, nil); strings.Contains(r.body, "only speaks") || r.status == 405 {
		t.Fatalf("2025 request refused on a bare -32601: %d %s", r.status, r.body)
	}
	if forwardedCount(t, b) != before+1 {
		t.Fatal("the 2025 request was not forwarded")
	}
	// The probe's DiscoverResult lists both versions: dual-era, not modern-only.
	releaseDirect(t, b)
	waitFor(t, "the probe's answer", func() bool { return verdictSourceOf(b) == srcProbe })
	if b.eraRefusesLegacy() {
		t.Fatal("a dual-era probe answer reads as modern-only")
	}
	if r := eraPost(t, b, acceptJSON, eraInitialize(3).body, nil); r.status != 200 || r.header.Get("Mcp-Session-Id") == "" {
		t.Fatalf("initialize after the probe: %d %s", r.status, r.body)
	}
}

// Modern-only evidence from an initialize rejection that arrives while a probe
// is in flight is remembered: if the probe times out it becomes the verdict,
// but a real probe answer still wins.
func TestEraRejectionEvidenceWhileProbeInFlight(t *testing.T) {
	args := []string{"--era=dual", "--reject-initialize=1", "--reject-modern-evidence", "--discover-hold"}
	startProbe := func(t *testing.T, b *Bridge) chan eraResponse {
		res := make(chan eraResponse, 1)
		go func() { res <- eraPost(t, b, acceptJSON, modernCall(1), nil) }()
		waitProbeInFlight(t, b)
		waitFor(t, "the child to receive the probe", func() bool { return discoverCount(t, b) >= 1 })
		if r := eraPost(t, b, acceptJSON, eraInitialize(2).body, nil); r.status != 400 || !strings.Contains(r.body, `"supported":["2026-07-28"]`) {
			t.Fatalf("rejection: %d %s", r.status, r.body)
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.era.m.verdict.kind != verdictNone || b.era.m.evidence == nil {
			t.Fatalf("evidence not held back: verdict=%v evidence=%v", b.era.m.verdict.kind, b.era.m.evidence)
		}
		return res
	}
	t.Run("timeout falls back to the rejection", func(t *testing.T) {
		// The default probe timeout is far away; the flight's timer is fired by
		// hand once the evidence is in, so nothing here races a clock.
		b := startEraBridge(t, nil, args...)
		res := startProbe(t, b)
		timeOutProbeByHand(t, b)
		if r := <-res; r.status != 200 {
			t.Fatalf("waiter: %d %s", r.status, r.body)
		}
		if !b.eraRefusesLegacy() || verdictSourceOf(b) != srcRejection {
			t.Fatalf("refuses2025=%v src=%v after the timeout", b.eraRefusesLegacy(), verdictSourceOf(b))
		}
		releaseDirect(t, b)
		waitFor(t, "the late answer", func() bool { return verdictSourceOf(b) == srcProbe })
		if b.eraRefusesLegacy() {
			t.Fatal("the late probe answer (both versions) did not outrank the rejection")
		}
	})
	t.Run("probe answer wins", func(t *testing.T) {
		b := startEraBridge(t, nil, args...)
		res := startProbe(t, b)
		releaseDirect(t, b)
		if r := <-res; r.status != 200 {
			t.Fatalf("waiter: %d %s", r.status, r.body)
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		m := b.era.m
		if m.verdict.kind != verdictModern || m.verdict.modernOnly() || m.evidence != nil {
			t.Fatalf("verdict=%v modernOnly=%v evidence=%v", m.verdict.kind, m.verdict.modernOnly(), m.evidence)
		}
	})
}

// A modern child rejecting initialize without data.supported: the client gets
// the child's own error, no session. A bare -32601 caches nothing by itself;
// the background probe it starts does, with the child's own list.
func TestEraModernChildRejectingInitializeWithoutSupported(t *testing.T) {
	for _, accept := range []string{acceptJSON, acceptSSE} {
		b := startEraBridge(t, nil, "--era=modern", "--modern-bare-initialize-rejection", "--discover-hold")
		r := eraPost(t, b, accept, eraInitialize(1).body, nil)
		want := `"error":{"code":-32601,"message":"Method not found: initialize"}`
		if r.status != 400 || !strings.Contains(r.body, want) || strings.Contains(r.body, "only speaks") || r.header.Get("Mcp-Session-Id") != "" {
			t.Fatalf("%s: %d %v %s", accept, r.status, r.header, r.body)
		}
		if sessionMinted(b) {
			t.Fatalf("%s: session from a rejected initialize", accept)
		}
		if b.eraRefusesLegacy() || eraName(b) != "unknown" {
			t.Fatalf("%s: the bare rejection cached a verdict: %q", accept, eraName(b))
		}
		// Not refused while the probe is out: initialize keeps being forwarded.
		again := eraPost(t, b, accept, eraInitialize(2).body, nil)
		if again.status != 400 || !strings.Contains(again.body, want) {
			t.Fatalf("%s: second initialize before the probe answers: %d %s", accept, again.status, again.body)
		}
		releaseDirect(t, b)
		waitFor(t, "the background probe", func() bool { return verdictSourceOf(b) == srcProbe })
		if n := discoverCount(t, b); n != 1 {
			t.Fatalf("%s: child saw %d probes, want 1", accept, n)
		}
		// The probe's own list completes the evidence the bare rejections
		// started: the next initialize gets the bridge's own refusal.
		r = eraPost(t, b, accept, eraInitialize(3).body, nil)
		if r.status != 400 || !strings.Contains(r.body, `"code":-32600`) || !strings.Contains(r.body, `"supported":["2026-07-28"]`) {
			t.Fatalf("%s: initialize after the probe: %d %s", accept, r.status, r.body)
		}
	}
}

// completeInitialize answers a rejection with the child's own error even when
// the child is already gone by then, and its background probe leaves nothing
// behind.
func TestEra_CompleteInitializeReturnsChildsOwnRejectionAfterChildExit(t *testing.T) {
	b := startEraBridge(t, nil, "--era=modern")
	child, st := b.currentChildEra()
	_ = child.Close()
	waitFor(t, "the child to exit", func() bool { return child.HasExited() })

	rec := httptest.NewRecorder()
	entry := &pendingEntry{originalID: 7, plain: rec, timer: time.NewTimer(time.Hour), done: make(chan struct{})}
	msg := map[string]any{"jsonrpc": "2.0", "id": 1, "error": map[string]any{
		"code": float64(-32601), "message": "nope", "data": map[string]any{"supported": []any{"2026-07-28"}},
	}}
	// As the child's pump and then the handler would.
	b.noteInitializeAnswer(st, msg)
	b.completeInitialize(entry, msg)

	body := rec.Body.String()
	if rec.Code != 400 || !strings.Contains(body, `"message":"nope"`) || !strings.Contains(body, `"id":7`) || strings.Contains(body, "only speaks") {
		t.Fatalf("%d %s", rec.Code, body)
	}
	waitFor(t, "the failed probe write to settle", func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return st.m.probe.phase == probeIdle
	})
	if timedOut, late := probeTimedOutVerdict(b); timedOut || late != "" {
		t.Fatalf("a probe to a dead child left state: timedOut=%v outstanding=%q", timedOut, late)
	}
}

// The hold deadline of a held initialize must not hand out a session after the
// response has released it, whichever way the response ends.
func TestEra_ReleasedProvisionalSessionIsNotReMintedByTheHoldDeadline(t *testing.T) {
	paths := map[string]func(e *pendingEntry){
		"respond":    func(e *pendingEntry) { e.respond(400, map[string]any{"error": map[string]any{"code": -32602}}) },
		"rejectWith": func(e *pendingEntry) { e.rejectWith(errUpstreamTimeout) },
		"abandon":    func(e *pendingEntry) { e.abandon() },
	}
	for name, finish := range paths {
		for _, committedFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/committedFirst=%v", name, committedFirst), func(t *testing.T) {
				s := newSession()
				rec := httptest.NewRecorder()
				sse := newHeldSSEResponder(rec, map[string]string{})
				sse.holdForInitialize(time.Hour, func() (map[string]string, bool) {
					id, tracked := s.provisionalMint()
					return map[string]string{"Mcp-Session-Id": id}, tracked
				})
				e := &pendingEntry{
					originalID: 1, sse: sse, plain: rec, extraHeaders: map[string]string{},
					timer: time.NewTimer(time.Hour), done: make(chan struct{}), revoke: s.revoke,
				}
				if committedFirst {
					sse.commit()
				}
				finish(e)
				if s.hasID {
					t.Fatal("a session survived a rejected or abandoned initialize")
				}
			})
		}
	}
}

// The hold deadline landing right after the response let go of its
// provisional session must not mint one nobody will revoke; landing before,
// the claim it left is still revoked.
func TestEra_HoldDeadlineAfterTakeProvisionalMintsNoSession(t *testing.T) {
	setup := func() (*session, *httptest.ResponseRecorder, *sseResponder) {
		s := newSession()
		rec := httptest.NewRecorder()
		sse := newHeldSSEResponder(rec, map[string]string{})
		sse.holdForInitialize(time.Hour, func() (map[string]string, bool) {
			id, tracked := s.provisionalMint()
			return map[string]string{"Mcp-Session-Id": id}, tracked
		})
		return s, rec, sse
	}
	t.Run("take then commit", func(t *testing.T) {
		s, rec, sse := setup()
		if sse.takeProvisional() {
			t.Fatal("a claim was outstanding before the head went out")
		}
		sse.commit()
		if sessionHeld(s) || rec.Header().Get("Mcp-Session-Id") != "" {
			t.Fatalf("the late commit minted a session: held=%v header=%q", sessionHeld(s), rec.Header().Get("Mcp-Session-Id"))
		}
	})
	t.Run("commit then take", func(t *testing.T) {
		s, rec, sse := setup()
		sse.commit()
		if !sessionHeld(s) || rec.Header().Get("Mcp-Session-Id") == "" {
			t.Fatal("setup: the commit minted no provisional session")
		}
		if !sse.takeProvisional() {
			t.Fatal("the claim the commit left was not outstanding")
		}
		s.revoke()
		if sessionHeld(s) {
			t.Fatal("the revoked provisional session is still live")
		}
	})
}

func sessionHeld(s *session) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hasID
}

// The same race end to end: the hold deadline and the child's rejection land
// within microseconds of each other, in both orders, and no session survives
// a rejected initialize.
func TestEra_RejectedInitializeRacingTheHoldDeadlineLeavesNoSession(t *testing.T) {
	for _, delayMs := range []int{2, 3} {
		t.Run(fmt.Sprintf("child delay %dms", delayMs), func(t *testing.T) {
			b := startEraBridge(t, func(o *StartBridgeOptions) { o.InitializeHoldTimeout = 2500 * time.Microsecond },
				"--era=legacy", fmt.Sprintf("--initialize-delay-ms=%d", delayMs), "--reject-initialize=100000")
			for i := 0; i < 40; i++ {
				r := eraPost(t, b, acceptSSE, eraInitialize(i).body, nil)
				if !strings.Contains(r.body, "bad initialize") {
					t.Fatalf("iteration %d: %d %s", i, r.status, r.body)
				}
				if sessionMinted(b) {
					t.Fatalf("iteration %d: a session outlived the rejected initialize (status %d)", i, r.status)
				}
			}
		})
	}
}

// With the child dead and not respawned, a 2025-shaped request gets the
// "not running" answer, not the refusal its stale verdict would give.
func TestEra_DeadChildIsNotRunningRatherThanRefused(t *testing.T) {
	b := startEraBridge(t, nil, "--era=modern")
	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 200 {
		t.Fatalf("setup: %d %s", r.status, r.body)
	}
	makeRefusing(t, b)
	child := b.GetCurrentChild()
	_ = child.Close()
	waitFor(t, "the child to exit", func() bool { return child.HasExited() })

	for name, body := range map[string]map[string]any{
		"request":         {"jsonrpc": "2.0", "id": 5, "method": "ping"},
		"notification":    {"jsonrpc": "2.0", "method": "notifications/initialized"},
		"client response": {"jsonrpc": "2.0", "id": 6, "result": map[string]any{}},
	} {
		r := eraPost(t, b, acceptJSON, body, nil)
		if r.status != 502 || !strings.Contains(r.body, "not running") || strings.Contains(r.body, "only speaks") {
			t.Errorf("%s: %d %s", name, r.status, r.body)
		}
	}
}

// timeOutProbeByHand fires the in-flight probe's timer now, once the probe has
// been written to the child, so a test needs neither a short probe timeout nor
// a sleep.
func timeOutProbeByHand(t *testing.T, b *Bridge) {
	t.Helper()
	var st *eraState
	var id string
	waitFor(t, "a written probe in flight", func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		st, id = b.era, b.era.m.probe.id
		return b.era.m.probe.phase == probeWritten
	})
	b.probeTimedOut(st, id)
}

// initRejection is a child's rejection of initialize, with data.supported if
// any versions are given.
func initRejection(code int, supported ...any) map[string]any {
	e := map[string]any{"code": float64(code), "message": "nope"}
	if supported != nil {
		e["data"] = map[string]any{"supported": supported}
	}
	return map[string]any{"jsonrpc": "2.0", "id": 1, "error": e}
}

// completeRejected runs a rejected initialize for b's current process as the
// child's pump and then the handler would, and returns what the client got.
func completeRejected(b *Bridge, msg map[string]any) *httptest.ResponseRecorder {
	_, st := b.currentChildEra()
	rec := httptest.NewRecorder()
	entry := &pendingEntry{originalID: 7, plain: rec, timer: time.NewTimer(time.Hour), done: make(chan struct{})}
	b.noteInitializeAnswer(st, msg)
	b.completeInitialize(entry, msg)
	return rec
}

func answerDiscover(versions ...any) map[string]any {
	return map[string]any{"jsonrpc": "2.0", "result": map[string]any{"supportedVersions": versions}}
}

func TestEraClassifyProbe(t *testing.T) {
	errMsg := func(code int, data map[string]any) map[string]any {
		e := map[string]any{"code": float64(code), "message": "x"}
		if data != nil {
			e["data"] = data
		}
		return map[string]any{"error": e}
	}
	cases := []struct {
		name string
		msg  map[string]any
		want probeOutcome
		vers string
	}{
		{"modern-only DiscoverResult", answerDiscover("2026-07-28"), probeModern, "2026-07-28"},
		{"dual-era DiscoverResult", answerDiscover("2026-07-28", "2025-11-25"), probeModern, "2026-07-28,2025-11-25"},
		{"DiscoverResult with an empty list", answerDiscover(), probeModern, ""},
		{"DiscoverResult without supportedVersions", map[string]any{"result": map[string]any{"capabilities": map[string]any{}}}, probeLegacy, ""},
		{"DiscoverResult with supportedVersions null", map[string]any{"result": map[string]any{"supportedVersions": nil}}, probeLegacy, ""},
		{"DiscoverResult with a non-string version", answerDiscover("2026-07-28", 7.0), probeModern, ""},
		{"DiscoverResult with a version that is no date", answerDiscover("2026-07-28", "latest"), probeModern, ""},
		{"DiscoverResult with supportedVersions not a list", map[string]any{"result": map[string]any{"supportedVersions": "2026-07-28"}}, probeModern, ""},
		{"-32020", errMsg(-32020, nil), probeModern, ""},
		{"-32021", errMsg(-32021, nil), probeModern, ""},
		{"-32022 with its list", errMsg(-32022, map[string]any{"supported": []any{"2026-07-28"}}), probeModern, "2026-07-28"},
		{"-32022 with an invalid list", errMsg(-32022, map[string]any{"supported": []any{"2026-07-28", true}}), probeModern, ""},
		{"-32601", errMsg(-32601, nil), probeLegacy, ""},
		{"-32600", errMsg(-32600, nil), probeSoftLegacy, ""},
		{"-32602", errMsg(-32602, nil), probeSoftLegacy, ""},
		{"-32603", errMsg(-32603, nil), probeSoftLegacy, ""},
		{"-32000", errMsg(-32000, nil), probeSoftLegacy, ""},
		{"error without a code", map[string]any{"error": map[string]any{"message": "x"}}, probeSoftLegacy, ""},
		// Codes compare as numbers; a fractional or non-numeric one is unrecognised.
		{"-32601.0", map[string]any{"error": map[string]any{"code": -32601.0}}, probeLegacy, ""},
		{"-32022.0 with its list", map[string]any{"error": map[string]any{"code": -32022.0, "data": map[string]any{"supported": []any{"2026-07-28"}}}}, probeModern, "2026-07-28"},
		{"-32603.0", map[string]any{"error": map[string]any{"code": -32603.0}}, probeSoftLegacy, ""},
		{"-32601.5", map[string]any{"error": map[string]any{"code": -32601.5}}, probeSoftLegacy, ""},
		{"-32020.5", map[string]any{"error": map[string]any{"code": -32020.5}}, probeSoftLegacy, ""},
		{"code as a string", map[string]any{"error": map[string]any{"code": "-32601"}}, probeSoftLegacy, ""},
		{"neither result nor error", map[string]any{"jsonrpc": "2.0"}, probeSoftLegacy, ""},
	}
	for _, c := range cases {
		got, vers := classifyProbe(c.msg)
		if got != c.want || strings.Join(vers, ",") != c.vers {
			t.Errorf("%s: got %v %v, want %v %q", c.name, got, vers, c.want, c.vers)
		}
	}
}

// A probe answer and the flight's timer land on the same flight in either
// order, or at once: the answer is never lost, and the verdict is the child's.
func TestEraProbe_AnswerAndTimerOnTheSameFlight(t *testing.T) {
	b := startEraBridge(t, nil, "--era=modern")
	// A fresh process state with a written probe in flight; the write itself
	// is reported by hand.
	fresh := func() (*eraState, string) {
		st := b.newEraState(b.GetCurrentChild())
		b.mu.Lock()
		b.era = st
		b.mu.Unlock()
		id := startProbeByHand(t, b, st)
		b.mu.Lock()
		b.applyEraLocked(st, eraEvent{kind: evWriteDone, id: id, now: time.Now()})
		b.mu.Unlock()
		return st, id
	}
	settled := func(t *testing.T, st *eraState) {
		t.Helper()
		b.mu.Lock()
		defer b.mu.Unlock()
		m := st.m
		if m.verdict.kind != verdictModern || len(m.outstanding) != 0 || m.probe.phase != probeIdle || st.wait != nil {
			t.Fatalf("answer lost: verdict=%v outstanding=%q phase=%v wait=%v", m.verdict.kind, m.outstanding, m.probe.phase, st.wait != nil)
		}
	}
	t.Run("timer first", func(t *testing.T) {
		st, id := fresh()
		b.probeTimedOut(st, id)
		b.handleProbeResponse(st, id, answerDiscover(eraTestVersion))
		settled(t, st)
	})
	t.Run("answer first", func(t *testing.T) {
		st, id := fresh()
		b.handleProbeResponse(st, id, answerDiscover(eraTestVersion))
		b.probeTimedOut(st, id)
		settled(t, st)
	})
	t.Run("at once", func(t *testing.T) {
		for i := 0; i < 300; i++ {
			st, id := fresh()
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); b.probeTimedOut(st, id) }()
			go func() { defer wg.Done(); b.handleProbeResponse(st, id, answerDiscover(eraTestVersion)) }()
			wg.Wait()
			settled(t, st)
		}
	})
}

// A failed probe write must leave no flight and no unsent probe behind,
// whichever of the write's failure and the flight's timer comes first.
func TestEraProbe_FailedWriteLeavesNoUnsentFlight(t *testing.T) {
	dead := newEraChild(t, "--era=modern")
	_ = dead.Close()
	waitFor(t, "the child to exit", func() bool { return dead.HasExited() })
	b := startEraBridge(t, nil, "--era=modern")
	setup := func() (*eraState, string) {
		st := b.newEraState(dead)
		b.mu.Lock()
		b.era = st
		b.mu.Unlock()
		return st, startProbeByHand(t, b, st)
	}
	// Nothing is left. A write that fails while its timer runs closes the
	// re-probe gate for an interval, so a child that fails every write at once
	// is not probed once per request; one blocked through its timer does not
	// (no second probe could start while it was blocked).
	clean := func(t *testing.T, st *eraState, armed bool) {
		t.Helper()
		b.mu.Lock()
		defer b.mu.Unlock()
		m := st.m
		if m.probe.phase != probeIdle || st.wait != nil || m.verdict.kind != verdictNone || len(m.outstanding) != 0 || m.quietUntil.IsZero() == armed {
			t.Fatalf("phase=%v wait=%v verdict=%v outstanding=%q quietUntil=%v", m.probe.phase, st.wait != nil, m.verdict.kind, m.outstanding, m.quietUntil)
		}
	}
	t.Run("timer first", func(t *testing.T) {
		st, id := setup()
		b.probeTimedOut(st, id) // not written yet: parked as unsent
		b.writeProbe(st, id)
		clean(t, st, false)
	})
	t.Run("failure first", func(t *testing.T) {
		st, id := setup()
		b.writeProbe(st, id)
		clean(t, st, true)
		b.probeTimedOut(st, id)
		clean(t, st, true)
	})
}

// A request that stops waiting while the flight's timer is due settles the
// flight as a timeout itself, so the verdict it returns is the one cached; one
// that stops waiting early leaves the flight alone.
func TestEraProbe_ExpiredWaiterSettlesADueFlight(t *testing.T) {
	start := func(t *testing.T, due bool) (*Bridge, *eraState, string) {
		b := startEraBridge(t, nil, "--era=modern", "--discover-hold")
		_, st := b.currentChildEra()
		id := startProbeByHand(t, b, st)
		if due {
			b.mu.Lock()
			st.m.probe.deadline = time.Now().Add(-time.Second)
			b.mu.Unlock()
		}
		b.writeProbe(st, id)
		return b, st, id
	}
	t.Run("due", func(t *testing.T) {
		b, st, id := start(t, true)
		if v := b.discover(context.Background(), st, time.Now().Add(20*time.Millisecond)); v != answerLegacy {
			t.Fatalf("answer %v", v)
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		m := st.m
		if m.probe.phase != probeIdle || m.verdict.kind != verdictTimeout || len(m.outstanding) != 1 || m.outstanding[0] != id {
			t.Fatalf("phase=%v verdict=%v outstanding=%q", m.probe.phase, m.verdict.kind, m.outstanding)
		}
	})
	t.Run("not due", func(t *testing.T) {
		b, st, id := start(t, false)
		if v := b.discover(context.Background(), st, time.Now().Add(20*time.Millisecond)); v != answerLegacy {
			t.Fatalf("answer %v", v)
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		if st.m.probe.id != id || !st.m.probe.live() || st.m.verdict.kind != verdictNone {
			t.Fatalf("probe kept=%v verdict=%v", st.m.probe.id == id && st.m.probe.live(), st.m.verdict.kind)
		}
	})
}

// The request that started a probe leaves only once the flight has settled:
// what it was told and what the bridge cached are the same, with nothing left
// for a timer goroutine to do afterwards.
func TestEraProbe_VerdictIsCachedBeforeTheStartersAnswer(t *testing.T) {
	for i := 0; i < 8; i++ {
		b := startEraBridge(t, func(o *StartBridgeOptions) { o.ProbeTimeout = 50 * time.Millisecond }, "--era=modern", "--discover-hold")
		if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 400 {
			t.Fatalf("iteration %d: %d %s", i, r.status, r.body)
		}
		b.mu.Lock()
		m := b.era.m
		cached := m.verdict.kind == verdictTimeout && m.probe.phase == probeIdle && len(m.outstanding) == 1
		b.mu.Unlock()
		if !cached {
			t.Fatalf("iteration %d: the 400 went out before the flight settled", i)
		}
	}
}

// A modern request on a verdict that is only a timeout is answered from
// the cache at once, never waiting on a probe; a background probe starts only
// once a probe timeout has passed since the last one timed out, and its
// answer upgrades the verdict.
func TestEraProbe_ModernRequestOnTimeoutVerdictIsAnsweredAtOnce(t *testing.T) {
	b := startEraBridge(t, nil, "--era=modern", "--discover-hold") // the default 15s probe timeout
	res := make(chan eraResponse, 1)
	go func() { res <- eraPost(t, b, acceptJSON, modernCall(1), nil) }()
	timeOutProbeByHand(t, b)
	if r := <-res; r.status != 400 {
		t.Fatalf("timed-out probe: %d %s", r.status, r.body)
	}
	atOnce := func(what string, id int) {
		t.Helper()
		r := eraPost(t, b, acceptJSON, modernCall(id), nil)
		if r.status != 400 || !strings.Contains(r.body, `"code":-32600`) || r.elapsed > 5*time.Second {
			t.Fatalf("%s: %d after %s: %s", what, r.status, r.elapsed, r.body)
		}
	}

	atOnce("not due", 2)
	if probeInFlight(b) || discoverCount(t, b) != 1 {
		t.Fatal("a probe started although the last one timed out a moment ago")
	}

	makeReprobeDue(b)
	atOnce("due", 3)
	waitProbeInFlight(t, b)
	atOnce("with a probe in flight", 4)
	waitFor(t, "the child to receive the second probe", func() bool { return discoverCount(t, b) >= 2 })
	if n := discoverCount(t, b); n != 2 {
		t.Fatalf("child saw %d probes, want 2 (one in flight at a time)", n)
	}

	releaseHeld(t, b)
	if got := eraName(b); got != "modern" || verdictSourceOf(b) != srcProbe {
		t.Fatalf("the answers did not upgrade the verdict: %q %v", got, verdictSourceOf(b))
	}
	if r := eraPost(t, b, acceptJSON, modernCall(5), nil); r.status != 200 {
		t.Fatalf("after the upgrade: %d %s", r.status, r.body)
	}
	if got := outstandingProbes(b); len(got) != 0 {
		t.Fatalf("outstanding after the answers: %q", got)
	}
}

// A modern-looking initialize rejection on a verdict that is only a
// timeout starts a background probe at once, without waiting on it and
// without waiting out the interval: the child has only been silent.
func TestEraProbe_ModernLookingRejectionOnTimeoutVerdictStartsAProbe(t *testing.T) {
	b := startEraBridge(t, nil, "--era=modern", "--modern-bare-initialize-rejection", "--discover-hold")
	res := make(chan eraResponse, 1)
	go func() { res <- eraPost(t, b, acceptJSON, modernCall(1), nil) }()
	timeOutProbeByHand(t, b)
	<-res
	if probeInFlight(b) || verdictSourceOf(b) != srcTimeout {
		t.Fatalf("setup: flight=%v src=%v", probeInFlight(b), verdictSourceOf(b))
	}

	// Right after the timeout, well inside the (default 15s) interval.
	r := eraPost(t, b, acceptJSON, eraInitialize(2).body, nil)
	if r.status != 400 || !strings.Contains(r.body, `"code":-32601`) || strings.Contains(r.body, "only speaks") || r.elapsed > 5*time.Second {
		t.Fatalf("initialize: %d after %s: %s", r.status, r.elapsed, r.body)
	}
	if !probeInFlight(b) {
		t.Fatal("no background probe was started")
	}
	if verdictSourceOf(b) != srcTimeout || b.eraRefusesLegacy() {
		t.Fatalf("a bare rejection changed the verdict: src=%v", verdictSourceOf(b))
	}
}

// After the child's own legacy answer a rejected initialize never
// probes again.
func TestEraProbe_NoReprobeAfterExplicitLegacyAnswer(t *testing.T) {
	b := startEraBridge(t, nil, "--era=legacy", "--reject-initialize=2", "--reject-code=-32601")
	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 400 || verdictSourceOf(b) != srcProbe {
		t.Fatalf("setup: %d %s src=%v", r.status, r.body, verdictSourceOf(b))
	}
	// Not a matter of the re-probe interval: there is none to wait out.
	makeReprobeDue(b)
	for i := 2; i <= 3; i++ {
		if r := eraPost(t, b, acceptJSON, eraInitialize(i).body, nil); r.status != 400 || !strings.Contains(r.body, `"code":-32601`) {
			t.Fatalf("initialize: %d %s", r.status, r.body)
		}
		if probeInFlight(b) {
			t.Fatal("a probe started after an explicit legacy answer")
		}
	}
	if r := eraPost(t, b, acceptJSON, modernCall(4), nil); r.status != 400 {
		t.Fatalf("modern request: %d %s", r.status, r.body)
	}
	if n := discoverCount(t, b); n != 1 {
		t.Fatalf("child saw %d probes, want 1", n)
	}
}

// A bare -32601 or -32022 caches nothing. 2025 traffic and initialize keep
// being forwarded, there is no bridge refusal and no 405, and a modern request
// is served by the probe as usual.
func TestEraInitializeBareRejectionCachesNothing(t *testing.T) {
	for _, code := range []string{"-32601", "-32022"} {
		t.Run(code, func(t *testing.T) {
			b := startEraBridge(t, nil, "--era=modern", "--modern-bare-initialize-rejection", "--reject-code="+code, "--discover-hold")
			for i := 1; i <= 2; i++ {
				r := eraPost(t, b, acceptJSON, eraInitialize(i).body, nil)
				if r.status != 400 || !strings.Contains(r.body, `"code":`+code) || strings.Contains(r.body, "only speaks") {
					t.Fatalf("initialize %d: %d %s", i, r.status, r.body)
				}
			}
			if eraName(b) != "unknown" || b.eraRefusesLegacy() {
				t.Fatalf("cached a verdict: %q", eraName(b))
			}
			before := forwardedCount(t, b)
			ping := map[string]any{"jsonrpc": "2.0", "id": 5, "method": "ping"}
			if r := eraPost(t, b, acceptJSON, ping, nil); strings.Contains(r.body, "only speaks") {
				t.Fatalf("2025 request refused: %d %s", r.status, r.body)
			}
			if forwardedCount(t, b) != before+1 {
				t.Fatal("the 2025 request was not forwarded")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for _, method := range []string{http.MethodGet, http.MethodDelete} {
				req, _ := http.NewRequestWithContext(ctx, method, b.URL(), nil)
				req.Header.Set("Accept", "text/event-stream")
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
				if resp.StatusCode == 405 {
					t.Fatalf("%s answered 405 on an unproven verdict", method)
				}
			}

			res := make(chan eraResponse, 1)
			go func() { res <- eraPost(t, b, acceptJSON, modernCall(9), nil) }()
			waitFor(t, "the child to receive the probe", func() bool { return discoverCount(t, b) >= 1 })
			releaseDirect(t, b)
			if r := <-res; r.status != 200 {
				t.Fatalf("modern request: %d %s", r.status, r.body)
			}
			if !b.eraRefusesLegacy() || verdictSourceOf(b) != srcProbe {
				t.Fatalf("the probe did not settle the verdict: src=%v", verdictSourceOf(b))
			}
		})
	}
}

// A data.supported listing only 2026-era versions caches modern-only with
// exactly those versions, whatever the code; a list naming a 2025 version too,
// or none, caches nothing. Evidence from a process that already accepted an
// initialize is ignored.
func TestEraInitializeRejectionVersionEvidence(t *testing.T) {
	t.Run("modern list is cached verbatim", func(t *testing.T) {
		b := startEraBridge(t, nil, "--era=modern", "--discover-hold")
		rec := completeRejected(b, initRejection(-32601, "2026-07-28", "2027-01-01"))
		if rec.Code != 400 || !strings.Contains(rec.Body.String(), `"message":"nope"`) {
			t.Fatalf("%d %s", rec.Code, rec.Body.String())
		}
		if !b.eraRefusesLegacy() || verdictSourceOf(b) != srcRejection {
			t.Fatalf("not cached: src=%v", verdictSourceOf(b))
		}
		r := eraPost(t, b, acceptJSON, map[string]any{"jsonrpc": "2.0", "id": 2, "method": "ping"}, nil)
		if r.status != 400 || !strings.Contains(r.body, `"supported":["2026-07-28","2027-01-01"]`) || strings.Contains(r.body, `"supported":["2026-07-28"]`) {
			t.Fatalf("refusal: %d %s", r.status, r.body)
		}
		for _, method := range []string{http.MethodGet, http.MethodDelete} {
			req, _ := http.NewRequest(method, b.URL(), nil)
			req.Header.Set("Accept", "text/event-stream")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != 405 {
				t.Fatalf("%s: %d, want 405", method, resp.StatusCode)
			}
		}
	})
	t.Run("a legacy version in the list", func(t *testing.T) {
		b := startEraBridge(t, nil, "--era=modern", "--discover-hold")
		completeRejected(b, initRejection(-32601, "2026-07-28", "2025-11-25"))
		completeRejected(b, initRejection(-32602, "2025-11-25"))
		if eraName(b) != "unknown" {
			t.Fatalf("cached a verdict: %q", eraName(b))
		}
	})
	t.Run("ignored once an initialize was accepted", func(t *testing.T) {
		b := startEraBridge(t, nil, "--era=legacy")
		if r := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil); r.status != 200 {
			t.Fatalf("initialize: %d %s", r.status, r.body)
		}
		completeRejected(b, initRejection(-32601, "2026-07-28"))
		if b.eraRefusesLegacy() || verdictSourceOf(b) == srcRejection {
			t.Fatalf("evidence after an accepted initialize was used: src=%v", verdictSourceOf(b))
		}
		if r := eraPost(t, b, acceptJSON, eraInitialize(2).body, nil); r.status != 200 || r.header.Get("Mcp-Session-Id") == "" {
			t.Fatalf("initialize after the ignored evidence: %d %s", r.status, r.body)
		}
	})
}

// A legacy child that says nothing before initialize and rejects one with
// -32601 is not locked out: nothing is cached, and the next initialize works.
func TestEraSilentLegacyChildRejectingOneInitializeIsNotLockedOut(t *testing.T) {
	b := startEraBridge(t, shortProbe, "--era=legacy-silent", "--reject-initialize=1", "--reject-code=-32601")
	r := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil)
	if r.status != 400 || !strings.Contains(r.body, `"code":-32601`) {
		t.Fatalf("initialize: %d %s", r.status, r.body)
	}
	waitFor(t, "the silent child's probe to time out", func() bool {
		timedOut, _ := probeTimedOutVerdict(b)
		return timedOut
	})
	if b.eraRefusesLegacy() {
		t.Fatal("locked out")
	}
	if good := eraPost(t, b, acceptJSON, eraInitialize(2).body, nil); good.status != 200 || good.header.Get("Mcp-Session-Id") == "" {
		t.Fatalf("second initialize: %d %v %s", good.status, good.header, good.body)
	}
}

// A modern-only child that discards its first probe during boot and rejects
// initialize with a bare -32601 reads as modern once a re-probe is answered.
func TestEraProbe_ModernChildDiscardingFirstProbeIsFoundByReprobe(t *testing.T) {
	b := startEraBridge(t, shortProbe, "--era=modern", "--modern-bare-initialize-rejection", "--discover-drop=1")
	if r := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil); r.status != 400 || !strings.Contains(r.body, `"code":-32601`) {
		t.Fatalf("first initialize: %d %s", r.status, r.body)
	}
	waitFor(t, "the discarded probe to time out", func() bool {
		timedOut, _ := probeTimedOutVerdict(b)
		return timedOut
	})
	// Still forwarded: a timeout is no proof; this rejection starts the
	// re-probe at once, the child having only been silent.
	if r := eraPost(t, b, acceptJSON, eraInitialize(2).body, nil); r.status != 400 || !strings.Contains(r.body, `"code":-32601`) || strings.Contains(r.body, "only speaks") {
		t.Fatalf("second initialize: %d %s", r.status, r.body)
	}
	waitFor(t, "the re-probe's answer", func() bool { return eraName(b) == "modern" })
	if !b.eraRefusesLegacy() || verdictSourceOf(b) != srcProbe {
		t.Fatalf("refuses2025=%v src=%v", b.eraRefusesLegacy(), verdictSourceOf(b))
	}
	if r := eraPost(t, b, acceptJSON, eraInitialize(3).body, nil); r.status != 400 || !strings.Contains(r.body, `"supported":["2026-07-28"]`) || !strings.Contains(r.body, "only speaks") {
		t.Fatalf("initialize once known: %d %s", r.status, r.body)
	}
	if r := eraPost(t, b, acceptJSON, modernCall(4), nil); r.status != 200 {
		t.Fatalf("modern call: %d %s", r.status, r.body)
	}
	if n := discoverCount(t, b); n != 2 {
		t.Fatalf("child saw %d probes, want 2", n)
	}
}

// The outstanding ids of timed-out probes are kept in a small capped set,
// oldest dropped, and a late answer on any id still in it upgrades the
// verdict, once.
func TestEraProbe_OutstandingProbeIDsAreCappedAndAnyLateAnswerUpgrades(t *testing.T) {
	b := startEraBridge(t, nil, "--era=modern", "--discover-hold")
	b.mu.Lock()
	st := b.era
	b.mu.Unlock()

	const probes = maxOutstandingProbes + 2
	res := make(chan eraResponse, 1)
	go func() { res <- eraPost(t, b, acceptJSON, modernCall(1), nil) }()
	for i := 1; i <= probes; i++ {
		if i > 1 {
			makeReprobeDue(b)
			if r := eraPost(t, b, acceptJSON, modernCall(i), nil); r.status != 400 {
				t.Fatalf("request %d: %d %s", i, r.status, r.body)
			}
		}
		timeOutProbeByHand(t, b)
	}
	if r := <-res; r.status != 400 {
		t.Fatalf("first request: %d %s", r.status, r.body)
	}

	var want []string
	for i := probes - maxOutstandingProbes + 1; i <= probes; i++ {
		want = append(want, fmt.Sprintf("%s%d", probeIDPrefix, i))
	}
	if got := outstandingProbes(b); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("outstanding %q, want %q", got, want)
	}
	if timedOut, _ := probeTimedOutVerdict(b); !timedOut {
		t.Fatal("the verdict should still be the timeout")
	}

	// The oldest was dropped: its answer is ignored.
	b.handleProbeResponse(st, probeIDPrefix+"1", answerDiscover(eraTestVersion))
	if timedOut, _ := probeTimedOutVerdict(b); !timedOut {
		t.Fatal("an answer to a dropped id changed the verdict")
	}
	// A kept id that is not the newest still upgrades it, once.
	b.handleProbeResponse(st, want[0], answerDiscover(eraTestVersion))
	if eraName(b) != "modern" || verdictSourceOf(b) != srcProbe || len(outstandingProbes(b)) != 0 {
		t.Fatalf("era=%q src=%v outstanding=%q", eraName(b), verdictSourceOf(b), outstandingProbes(b))
	}
	b.handleProbeResponse(st, want[len(want)-1], map[string]any{"error": map[string]any{"code": float64(-32601)}})
	if eraName(b) != "modern" {
		t.Fatalf("a second late answer overwrote the verdict: %q", eraName(b))
	}
}

func TestIsModernShaped(t *testing.T) {
	cases := []struct {
		name string
		msg  map[string]any
		want bool
	}{
		{"modern", modernCall(1), true},
		{"version key with empty value", map[string]any{"method": "x", "params": map[string]any{"_meta": map[string]any{protocolVersionMetaKey: ""}}}, true},
		{"progress token only", map[string]any{"method": "x", "params": map[string]any{"_meta": map[string]any{"progressToken": "t"}}}, false},
		{"no meta", map[string]any{"method": "x", "params": map[string]any{}}, false},
		{"no params", map[string]any{"method": "x"}, false},
		{"meta not an object", map[string]any{"method": "x", "params": map[string]any{"_meta": "v"}}, false},
	}
	for _, c := range cases {
		if got := isModernShaped(c.msg); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
