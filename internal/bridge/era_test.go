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
	"strings"
	"sync"
	"testing"
	"time"
)

// Version behaviour beyond the matrix: waiting for the verdict, head holding,
// sessions, and the races around them. They run against fakemcp --era=<mode>;
// its debug/* methods answer under every mode and shape, so a test can poke a
// child in whatever shape the bridge currently takes.

const eraTestVersion = "2026-07-28"

// startEraBridge starts a bridge and waits for its process's verdict.
func startEraBridge(t *testing.T, mutate func(*StartBridgeOptions), args ...string) *Bridge {
	t.Helper()
	b := startEraBridgeCold(t, mutate, args...)
	waitForVerdict(t, b)
	return b
}

// startEraBridgeCold is startEraBridge without waiting for the verdict.
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

func waitForVerdict(t *testing.T, b *Bridge) {
	t.Helper()
	waitFor(t, "the verdict", func() bool { return versionOf(b) != versionUnknown })
}

func versionOf(b *Bridge) mcpVersion {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.era.m.version()
}

func phaseOf(b *Bridge) eraPhase {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.era.m.phase
}

func v2VersionsOf(b *Bridge) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.era.m.versions...)
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

// reachCtx is a request context that closes reached on the first call to
// Done. With no EnsureChild set, nothing on a request's path asks for Done
// before its wait for the verdict, so reached means the handler is waiting.
type reachCtx struct {
	context.Context
	once    sync.Once
	reached chan struct{}
}

func (c *reachCtx) Done() <-chan struct{} {
	c.once.Do(func() { close(c.reached) })
	return c.Context.Done()
}

// waiter is a request run straight through b.handle on its own goroutine.
type waiter struct {
	rec  *httptest.ResponseRecorder
	done chan struct{} // closed when the handler has returned
}

// startWaiting runs req through b.handle and returns once the handler waits
// for the verdict.
func startWaiting(t *testing.T, b *Bridge, req *http.Request) *waiter {
	t.Helper()
	ctx := &reachCtx{Context: req.Context(), reached: make(chan struct{})}
	w := &waiter{rec: httptest.NewRecorder(), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		b.handle(w.rec, req.WithContext(ctx))
	}()
	select {
	case <-ctx.reached:
	case <-w.done:
		t.Fatalf("answered without waiting for the verdict: %d %s", w.rec.Code, w.rec.Body)
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the request to wait for the verdict")
	}
	return w
}

// answered reports whether the handler has returned.
func (w *waiter) answered() bool {
	select {
	case <-w.done:
		return true
	default:
		return false
	}
}

// response waits for the handler to return and gives what it wrote.
func (w *waiter) response(t *testing.T) *httptest.ResponseRecorder {
	t.Helper()
	select {
	case <-w.done:
	case <-time.After(20 * time.Second):
		t.Fatal("the handler did not return")
	}
	return w.rec
}

// waitingPost is startWaiting for a POST built as eraSend builds it.
func waitingPost(t *testing.T, b *Bridge, ctx context.Context, accept string, body map[string]any) *waiter {
	t.Helper()
	req, err := eraRequest(ctx, b.URL(), accept, body, nil)
	if err != nil {
		t.Fatal(err)
	}
	return startWaiting(t, b, req)
}

func shortProbe(o *StartBridgeOptions) { o.ProbeTimeout = 200 * time.Millisecond }

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
	req, err := eraRequest(ctx, url, accept, body, hdr)
	if err != nil {
		return nil, 0, err
	}
	start := time.Now()
	tr := &http.Transport{DisableKeepAlives: true}
	defer tr.CloseIdleConnections()
	resp, err := (&http.Client{Transport: tr}).Do(req)
	return resp, time.Since(start), err
}

// eraRequest builds the POST eraSend sends.
func eraRequest(ctx context.Context, url, accept string, body map[string]any, hdr map[string]string) (*http.Request, error) {
	raw, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return nil, err
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
	return req, nil
}

func eraPost(t *testing.T, b *Bridge, accept string, body map[string]any, hdr map[string]string) eraResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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
// the bridge will currently take: modern-shaped on a v2 process, otherwise
// 2025-style, carrying the session id if one exists. It returns the answer's
// result.
func debugCall(t *testing.T, b *Bridge, method string, params map[string]any) map[string]any {
	t.Helper()
	var body map[string]any
	hdr := map[string]string{}
	if versionOf(b) == versionV2 {
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

// forwardedCount is how many requests (other than discover and debug calls)
// the child has been sent.
func forwardedCount(t *testing.T, b *Bridge) int { return debugCount(t, b, "debug/requestCount") }

// releaseHeld makes the fake send every answer it is holding; they are on
// the wire, and so handled by the bridge, before this returns.
func releaseHeld(t *testing.T, b *Bridge) {
	t.Helper()
	debugCall(t, b, "debug/release", nil)
}

// releaseDirect makes the fake send its held answers without an HTTP round
// trip, which a bridge still waiting for its verdict could not give.
func releaseDirect(t *testing.T, b *Bridge) {
	t.Helper()
	if err := b.GetCurrentChild().Send(map[string]any{"jsonrpc": "2.0", "id": 9001, "method": "debug/release"}); err != nil {
		t.Fatal(err)
	}
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

func sessionMinted(b *Bridge) bool {
	b.session.mu.Lock()
	defer b.session.mu.Unlock()
	return b.session.hasID
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

func (l *logCapture) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func (l *logCapture) count(sub string) int {
	return strings.Count(l.text(), sub)
}

func (l *logCapture) logger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(l, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// logged is how often line went to the log as a message.
func (l *logCapture) logged(line string) int {
	quoted, _ := json.Marshal(line)
	return l.count(`"msg":` + string(quoted))
}

// loggedInOrder reports whether each line went to the log exactly once, in
// the order given.
func (l *logCapture) loggedInOrder(lines ...string) bool {
	all := l.text()
	at := -1
	for _, line := range lines {
		quoted, _ := json.Marshal(line)
		msg := `"msg":` + string(quoted)
		i := strings.Index(all, msg)
		if strings.Count(all, msg) != 1 || i < at {
			return false
		}
		at = i
	}
	return true
}

// Lines are written once Bridge.mu is released, so a test that has seen the
// state change behind a line waits for the line too. A count read after a
// later line of the same process, or after Close (which writes out what the
// process still has queued), is final.

// waitLogged waits until line has gone to the log n times.
func (l *logCapture) waitLogged(t *testing.T, line string, n int) {
	t.Helper()
	l.waitLog(t, fmt.Sprintf("%q to be logged %d times", line, n), func() bool { return l.logged(line) >= n })
}

// waitInOrder waits until loggedInOrder(lines...) holds.
func (l *logCapture) waitInOrder(t *testing.T, lines ...string) {
	t.Helper()
	l.waitLog(t, fmt.Sprintf("%q to be logged in order", lines), func() bool { return l.loggedInOrder(lines...) })
}

func (l *logCapture) waitLog(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s; log:\n%s", what, l.text())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// closeAndCount closes b, so that every line its process queued is written,
// and returns how often line went to the log.
func (l *logCapture) closeAndCount(b *Bridge, line string) int {
	_ = b.Close()
	return l.logged(line)
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

// getOrDelete sends a GET (as an SSE stream request) or a DELETE and returns
// the status, Allow header and body; a 200 stream is not read.
func getOrDelete(t *testing.T, b *Bridge, method string, hdr map[string]string) (int, string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, method, b.URL(), nil)
	req.Header.Set("Accept", "text/event-stream")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	tr := &http.Transport{DisableKeepAlives: true}
	defer tr.CloseIdleConnections()
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		return resp.StatusCode, resp.Header.Get("Allow"), "(stream)"
	}
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header.Get("Allow"), string(body)
}

func TestEra_VerdictPerChild(t *testing.T) {
	for _, c := range []struct {
		args  []string
		phase eraPhase
	}{
		{[]string{"--era=modern"}, phaseV2},
		{[]string{"--era=legacy"}, phaseV1},
		{[]string{"--era=modern", "--discover-error=-32603"}, phaseV1},
		{[]string{"--era=modern", "--discover-no-versions"}, phaseV1},
		{nil, phaseV1},
	} {
		b := startEraBridge(t, nil, c.args...)
		if got := phaseOf(b); got != c.phase {
			t.Errorf("%v: phase %s, want %s", c.args, describe(mach(got)), describe(mach(c.phase)))
		}
	}
	b := startEraBridge(t, shortProbe, "--era=legacy-silent")
	if got := phaseOf(b); got != phaseTimeout {
		t.Errorf("legacy-silent: phase %s, want timeout", describe(mach(got)))
	}
	if got := v2VersionsOf(startEraBridge(t, nil, "--era=modern")); strings.Join(got, ",") != eraTestVersion {
		t.Errorf("modern versions %q", got)
	}
}

// The probe is the child's first stdin line, written at spawn, and the only
// one: requests of either era wait for its answer rather than starting one.
func TestEra_RequestsWaitForTheVerdict(t *testing.T) {
	b := startEraBridgeCold(t, nil, "--era=modern", "--discover-hold")
	const n = 4
	var modern, legacy []*waiter
	for i := 0; i < n; i++ {
		modern = append(modern, waitingPost(t, b, context.Background(), acceptSSE, modernCall(i)))
		legacy = append(legacy, waitingPost(t, b, context.Background(), acceptJSON, map[string]any{"jsonrpc": "2.0", "id": i, "method": "ping"}))
	}
	for _, w := range append(append([]*waiter(nil), modern...), legacy...) {
		if w.answered() {
			t.Fatalf("answered before the verdict: %d %s", w.rec.Code, w.rec.Body)
		}
	}
	releaseDirect(t, b)
	for _, w := range modern {
		if r := w.response(t); r.Code != 200 {
			t.Errorf("modern request: %d %s", r.Code, r.Body)
		}
	}
	for _, w := range legacy {
		if r := w.response(t); r.Code != 400 || !strings.Contains(r.Body.String(), "does not accept older MCP v1 requests") {
			t.Errorf("2025 request: %d %s", r.Code, r.Body)
		}
	}
	if got := discoverCount(t, b); got != 1 {
		t.Fatalf("child saw %d discover requests, want 1", got)
	}
	got := childReceived(t, b)
	if len(got) == 0 || got[0] != "server/discover" {
		t.Fatalf("the probe is not the child's first line: %q", got)
	}
}

// A 2025 initialize and a 2026 request that both wait for the verdict each
// get that verdict's answer.
func TestEra_InitializeAndModernRequestWaitingGetTheVerdictsAnswer(t *testing.T) {
	// waitBoth starts both requests and returns once both wait for the verdict.
	waitBoth := func(t *testing.T, b *Bridge) (initialize, modern *waiter) {
		t.Helper()
		initialize = waitingPost(t, b, context.Background(), acceptJSON, eraInitialize(1).body)
		modern = waitingPost(t, b, context.Background(), acceptJSON, modernCall(2))
		if got := versionOf(b); got != versionUnknown {
			t.Fatalf("setup: the verdict came before both requests waited: version %d", got)
		}
		return initialize, modern
	}
	t.Run("v2", func(t *testing.T) {
		b := startEraBridgeCold(t, nil, "--era=modern", "--discover-hold")
		initialize, modern := waitBoth(t, b)
		releaseDirect(t, b)
		r := initialize.response(t)
		var m struct {
			Error struct {
				Code int `json:"code"`
				Data struct {
					Supported []string `json:"supported"`
					Requested string   `json:"requested"`
				} `json:"data"`
			} `json:"error"`
		}
		if err := json.Unmarshal(r.Body.Bytes(), &m); err != nil || r.Code != 400 || m.Error.Code != -32022 ||
			strings.Join(m.Error.Data.Supported, ",") != eraTestVersion || m.Error.Data.Requested != "2025-11-25" {
			t.Errorf("initialize: %d %s", r.Code, r.Body)
		}
		if r := modern.response(t); r.Code != 200 {
			t.Errorf("2026 request: %d %s", r.Code, r.Body)
		}
	})
	t.Run("v1", func(t *testing.T) {
		b := startEraBridgeCold(t, nil, "--era=legacy", "--discover-delay-ms=3000")
		initialize, modern := waitBoth(t, b)
		if r := initialize.response(t); r.Code != 200 || r.Header().Get("Mcp-Session-Id") == "" {
			t.Errorf("initialize: %d, session %q: %s", r.Code, r.Header().Get("Mcp-Session-Id"), r.Body)
		}
		r := modern.response(t)
		var m struct {
			Error map[string]any `json:"error"`
		}
		err := json.Unmarshal(r.Body.Bytes(), &m)
		if _, hasData := m.Error["data"]; err != nil || r.Code != 400 || m.Error["code"] != -32600.0 ||
			m.Error["message"] != v1Fallback || hasData {
			t.Errorf("2026 request: %d %s", r.Code, r.Body)
		}
	})
}

func TestEraProbe_DefaultTimeoutFitsTheRelaysHeaderTimeout(t *testing.T) {
	if defaultProbeTimeout != 15*time.Second {
		t.Fatalf("defaultProbeTimeout = %s", defaultProbeTimeout)
	}
	// ensureChild (when a respawn is needed), the verdict, then the head
	// hold. A retry after a respawn shares the verdict wait's one deadline
	// (admit), so it adds nothing: 5s + 15s + 2s.
	if total := ensureChildTimeout + defaultProbeTimeout + defaultHeadHold; total >= 30*time.Second {
		t.Fatalf("%s leaves no room under the relay's 30s response-header timeout", total)
	}
	// A 2025 initialize's hold counts from its arrival, so ensureChild and
	// the verdict are inside it; if they outlast it the head goes out as soon
	// as they are done.
	if total := max(defaultInitializeHold, ensureChildTimeout+defaultProbeTimeout); total >= 30*time.Second {
		t.Fatalf("an initialize head after %s leaves no room under the relay's 30s", total)
	}
	if b := startTestBridge(t); b.probeTimeout != defaultProbeTimeout {
		t.Fatalf("bridge probe timeout %s", b.probeTimeout)
	}
	if b := startEraBridge(t, shortProbe, "--era=modern"); b.probeTimeout != 200*time.Millisecond {
		t.Fatalf("override ignored: %s", b.probeTimeout)
	}
}

// The hold of a 2025 initialize counts from the client's arrival: time spent
// waiting for the verdict is taken off it, not added to it.
func TestEra_InitializeHoldCountsFromArrival(t *testing.T) {
	const verdictAfter, hold = time.Second, 3 * time.Second
	b := startEraBridgeCold(t, func(o *StartBridgeOptions) { o.InitializeHoldTimeout = hold },
		"--era=legacy", "--discover-delay-ms=1000", "--initialize-hold")
	start := time.Now()
	resp := sendHeadOnly(t, b, acceptSSE, eraInitialize(1).body)
	headAt := time.Since(start)
	if resp.StatusCode != 200 || resp.Header.Get("Mcp-Session-Id") == "" {
		t.Fatalf("head: %d, session %q", resp.StatusCode, resp.Header.Get("Mcp-Session-Id"))
	}
	// Counted from arrival the head goes out at about hold, counted from the
	// verdict at about verdictAfter+hold: the bound is the midpoint.
	if headAt < verdictAfter || headAt >= hold+verdictAfter/2 {
		t.Fatalf("head after %s, want within the %s hold from arrival (the verdict took %s)", headAt, hold, verdictAfter)
	}
}

// A request whose wait ends in a ReplaceChild retries against the new
// process, within the same one probe timeout, and is "not running" if the
// new process has no verdict by then.
func TestEra_RetryAfterReplaceChildSharesOneDeadline(t *testing.T) {
	const probe = 2 * time.Second
	b := startEraBridgeCold(t, func(o *StartBridgeOptions) { o.ProbeTimeout = probe }, "--era=modern", "--discover-hold")
	repl := newEraChild(t, "--era=modern", "--discover-hold")

	res := make(chan eraResponse, 1)
	go func() { res <- eraPost(t, b, acceptJSON, modernCall(1), nil) }()
	time.Sleep(probe / 2)
	b.ReplaceChild(repl)

	r := <-res
	if r.status != 502 || !strings.Contains(r.body, "not running") {
		t.Fatalf("a retry that runs out of time: %d %s", r.status, r.body)
	}
	if r.elapsed < probe*3/4 || r.elapsed > probe+probe/4 {
		t.Fatalf("waited %s in total, want about one probe timeout (%s)", r.elapsed, probe)
	}
}

// A respawn that lands while a request waits sends it to the new process,
// and the old process's answer, when it comes, does not touch the new one.
func TestEra_ReplaceChildRetargetsWaitersAndDropsStaleAnswer(t *testing.T) {
	logs := &logCapture{}
	b := startEraBridgeCold(t, func(o *StartBridgeOptions) { o.Log = logs.logger() }, "--era=modern", "--discover-hold")
	old := b.GetCurrentChild()
	t.Cleanup(func() { _ = old.Close() })

	res := make(chan eraResponse, 1)
	go func() { res <- eraPost(t, b, acceptJSON, modernCall(1), nil) }()
	time.Sleep(100 * time.Millisecond)
	b.ReplaceChild(newEraChild(t, "--era=legacy"))
	if r := <-res; r.status != 400 || !strings.Contains(r.body, v1Fallback) {
		t.Fatalf("waiter on a replaced child should get the new (v1) child's answer: %d %s", r.status, r.body)
	}

	ignoredBefore := logs.count("era event ignored")
	if err := old.Send(map[string]any{"jsonrpc": "2.0", "id": 9001, "method": "debug/release"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the stale probe answer to be dropped", func() bool { return logs.count("era event ignored") > ignoredBefore })
	if got := phaseOf(b); got != phaseV1 {
		t.Fatalf("stale probe answer reached the new process: %s", describe(mach(got)))
	}
}

func TestEra_RespawnGetsAFreshVerdict(t *testing.T) {
	b := startEraBridge(t, nil, "--era=legacy")
	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 400 {
		t.Fatalf("v1 child: %d %s", r.status, r.body)
	}
	b.ReplaceChild(newEraChild(t, "--era=modern"))
	if r := eraPost(t, b, acceptJSON, modernCall(2), nil); r.status != 200 {
		t.Fatalf("v2 replacement: %d %s", r.status, r.body)
	}
	if got := versionOf(b); got != versionV2 {
		t.Fatalf("version %d", got)
	}
}

func TestEra_ChildExitReleasesWaiters(t *testing.T) {
	b := startEraBridgeCold(t, nil, "--era=modern", "--discover-hold")
	res := make(chan eraResponse, 1)
	go func() { res <- eraPost(t, b, acceptSSE, modernCall(1), nil) }()
	time.Sleep(100 * time.Millisecond)
	_ = b.GetCurrentChild().Close()
	if r := <-res; r.status != 502 {
		t.Fatalf("waiter on an exited child: %d %s", r.status, r.body)
	}
}

// A waiter whose client leaves goes; the verdict still lands for the next.
func TestEra_ClientDisconnectWhileWaiting(t *testing.T) {
	b := startEraBridgeCold(t, nil, "--era=modern", "--discover-hold")
	// The handler's return is seen directly; the client leaving is its
	// request's context ending.
	ctx, cancel := context.WithCancel(context.Background())
	w := waitingPost(t, b, ctx, acceptSSE, modernCall(1))
	cancel()
	if rec := w.response(t); rec.Body.Len() != 0 {
		t.Fatalf("the handler answered a client that had left: %d %s", rec.Code, rec.Body)
	}
	releaseDirect(t, b)
	waitForVerdict(t, b)
	if r := eraPost(t, b, acceptJSON, modernCall(2), nil); r.status != 200 {
		t.Fatalf("after the verdict: %d %s", r.status, r.body)
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
	if n := discoverCount(t, b); n != 1 {
		t.Errorf("discover count %d", n)
	}
}

func TestEraV1ChildFallbackWinsOverBadHeaders(t *testing.T) {
	b := startEraBridge(t, nil, "--era=legacy")
	r := eraPost(t, b, acceptJSON, modernCall(1), map[string]string{"MCP-Protocol-Version": "1900-01-01", "Mcp-Method": "nope", "Mcp-Name": ""})
	if r.status != 400 || !strings.Contains(r.body, v1Fallback) || strings.Contains(r.body, "-32020") {
		t.Fatalf("a v1 child must answer with the fallback whatever the headers: %d %s", r.status, r.body)
	}
	if n := forwardedCount(t, b); n != 0 {
		t.Fatalf("the refused request reached the v1 child (%d)", n)
	}
}

// The plain fake answers discover as an unknown method, so it is v1.
func TestEraDefaultFakeIsV1(t *testing.T) {
	b := startTestBridge(t)
	r := eraPost(t, b, acceptJSON, modernBody(1, "ping", nil, nil), nil)
	if r.status != 400 || !strings.Contains(r.body, v1Fallback) {
		t.Fatalf("modern-shaped ping on the default fake: %d %s", r.status, r.body)
	}
	if got := phaseOf(b); got != phaseV1 {
		t.Fatalf("phase %s", describe(mach(got)))
	}
}

func TestEraV1ChildRejectingOneInitializeIsNotLockedOut(t *testing.T) {
	for _, accept := range []string{acceptJSON, acceptSSE} {
		b := startEraBridge(t, nil, "--era=legacy", "--reject-initialize=1")
		bad := eraPost(t, b, accept, eraInitialize(1).body, nil)
		if bad.status != 400 || !strings.Contains(bad.body, "bad initialize") {
			t.Fatalf("%s: want the child's own error as a 400, got %d %s", accept, bad.status, bad.body)
		}
		if bad.header.Get("Mcp-Session-Id") != "" || sessionMinted(b) {
			t.Fatalf("%s: session from a rejected initialize", accept)
		}
		good := eraPost(t, b, accept, eraInitialize(2).body, nil)
		if good.status != 200 || good.header.Get("Mcp-Session-Id") == "" {
			t.Fatalf("%s: good initialize after the bad one: %d %v", accept, good.status, good.header)
		}
	}
}

func TestEra_GetAndDeleteAre405OnV2(t *testing.T) {
	b := startEraBridge(t, nil, "--era=modern")
	want := "This server speaks MCP v2 (protocol 2026-07-28), which has no sessions and no GET stream."
	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		status, allow, body := getOrDelete(t, b, method, map[string]string{"Mcp-Session-Id": "from-before-a-restart"})
		if status != 405 || allow != "POST" || body != want {
			t.Errorf("%s: %d, Allow %q, %q", method, status, allow, body)
		}
	}
	// The version refusal comes before the GET's own Accept check.
	resp, err := http.Get(b.URL())
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 405 || resp.Header.Get("Allow") != "POST" || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/plain") || string(body) != want {
		t.Errorf("GET without Accept: %d, Allow %q, %q %q", resp.StatusCode, resp.Header.Get("Allow"), resp.Header.Get("Content-Type"), body)
	}
}

// GET and DELETE wait for the verdict like a POST: one whose process ends
// while it waits is "not running", not served as v1.
func TestEra_GetAndDeleteWhoseProcessEndsUndecidedAreNotRunning(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		b := startEraBridgeCold(t, nil, "--era=modern", "--discover-hold")
		req, _ := http.NewRequest(method, b.URL(), nil)
		req.Header.Set("Accept", "text/event-stream")
		w := startWaiting(t, b, req)
		_ = b.GetCurrentChild().Close()
		if r := w.response(t); r.Code != 502 || !strings.Contains(r.Body.String(), "not running") {
			t.Errorf("%s: %d %s", method, r.Code, r.Body)
		}
	}
}

// A process that has already ended answers nothing for its version: GET and
// DELETE take their session path, and a 2025 POST meets the session gate
// before "not running".
func TestEra_DeadChildKeepsTheSessionPath(t *testing.T) {
	b := startEraBridge(t, nil, "--era=legacy")
	sid := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil).header.Get("Mcp-Session-Id")
	_ = b.GetCurrentChild().Close()
	waitFor(t, "the process to end", func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.era.m.ended
	})
	if status, _, body := getOrDelete(t, b, http.MethodGet, map[string]string{"Mcp-Session-Id": "unknown"}); status != 404 {
		t.Errorf("GET with an unknown session: %d %s", status, body)
	}
	if status, _, body := getOrDelete(t, b, http.MethodGet, map[string]string{"Mcp-Session-Id": sid}); status != 200 || body != "(stream)" {
		t.Errorf("GET: %d %s", status, body)
	}
	ping := map[string]any{"jsonrpc": "2.0", "id": 2, "method": "ping"}
	if r := eraPost(t, b, acceptJSON, ping, map[string]string{"Mcp-Session-Id": "unknown"}); r.status != 404 {
		t.Errorf("POST with an unknown session: %d %s", r.status, r.body)
	}
	if r := eraPost(t, b, acceptJSON, ping, nil); r.status != 400 || !strings.Contains(r.body, "Mcp-Session-Id header is required") {
		t.Errorf("POST without a session: %d %s", r.status, r.body)
	}
	if r := eraPost(t, b, acceptJSON, ping, map[string]string{"Mcp-Session-Id": sid}); r.status != 502 || !strings.Contains(r.body, "not running") {
		t.Errorf("POST with the session: %d %s", r.status, r.body)
	}
	if status, _, body := getOrDelete(t, b, http.MethodDelete, map[string]string{"Mcp-Session-Id": "unknown"}); status != 404 {
		t.Errorf("DELETE with an unknown session: %d %s", status, body)
	}
	if status, _, body := getOrDelete(t, b, http.MethodDelete, map[string]string{"Mcp-Session-Id": sid}); status != 204 {
		t.Errorf("DELETE: %d %s", status, body)
	}
}

// A failed service answers everything with its named 502 but DELETE, which
// still ends the session.
func TestEra_DeleteOnAFailedServiceEndsTheSession(t *testing.T) {
	b := startEraBridge(t, nil, "--era=legacy")
	sid := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil).header.Get("Mcp-Session-Id")
	_ = b.GetCurrentChild().Close()
	waitFor(t, "the process to end", func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.era.m.ended
	})
	b.MarkFailed("gave up")
	hdr := map[string]string{"Mcp-Session-Id": sid}
	if status, _, body := getOrDelete(t, b, http.MethodGet, hdr); status != 502 || !strings.Contains(body, "local server 'fake-era' is not running") {
		t.Errorf("GET: %d %s", status, body)
	}
	if status, _, body := getOrDelete(t, b, http.MethodDelete, hdr); status != 204 {
		t.Errorf("DELETE: %d %s", status, body)
	}
}

func TestEra_GetAndDeleteServeAV1Child(t *testing.T) {
	b := startEraBridge(t, nil, "--era=legacy")
	sid := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil).header.Get("Mcp-Session-Id")
	if status, _, body := getOrDelete(t, b, http.MethodGet, map[string]string{"Mcp-Session-Id": sid}); status != 200 || body != "(stream)" {
		t.Errorf("GET on a v1 child: %d %s", status, body)
	}
	if status, _, _ := getOrDelete(t, b, http.MethodDelete, map[string]string{"Mcp-Session-Id": sid}); status != 204 {
		t.Errorf("DELETE on a v1 child: %d", status)
	}
}

func TestEra_NoSessionAfterRefusedInitialize(t *testing.T) {
	for _, accept := range []string{acceptJSON, acceptSSE} {
		b := startEraBridge(t, nil, "--era=modern")
		for i := 0; i < 2; i++ {
			r := eraPost(t, b, accept, eraInitialize(i+1).body, nil)
			if r.status != 400 || r.header.Get("Mcp-Session-Id") != "" || !strings.Contains(r.body, `"supported":["2026-07-28"]`) {
				t.Fatalf("%s: refused initialize: %d %v %s", accept, r.status, r.header, r.body)
			}
		}
		if sessionMinted(b) {
			t.Fatalf("%s: a session was minted for a refused initialize", accept)
		}
		r := eraPost(t, b, accept, map[string]any{"jsonrpc": "2.0", "id": 5, "method": "ping"}, nil)
		if r.status != 400 || !strings.Contains(r.body, `"code":-32600`) || !strings.Contains(r.body, `"supported":["2026-07-28"]`) {
			t.Fatalf("%s: header-less 2025-style request: %d %s", accept, r.status, r.body)
		}
	}
}

// A v2 process gives the child nothing 2025-style: every such message, a bare
// cancel included, gets the bridge's own 400 echoing its id, and none reaches
// the child.
func TestEra_V1TrafficIsRefusedOnV2(t *testing.T) {
	b := startEraBridge(t, nil, "--era=modern")
	before := forwardedCount(t, b)
	call := func(id int) map[string]any {
		return map[string]any{"jsonrpc": "2.0", "id": id, "method": "tools/call", "params": map[string]any{"name": "echo"}}
	}
	requestRefusal := func(id string) string {
		return `{"error":{"code":-32600,"data":{"supported":["2026-07-28"]},"message":"This server speaks MCP v2 (protocol 2026-07-28) and does not accept older MCP v1 requests."},` + id + `"jsonrpc":"2.0"}`
	}
	cases := []struct {
		name string
		body map[string]any
		hdr  map[string]string
		want string
	}{
		{"tools/call", call(11), nil, requestRefusal(`"id":11,`)},
		{"tools/call with an unknown session", call(12), map[string]string{"Mcp-Session-Id": "whatever"}, requestRefusal(`"id":12,`)},
		{"ping", map[string]any{"jsonrpc": "2.0", "id": 13, "method": "ping"}, nil, requestRefusal(`"id":13,`)},
		{"subscriptions/listen", map[string]any{"jsonrpc": "2.0", "id": 14, "method": "subscriptions/listen", "params": map[string]any{}}, nil, requestRefusal(`"id":14,`)},
		{"initialize", eraInitialize(15).body, nil,
			`{"error":{"code":-32022,"data":{"requested":"2025-11-25","supported":["2026-07-28"]},"message":"This server speaks MCP v2 (protocol 2026-07-28) and does not accept the older MCP v1 initialize handshake."},"id":15,"jsonrpc":"2.0"}`},
		{"initialize naming 2026-07-28", map[string]any{"jsonrpc": "2.0", "id": 16, "method": "initialize", "params": map[string]any{"protocolVersion": "2026-07-28"}}, nil,
			`{"error":{"code":-32022,"data":{"requested":"2026-07-28","supported":["2026-07-28"]},"message":"This server speaks MCP v2 (protocol 2026-07-28) and does not accept the older MCP v1 initialize handshake."},"id":16,"jsonrpc":"2.0"}`},
		{"initialize without a version", map[string]any{"jsonrpc": "2.0", "id": 17, "method": "initialize"}, nil,
			`{"error":{"code":-32022,"data":{"requested":"","supported":["2026-07-28"]},"message":"This server speaks MCP v2 (protocol 2026-07-28) and does not accept the older MCP v1 initialize handshake."},"id":17,"jsonrpc":"2.0"}`},
		{"enveloped initialize", modernBody(18, "initialize", map[string]any{"protocolVersion": "2025-11-25"}, nil), nil,
			`{"error":{"code":-32022,"data":{"requested":"2025-11-25","supported":["2026-07-28"]},"message":"This server speaks MCP v2 (protocol 2026-07-28) and does not accept the older MCP v1 initialize handshake."},"id":18,"jsonrpc":"2.0"}`},
		{"notification", map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"}, nil, requestRefusal("")},
		{"bare cancel", bareCancel(float64(1)), nil, requestRefusal("")},
		{"client response", map[string]any{"jsonrpc": "2.0", "id": 19, "result": map[string]any{}}, nil, requestRefusal(`"id":19,`)},
		{"client error response", map[string]any{"jsonrpc": "2.0", "id": "s-1", "error": map[string]any{"code": -1, "message": "no"}}, nil, requestRefusal(`"id":"s-1",`)},
	}
	for _, c := range cases {
		for _, accept := range []string{acceptJSON, acceptSSE} {
			r := eraPost(t, b, accept, c.body, c.hdr)
			var v any
			_ = json.Unmarshal([]byte(r.body), &v)
			norm, _ := json.Marshal(v)
			if r.status != 400 || string(norm) != c.want || r.header.Get("Mcp-Session-Id") != "" {
				t.Errorf("%s (%s): %d %s\n  want %s", c.name, accept, r.status, norm, c.want)
			}
		}
	}
	if n := forwardedCount(t, b); n != before {
		t.Fatalf("a refused request reached the child (%d -> %d)", before, n)
	}
	if sessionMinted(b) {
		t.Fatal("a refused request minted a session")
	}
	// A message that is not JSON-RPC at all is no 2025 message: the plain
	// invalid-request answer, not the refusal.
	for _, raw := range []string{
		`{"jsonrpc":"2.0"}`,
		`7`,
		`[{"jsonrpc":"2.0","id":1,"method":"ping"}]`,
		`{"jsonrpc":"2.0","id":1}`,
		`{"jsonrpc":"2.0","id":1,"method":5}`,
		`{"jsonrpc":"2.0","id":1,"method":""}`,
		`{"jsonrpc":"2.0","method":5}`,
	} {
		resp, err := http.Post(b.URL(), "application/json", strings.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if want := `{"error":{"code":-32600,"message":"Invalid Request"},"id":null,"jsonrpc":"2.0"}`; resp.StatusCode != 400 || string(body) != want {
			t.Errorf("%s: %d %s\n  want %s", raw, resp.StatusCode, body, want)
		}
	}
	if n := forwardedCount(t, b); n != before {
		t.Fatalf("a malformed message reached the child (%d -> %d)", before, n)
	}
	note := map[string]any{"jsonrpc": "2.0", "method": "notifications/whatever", "params": map[string]any{"_meta": eraMeta(eraTestVersion)}}
	if r := eraPost(t, b, acceptJSON, note, nil); r.status != 202 {
		t.Fatalf("modern-shaped notification: %d %s", r.status, r.body)
	}
}

// Unless the process is a live v2 one, a body that is not one JSON-RPC message
// gets its own answer at once, before the verdict, and one with neither a
// method nor an id meets the session gate first. A v1 process forwards what
// only a v2 one counts as malformed.
func TestEra_MalformedMessagesAreAnsweredAtOnce(t *testing.T) {
	rawPost := func(t *testing.T, b *Bridge, raw string, hdr map[string]string) (int, string) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, b.URL(), strings.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		// Well under the 15 s a wait for the verdict could take.
		resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}
	pending := startEraBridgeCold(t, nil, "--era=modern", "--discover-hold")
	for raw, want := range map[string]string{
		`[{"jsonrpc":"2.0","id":1,"method":"ping"}]`: "batch requests are not supported",
		`7`:                 "invalid request: body must be a JSON object",
		`{"jsonrpc":"2.0"}`: "invalid request: body must have a method, an id, or both",
	} {
		if status, body := rawPost(t, pending, raw, nil); status != 400 || !strings.Contains(body, want) {
			t.Errorf("before the verdict, %s: %d %s", raw, status, body)
		}
	}
	if got := versionOf(pending); got != versionUnknown {
		t.Fatalf("version %d", got)
	}

	b := startEraBridge(t, nil, "--era=legacy")
	sid := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil).header.Get("Mcp-Session-Id")
	if status, body := rawPost(t, b, `{"jsonrpc":"2.0"}`, nil); status != 400 || !strings.Contains(body, "Mcp-Session-Id header is required") {
		t.Errorf("v1 without a session: %d %s", status, body)
	}
	if status, body := rawPost(t, b, `{"jsonrpc":"2.0"}`, map[string]string{"Mcp-Session-Id": sid}); status != 400 || !strings.Contains(body, "body must have a method, an id, or both") {
		t.Errorf("v1 with the session: %d %s", status, body)
	}
	if status, body := rawPost(t, b, `{"jsonrpc":"2.0","id":1}`, map[string]string{"Mcp-Session-Id": sid}); status != 202 {
		t.Errorf("v1, an id alone: %d %s", status, body)
	}
}

// A modern-shaped message whose id is null is an invalid request, answered at
// once whatever the verdict, and never forwarded.
func TestEra_ModernNullIDIsAnInvalidRequest(t *testing.T) {
	const want = `{"error":{"code":-32600,"message":"Invalid Request"},"id":null,"jsonrpc":"2.0"}`
	check := func(t *testing.T, b *Bridge, what string) {
		t.Helper()
		if r := eraPost(t, b, acceptJSON, modernCall(nil), nil); r.status != 400 || r.body != want || r.elapsed > 5*time.Second {
			t.Errorf("%s: %d after %s: %s", what, r.status, r.elapsed, r.body)
		}
	}
	pending := startEraBridgeCold(t, nil, "--era=modern", "--discover-hold")
	check(t, pending, "before the verdict")
	if got := versionOf(pending); got != versionUnknown {
		t.Fatalf("version %d", got)
	}
	for _, args := range []string{"--era=legacy", "--era=modern"} {
		b := startEraBridge(t, nil, args)
		before := forwardedCount(t, b)
		check(t, b, args)
		if n := forwardedCount(t, b); n != before {
			t.Errorf("%s: the message reached the child (%d -> %d)", args, before, n)
		}
	}
}

// Methods other than GET, POST and DELETE: only a running v2 process allows
// POST alone.
func TestEra_OtherMethodsName405Allow(t *testing.T) {
	put := func(t *testing.T, b *Bridge) (int, string) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPut, b.URL(), nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode, resp.Header.Get("Allow")
	}
	for args, want := range map[string]string{"--era=modern": "POST", "--era=legacy": "GET, POST, DELETE"} {
		b := startEraBridge(t, nil, args)
		if status, allow := put(t, b); status != 405 || allow != want {
			t.Errorf("%s: %d, Allow %q, want %q", args, status, allow, want)
		}
	}
	b := startEraBridge(t, nil, "--era=modern")
	child := b.GetCurrentChild()
	_ = child.Close()
	waitFor(t, "the process to end", func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.era.m.ended
	})
	if status, allow := put(t, b); status != 405 || allow != "GET, POST, DELETE" {
		t.Errorf("ended v2 process: %d, Allow %q", status, allow)
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
	if b.session.check(sid, true).kind != sessionOK {
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
		if b.session.check(sid, true).kind != sessionOK {
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
			if b.session.check(a.sid, true).kind != sessionOK {
				t.Fatal("the accepted initialize's session is not the bridge's")
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
	s.initialize()
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
		name  string
		body  map[string]any
		phase eraPhase
	}{
		{"modern", modernCall(1), phaseV2},
		{"initialize", eraInitialize(1).body, phaseV1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := startEraBridge(t, func(o *StartBridgeOptions) {
				o.HeadHoldTimeout = time.Hour
				o.InitializeHoldTimeout = time.Hour
			}, "--era=modern")
			// A child with no stdin, decided by hand: Send panics after the
			// head is held.
			st := newEraState(newBareChild(), ServeAuto)
			st.m = mach(c.phase, vers(eraTestVersion))
			close(st.decided)
			b.mu.Lock()
			realChild, realEra := b.child, b.era
			b.child, b.era = st.child, st
			b.mu.Unlock()
			t.Cleanup(func() {
				b.mu.Lock()
				b.child, b.era = realChild, realEra
				b.mu.Unlock()
			})

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

// Close answers what it releases with a 502 that is flushed before the
// handler returns, so the listener's hard close cannot cut it off.
func TestEra_CloseReleasesHeldHeadAndWaiters(t *testing.T) {
	type result struct {
		status int
		err    error
	}
	t.Run("held head", func(t *testing.T) {
		b := startEraBridge(t, func(o *StartBridgeOptions) { o.HeadHoldTimeout = time.Hour }, "--era=modern")
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
	t.Run("waiting for the verdict", func(t *testing.T) {
		b := startEraBridgeCold(t, nil, "--era=modern", "--discover-hold")
		w := waitingPost(t, b, context.Background(), acceptJSON, modernCall(1))
		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if r := w.response(t); r.Code != 502 || !r.Flushed {
			t.Fatalf("waiter after Close: %d, flushed %v", r.Code, r.Flushed)
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

// With the child dead and not respawned, any POST gets the "not running"
// answer, not the refusal its verdict would give.
func TestEra_DeadChildIsNotRunningRatherThanRefused(t *testing.T) {
	for _, args := range []string{"--era=modern", "--era=legacy"} {
		b := startEraBridge(t, nil, args)
		child := b.GetCurrentChild()
		_ = child.Close()
		waitFor(t, "the child to exit", func() bool { return child.HasExited() })

		for name, body := range map[string]map[string]any{
			"request":         {"jsonrpc": "2.0", "id": 5, "method": "ping"},
			"modern request":  modernCall(6),
			"notification":    {"jsonrpc": "2.0", "method": "notifications/initialized"},
			"client response": {"jsonrpc": "2.0", "id": 7, "result": map[string]any{}},
		} {
			r := eraPost(t, b, acceptJSON, body, nil)
			if r.status != 502 || !strings.Contains(r.body, "not running") {
				t.Errorf("%s %s: %d %s", args, name, r.status, r.body)
			}
		}
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
