package bridge

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Process-level checks of the version rules whose effect a transition table
// cannot show: real writers, timers and children, the log, and what the
// client sees.

// startTimedOut starts a bridge whose child never answers the probe and waits
// until it is v1 for now and the child has read the probe, which is written
// on a goroutine of its own and so may still be on its way at the timeout.
func startTimedOut(t *testing.T, log *slog.Logger, args ...string) *Bridge {
	t.Helper()
	b := startEraBridge(t, func(o *StartBridgeOptions) {
		shortProbe(o)
		if log != nil {
			o.Log = log
		}
	}, args...)
	if got := phaseOf(b); got != phaseTimeout {
		t.Fatalf("setup: phase %s, want timeout", describe(mach(got)))
	}
	waitFor(t, "the child to read the probe", func() bool {
		return indexOf(childReceived(t, b), "server/discover") >= 0
	})
	return b
}

// lateModernAnswer feeds the current process a modern answer to its probe, as
// a child that answers late would.
func lateModernAnswer(b *Bridge) {
	_, st, _ := b.snapshotEra()
	b.handleProbeResponse(st, map[string]any{"jsonrpc": "2.0", "id": probeID,
		"result": map[string]any{"supportedVersions": []any{eraTestVersion}}})
}

// heldInitialize sends a 2025 initialize the child holds, and waits until it
// is in flight.
func heldInitialize(t *testing.T, b *Bridge, ctx context.Context) chan eraResponse {
	t.Helper()
	res := make(chan eraResponse, 1)
	go func() {
		resp, _, err := eraSend(ctx, b.URL(), acceptJSON, eraInitialize(1).body, nil)
		if err != nil {
			close(res)
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		res <- eraResponse{status: resp.StatusCode, header: resp.Header, body: string(body)}
	}()
	waitFor(t, "the initialize to be in flight", func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.era.m.inits == 1
	})
	return res
}

// A child that is already gone when the probe is written is a dead child, not
// a verdict, and StartBridge does not fail because of it.
func TestEra_ChildGoneAtSpawnIsNotRunning(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("needs the true command")
	}
	b, err := StartBridge(StartBridgeOptions{Name: "true", SpawnSpec: SpawnSpec{Command: "true"}, Log: testLogger()})
	if err != nil {
		t.Fatalf("StartBridge: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	waitFor(t, "the child to exit", func() bool { return b.GetCurrentChild().HasExited() })
	r := eraPost(t, b, acceptJSON, map[string]any{"jsonrpc": "2.0", "id": 1, "method": "ping"}, nil)
	if r.status != 502 || !strings.Contains(r.body, "not running") {
		t.Fatalf("%d %s", r.status, r.body)
	}
}

// Any failed write to stdin, not only the probe's, ends the process's state
// and closes the child, so its exit takes the usual restart path.
func TestEra_FailedStdinWriteClosesTheChild(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("needs /bin/sh")
	}
	// The child reads the probe, so that write succeeded, then closes its
	// stdin. A request written in the moment before the close is never
	// answered and runs into the short request timeout; the next is tried.
	b, err := StartBridge(StartBridgeOptions{
		Name:           "closes-stdin",
		SpawnSpec:      SpawnSpec{Command: "/bin/sh", Args: []string{"-c", "IFS= read -r line; exec 0<&-; sleep 30"}},
		Log:            testLogger(),
		ProbeTimeout:   100 * time.Millisecond,
		RequestTimeout: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("StartBridge: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	waitForVerdict(t, b)
	child := b.GetCurrentChild()
	var r eraResponse
	waitFor(t, "a request whose write failed", func() bool {
		r = eraPost(t, b, acceptJSON, map[string]any{"jsonrpc": "2.0", "id": 1, "method": "ping"}, nil)
		return r.status != 504
	})
	if r.status != 502 || !strings.Contains(r.body, "not running") {
		t.Fatalf("request whose write failed: %d %s", r.status, r.body)
	}
	waitFor(t, "the bridge to close the child", child.HasExited)
	if r := eraPost(t, b, acceptJSON, map[string]any{"jsonrpc": "2.0", "id": 2, "method": "ping"}, nil); r.status != 502 {
		t.Fatalf("after the failed write: %d %s", r.status, r.body)
	}
}

// An error answer is v1 at once, not after the timeout.
func TestEra_ErrorAnswerIsV1AtOnce(t *testing.T) {
	for _, code := range []string{"-32603", "-32020", "-32021", "-32602"} {
		b := startEraBridgeCold(t, nil, "--era=modern", "--discover-error="+code) // the default 15s timeout
		r := eraPost(t, b, acceptJSON, modernCall(1), nil)
		if r.status != 400 || !strings.Contains(r.body, v1Fallback) || r.elapsed > 5*time.Second {
			t.Errorf("%s: %d after %s: %s", code, r.status, r.elapsed, r.body)
		}
	}
}

// The verdict lines, one per verdict.
func TestEra_VerdictIsLogged(t *testing.T) {
	for _, c := range []struct {
		args []string
		line string
	}{
		{[]string{"--era=modern"}, "The server speaks MCP v2 (protocol 2026-07-28)."},
		{[]string{"--era=legacy"}, "The server speaks MCP v1 (protocol 2025-11-25 and earlier)."},
		{[]string{"--era=legacy-silent"}, "No answer from the server within 15 s; serving it as MCP v1 until it answers."},
	} {
		logs := &logCapture{}
		b := startEraBridge(t, func(o *StartBridgeOptions) { shortProbe(o); o.Log = logs.logger() }, c.args...)
		logs.waitLogged(t, c.line, 1)
		if n := logs.closeAndCount(b, c.line); n != 1 {
			t.Errorf("%v: %q logged %d times", c.args, c.line, n)
		}
	}
}

// A modern child slow to answer is v1 for now, then v2 on its late answer,
// which arrives because the probe was never cancelled.
func TestEra_LateModernAnswerFlipsTheTimeoutState(t *testing.T) {
	logs := &logCapture{}
	b := startTimedOut(t, logs.logger(), "--era=modern", "--discover-hold")
	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 400 || !strings.Contains(r.body, v1Fallback) {
		t.Fatalf("timeout state: %d %s", r.status, r.body)
	}
	releaseHeld(t, b)
	if got := versionOf(b); got != versionV2 {
		t.Fatalf("version %d after the late answer", got)
	}
	late := "The server answered late; it speaks MCP v2 (protocol 2026-07-28)."
	logs.waitLogged(t, late, 1)
	if r := eraPost(t, b, acceptJSON, modernCall(2), nil); r.status != 200 {
		t.Fatalf("after the flip: %d %s", r.status, r.body)
	}
	if n := logs.closeAndCount(b, late); n != 1 {
		t.Fatalf("late line logged %d times", n)
	}
}

// A late answer that is not modern makes the timeout state an answered v1,
// from which nothing flips. Whichever of it and the first successful
// initialize comes first logs the v1 line; the other logs nothing more.
func TestEra_LateV1AnswerEndsTheTimeoutState(t *testing.T) {
	t.Run("answer first", func(t *testing.T) {
		logs := &logCapture{}
		b := startTimedOut(t, logs.logger(), "--era=legacy", "--discover-delay-ms=600")
		waitFor(t, "the late answer", func() bool { return phaseOf(b) == phaseV1 })
		logs.waitInOrder(t, logNoAnswer, logSpeaksV1)
		if r := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil); r.status != 200 {
			t.Fatalf("initialize: %d %s", r.status, r.body)
		}
		_ = b.Close()
		if !logs.loggedInOrder(logNoAnswer, logSpeaksV1) {
			t.Fatalf("log:\n%s", logs.text())
		}
	})
	t.Run("initialize first", func(t *testing.T) {
		logs := &logCapture{}
		b := startTimedOut(t, logs.logger(), "--era=legacy", "--discover-delay-ms=3000")
		if r := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil); r.status != 200 {
			t.Fatalf("initialize: %d %s", r.status, r.body)
		}
		if phaseOf(b) != phaseTimeout {
			t.Fatal("the late answer came before the initialize")
		}
		waitFor(t, "the late answer", func() bool { return phaseOf(b) == phaseV1 })
		logs.waitInOrder(t, logNoAnswer, logSpeaksV1)
		_ = b.Close()
		if !logs.loggedInOrder(logNoAnswer, logSpeaksV1) {
			t.Fatalf("log:\n%s", logs.text())
		}
	})
}

// The one flip, from the timeout state, on a rejected initialize naming only
// modern versions; the client gets the child's own rejection, and the next
// initialize the bridge's.
func TestEra_QualifyingRejectionFlipsTheTimeoutState(t *testing.T) {
	logs := &logCapture{}
	b := startTimedOut(t, logs.logger(), "--era=modern", "--discover-hold")
	r := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil)
	if r.status != 400 || !strings.Contains(r.body, `"message":"Unsupported protocol version"`) || r.header.Get("Mcp-Session-Id") != "" {
		t.Fatalf("forwarded initialize: %d %s", r.status, r.body)
	}
	if got := versionOf(b); got != versionV2 {
		t.Fatalf("version %d after the rejection", got)
	}
	v2 := "The server speaks MCP v2 (protocol 2026-07-28)."
	logs.waitLogged(t, v2, 1)
	r = eraPost(t, b, acceptJSON, eraInitialize(2).body, nil)
	if r.status != 400 || !strings.Contains(r.body, "does not accept the older MCP v1 initialize handshake") {
		t.Fatalf("second initialize: %d %s", r.status, r.body)
	}
	if n := logs.closeAndCount(b, v2); n != 1 {
		t.Fatalf("v2 line logged %d times", n)
	}
}

// The flip never happens from an answered v1.
func TestEra_QualifyingRejectionLeavesAnAnsweredV1Alone(t *testing.T) {
	b := startEraBridge(t, nil, "--era=modern", "--discover-error=-32603")
	for i := 1; i <= 2; i++ {
		r := eraPost(t, b, acceptJSON, eraInitialize(i).body, nil)
		if r.status != 400 || !strings.Contains(r.body, `"message":"Unsupported protocol version"`) {
			t.Fatalf("initialize %d: %d %s", i, r.status, r.body)
		}
	}
	if got := phaseOf(b); got != phaseV1 {
		t.Fatalf("phase %s", describe(mach(got)))
	}
}

// An initialize the child accepts after another initialize's rejection has
// turned the process v2 gets the bridge's v2 refusal instead of the child's
// result, and no session, whether or not its head has already gone out.
func TestEra_InitializeAcceptedAfterTheFlipIsRefused(t *testing.T) {
	for _, c := range []struct {
		name    string
		accept  string
		headOut bool
	}{
		{"json", acceptJSON, false},
		{"sse, head held", acceptSSE, false},
		{"sse, head out", acceptSSE, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := startEraBridge(t, func(o *StartBridgeOptions) {
				shortProbe(o)
				if c.headOut {
					o.InitializeHoldTimeout = 150 * time.Millisecond
				}
			}, "--era=legacy-silent", "--initialize-hold")
			if got := phaseOf(b); got != phaseTimeout {
				t.Fatalf("setup: phase %s, want timeout", describe(mach(got)))
			}
			first := heldInitialize(t, b, context.Background())
			firstID := firstPendingID(t, b)

			second := make(chan eraResponse, 1)
			var head *http.Response
			if c.headOut {
				head = sendHeadOnly(t, b, c.accept, eraInitialize(2).body)
				if head.StatusCode != 200 || head.Header.Get("Mcp-Session-Id") == "" {
					t.Fatalf("setup: head %d, session %q; want a 200 with a provisional session", head.StatusCode, head.Header.Get("Mcp-Session-Id"))
				}
			} else {
				go func() { second <- eraPost(t, b, c.accept, eraInitialize(2).body, nil) }()
			}
			waitFor(t, "the second initialize to be in flight", func() bool {
				b.mu.Lock()
				defer b.mu.Unlock()
				return b.era.m.inits == 2
			})

			// The child rejects the first as 2026-only while it holds its
			// acceptance of the second.
			rawLines(t, b, jsonLine(t, map[string]any{"jsonrpc": "2.0", "id": firstID, "error": map[string]any{
				"code": -32022, "message": "Unsupported protocol version",
				"data": map[string]any{"supported": []any{eraTestVersion}},
			}}))
			if r := <-first; r.status != 400 || !strings.Contains(r.body, "Unsupported protocol version") {
				t.Fatalf("first initialize: %d %s", r.status, r.body)
			}
			if got := versionOf(b); got != versionV2 {
				t.Fatalf("version %d after the rejection", got)
			}
			releaseUntil(t, b, 2)

			// With its head out, the refusal is a frame in the 200 stream.
			status, wantStatus, body := 0, 400, ""
			if c.headOut {
				raw, _ := io.ReadAll(head.Body)
				status, wantStatus, body = head.StatusCode, 200, string(raw)
			} else {
				r := <-second
				if sid := r.header.Get("Mcp-Session-Id"); sid != "" {
					t.Fatalf("the refusal carries session %q", sid)
				}
				status, body = r.status, r.body
			}
			for _, want := range []string{`"id":2`, `"code":-32022`, v2InitializeRefusal([]string{eraTestVersion}),
				`"supported":["` + eraTestVersion + `"]`, `"requested":"2025-11-25"`} {
				if !strings.Contains(body, want) {
					t.Fatalf("second initialize: %d %s; missing %s", status, body, want)
				}
			}
			if status != wantStatus || strings.Contains(body, "serverInfo") {
				t.Fatalf("second initialize: %d %s", status, body)
			}
			if sessionMinted(b) {
				t.Fatal("a session was minted on a v2 process")
			}
		})
	}
}

// The first successful initialize settles a silent child as v1, with its
// verdict line; a modern answer after it is one more line.
func TestEra_LateModernAnswerAfterAV1ClientIsALogLine(t *testing.T) {
	logs := &logCapture{}
	b := startTimedOut(t, logs.logger(), "--era=legacy-silent")
	if r := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil); r.status != 200 {
		t.Fatalf("initialize: %d %s", r.status, r.body)
	}
	lateModernAnswer(b)
	if got := versionOf(b); got != versionV1 {
		t.Fatalf("version %d", got)
	}
	lines := []string{logNoAnswer, logSpeaksV1, logLateAfterV1([]string{eraTestVersion})}
	logs.waitInOrder(t, lines...)
	_ = b.Close()
	if !logs.loggedInOrder(lines...) {
		t.Fatalf("log:\n%s", logs.text())
	}
}

// A late modern answer while an initialize is in flight is held: v1 if the
// child accepts that initialize, v2 with the held list if it rejects it.
// Once the initialize is written only the child's answer settles it: the
// client leaving or timing out does not.
func TestEra_LateModernAnswerIsHeldWhileAnInitializeIsInFlight(t *testing.T) {
	t.Run("initialize succeeds", func(t *testing.T) {
		logs := &logCapture{}
		b := startTimedOut(t, logs.logger(), "--era=legacy-silent", "--initialize-hold")
		res := heldInitialize(t, b, context.Background())
		lateModernAnswer(b)
		if got := versionOf(b); got != versionV1 {
			t.Fatalf("the held answer was applied: version %d", got)
		}
		releaseUntil(t, b, 1)
		if r := <-res; r.status != 200 {
			t.Fatalf("initialize: %d %s", r.status, r.body)
		}
		if got := versionOf(b); got != versionV1 {
			t.Fatalf("version %d", got)
		}
		lines := []string{logNoAnswer, logSpeaksV1, logLateAfterV1([]string{eraTestVersion})}
		logs.waitInOrder(t, lines...)
		_ = b.Close()
		if !logs.loggedInOrder(lines...) {
			t.Fatalf("log:\n%s", logs.text())
		}
	})
	t.Run("initialize fails", func(t *testing.T) {
		logs := &logCapture{}
		b := startTimedOut(t, logs.logger(), "--era=legacy-silent", "--initialize-hold", "--reject-initialize=1")
		res := heldInitialize(t, b, context.Background())
		lateModernAnswer(b)
		releaseUntil(t, b, 1)
		if r := <-res; r.status != 400 || !strings.Contains(r.body, "bad initialize") {
			t.Fatalf("initialize: %d %s", r.status, r.body)
		}
		if got := versionOf(b); got != versionV2 || strings.Join(v2VersionsOf(b), ",") != eraTestVersion {
			t.Fatalf("version %d %q", got, v2VersionsOf(b))
		}
		late := "The server answered late; it speaks MCP v2 (protocol 2026-07-28)."
		logs.waitLogged(t, late, 1)
		if n := logs.closeAndCount(b, late); n != 1 {
			t.Fatalf("late line logged %d times", n)
		}
	})
	for _, end := range []string{"client leaves", "request timeout"} {
		for _, rejects := range []bool{false, true} {
			outcome := "accepts"
			if rejects {
				outcome = "rejects"
			}
			t.Run(end+", then the child "+outcome, func(t *testing.T) {
				logs := &logCapture{}
				args := []string{"--era=legacy-silent", "--initialize-hold"}
				if rejects {
					args = append(args, "--reject-initialize=1")
				}
				b := startEraBridge(t, func(o *StartBridgeOptions) {
					shortProbe(o)
					o.Log = logs.logger()
					if end == "request timeout" {
						o.RequestTimeout = 300 * time.Millisecond
					}
				}, args...)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				res := heldInitialize(t, b, ctx)
				id := firstPendingID(t, b)
				lateModernAnswer(b)
				if end == "client leaves" {
					cancel()
					<-res
					waitFor(t, "the handler to let its client go", func() bool { return b.isAbandoned(id) })
				} else if r := <-res; r.status != 504 {
					t.Fatalf("initialize: %d %s", r.status, r.body)
				}
				b.mu.Lock()
				inFlight, heldAnswer := b.era.m.inits, b.era.m.held
				b.mu.Unlock()
				if inFlight != 1 || heldAnswer == nil || versionOf(b) != versionV1 {
					t.Fatalf("settled without the child's answer: %d in flight, held %v, version %d", inFlight, heldAnswer, versionOf(b))
				}

				releaseUntil(t, b, 1)
				want, line := versionV1, "The server said it speaks MCP v2 (protocol 2026-07-28) after an MCP v1 client had connected; it stays MCP v1 until it restarts."
				if rejects {
					want, line = versionV2, "The server answered late; it speaks MCP v2 (protocol 2026-07-28)."
				}
				logs.waitLogged(t, line, 1)
				if got := versionOf(b); got != want {
					t.Fatalf("version %d, want %d", got, want)
				}
				b.mu.Lock()
				left := len(b.pending)
				b.mu.Unlock()
				if left != 0 {
					t.Fatalf("%d entries still pending after the child's answer", left)
				}
				if n := logs.closeAndCount(b, line); n != 1 {
					t.Fatalf("%q logged %d times", line, n)
				}
			})
		}
	}
}

// The bridge never cancels an initialize at the child, whatever its client
// does. One whose client left stays pending for the child's answer only while
// that answer can still change the version: in the timeout state, before an
// initialize has succeeded. From then on it is abandoned like any request.
func TestEra_AbandonedInitializeIsNotCancelled(t *testing.T) {
	// leave sends an initialize straight to the handler, lets its client go
	// once it is pending, and returns its bridge id once the handler is done.
	leave := func(t *testing.T, b *Bridge) int64 {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(jsonLine(t, eraInitialize(7).body))).WithContext(ctx)
		req.Header.Set("Accept", acceptJSON)
		returned := make(chan struct{})
		go func() {
			b.handle(httptest.NewRecorder(), req)
			close(returned)
		}()
		id := firstPendingID(t, b)
		cancel()
		select {
		case <-returned:
		case <-time.After(10 * time.Second):
			t.Fatal("the handler did not return")
		}
		return id
	}
	pending := func(b *Bridge) int {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.pending)
	}

	t.Run("answered v1", func(t *testing.T) {
		b := startEraBridge(t, nil, "--era=legacy", "--initialize-hold")
		id := leave(t, b)
		if n := pending(b); n != 0 {
			t.Fatalf("%d pending", n)
		}
		if n := childCancelCount(t, b, float64(id)); n != 0 {
			t.Fatalf("the child got %d cancels for the initialize", n)
		}
	})
	t.Run("timeout, before a success", func(t *testing.T) {
		b := startTimedOut(t, nil, "--era=legacy-silent", "--initialize-hold")
		id := leave(t, b)
		if n := pending(b); n != 1 {
			t.Fatalf("%d pending, want the initialize kept for the child's answer", n)
		}
		if n := childCancelCount(t, b, float64(id)); n != 0 {
			t.Fatalf("the child got %d cancels for the initialize", n)
		}
		releaseDirect(t, b.GetCurrentChild(), func() bool { return pending(b) == 0 })
	})
	t.Run("timeout, another initialize succeeds while it waits", func(t *testing.T) {
		b := startTimedOut(t, nil, "--era=legacy-silent", "--initialize-hold")
		first := leave(t, b)
		res := make(chan eraResponse, 1)
		go func() { res <- eraPost(t, b, acceptJSON, eraInitialize(8).body, nil) }()
		var second int64
		waitFor(t, "the second initialize to be in flight", func() bool {
			b.mu.Lock()
			defer b.mu.Unlock()
			for id := range b.pending {
				if id != first && b.era.m.inits == 2 {
					second = id
					return true
				}
			}
			return false
		})
		// The child answers the second initialize while it still holds its
		// answer to the first.
		rawLines(t, b, childResultLine(t, second, map[string]any{
			"protocolVersion": "2025-11-25",
			"capabilities":    map[string]any{},
			"serverInfo":      map[string]any{"name": "raw", "version": "0"},
		}))
		if r := <-res; r.status != 200 {
			t.Fatalf("second initialize: %d %s", r.status, r.body)
		}
		if n := pending(b); n != 0 || !b.isAbandoned(first) {
			t.Fatalf("%d pending, first initialize abandoned %v; want it let go at the first success", n, b.isAbandoned(first))
		}
		releaseHeld(t, b)
		if n := childCancelCount(t, b, float64(first)); n != 0 {
			t.Fatalf("the child got %d cancels for the initialize", n)
		}
	})
	t.Run("timeout, after a success", func(t *testing.T) {
		b := startTimedOut(t, nil, "--era=legacy-silent", "--initialize-hold")
		res := heldInitialize(t, b, context.Background())
		releaseUntil(t, b, 1)
		if r := <-res; r.status != 200 {
			t.Fatalf("first initialize: %d %s", r.status, r.body)
		}
		id := leave(t, b)
		if n := pending(b); n != 0 {
			t.Fatalf("%d pending after the first success", n)
		}
		if n := childCancelCount(t, b, float64(id)); n != 0 {
			t.Fatalf("the child got %d cancels for the initialize", n)
		}
	})
}

// In the timeout state a rejection that does not name modern versions alone,
// with no answer held, changes nothing: the next initialize succeeds and the
// process stays v1.
func TestEra_NonQualifyingRejectionKeepsTheTimeoutState(t *testing.T) {
	b := startTimedOut(t, nil, "--era=legacy-silent", "--reject-initialize=1")
	if r := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil); r.status != 400 || !strings.Contains(r.body, "bad initialize") {
		t.Fatalf("first initialize: %d %s", r.status, r.body)
	}
	if got := phaseOf(b); got != phaseTimeout {
		t.Fatalf("after the rejection: phase %s", describe(mach(got)))
	}
	if r := eraPost(t, b, acceptJSON, eraInitialize(2).body, nil); r.status != 200 || r.header.Get("Mcp-Session-Id") == "" {
		t.Fatalf("second initialize: %d %s", r.status, r.body)
	}
	if got := versionOf(b); got != versionV1 {
		t.Fatalf("version %d", got)
	}
}

// A client arriving once a silent child's probe deadline has passed is
// served at once: nothing waits the deadline out again.
func TestEra_ClientAfterTheDeadlineIsServedAtOnce(t *testing.T) {
	const probe = time.Second
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.ProbeTimeout = probe }, "--era=legacy-silent")
	if r := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil); r.status != 200 || r.elapsed >= probe/2 {
		t.Fatalf("initialize: %d after %s: %s", r.status, r.elapsed, r.body)
	}
	if r := eraPost(t, b, acceptJSON, modernCall(2), nil); r.status != 400 || !strings.Contains(r.body, v1Fallback) || r.elapsed >= probe/2 {
		t.Fatalf("2026 request: %d after %s: %s", r.status, r.elapsed, r.body)
	}
}

// A 2026 request while an initialize is in flight in the timeout state gets
// the fallback at once.
func TestEra_ModernRequestBesideAnInitializeInFlightGetsTheFallback(t *testing.T) {
	b := startTimedOut(t, nil, "--era=legacy-silent", "--initialize-hold")
	heldInitialize(t, b, context.Background())
	r := eraPost(t, b, acceptJSON, modernCall(2), nil)
	if r.status != 400 || !strings.Contains(r.body, v1Fallback) || r.elapsed > 5*time.Second {
		t.Fatalf("%d after %s: %s", r.status, r.elapsed, r.body)
	}
	releaseUntil(t, b, 1)
}

// A v2 process has no push stream, and one open in the timeout state ends
// when the process turns v2.
func TestEra_V2HasNoPushStream(t *testing.T) {
	t.Run("dropped", func(t *testing.T) {
		b := startEraBridge(t, nil, listenFake)
		rawLines(t, b, plainNotification(t, "x/untagged"), jsonLine(t, map[string]any{"jsonrpc": "2.0", "id": 5, "method": "server/ping"}))
		debugCall(t, b, "debug/listens", nil) // the lines above have been handled
		if queued := pushBacklog(b); queued != 0 {
			t.Fatalf("%d messages queued for a push stream", queued)
		}
	})
	t.Run("closed on the flip", func(t *testing.T) {
		b := startTimedOut(t, nil, "--era=modern", "--discover-hold")
		get := openGet(t, b)
		releaseHeld(t, b)
		if rest := get.drain(t); len(rest) != 0 {
			t.Fatalf("the GET stream carried %q", rest)
		}
		if got := versionOf(b); got != versionV2 {
			t.Fatalf("version %d", got)
		}
	})
}

// The refused-client line: once per client name per process, at most 16
// names; every client without a string name shares one entry and its own
// line, and an empty name is a name.
func TestEra_RefusedClientIsLoggedOncePerName(t *testing.T) {
	logs := &logCapture{}
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.Log = logs.logger() }, "--era=modern")
	named := func(id int, name any) map[string]any {
		body := eraInitialize(id).body
		body["params"].(map[string]any)["clientInfo"] = map[string]any{"name": name, "version": "1"}
		return body
	}
	nameless := eraInitialize(99).body
	delete(nameless["params"].(map[string]any), "clientInfo")
	refused := "uses MCP v1 and was refused"
	namelessLine := `A client uses MCP v1 and was refused: this server speaks MCP v2 (protocol 2026-07-28). If the server also supports v1, set its MCP version to v1 in its config entry ("mcp_version": "v1").`
	emptyLine := `The client "" uses MCP v1 and was refused: this server speaks MCP v2 (protocol 2026-07-28). If the server also supports v1, set its MCP version to v1 in its config entry ("mcp_version": "v1").`
	claude := `The client "Claude" uses MCP v1 and was refused: this server speaks MCP v2 (protocol 2026-07-28). If the server also supports v1, set its MCP version to v1 in its config entry ("mcp_version": "v1").`
	eraPost(t, b, acceptJSON, nameless, nil)
	eraPost(t, b, acceptJSON, named(98, 7), nil)
	logs.waitLogged(t, namelessLine, 1)
	eraPost(t, b, acceptJSON, named(97, ""), nil)
	logs.waitLogged(t, emptyLine, 1)
	for i := 0; i < 3; i++ {
		eraPost(t, b, acceptJSON, named(i, "Claude"), nil)
	}
	logs.waitLogged(t, claude, 1)
	for i := 0; i < 20; i++ {
		eraPost(t, b, acceptJSON, named(i, "client-"+string(rune('a'+i))), nil)
	}
	logs.waitLog(t, "the refused-client lines", func() bool { return logs.count(refused) >= maxRefusedNames })
	// A process's lines go out in order, so a second line for an earlier
	// client would have come before the last of these.
	for _, line := range []string{namelessLine, emptyLine, claude} {
		if n := logs.logged(line); n != 1 {
			t.Fatalf("%q logged %d times", line, n)
		}
	}
	// A new process logs afresh; the replaced one's queue is written out
	// before ReplaceChild returns.
	b.ReplaceChild(newEraChild(t, "--era=modern"))
	if n := logs.count(refused); n != maxRefusedNames {
		t.Fatalf("%d refused-client lines, want %d", n, maxRefusedNames)
	}
	waitForVerdict(t, b)
	eraPost(t, b, acceptJSON, named(1, "Claude"), nil)
	logs.waitLogged(t, claude, 2)
	if n := logs.closeAndCount(b, claude); n != 2 {
		t.Fatalf("after a respawn: logged %d times", n)
	}
}

// On GET and DELETE as well as POST, a session from before a restart meets
// the v2 refusal, never a 404.
func TestEra_StaleSessionOnV2IsRefusedNot404(t *testing.T) {
	b := startEraBridge(t, nil, "--era=legacy")
	sid := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil).header.Get("Mcp-Session-Id")
	b.ReplaceChild(newEraChild(t, "--era=modern"))
	waitForVerdict(t, b)
	hdr := map[string]string{"Mcp-Session-Id": sid + "-stale"}
	r := eraPost(t, b, acceptJSON, eraLegacyToolsCall(2).body, hdr)
	if r.status != 400 || !strings.Contains(r.body, "does not accept older MCP v1 requests") {
		t.Fatalf("POST: %d %s", r.status, r.body)
	}
	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		if status, _, _ := getOrDelete(t, b, method, hdr); status != 405 {
			t.Errorf("%s: %d", method, status)
		}
	}
}
