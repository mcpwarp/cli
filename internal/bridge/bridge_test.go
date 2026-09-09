package bridge

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// readSSELineContaining polls reader until a line containing want arrives
// (or a 5s deadline passes), returning that line.
func readSSELineContaining(t *testing.T, reader *bufio.Reader, want string) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		if strings.Contains(line, want) {
			return line
		}
		if err != nil {
			break
		}
	}
	t.Fatalf("did not observe a line containing %q", want)
	return ""
}

func startTestBridge(t *testing.T) *Bridge {
	t.Helper()
	bin := buildFakeMCP(t)
	b, err := StartBridge(StartBridgeOptions{
		Name:      "fake",
		SpawnSpec: SpawnSpec{Command: bin},
		Log:       testLogger(),
	})
	if err != nil {
		t.Fatalf("StartBridge: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func postJSON(t *testing.T, url string, body map[string]any, headers map[string]string) *http.Response {
	t.Helper()
	b, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func decodeBody(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer resp.Body.Close()
	var m map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	return m
}

func TestBridge_SessionBasedInitializeRoundTrip(t *testing.T) {
	b := startTestBridge(t)

	resp := postJSON(t, b.URL(), map[string]any{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": map[string]any{}}, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	sessionID := resp.Header.Get("Mcp-Session-Id")
	if sessionID == "" {
		t.Fatal("expected an Mcp-Session-Id header")
	}
	body := decodeBody(t, resp)
	if body["id"] != float64(1) {
		t.Errorf("id not rewritten back to client's original: %+v", body)
	}
	result, _ := body["result"].(map[string]any)
	if result == nil || result["ok"] != true {
		t.Errorf("unexpected result: %+v", body)
	}

	// A follow-up request without the session header is rejected.
	missing := postJSON(t, b.URL(), map[string]any{"jsonrpc": "2.0", "id": 2, "method": "ping"}, nil)
	if missing.StatusCode != 400 {
		t.Fatalf("want 400 for missing session header, got %d", missing.StatusCode)
	}
	missing.Body.Close()

	// With the header, it succeeds.
	ok := postJSON(t, b.URL(), map[string]any{"jsonrpc": "2.0", "id": 2, "method": "ping"}, map[string]string{"Mcp-Session-Id": sessionID})
	if ok.StatusCode != 200 {
		t.Fatalf("status = %d", ok.StatusCode)
	}
	okBody := decodeBody(t, ok)
	if okBody["result"] != "pong" {
		t.Fatalf("unexpected result: %+v", okBody)
	}
}

func TestBridge_StatelessRequestNoSessionHeader(t *testing.T) {
	b := startTestBridge(t)

	resp := postJSON(t, b.URL(), map[string]any{"jsonrpc": "2.0", "id": 7, "method": "ping"}, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if resp.Header.Get("Mcp-Session-Id") != "" {
		t.Fatal("stateless response must not carry a session id")
	}
	body := decodeBody(t, resp)
	if body["result"] != "pong" {
		t.Fatalf("unexpected result: %+v", body)
	}
}

func TestBridge_OriginRejected(t *testing.T) {
	b := startTestBridge(t)
	req, _ := http.NewRequest(http.MethodPost, b.URL(), strings.NewReader(`{}`))
	req.Header.Set("Origin", "http://evil.example.com")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("want 403, got %d", resp.StatusCode)
	}
}

func TestBridge_OriginAllowedForLocalhostNullAndAbsent(t *testing.T) {
	b := startTestBridge(t)
	for _, origin := range []string{"", "null", "http://localhost:3000", "http://127.0.0.1:9999/x"} {
		req, _ := http.NewRequest(http.MethodPost, b.URL(), strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("origin %q: want 200, got %d", origin, resp.StatusCode)
		}
	}
}

func TestBridge_SSEResponseCarriesFinalResult(t *testing.T) {
	b := startTestBridge(t)
	req, _ := http.NewRequest(http.MethodPost, b.URL(), strings.NewReader(
		`{"jsonrpc":"2.0","id":1,"method":"notifications/emit"}`))
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	buf, _ := io.ReadAll(resp.Body)
	text := string(buf)
	if !strings.Contains(text, `"result":{"ok":true}`) {
		t.Fatalf("expected the final response event, got: %s", text)
	}
}

// TestBridge_ProgressNotificationArrivesOnSSEResponseWithRewrittenToken
// exercises with-progress: the fixture emits one notifications/progress
// carrying the *bridge-minted* p<id> progress token before replying, and
// the bridge must rewrite it back to the client's own progressToken on
// the way out, landing on the same SSE stream as the eventual result.
func TestBridge_ProgressNotificationArrivesOnSSEResponseWithRewrittenToken(t *testing.T) {
	b := startTestBridge(t)
	req, _ := http.NewRequest(http.MethodPost, b.URL(), strings.NewReader(
		`{"jsonrpc":"2.0","id":1,"method":"with-progress","params":{"_meta":{"progressToken":"client-tok"}}}`))
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	buf, _ := io.ReadAll(resp.Body)
	text := string(buf)
	if !strings.Contains(text, `"method":"notifications/progress"`) {
		t.Fatalf("expected a progress notification event, got: %s", text)
	}
	if !strings.Contains(text, `"progressToken":"client-tok"`) {
		t.Fatalf("expected the progress token rewritten back to the client's own, got: %s", text)
	}
	if strings.Contains(text, `"progressToken":"p1"`) && !strings.Contains(text, `"client-tok"`) {
		t.Fatalf("bridge-internal progress token leaked to the client: %s", text)
	}
	if !strings.Contains(text, `"result":{"ok":true}`) {
		t.Fatalf("expected the final response event, got: %s", text)
	}
}

// TestBridge_LegacyPushStreamDeliversNotification opens a GET push stream
// then triggers notifications/emit; the fixture replies to the request
// and separately emits an id-less "notify" notification, which must land
// on the open GET stream.
func TestBridge_LegacyPushStreamDeliversNotification(t *testing.T) {
	b := startTestBridge(t)

	getReq, _ := http.NewRequest(http.MethodGet, b.URL(), nil)
	getReq.Header.Set("Accept", "text/event-stream")
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatal(err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != 200 {
		t.Fatalf("GET status = %d", getResp.StatusCode)
	}

	// Give the GET stream a turn to actually register as the open sink.
	time.Sleep(200 * time.Millisecond)

	postResp := postJSON(t, b.URL(), map[string]any{"jsonrpc": "2.0", "id": 1, "method": "notifications/emit"}, nil)
	if postResp.StatusCode != 200 {
		t.Fatalf("POST status = %d", postResp.StatusCode)
	}
	postResp.Body.Close()

	reader := bufio.NewReader(getResp.Body)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		if strings.Contains(line, `"method":"notify"`) {
			return
		}
		if err != nil {
			break
		}
	}
	t.Fatal("did not observe the pushed notification on the GET stream")
}

// TestBridge_LegacyPushStreamRaceWithDispatchOnDisconnect races a GET
// stream's client-disconnect teardown against a concurrent Dispatch (a
// notification arriving from the child): the GET handler must
// synchronously deregister and end its sink before returning, or a
// still-running Dispatch/keepalive write can land on the connection after
// net/http has already finalized it. Run under -race.
func TestBridge_LegacyPushStreamRaceWithDispatchOnDisconnect(t *testing.T) {
	for i := 0; i < 50; i++ {
		b := startTestBridge(t)
		ctx, cancel := context.WithCancel(context.Background())
		getReq, err := http.NewRequestWithContext(ctx, http.MethodGet, b.URL(), nil)
		if err != nil {
			t.Fatal(err)
		}
		getReq.Header.Set("Accept", "text/event-stream")
		done := make(chan struct{})
		go func() {
			resp, err := http.DefaultClient.Do(getReq)
			if err == nil {
				resp.Body.Close()
			}
			close(done)
		}()

		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) && !b.pushHub.HasOpenStream() {
			time.Sleep(time.Millisecond)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			cancel()
		}()
		go func() {
			defer wg.Done()
			b.pushHub.Dispatch(map[string]any{"jsonrpc": "2.0", "method": "notify"})
		}()
		wg.Wait()
		<-done
	}
}

func TestBridge_DeleteRotatesSession(t *testing.T) {
	b := startTestBridge(t)
	resp := postJSON(t, b.URL(), map[string]any{"jsonrpc": "2.0", "id": 1, "method": "initialize"}, nil)
	sessionID := resp.Header.Get("Mcp-Session-Id")
	resp.Body.Close()

	req, _ := http.NewRequest(http.MethodDelete, b.URL(), nil)
	req.Header.Set("Mcp-Session-Id", sessionID)
	del, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer del.Body.Close()
	if del.StatusCode != 204 {
		t.Fatalf("status = %d", del.StatusCode)
	}
	newID := del.Header.Get("Mcp-Session-Id")
	if newID == "" || newID == sessionID {
		t.Fatalf("expected a fresh session id, got %q (was %q)", newID, sessionID)
	}
}

func TestBridge_BatchRequestsRejected(t *testing.T) {
	b := startTestBridge(t)
	req, _ := http.NewRequest(http.MethodPost, b.URL(), strings.NewReader(`[{"jsonrpc":"2.0","id":1,"method":"ping"}]`))
	got, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	if got.StatusCode != 400 {
		t.Fatalf("want 400 for a batch request, got %d", got.StatusCode)
	}
}

// panicOnFirstWriteHeader panics on the first WriteHeader call (simulating
// some downstream panic mid-response), then behaves normally — giving
// b.handle's recover middleware something real to catch.
type panicOnFirstWriteHeader struct {
	*httptest.ResponseRecorder
	panicked bool
}

func (p *panicOnFirstWriteHeader) WriteHeader(status int) {
	if !p.panicked {
		p.panicked = true
		panic("simulated downstream panic")
	}
	p.ResponseRecorder.WriteHeader(status)
}

// TestBridge_PanicInHandlerAnswers502InsteadOfCrashing exercises a
// synchronous panic within b.handle's own call stack (an unsupported
// method, answered by writeJSON directly, with no goroutine hand-off) —
// recover() only protects the goroutine it runs in, so this deliberately
// avoids a path (like a request awaiting the child's async reply) whose
// write happens on a different goroutine.
func TestBridge_PanicInHandlerAnswers502InsteadOfCrashing(t *testing.T) {
	b := startTestBridge(t)
	rec := &panicOnFirstWriteHeader{ResponseRecorder: httptest.NewRecorder()}
	req, _ := http.NewRequest(http.MethodPut, "http://x/mcp", nil)

	b.handle(rec, req)

	if rec.Code != 502 {
		t.Fatalf("want 502 after a recovered panic, got %d", rec.Code)
	}
}

// panicOnFirstWrite lets WriteHeader through normally, then panics on the
// first body Write — simulating a downstream panic that happens after a
// real response has already started, so the recover middleware must not
// attempt a second writeJSON on top of it.
type panicOnFirstWrite struct {
	*httptest.ResponseRecorder
	headerCalls int
	writeCalls  int
}

func (p *panicOnFirstWrite) WriteHeader(status int) {
	p.headerCalls++
	p.ResponseRecorder.WriteHeader(status)
}

func (p *panicOnFirstWrite) Write(b []byte) (int, error) {
	p.writeCalls++
	if p.writeCalls == 1 {
		panic("simulated panic after headers already sent")
	}
	return p.ResponseRecorder.Write(b)
}

// TestBridge_PanicAfterHeadersSent_DoesNotWriteJSONAgain reproduces F7's
// "don't writeJSON after headers sent": once WriteHeader has actually gone
// out, a later panic (here, mid-body-write) must not trigger a second
// writeJSON — net/http would log "superfluous WriteHeader call" and the
// body would be nonsense mixed with whatever was already sent.
func TestBridge_PanicAfterHeadersSent_DoesNotWriteJSONAgain(t *testing.T) {
	b := startTestBridge(t)
	rec := &panicOnFirstWrite{ResponseRecorder: httptest.NewRecorder()}
	req, _ := http.NewRequest(http.MethodPut, "http://x/mcp", nil)

	b.handle(rec, req)

	if rec.headerCalls != 1 {
		t.Fatalf("want exactly 1 WriteHeader call (no second attempt post-panic), got %d", rec.headerCalls)
	}
	if rec.Code != 405 {
		t.Fatalf("want the original 405 to stick, got %d", rec.Code)
	}
}

// TestBridge_PanicWithErrAbortHandler_RePanics proves the recover
// middleware special-cases http.ErrAbortHandler (net/http's own sentinel
// for "abort silently") by re-panicking it rather than swallowing it into
// a bogus 502 response.
func TestBridge_PanicWithErrAbortHandler_RePanics(t *testing.T) {
	b := startTestBridge(t)
	rec := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "http://x/mcp", strings.NewReader(""))

	defer func() {
		rec := recover()
		if rec != http.ErrAbortHandler {
			t.Fatalf("want http.ErrAbortHandler to propagate, got %v", rec)
		}
	}()

	// acquireRequestSlot succeeds, handlePost reads an empty body fine and
	// proceeds to parse it as `{}` — swap in a body that panics on Read to
	// reach handle's recover with ErrAbortHandler instead.
	req.Body = panicReadCloser{}
	b.handle(rec, req)
	t.Fatal("expected a panic to propagate past b.handle")
}

// panicReadCloser panics with http.ErrAbortHandler on Read, simulating a
// client-abort scenario net/http itself would surface the same way.
type panicReadCloser struct{}

func (panicReadCloser) Read([]byte) (int, error) { panic(http.ErrAbortHandler) }
func (panicReadCloser) Close() error             { return nil }

// TestBridge_CloseContext_ReturnsEarlyOnCancellation proves CloseContext
// returns as soon as ctx is done rather than waiting out closeBarrier
// (2s) for a slow in-flight handler to drain — the old no-arg Close()
// (context.Background(), never done) still waits the barrier out.
func TestBridge_CloseContext_ReturnsEarlyOnCancellation(t *testing.T) {
	bin := buildFakeMCP(t)
	b, err := StartBridge(StartBridgeOptions{Name: "fake", SpawnSpec: SpawnSpec{Command: bin}, Log: testLogger()})
	if err != nil {
		t.Fatalf("StartBridge: %v", err)
	}

	reqDone := make(chan *http.Response, 1)
	go func() {
		resp, err := http.DefaultClient.Do(mustRequest(t, b.URL(), `{"jsonrpc":"2.0","id":1,"method":"slow","params":{"delayMs":5000}}`))
		if err != nil {
			reqDone <- nil
			return
		}
		reqDone <- resp
	}()
	_ = firstPendingID(t, b) // wait for it to register as pending/in-flight

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_ = b.CloseContext(ctx)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("CloseContext took %v — did not return early on ctx.Done()", elapsed)
	}

	resp := <-reqDone
	if resp != nil {
		resp.Body.Close()
	}
}

func TestBridge_UnknownMethodOrPath(t *testing.T) {
	b := startTestBridge(t)
	req, _ := http.NewRequest(http.MethodPut, b.URL(), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 405 {
		t.Fatalf("want 405 for PUT, got %d", resp.StatusCode)
	}
}

func TestBridge_ChildExitRejectsInFlightRequestThenReplaceChildRecovers(t *testing.T) {
	b := startTestBridge(t)

	// This test verifies the child-exit path: killing the child out from
	// under a pending request rejects it with -32000, and a subsequent
	// ReplaceChild (what the supervisor does on a crash restart) restores
	// normal routing for new requests.
	child := b.GetCurrentChild()

	done := make(chan *http.Response, 1)
	go func() {
		resp, err := http.DefaultClient.Do(mustRequest(t, b.URL(), `{"jsonrpc":"2.0","id":99,"method":"slow","params":{"delayMs":5000}}`))
		if err != nil {
			t.Log(err)
			done <- nil
			return
		}
		done <- resp
	}()

	// Wait for the request to register as pending, then kill the child
	// directly (not via Close(), which would also tear down the bridge).
	_ = firstPendingID(t, b)
	_ = child.Close()

	resp := <-done
	if resp == nil {
		t.Fatal("request errored instead of getting a response")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 502 {
		t.Fatalf("want 502 for a child that exited mid-request, got %d", resp.StatusCode)
	}
	body := decodeBody(t, resp)
	errObj, _ := body["error"].(map[string]any)
	if errObj == nil || errObj["code"] != float64(-32000) {
		t.Fatalf("expected the -32000 local-server-restarted shape, got %+v", body)
	}

	bin := buildFakeMCP(t)
	replacement, err := NewStdioChild(SpawnSpec{Command: bin}, testLogger(), "fake")
	if err != nil {
		t.Fatalf("spawn replacement: %v", err)
	}
	t.Cleanup(func() { _ = replacement.Close() })
	b.ReplaceChild(replacement)

	after := postJSON(t, b.URL(), map[string]any{"jsonrpc": "2.0", "id": 100, "method": "ping"}, nil)
	if after.StatusCode != 200 {
		t.Fatalf("want 200 after ReplaceChild, got %d", after.StatusCode)
	}
	afterBody := decodeBody(t, after)
	if afterBody["result"] != "pong" {
		t.Fatalf("unexpected result after replace: %+v", afterBody)
	}
}

// TestBridge_ChildResponseRaceWithClientDisconnect races a client context
// cancellation against the (already in-flight) child's reply: once
// resolveWith has removed the entry from b.pending, handleRegularRequest's
// ctx.Done() case must not return while resolveWith is still writing the
// response — both would otherwise write http.ResponseWriter concurrently,
// a real -race failure. Run under -race.
func firstPendingID(t *testing.T, b *Bridge) int64 {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b.mu.Lock()
		for id := range b.pending {
			b.mu.Unlock()
			return id
		}
		b.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	t.Fatal("no pending request registered in time")
	return 0
}

// TestBridge_ChildResponseRaceWithClientDisconnect races a real client
// disconnect against a child response landing for the same request. The
// response is driven directly via handleChildResponse (rather than the
// fixture's own timing) so the race doesn't depend on winning a timing
// lottery against real child I/O — this goes over the real net/http
// server (not a direct handlePost/httptest.Recorder call), since the
// actual failure mode is net/http's own connection bookkeeping racing a
// still-writing background goroutine after the handler has returned.
func TestBridge_ChildResponseRaceWithClientDisconnect(t *testing.T) {
	for i := 0; i < 50; i++ {
		b := startTestBridge(t)
		ctx, cancel := context.WithCancel(context.Background())
		// "slow" won't reply from the child side for a long while.
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.URL(), strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"slow","params":{"delayMs":60000}}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		done := make(chan struct{})
		go func() {
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				resp.Body.Close()
			}
			close(done)
		}()

		id := firstPendingID(t, b)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			cancel()
		}()
		go func() {
			defer wg.Done()
			b.handleChildResponse(id, map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"ok": true}})
		}()
		wg.Wait()
		<-done
	}
}

func mustRequest(t *testing.T, url, body string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	return req
}

// TestBridge_LegacyGETStream_EndsClientSideAfterChildExit reproduces F2:
// the legacy GET push stream (pre-2025-11-25 servers) must reach
// client-side EOF once the child exits and pushHub.Close() tears the sink
// down server-side — not just stop accepting new writes while leaving the
// GET handler (and so the HTTP response) hanging open indefinitely.
func TestBridge_LegacyGETStream_EndsClientSideAfterChildExit(t *testing.T) {
	b := startTestBridge(t)

	req, err := http.NewRequest(http.MethodGet, b.URL(), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	time.Sleep(200 * time.Millisecond) // let it register as the open push sink

	child := b.GetCurrentChild()
	_ = child.Close()

	eofCh := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, resp.Body)
		eofCh <- err
	}()

	select {
	case <-eofCh:
	case <-time.After(2 * time.Second):
		t.Fatal("GET push stream never reached client-side EOF after the child exited")
	}
}

// TestBridge_SubscriptionsListenPushFinishRewritesTagToClientID exercises
// the subscriptions map end to end: acknowledge, a pushed notification,
// and the final response are all tagged/addressed in the *client's* id
// space, even though subscriptions/push and subscriptions/finish below
// address the subscription by the bridge-minted internal id (1, since
// this is the first request on a fresh bridge).
func TestBridge_SubscriptionsListenPushFinishRewritesTagToClientID(t *testing.T) {
	b := startTestBridge(t)

	req, _ := http.NewRequest(http.MethodPost, b.URL(), strings.NewReader(
		`{"jsonrpc":"2.0","id":"client-sub","method":"subscriptions/listen","params":{}}`))
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	reader := bufio.NewReader(resp.Body)

	ack := readSSELineContaining(t, reader, "notifications/subscriptions/acknowledged")
	if !strings.Contains(ack, `"io.modelcontextprotocol/subscriptionId":"client-sub"`) {
		t.Fatalf("ack not tagged with the client's own subscription id: %s", ack)
	}

	pushResp := postJSON(t, b.URL(), map[string]any{
		"jsonrpc": "2.0", "method": "subscriptions/push",
		"params": map[string]any{"subscriptionId": 1, "notifyMethod": "sub/event", "notifyParams": map[string]any{"n": 7}},
	}, nil)
	if pushResp.StatusCode != 202 {
		t.Fatalf("push notification status = %d", pushResp.StatusCode)
	}
	pushResp.Body.Close()

	pushed := readSSELineContaining(t, reader, "sub/event")
	if !strings.Contains(pushed, `"io.modelcontextprotocol/subscriptionId":"client-sub"`) {
		t.Fatalf("pushed notification not rewritten to the client's subscription id: %s", pushed)
	}

	finishResp := postJSON(t, b.URL(), map[string]any{
		"jsonrpc": "2.0", "method": "subscriptions/finish",
		"params": map[string]any{"subscriptionId": 1, "result": map[string]any{"resultType": "complete"}},
	}, nil)
	if finishResp.StatusCode != 202 {
		t.Fatalf("finish notification status = %d", finishResp.StatusCode)
	}
	finishResp.Body.Close()

	final := readSSELineContaining(t, reader, `"id":"client-sub"`)
	if !strings.Contains(final, `"resultType":"complete"`) {
		t.Fatalf("expected the final subscription response, got: %s", final)
	}
}

// TestBridge_ServerInitiatedRequestRoutedByShapeEvenOnIDCollision proves
// server-initiated requests are routed to the legacy push stream by
// shape (has both id and method) rather than by id, even when the
// server's own request id collides with the bridge's internal id for a
// concurrently in-flight client request.
func TestBridge_ServerInitiatedRequestRoutedByShapeEvenOnIDCollision(t *testing.T) {
	b := startTestBridge(t)

	getReq, _ := http.NewRequest(http.MethodGet, b.URL(), nil)
	getReq.Header.Set("Accept", "text/event-stream")
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatal(err)
	}
	defer getResp.Body.Close()
	time.Sleep(200 * time.Millisecond) // let it register as the open push sink

	// The first request on a fresh bridge is minted internal id 1 — the
	// same id the fixture's server/request-1 assigns its own
	// server-initiated request below.
	reqDone := make(chan *http.Response, 1)
	go func() {
		resp, err := http.DefaultClient.Do(mustRequest(t, b.URL(), `{"jsonrpc":"2.0","id":55,"method":"server/request-1"}`))
		if err != nil {
			t.Log(err)
			reqDone <- nil
			return
		}
		reqDone <- resp
	}()

	reader := bufio.NewReader(getResp.Body)
	serverReqLine := readSSELineContaining(t, reader, `"method":"server/ping"`)
	if !strings.Contains(serverReqLine, `"id":1`) {
		t.Fatalf("expected the server-initiated request's own colliding id 1, got: %s", serverReqLine)
	}

	// Answer it in the child's own id space (id 1, no method) — a client
	// answering a server-initiated request, never id-rewritten.
	ansResp := postJSON(t, b.URL(), map[string]any{"jsonrpc": "2.0", "id": 1, "result": map[string]any{"pong": true}}, nil)
	if ansResp.StatusCode != 202 {
		t.Fatalf("answer status = %d", ansResp.StatusCode)
	}
	ansResp.Body.Close()

	final := <-reqDone
	if final == nil {
		t.Fatal("original client request errored")
	}
	defer final.Body.Close()
	if final.StatusCode != 200 {
		t.Fatalf("status = %d", final.StatusCode)
	}
	body := decodeBody(t, final)
	if body["id"] != float64(55) || body["result"] == nil {
		t.Fatalf("unexpected final response: %+v", body)
	}
}

// TestBridge_DisconnectSendsSynthesizedNotificationsCancelled covers one
// direction of notifications/cancelled: the bridge's own synthesized
// notification, sent to the child (addressed by internal id) when a
// client disconnects mid-request.
func TestBridge_DisconnectSendsSynthesizedNotificationsCancelled(t *testing.T) {
	b := startTestBridge(t)

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.URL(), strings.NewReader(
		`{"jsonrpc":"2.0","id":"abc","method":"slow","params":{"delayMs":60000}}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	done := make(chan struct{})
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		close(done)
	}()
	_ = firstPendingID(t, b) // wait for it to register as pending
	cancel()
	<-done

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		askResp := postJSON(t, b.URL(), map[string]any{"jsonrpc": "2.0", "id": 999, "method": "debug/wasCancelled", "params": map[string]any{"requestId": float64(1)}}, nil)
		askBody := decodeBody(t, askResp)
		if result, _ := askBody["result"].(map[string]any); result != nil && result["cancelled"] == true {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("child never observed notifications/cancelled for internal id 1")
}

// TestBridge_ClientSentNotificationsCancelledRewritesToInternalID covers
// the other direction: a legacy client's own notifications/cancelled,
// naming the request by the client's original id, must have
// params.requestId rewritten to the internal id before reaching the
// child.
func TestBridge_ClientSentNotificationsCancelledRewritesToInternalID(t *testing.T) {
	b := startTestBridge(t)

	go func() {
		resp, err := http.DefaultClient.Do(mustRequest(t, b.URL(), `{"jsonrpc":"2.0","id":"client-id","method":"hang"}`))
		if err == nil {
			resp.Body.Close()
		}
	}()
	_ = firstPendingID(t, b)

	cancelResp := postJSON(t, b.URL(), map[string]any{
		"jsonrpc": "2.0", "method": "notifications/cancelled",
		"params": map[string]any{"requestId": "client-id"},
	}, nil)
	if cancelResp.StatusCode != 202 {
		t.Fatalf("cancel status = %d", cancelResp.StatusCode)
	}
	cancelResp.Body.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		askResp := postJSON(t, b.URL(), map[string]any{"jsonrpc": "2.0", "id": 999, "method": "debug/wasCancelled", "params": map[string]any{"requestId": float64(1)}}, nil)
		askBody := decodeBody(t, askResp)
		if result, _ := askBody["result"].(map[string]any); result != nil && result["cancelled"] == true {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("requestId was not rewritten to internal id 1")
}

// TestBridge_TimeoutMarksAbandonedAndDropsLateResponse exercises the
// request-timeout path with a short injected timeout: a late child
// response for the now-abandoned id must be dropped, not misdelivered to
// a later request reusing the connection, and the bridge must keep
// serving new requests normally afterward.
func TestBridge_TimeoutMarksAbandonedAndDropsLateResponse(t *testing.T) {
	bin := buildFakeMCP(t)
	b, err := StartBridge(StartBridgeOptions{
		Name:           "fake",
		SpawnSpec:      SpawnSpec{Command: bin},
		Log:            testLogger(),
		RequestTimeout: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("StartBridge: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	// A cheap warm-up round trip first, so the timeout below is measuring
	// the fixture's actual reply delay rather than process-spawn/first-
	// exchange overhead in a slow sandbox.
	warmup := postJSON(t, b.URL(), map[string]any{"jsonrpc": "2.0", "id": 0, "method": "ping"}, nil)
	decodeBody(t, warmup)

	resp := postJSON(t, b.URL(), map[string]any{"jsonrpc": "2.0", "id": 1, "method": "slow", "params": map[string]any{"delayMs": 3000}}, nil)
	if resp.StatusCode != 504 {
		t.Fatalf("want 504 upstream timeout, got %d", resp.StatusCode)
	}
	body := decodeBody(t, resp)
	if body["error"] != "upstream timeout" {
		t.Fatalf("unexpected body: %+v", body)
	}

	after := postJSON(t, b.URL(), map[string]any{"jsonrpc": "2.0", "id": 2, "method": "ping"}, nil)
	if after.StatusCode != 200 {
		t.Fatalf("status = %d", after.StatusCode)
	}
	decodeBody(t, after)

	// Let the fixture's late "slow" reply (for the now-abandoned id 1)
	// land and be silently dropped.
	time.Sleep(2800 * time.Millisecond)
}

// TestBridge_ConcurrentRequestsGetDistinctInternalIDsRewrittenBack races
// many concurrent client requests through one bridge, each with its own
// client-chosen id, verifying the bridge's internal id rewriting never
// crosses wires between them.
func TestBridge_ConcurrentRequestsGetDistinctInternalIDsRewrittenBack(t *testing.T) {
	b := startTestBridge(t)
	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp := postJSON(t, b.URL(), map[string]any{"jsonrpc": "2.0", "id": i, "method": "ping"}, nil)
			defer resp.Body.Close()
			body := decodeBody(t, resp)
			if body["id"] != float64(i) {
				errs <- fmt.Errorf("client id %d: got id %v back", i, body["id"])
				return
			}
			if body["result"] != "pong" {
				errs <- fmt.Errorf("client id %d: unexpected result %+v", i, body)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
