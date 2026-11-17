package bridge

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// subscriptions/listen: the era gate, the held head, the per-listen queue and
// how the child ends one. They run against fakemcp --era=<mode> --listen; the
// fake's debug/raw writes the child's side of a scenario in a chosen order.

const listenWait = 10 * time.Second

// sseStream reads an SSE body on its own goroutine, so a test waits for an
// event with a timeout instead of blocking in Read.
type sseStream struct {
	lines chan string
	seen  []string
}

func newSSEStream(body io.Reader) *sseStream {
	s := &sseStream{lines: make(chan string, 4096)}
	go func() {
		defer close(s.lines)
		br := bufio.NewReader(body)
		for {
			line, err := br.ReadString('\n')
			if line = strings.TrimRight(line, "\r\n"); line != "" {
				s.lines <- line
			}
			if err != nil {
				return
			}
		}
	}()
	return s
}

// next returns the next non-empty line, false at EOF.
func (s *sseStream) next(t *testing.T) (string, bool) {
	t.Helper()
	select {
	case line, ok := <-s.lines:
		if ok {
			s.seen = append(s.seen, line)
		}
		return line, ok
	case <-time.After(listenWait):
		t.Fatalf("timed out waiting for the next SSE line; seen so far: %q", s.seen)
	}
	return "", false
}

func (s *sseStream) waitFor(t *testing.T, sub string) string {
	t.Helper()
	for {
		line, ok := s.next(t)
		if !ok {
			t.Fatalf("stream ended before a line containing %q; seen: %q", sub, s.seen)
		}
		if strings.Contains(line, sub) {
			return line
		}
	}
}

// drain reads to EOF and returns what it read.
func (s *sseStream) drain(t *testing.T) []string {
	t.Helper()
	var got []string
	for {
		line, ok := s.next(t)
		if !ok {
			return got
		}
		got = append(got, line)
	}
}

func (s *sseStream) sawAny(sub string) bool {
	for _, l := range s.seen {
		if strings.Contains(l, sub) {
			return true
		}
	}
	return false
}

// frameData decodes the JSON of a "data: " line, keeping numbers as written.
func frameData(t *testing.T, line string) map[string]any {
	t.Helper()
	text, ok := strings.CutPrefix(line, "data: ")
	if !ok {
		t.Fatalf("not a data line: %q", line)
	}
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("data line is not JSON: %q", line)
	}
	return m
}

// listenCall is a subscriptions/listen POST in flight.
type listenCall struct {
	cancel context.CancelFunc
	head   chan *http.Response // closed without a value if the request failed
}

func startListen(t *testing.T, b *Bridge, clientID any, progressToken any) *listenCall {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := &listenCall{cancel: cancel, head: make(chan *http.Response, 1)}
	body := modernBody(clientID, "subscriptions/listen", nil, progressToken)
	go func() {
		resp, _, err := eraSend(ctx, b.URL(), acceptSSE, body, nil)
		if err != nil {
			close(c.head)
			return
		}
		c.head <- resp
	}()
	return c
}

// response waits for the head.
func (c *listenCall) response(t *testing.T) *http.Response {
	t.Helper()
	select {
	case resp, ok := <-c.head:
		if !ok {
			t.Fatal("listen request failed before its head arrived")
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	case <-time.After(listenWait):
		t.Fatal("timed out waiting for the listen's head")
	}
	return nil
}

// stream waits for a 200 stream head.
func (c *listenCall) stream(t *testing.T) *sseStream {
	t.Helper()
	resp := c.response(t)
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("listen head: %d %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	return newSSEStream(resp.Body)
}

// openListen starts a listen against a child that acknowledges it and returns
// its stream once the acknowledgement is in.
func openListen(t *testing.T, b *Bridge, clientID any) (*listenCall, *sseStream, int64) {
	t.Helper()
	c := startListen(t, b, clientID, nil)
	s := c.stream(t)
	s.waitFor(t, "notifications/subscriptions/acknowledged")
	return c, s, waitListenID(t, b, clientID)
}

func waitListenID(t *testing.T, b *Bridge, clientID any) int64 {
	t.Helper()
	var id int64
	waitFor(t, fmt.Sprintf("listen %v to register", clientID), func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		for k, l := range b.listens {
			if l.originalID == clientID {
				id = k
				return true
			}
		}
		return false
	})
	return id
}

func listenCount(b *Bridge) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.listens)
}

// childListens is what the fake has seen of listens and not seen cancelled.
func childListens(t *testing.T, b *Bridge) []any {
	t.Helper()
	ids, _ := debugCall(t, b, "debug/listens", nil)["ids"].([]any)
	return ids
}

func openGet(t *testing.T, b *Bridge) *sseStream {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, b.URL(), nil)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	if resp.StatusCode != 200 {
		t.Fatalf("GET: %d", resp.StatusCode)
	}
	waitFor(t, "the GET stream to register", b.pushHub.HasOpenStream)
	return newSSEStream(resp.Body)
}

func jsonLine(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// rawLines has the fake write lines to the bridge verbatim, in one write.
func rawLines(t *testing.T, b *Bridge, lines ...string) {
	t.Helper()
	args := make([]any, len(lines))
	for i, l := range lines {
		args[i] = l
	}
	if got, _ := debugCall(t, b, "debug/raw", map[string]any{"lines": args})["written"].(float64); int(got) != len(lines) {
		t.Fatalf("debug/raw wrote %v of %d lines", got, len(lines))
	}
}

func taggedLine(t *testing.T, method string, tag any, params map[string]any) string {
	p := map[string]any{"_meta": map[string]any{subscriptionIDMetaKey: tag}}
	for k, v := range params {
		p[k] = v
	}
	return jsonLine(t, map[string]any{"jsonrpc": "2.0", "method": method, "params": p})
}

func plainNotification(t *testing.T, method string) string {
	return jsonLine(t, map[string]any{"jsonrpc": "2.0", "method": method, "params": map[string]any{}})
}

func childCancelLine(t *testing.T, requestID any) string {
	return jsonLine(t, map[string]any{"jsonrpc": "2.0", "method": "notifications/cancelled", "params": map[string]any{"requestId": requestID}})
}

func childResultLine(t *testing.T, id any, result map[string]any) string {
	return jsonLine(t, map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

const listenFake, listenFakeDual = "--era=modern", "--era=dual"

func TestListen_ChildWithoutListenSupportIs404(t *testing.T) {
	b := startEraBridge(t, nil, listenFake)
	r := eraPost(t, b, acceptSSE, modernBody("L1", "subscriptions/listen", nil, nil), nil)
	if r.status != 404 || !strings.HasPrefix(r.header.Get("Content-Type"), "application/json") {
		t.Fatalf("status %d %q: %s", r.status, r.header.Get("Content-Type"), r.body)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(r.body), &m); err != nil {
		t.Fatal(err)
	}
	if e, _ := m["error"].(map[string]any); e == nil || e["code"] != float64(-32601) || m["id"] != "L1" {
		t.Fatalf("body: %s", r.body)
	}
	if n := listenCount(b); n != 0 {
		t.Fatalf("%d listens left registered", n)
	}
}

func TestListen_HeadCommitsOnAcknowledgement(t *testing.T) {
	// The hold never expires, so only the acknowledgement can have committed
	// the head.
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.HeadHoldTimeout = time.Hour }, listenFake, "--listen")
	s := startListen(t, b, "L1", nil).stream(t)
	if ack := s.waitFor(t, "acknowledged"); !strings.Contains(ack, `"io.modelcontextprotocol/subscriptionId":"L1"`) {
		t.Fatalf("acknowledgement is not tagged with the client's id: %s", ack)
	}
}

func TestListen_HeadCommitsAtHoldWithoutAcknowledgement(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.HeadHoldTimeout = 100 * time.Millisecond }, listenFake, "--listen-no-ack")
	c := startListen(t, b, "L1", nil)
	s := c.stream(t) // nothing was queued: the hold deadline committed it
	id := waitListenID(t, b, "L1")
	rawLines(t, b, taggedLine(t, "x/event", id, nil))
	s.waitFor(t, `"io.modelcontextprotocol/subscriptionId":"L1"`)
}

func TestListen_OutlivesHeadHoldAndRequestTimeout(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) {
		o.HeadHoldTimeout = 100 * time.Millisecond
		o.RequestTimeout = 200 * time.Millisecond
	}, listenFake, "--listen-no-ack")
	// Without an acknowledgement only the hold deadline can commit the head.
	c := startListen(t, b, "L1", nil)
	s := c.stream(t)
	id := waitListenID(t, b, "L1")

	// A request begun after the listen is cut by the request timeout, which
	// puts the listen's own age past it.
	r := eraPost(t, b, acceptJSON, modernBody(1, "debug/heldError", map[string]any{"code": -32000}, nil), nil)
	if r.status != 504 {
		t.Fatalf("setup: the request timeout did not fire: %d %s", r.status, r.body)
	}
	if listenCount(b) != 1 {
		t.Fatal("the listen was cut")
	}
	rawLines(t, b, taggedLine(t, "x/event", id, nil))
	s.waitFor(t, "x/event")
}

func TestListen_ChildCancelWhileHeadHeldIsAnEmpty200(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.HeadHoldTimeout = time.Hour }, listenFake, "--listen-no-ack")
	c := startListen(t, b, "L1", nil)
	id := waitListenID(t, b, "L1")
	waitFor(t, "the child to see the listen", func() bool { return len(childListens(t, b)) == 1 })
	rawLines(t, b, childCancelLine(t, id))
	s := c.stream(t)
	if rest := s.drain(t); len(rest) != 0 {
		t.Fatalf("expected an empty stream, got %q", rest)
	}
}

func TestListen_SendFailureWhileHeadHeldIs502(t *testing.T) {
	b := startEraBridge(t, nil, listenFake, "--listen")
	dead := newBareChild()
	dead.exited = true
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Header.Set("Accept", acceptSSE)
	b.handleSubscriptionsListen(rec, req, modernBody("L1", "subscriptions/listen", nil, nil), dead)
	if rec.Code != 502 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var m map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &m)
	if m["error"] != "local server is not running" {
		t.Fatalf("body: %s", rec.Body)
	}
	if listenCount(b) != 0 {
		t.Fatal("the failed listen is still registered")
	}
}

func TestListen_WithoutEventStreamAcceptIs406(t *testing.T) {
	b := startEraBridge(t, nil, listenFake, "--listen")
	for _, accept := range []string{"application/json", "text/event-stream;q=0", "application/json, text/event-stream; q=0.0", "text/*", ""} {
		r := eraPost(t, b, accept, modernBody("L1", "subscriptions/listen", nil, nil), nil)
		if r.status != 406 {
			t.Fatalf("Accept %q: status %d: %s", accept, r.status, r.body)
		}
		var m map[string]any
		_ = json.Unmarshal([]byte(r.body), &m)
		e, _ := m["error"].(map[string]any)
		if e == nil || e["code"] != float64(-32600) || e["message"] != listenNeedsSSE || m["id"] != "L1" {
			t.Fatalf("Accept %q: body %s", accept, r.body)
		}
	}
	if n := len(childListens(t, b)); n != 0 || listenCount(b) != 0 {
		t.Fatalf("a refused listen reached the child (%d) or the bridge (%d)", n, listenCount(b))
	}
}

func TestAcceptsEventStream(t *testing.T) {
	for accept, want := range map[string]bool{
		"text/event-stream":                   true,
		"application/json, text/event-stream": true,
		"TEXT/Event-Stream":                   true,
		"text/event-stream;q=0.5":             true,
		"text/event-stream; q=0":              false,
		"text/event-stream;q=0.000":           false,
		"application/json":                    false,
		"text/*, */*":                         false,
		"":                                    false,
		"application/json;q=0, text/event-stream":    true,
		"text/event-stream-not, application/json":    false,
		"text/event-stream;charset=utf-8; q=0.8,x/y": true,
	} {
		if got := acceptsEventStream([]string{accept}); got != want {
			t.Errorf("%q: got %v, want %v", accept, got, want)
		}
	}
	if !acceptsEventStream([]string{"application/json", "text/event-stream"}) {
		t.Error("Accept sent as two header lines")
	}
}

func TestListen_CapsAreSeparateFromPending(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) {
		o.MaxPending = 1
		o.MaxListens = 1
	}, listenFake, "--listen")
	a, _, _ := openListen(t, b, "A")

	// An open listen leaves the one pending slot free.
	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 200 {
		t.Fatalf("request beside a listen: %d %s", r.status, r.body)
	}
	r := eraPost(t, b, acceptSSE, modernBody("B", "subscriptions/listen", nil, nil), nil)
	var m map[string]any
	_ = json.Unmarshal([]byte(r.body), &m)
	if r.status != 503 || m["error"] == nil {
		t.Fatalf("second listen: %d %s", r.status, r.body)
	}

	a.cancel()
	waitFor(t, "the first listen's slot to free", func() bool { return listenCount(b) == 0 })
	startListen(t, b, "C", nil).stream(t)
}

func TestListen_PendingRequestsDoNotCountAgainstTheListenCap(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.MaxListens = 1 }, listenFake, "--listen")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		if resp, _, err := eraSend(ctx, b.URL(), acceptJSON, modernBody(1, "debug/heldError", map[string]any{"code": -32000}, nil), nil); err == nil {
			resp.Body.Close()
		}
	}()
	firstPendingID(t, b)
	startListen(t, b, "A", nil).stream(t)
}

func TestListen_WaitingBehindTheProbeIsNotCounted(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.MaxListens = 1 }, listenFakeDual, "--listen", "--discover-hold")
	// The cap is already full, as an earlier listen would leave it.
	occupant := &listenEntry{originalID: "occupant", child: b.GetCurrentChild(), budget: 1, wake: make(chan struct{}, 1)}
	b.mu.Lock()
	b.listens[firstBridgeID+900] = occupant
	b.mu.Unlock()
	t.Cleanup(func() {
		b.mu.Lock()
		delete(b.listens, firstBridgeID+900)
		b.mu.Unlock()
	})

	c := startListen(t, b, "W1", nil)
	// The request starts the probe itself, so a probe in flight means it has
	// reached the era gate; a cap checked before the gate would have refused
	// it there and no probe would ever start.
	waitProbeInFlight(t, b)
	select {
	case <-c.head:
		t.Fatal("the listen was answered before the probe was")
	default:
	}
	releaseHeld(t, b)
	if got := c.response(t).StatusCode; got != 503 {
		t.Fatalf("after the probe: %d, want 503 from the full cap", got)
	}
}

func TestListen_OpenListensDoNotBlockInitialize(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) {
		o.MaxPending = 1
		o.MaxListens = 1
	}, listenFakeDual, "--listen")
	openListen(t, b, "A")
	r := eraPost(t, b, acceptJSON, eraInitialize(5).body, nil)
	if r.status != 200 || r.header.Get("Mcp-Session-Id") == "" {
		t.Fatalf("initialize beside a listen: %d %s", r.status, r.body)
	}
}

func TestListen_UnroutableChildMessagesAreDroppedNotPushed(t *testing.T) {
	b := startEraBridge(t, nil, listenFakeDual, "--listen")
	get := openGet(t, b)
	_, s, id := openListen(t, b, "L1")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		if resp, _, err := eraSend(ctx, b.URL(), acceptJSON, modernBody(1, "debug/heldError", map[string]any{"code": -32000}, nil), nil); err == nil {
			resp.Body.Close()
		}
	}()
	plainID := firstPendingID(t, b)

	rawLines(t, b,
		taggedLine(t, "x/orphan", id+1000, nil),
		taggedLine(t, "x/plain", plainID, nil),
		taggedLine(t, "x/string", "L1", nil),
		taggedLine(t, "x/fraction", json.RawMessage(fmt.Sprintf("%d.5", id)), nil),
		taggedLine(t, "x/small", 5, nil),
		taggedLine(t, "x/null", nil, nil),
		jsonLine(t, map[string]any{"jsonrpc": "2.0", "id": "str-1", "result": map[string]any{}}),
		plainNotification(t, "x/ok"),
	)
	get.waitFor(t, "x/ok")
	for _, leaked := range []string{"x/orphan", "x/plain", "x/string", "x/fraction", "x/small", "x/null", "str-1"} {
		if get.sawAny(leaked) {
			t.Errorf("%s reached the GET stream", leaked)
		}
	}
	rawLines(t, b, taggedLine(t, "x/event", id, nil))
	s.waitFor(t, "x/event")
	if s.sawAny("x/orphan") || s.sawAny("x/string") || s.sawAny("x/fraction") {
		t.Errorf("an unroutable message reached the listen: %q", s.seen)
	}
}

// A child's cancel for a listen ends its stream and stays off the GET stream.
func TestListen_ChildCancelEndsTheStreamAndIsNotRelayed(t *testing.T) {
	b := startEraBridge(t, nil, listenFakeDual, "--listen")
	get := openGet(t, b)
	_, s, id := openListen(t, b, "L1")
	rawLines(t, b, childCancelLine(t, id), plainNotification(t, "x/marker"))
	if rest := s.drain(t); len(rest) != 0 {
		t.Fatalf("lines after the cancel: %q", rest)
	}
	get.waitFor(t, "x/marker")
	if get.sawAny("notifications/cancelled") {
		t.Fatal("the cancel was relayed to the GET stream")
	}
	if childWasCancelled(t, b, float64(id)) {
		t.Fatal("the bridge cancelled a listen the child had ended")
	}
	if listenCount(b) != 0 {
		t.Fatal("the listen is still registered")
	}
}

func TestListen_ResultThenCancelKeepsTheResult(t *testing.T) {
	b := startEraBridge(t, nil, listenFakeDual, "--listen")
	get := openGet(t, b)
	_, s, id := openListen(t, b, "L1")
	rawLines(t, b,
		childResultLine(t, id, map[string]any{"resultType": "complete"}),
		childCancelLine(t, id),
		plainNotification(t, "x/marker"),
	)
	rest := s.drain(t)
	if len(rest) != 2 || !strings.HasPrefix(rest[1], "data: ") {
		t.Fatalf("stream after the result: %q", rest)
	}
	final := frameData(t, rest[1])
	if final["id"] != "L1" || final["result"] == nil {
		t.Fatalf("final: %v", final)
	}
	get.waitFor(t, "x/marker")
	if get.sawAny("notifications/cancelled") {
		t.Fatal("the later cancel leaked to the GET stream")
	}
	if childWasCancelled(t, b, float64(id)) {
		t.Fatal("the bridge cancelled a listen the child had answered")
	}
}

func TestListen_CancelThenResultEndsWithoutTheResult(t *testing.T) {
	b := startEraBridge(t, nil, listenFakeDual, "--listen")
	get := openGet(t, b)
	_, s, id := openListen(t, b, "L1")
	rawLines(t, b,
		childCancelLine(t, id),
		childResultLine(t, id, map[string]any{"resultType": "complete"}),
		plainNotification(t, "x/marker"),
	)
	if rest := s.drain(t); len(rest) != 0 {
		t.Fatalf("the stream carried %q after the cancel", rest)
	}
	get.waitFor(t, "x/marker")
	if get.sawAny("resultType") {
		t.Fatal("the late result leaked to the GET stream")
	}
}

func TestListen_ClientClosingTheStreamCancelsAtTheChild(t *testing.T) {
	b := startEraBridge(t, nil, listenFake, "--listen")
	c, _, id := openListen(t, b, "L1")
	c.cancel()
	waitFor(t, "the listen to be freed", func() bool { return listenCount(b) == 0 })
	waitFor(t, "the child to see the cancel", func() bool { return childWasCancelled(t, b, float64(id)) })
	waitFor(t, "the child's listen to close", func() bool { return len(childListens(t, b)) == 0 })
	if !b.isAbandoned(id) {
		t.Fatal("the cancelled listen's id is not remembered")
	}
}

func TestListen_Legacy2025ShapedListenIsOrdinaryTraffic(t *testing.T) {
	t.Run("answered like any request", func(t *testing.T) {
		b := startEraBridge(t, nil, listenFakeDual)
		session := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil).header.Get("Mcp-Session-Id")
		r := eraPost(t, b, acceptJSON, map[string]any{"jsonrpc": "2.0", "id": "L1", "method": "subscriptions/listen", "params": map[string]any{}},
			map[string]string{"Mcp-Session-Id": session})
		var m map[string]any
		_ = json.Unmarshal([]byte(r.body), &m)
		if e, _ := m["error"].(map[string]any); r.status != 200 || e == nil || e["code"] != float64(-32601) || m["id"] != "L1" {
			t.Fatalf("%d %s", r.status, r.body)
		}
	})
	t.Run("counted in pending", func(t *testing.T) {
		b := startTestBridge(t) // the plain fake takes a listen and never answers
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		go func() {
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, b.URL(), strings.NewReader(
				`{"jsonrpc":"2.0","id":"L1","method":"subscriptions/listen","params":{}}`))
			req.Header.Set("Accept", "text/event-stream")
			if resp, err := http.DefaultClient.Do(req); err == nil {
				resp.Body.Close()
			}
		}()
		firstPendingID(t, b)
		if listenCount(b) != 0 {
			t.Fatal("a 2025-shaped listen was registered as a listen")
		}
	})
	t.Run("without an id it is a notification", func(t *testing.T) {
		b := startEraBridge(t, nil, listenFake, "--listen")
		body := modernBody(nil, "subscriptions/listen", nil, nil)
		delete(body, "id")
		r := eraPost(t, b, acceptSSE, body, nil)
		if r.status != 202 || listenCount(b) != 0 {
			t.Fatalf("%d %s (%d listens)", r.status, r.body, listenCount(b))
		}
	})
}

func TestListen_ResultTagIsRewrittenToTheClientsID(t *testing.T) {
	b := startEraBridge(t, nil, listenFake, "--listen")
	for _, clientID := range []any{"str-id", float64(7)} {
		_, s, id := openListen(t, b, clientID)
		rawLines(t, b, childResultLine(t, id, map[string]any{
			"resultType": "complete",
			"_meta":      map[string]any{subscriptionIDMetaKey: id},
		}))
		rest := s.drain(t)
		final := frameData(t, rest[len(rest)-1])
		meta := final["result"].(map[string]any)["_meta"].(map[string]any)
		want := fmt.Sprint(clientID)
		if _, isString := clientID.(string); !isString {
			want = "7"
		}
		if fmt.Sprint(meta[subscriptionIDMetaKey]) != want || fmt.Sprint(final["id"]) != want {
			t.Fatalf("client id %v: id %v tag %v", clientID, final["id"], meta[subscriptionIDMetaKey])
		}
		_, tagIsString := meta[subscriptionIDMetaKey].(string)
		if _, isString := clientID.(string); tagIsString != isString {
			t.Fatalf("client id %v: tag %#v changed type", clientID, meta[subscriptionIDMetaKey])
		}
	}
}

func TestListen_ProgressTokenRoundTrips(t *testing.T) {
	b := startEraBridge(t, nil, listenFake, "--listen")
	c := startListen(t, b, "L1", "client-tok")
	s := c.stream(t)
	id := waitListenID(t, b, "L1")
	rawLines(t, b, jsonLine(t, map[string]any{"jsonrpc": "2.0", "method": "notifications/progress",
		"params": map[string]any{"progressToken": internalProgressToken(id), "progress": 1}}))
	line := s.waitFor(t, "notifications/progress")
	if !strings.Contains(line, `"progressToken":"client-tok"`) {
		t.Fatalf("progress not rewritten to the client's token: %s", line)
	}
}

func TestListen_BridgeIDsStartAbove2p30(t *testing.T) {
	b := startTestBridge(t)
	go func() {
		resp, err := http.DefaultClient.Do(mustRequest(t, b.URL(), `{"jsonrpc":"2.0","id":1,"method":"hang"}`))
		if err == nil {
			resp.Body.Close()
		}
	}()
	if id := firstPendingID(t, b); id < firstBridgeID || id > maxBridgeID {
		t.Fatalf("first minted id %d is outside [%d, %d]", id, firstBridgeID, maxBridgeID)
	}
}

func TestMintID_WrapsAndSkipsIDsInUse(t *testing.T) {
	b := &Bridge{
		pending:   map[int64]*pendingEntry{maxBridgeID: {}},
		listens:   map[int64]*listenEntry{firstBridgeID: {}},
		abandoned: map[int64]struct{}{firstBridgeID + 1: {}},
		nextID:    maxBridgeID,
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if id := b.mintIDLocked(); id != firstBridgeID+2 {
		t.Fatalf("minted %d, want %d", id, int64(firstBridgeID+2))
	}
	if id := b.mintIDLocked(); id != firstBridgeID+3 {
		t.Fatalf("minted %d, want %d", id, int64(firstBridgeID+3))
	}
	// The ceiling itself is a valid id; the next one after it wraps, past
	// everything still in use.
	delete(b.pending, maxBridgeID)
	b.nextID = maxBridgeID
	if id := b.mintIDLocked(); id != maxBridgeID {
		t.Fatalf("minted %d at the ceiling, want %d", id, int64(maxBridgeID))
	}
	if id := b.mintIDLocked(); id != firstBridgeID+2 {
		t.Fatalf("minted %d after the wrap, want %d", id, int64(firstBridgeID+2))
	}
}

func TestBridgeIDFrom(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   any
		want int64
		ok   bool
	}{
		{"first", float64(firstBridgeID), firstBridgeID, true},
		{"integral float form", float64(firstBridgeID + 7), firstBridgeID + 7, true},
		{"ceiling", float64(maxBridgeID), maxBridgeID, true},
		{"json.Number integer", json.Number("1073741825"), 1073741825, true},
		{"json.Number float form", json.Number("1073741825.0"), 1073741825, true},
		{"json.Number fraction", json.Number("1073741825.5"), 0, false},
		{"json.Number junk", json.Number("abc"), 0, false},
		{"fraction", float64(firstBridgeID) + 0.5, 0, false},
		{"small fraction", 7.5, 0, false},
		{"small", float64(7), 0, false},
		{"below the floor", float64(firstBridgeID - 1), 0, false},
		{"above the ceiling", float64(maxBridgeID) + 1, 0, false},
		{"huge", float64(1 << 62), 0, false},
		{"negative", float64(-firstBridgeID), 0, false},
		{"string of an id", "1073741825", 0, false},
		{"string", "7", 0, false},
		{"NaN", math.NaN(), 0, false},
		{"Inf", math.Inf(1), 0, false},
		{"nil", nil, 0, false},
		{"bool", true, 0, false},
	} {
		got, ok := bridgeIDFrom(tc.in)
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("%s: bridgeIDFrom(%#v) = %d, %v; want %d, %v", tc.name, tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

func TestListen_FloatFormIDsAreRoutedAndRewritten(t *testing.T) {
	b := startEraBridge(t, nil, listenFake, "--listen")
	_, s, id := openListen(t, b, "L1")
	floatForm := json.RawMessage(fmt.Sprintf("%d.0", id))
	rawLines(t, b,
		taggedLine(t, "x/event", floatForm, nil),
		jsonLine(t, map[string]any{"jsonrpc": "2.0", "id": floatForm, "result": map[string]any{"resultType": "complete"}}),
	)
	event := s.waitFor(t, "x/event")
	if !strings.Contains(event, `"io.modelcontextprotocol/subscriptionId":"L1"`) {
		t.Fatalf("float-form tag not rewritten: %s", event)
	}
	rest := s.drain(t)
	if len(rest) != 2 || frameData(t, rest[1])["id"] != "L1" {
		t.Fatalf("float-form response id did not end the listen: %q", rest)
	}
}

func TestListen_ChildMintedSmallIDsDoNotTouchOpenListens(t *testing.T) {
	b := startEraBridge(t, nil, listenFakeDual, "--listen")
	get := openGet(t, b)
	_, s, id := openListen(t, b, "L1")
	rawLines(t, b,
		jsonLine(t, map[string]any{"jsonrpc": "2.0", "id": 5, "method": "server/ping", "params": map[string]any{}}),
		childCancelLine(t, 5),
		plainNotification(t, "x/marker"),
	)
	get.waitFor(t, "x/marker")
	if !get.sawAny(`"method":"server/ping"`) || !get.sawAny(`"requestId":5`) {
		t.Fatalf("the child's own request or its cancel missed the GET stream: %q", get.seen)
	}
	if listenCount(b) != 1 {
		t.Fatal("the child's cancel for its own request closed the listen")
	}
	rawLines(t, b, taggedLine(t, "x/still", id, nil))
	s.waitFor(t, "x/still")
}

func TestListenEntry_EnqueueOverBudget(t *testing.T) {
	l := &listenEntry{budget: 100, wake: make(chan struct{}, 1)}
	frame := strings.Repeat("a", 60)
	if l.enqueue(frame) {
		t.Fatal("first frame overflowed")
	}
	<-l.wake
	if !l.enqueue(frame) {
		t.Fatal("second frame did not overflow")
	}
	select {
	case <-l.wake:
	default:
		t.Fatal("overflow did not wake the handler")
	}
	if l.enqueue("x") {
		t.Fatal("an ended listen reported a second overflow")
	}
	frames, final, reason := l.drain()
	if len(frames) != 1 || frames[0] != frame || final != nil || reason != listenOverflow {
		t.Fatalf("drain: %q %v %v", frames, final, reason)
	}
}

func TestListen_SlowClientIsCancelledWhenItsQueueOverflows(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.ListenBudget = 16 << 10 }, listenFake, "--listen")
	// The client reads nothing: the response fills the socket buffers, then the
	// handler's queue.
	c := startListen(t, b, "slow", nil)
	c.response(t)
	id := waitListenID(t, b, "slow")
	debugCall(t, b, "debug/flood", map[string]any{"subscriptionId": id, "count": 20000, "size": 2048})
	waitFor(t, "the slow listen to be dropped", func() bool { return listenCount(b) == 0 })
	waitFor(t, "the child to be told", func() bool { return childWasCancelled(t, b, float64(id)) })
	waitFor(t, "the child's listen to close", func() bool { return len(childListens(t, b)) == 0 })
}

func TestListen_ChildExitEndsTheStreamWithoutAnError(t *testing.T) {
	b := startEraBridge(t, nil, listenFake, "--listen")
	_, s, _ := openListen(t, b, "L1")
	_ = b.GetCurrentChild().Close()
	if rest := s.drain(t); len(rest) != 0 {
		t.Fatalf("lines after the child exited: %q", rest)
	}
	if s.sawAny("error") {
		t.Fatalf("an error frame: %q", s.seen)
	}
}

func TestListen_ReplaceChildEndsOnlyTheOldChildsListens(t *testing.T) {
	b := startEraBridge(t, nil, listenFake, "--listen")
	_, old, _ := openListen(t, b, "old")
	previous := b.GetCurrentChild()
	t.Cleanup(func() { _ = previous.Close() })
	next := newEraChild(t, listenFake, "--listen")
	// A listen already bound to the new child, as one admitted while the
	// swap was landing would be.
	keep := &listenEntry{originalID: "keep", child: next, budget: 1, wake: make(chan struct{}, 1)}
	b.mu.Lock()
	b.listens[firstBridgeID+500] = keep
	b.mu.Unlock()

	b.ReplaceChild(next)
	if rest := old.drain(t); len(rest) != 0 {
		t.Fatalf("lines after the replace: %q", rest)
	}
	b.mu.Lock()
	_, kept := b.listens[firstBridgeID+500]
	b.mu.Unlock()
	if !kept || listenCount(b) != 1 {
		t.Fatalf("the new child's listen was ended (kept=%v, %d open)", kept, listenCount(b))
	}
	keep.mu.Lock()
	reason := keep.reason
	keep.mu.Unlock()
	if reason != listenOpen {
		t.Fatalf("the new child's listen was ended: %v", reason)
	}
	b.mu.Lock()
	delete(b.listens, firstBridgeID+500)
	b.mu.Unlock()
}

func TestListen_CloseEndsListensWithinTheShutdownBarrier(t *testing.T) {
	b := startEraBridge(t, nil, listenFake, "--listen")
	_, s, _ := openListen(t, b, "L1")
	start := time.Now()
	closed := make(chan struct{})
	go func() {
		_ = b.Close()
		close(closed)
	}()
	if rest := s.drain(t); len(rest) != 0 {
		t.Fatalf("lines after Close: %q", rest)
	}
	if elapsed := time.Since(start); elapsed >= closeBarrier {
		t.Fatalf("the stream ended after %v: the barrier, not the handler, ended it", elapsed)
	}
	select {
	case <-closed:
	case <-time.After(listenWait):
		t.Fatal("Close did not return")
	}
}

func TestBridge_UnmatchedClientCancelIsNotForwarded(t *testing.T) {
	b := startTestBridge(t)
	go func() {
		resp, err := http.DefaultClient.Do(mustRequest(t, b.URL(), `{"jsonrpc":"2.0","id":"client-id","method":"hang"}`))
		if err == nil {
			resp.Body.Close()
		}
	}()
	internalID := firstPendingID(t, b)
	for _, requestID := range []any{"nobody", float64(internalID), float64(3)} {
		resp := postJSON(t, b.URL(), map[string]any{
			"jsonrpc": "2.0", "method": "notifications/cancelled",
			"params": map[string]any{"requestId": requestID},
		}, nil)
		resp.Body.Close()
		if resp.StatusCode != 202 {
			t.Fatalf("cancel of %v: %d", requestID, resp.StatusCode)
		}
	}
	// The child reads in order, so these answers follow any cancel it was sent.
	for _, requestID := range []any{"nobody", float64(internalID), float64(3), "client-id"} {
		ask := decodeBody(t, postJSON(t, b.URL(), map[string]any{
			"jsonrpc": "2.0", "id": 999, "method": "debug/wasCancelled", "params": map[string]any{"requestId": requestID},
		}, nil))
		if result, _ := ask["result"].(map[string]any); result == nil || result["cancelled"] != false {
			t.Fatalf("a cancel for %v reached the child: %v", requestID, ask)
		}
	}
}

func TestListen_ClientCancelDoesNotMatchAListen(t *testing.T) {
	b := startEraBridge(t, nil, listenFakeDual, "--listen")
	_, _, id := openListen(t, b, "L1")
	resp := postJSON(t, b.URL(), map[string]any{
		"jsonrpc": "2.0", "method": "notifications/cancelled",
		"params": map[string]any{"requestId": "L1"},
	}, nil)
	resp.Body.Close()
	if resp.StatusCode != 202 {
		t.Fatalf("cancel: %d", resp.StatusCode)
	}
	if childWasCancelled(t, b, "L1") || childWasCancelled(t, b, float64(id)) {
		t.Fatal("a client's cancel reached the child for a listen")
	}
	if listenCount(b) != 1 {
		t.Fatal("the listen was ended by a client cancel")
	}
}
