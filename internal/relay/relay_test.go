package relay

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mcpwarp/cli/internal/appproto"
	"github.com/mcpwarp/cli/internal/registry"
	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// fakeStream is an in-memory RelayStream target: one io.Pipe per direction,
// so a test can write a request head/body on one side and read the
// response on the other, exactly like a real *wsmixer.Stream's two ends.
type fakeStream struct {
	in  *io.PipeReader
	out *io.PipeWriter

	mu          sync.Mutex
	resetCalled bool
	resetCode   wsmixer.ErrorCode
	resetMsg    string
}

func (f *fakeStream) Read(p []byte) (int, error)  { return f.in.Read(p) }
func (f *fakeStream) Write(p []byte) (int, error) { return f.out.Write(p) }
func (f *fakeStream) CloseWrite() error           { return f.out.Close() }
func (f *fakeStream) Reset(code wsmixer.ErrorCode, msg string) error {
	f.mu.Lock()
	f.resetCalled, f.resetCode, f.resetMsg = true, code, msg
	f.mu.Unlock()
	// A real wsmixer.Stream's Reset tears down both directions — mirror
	// that here so a Read blocked mid-head (e.g. the head-deadline test)
	// actually unblocks instead of hanging forever.
	_ = f.in.CloseWithError(fmt.Errorf("reset: %s", msg))
	return f.out.CloseWithError(fmt.Errorf("reset: %s", msg))
}

// newFakeStream returns the stream to hand to RelayStream, plus the test
// harness's own ends: reqW (write the inbound request here) and respR
// (read the outbound response here).
func newFakeStream() (stream *fakeStream, reqW *io.PipeWriter, respR *io.PipeReader) {
	reqR, reqW := io.Pipe()
	respR, respW := io.Pipe()
	return &fakeStream{in: reqR, out: respW}, reqW, respR
}

func testOpts(t *testing.T, reg *registry.Registry, targets map[string]*url.URL, fwd *Forwarder) Options {
	t.Helper()
	return Options{
		Registry:  reg,
		Forwarder: fwd,
		Targets:   targets,
		Log:       slog.New(slog.DiscardHandler),
	}
}

func registryWithService(name, id, publicURL string) *registry.Registry {
	r := registry.New()
	r.ApplyRegistered([]appproto.RegisteredService{{Name: name, ID: id, URL: publicURL, Created: true}}, map[string]string{name: "http"})
	return r
}

func TestRelayStreamJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	target, _ := url.Parse(srv.URL + "/mcp")

	reg := registryWithService("svc", "id1", "https://svc.tunnel.example/mcp")
	fwd := NewForwarder()
	defer fwd.Close()
	opts := testOpts(t, reg, map[string]*url.URL{"svc": target}, fwd)

	stream, reqW, respR := newFakeStream()
	done := make(chan struct{})
	go func() { RelayStream(stream, opts); close(done) }()

	fmt.Fprintf(reqW, "GET /mcp HTTP/1.1\r\nHost: svc.tunnel.example\r\n\r\n")

	resp, err := http.ReadResponse(bufio.NewReader(respR), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != `{"ok":true}` {
		t.Fatalf("unexpected body: %s", body)
	}
	<-done
}

// TestRelayStreamUnknownHost covers MCPWARP.md §4.8 row 3b
// (bridging/multi_service_unknown_service_503): a /mcp request whose Host
// names no known service answers 503 service unavailable.
func TestRelayStreamUnknownHost(t *testing.T) {
	reg := registry.New()
	fwd := NewForwarder()
	defer fwd.Close()
	opts := testOpts(t, reg, nil, fwd)

	status, ctype, body := relayOnce(t, opts, "GET /mcp HTTP/1.1\r\nHost: nowhere.example\r\n\r\n")
	if status != 503 || ctype != "application/json" || body != `{"error":"service unavailable"}` {
		t.Fatalf("GET /mcp (unknown host) = %d %q %q, want 503 application/json service unavailable", status, ctype, body)
	}
}

// relayOnce runs RelayStream over a fresh fakeStream for one raw request and
// returns the response, waiting for RelayStream to return.
func relayOnce(t *testing.T, opts Options, raw string) (status int, contentType, body string) {
	t.Helper()
	stream, reqW, respR := newFakeStream()
	done := make(chan struct{})
	go func() { RelayStream(stream, opts); close(done) }()
	go func() { _, _ = io.WriteString(reqW, raw) }()

	resp, err := http.ReadResponse(bufio.NewReader(respR), nil)
	if err != nil {
		t.Fatalf("%q: read response: %v", raw, err)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%q: read body: %v", raw, err)
	}
	resp.Body.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("%q: RelayStream never returned", raw)
	}
	return resp.StatusCode, resp.Header.Get("Content-Type"), string(b)
}

// TestRelayStreamPathRule covers MCPWARP.md §4.3's table: only the exact,
// case-sensitive path /mcp is forwarded, with any query, in origin- and
// absolute-form; a trailing slash, a sub-path, a different case, or a path
// that only decodes or cleans to /mcp answers 404 without reaching the
// local server.
func TestRelayStreamPathRule(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.URL.RequestURI())
		mu.Unlock()
		_, _ = io.WriteString(w, "ok")
	}))
	defer srv.Close()
	target, _ := url.Parse(srv.URL + "/local")

	reg := registryWithService("svc", "id1", "https://svc.tunnel.example/mcp")
	fwd := NewForwarder()
	defer fwd.Close()
	opts := testOpts(t, reg, map[string]*url.URL{"svc": target}, fwd)

	const notFound = `{"error":"not_found"}`
	cases := []struct {
		target     string
		wantStatus int
		wantBody   string
		wantLocal  string
	}{
		{"/mcp", 200, "ok", "/local"},
		{"/mcp?x=1", 200, "ok", "/local?x=1"},
		{"https://svc.tunnel.example/mcp?x=1", 200, "ok", "/local?x=1"},
		{"/mcp/", 404, notFound, ""},
		{"/mcp/sub", 404, notFound, ""},
		{"/mcp/?x=1", 404, notFound, ""},
		{"/MCP", 404, notFound, ""},
		{"/", 404, notFound, ""},
		{"/healthz", 404, notFound, ""},
		{"/a/../mcp", 404, notFound, ""},
		{"/%6Dcp", 404, notFound, ""},
		{"/sse", 404, notFound, ""},
		{"/messages?sessionId=1", 404, notFound, ""},
		{"https://svc.tunnel.example/healthz", 404, notFound, ""},
	}
	for _, tc := range cases {
		mu.Lock()
		seen = nil
		mu.Unlock()
		status, ctype, body := relayOnce(t, opts, "GET "+tc.target+" HTTP/1.1\r\nHost: svc.tunnel.example\r\n\r\n")
		if status != tc.wantStatus || body != tc.wantBody {
			t.Errorf("GET %s = %d %q, want %d %q", tc.target, status, body, tc.wantStatus, tc.wantBody)
		}
		if tc.wantStatus == 404 && ctype != "application/json" {
			t.Errorf("GET %s: Content-Type %q, want application/json", tc.target, ctype)
		}
		mu.Lock()
		got := strings.Join(seen, ",")
		mu.Unlock()
		if got != tc.wantLocal {
			t.Errorf("GET %s: the local server saw %q, want %q", tc.target, got, tc.wantLocal)
		}
	}
}

// recordingHandler is a slog.Handler that keeps every record, so a test can
// assert on the log lines a code path emitted (as internal/tunnel's tests do).
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler            { return h }
func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

// TestRelayStreamPathRefusalWarnsOncePerService: the first refused non-/mcp
// request per known service logs a Warn naming the path and calls
// OnPathRefused, later ones for the same service and any for an unknown host
// stay at Debug with no call.
func TestRelayStreamPathRefusalWarnsOncePerService(t *testing.T) {
	reg := registry.New()
	reg.ApplyRegistered([]appproto.RegisteredService{
		{Name: "svc", ID: "id1", URL: "https://svc.tunnel.example/mcp", Created: true},
		{Name: "other", ID: "id2", URL: "https://other.tunnel.example/mcp", Created: true},
	}, map[string]string{"svc": "http", "other": "http"})
	fwd := NewForwarder()
	defer fwd.Close()
	h := &recordingHandler{}
	opts := testOpts(t, reg, map[string]*url.URL{}, fwd)
	opts.Log = slog.New(h)
	opts.UnreachableWarn = NewUnreachableWarnLimiter()
	var hookMu sync.Mutex
	var hookCalls []string
	opts.OnPathRefused = func(service, path, publicURL string) {
		hookMu.Lock()
		hookCalls = append(hookCalls, service+" "+path+" "+publicURL)
		hookMu.Unlock()
	}

	for _, rq := range []struct{ target, host string }{
		{"/sse", "svc.tunnel.example"},
		{"/messages?sessionId=1", "svc.tunnel.example"},
		{"/sse", "other.tunnel.example"},
		{"/sse", "nowhere.example"},
	} {
		if status, _, _ := relayOnce(t, opts, "GET "+rq.target+" HTTP/1.1\r\nHost: "+rq.host+"\r\n\r\n"); status != 404 {
			t.Fatalf("GET %s (Host %s) = %d, want 404", rq.target, rq.host, status)
		}
	}

	const debugMsg = "request for a path other than /mcp"
	want := []string{
		"WARN " + pathRefusedWarn + " service=svc path=/sse url=https://svc.tunnel.example/mcp",
		"DEBUG " + debugMsg,
		"WARN " + pathRefusedWarn + " service=other path=/sse url=https://other.tunnel.example/mcp",
		"DEBUG " + debugMsg,
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	var got []string
	for _, r := range h.records {
		line := r.Level.String() + " " + r.Message
		if r.Level == slog.LevelWarn {
			r.Attrs(func(a slog.Attr) bool {
				line += " " + a.Key + "=" + a.Value.String()
				return true
			})
		}
		got = append(got, line)
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("log records:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}

	hookMu.Lock()
	defer hookMu.Unlock()
	wantCalls := "svc /sse https://svc.tunnel.example/mcp\nother /sse https://other.tunnel.example/mcp"
	if strings.Join(hookCalls, "\n") != wantCalls {
		t.Fatalf("OnPathRefused calls:\n%s\nwant:\n%s", strings.Join(hookCalls, "\n"), wantCalls)
	}
}

// TestFirstPathRefusalConcurrent: concurrent streams for one service get
// exactly one first refusal between them.
func TestFirstPathRefusalConcurrent(t *testing.T) {
	l := NewUnreachableWarnLimiter()
	var firsts atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.FirstPathRefusal("svc") {
				firsts.Add(1)
			}
		}()
	}
	wg.Wait()
	if n := firsts.Load(); n != 1 {
		t.Fatalf("FirstPathRefusal returned true %d times, want 1", n)
	}
}

// TestRelayStreamPathCheckPrecedesServiceChecks covers MCPWARP.md §4.4 row
// 2 and §4.8's precedence: a non-/mcp path answers 404 not_found before the
// service is resolved, so it beats an unknown service's and a disabled
// one's 503 alike (bridging/multi_service_unknown_service_non_mcp_404).
func TestRelayStreamPathCheckPrecedesServiceChecks(t *testing.T) {
	reg := registryWithService("off", "id-off", "https://off.tunnel.example/mcp")
	reg.ApplyDisable("id-off", "paused in the dashboard")
	fwd := NewForwarder()
	defer fwd.Close()
	opts := testOpts(t, reg, map[string]*url.URL{}, fwd)

	cases := []struct {
		target, host string
		wantStatus   int
		wantBody     string
	}{
		{"/healthz", "nowhere.example", 404, `{"error":"not_found"}`},
		{"/mcp", "nowhere.example", 503, `{"error":"service unavailable"}`},
		{"/healthz", "off.tunnel.example", 404, `{"error":"not_found"}`},
		{"/mcp", "off.tunnel.example", 503, `{"error":"service disabled"}`},
	}
	for _, tc := range cases {
		status, _, body := relayOnce(t, opts, "GET "+tc.target+" HTTP/1.1\r\nHost: "+tc.host+"\r\n\r\n")
		if status != tc.wantStatus || body != tc.wantBody {
			t.Errorf("GET %s (Host %s) = %d %q, want %d %q", tc.target, tc.host, status, body, tc.wantStatus, tc.wantBody)
		}
	}
}

// TestRelayStreamRefusedPathDrainsBody covers MCPWARP.md §4.4 "Ending a
// refused stream": after the 404 the unread request body is read and
// dropped, then the stream is closed, never reset.
func TestRelayStreamRefusedPathDrainsBody(t *testing.T) {
	reg := registryWithService("svc", "id1", "https://svc.tunnel.example/mcp")
	fwd := NewForwarder()
	defer fwd.Close()
	opts := testOpts(t, reg, map[string]*url.URL{}, fwd)

	reqBody := strings.Repeat("x", 256*1024)
	raw := fmt.Sprintf("POST /messages HTTP/1.1\r\nHost: svc.tunnel.example\r\nContent-Length: %d\r\n\r\n%s", len(reqBody), reqBody)
	assertRefusalDrainsBody(t, opts, raw, 404, `{"error":"not_found"}`)
}

// TestRelayStreamDisabledServiceDrainsBody: the same drain after a disabled
// service's 503, with a chunked body (bridging/disabled_chunked_body_503).
func TestRelayStreamDisabledServiceDrainsBody(t *testing.T) {
	reg := registryWithService("svc", "id1", "https://svc.tunnel.example/mcp")
	reg.ApplyDisable("id1", "paused in the dashboard")
	fwd := NewForwarder()
	defer fwd.Close()
	opts := testOpts(t, reg, map[string]*url.URL{}, fwd)

	chunk := strings.Repeat("x", 64*1024)
	var raw strings.Builder
	raw.WriteString("POST /mcp HTTP/1.1\r\nHost: svc.tunnel.example\r\nTransfer-Encoding: chunked\r\n\r\n")
	for i := 0; i < 4; i++ {
		fmt.Fprintf(&raw, "%x\r\n%s\r\n", len(chunk), chunk)
	}
	raw.WriteString("0\r\n\r\n")
	assertRefusalDrainsBody(t, opts, raw.String(), 503, `{"error":"service disabled"}`)
}

// assertRefusalDrainsBody sends raw (head and body in one write, which
// blocks until RelayStream has read all of it) and expects the refusal
// wantStatus/wantBody, then the body drained and the stream closed, not
// reset.
func assertRefusalDrainsBody(t *testing.T, opts Options, raw string, wantStatus int, wantBody string) {
	t.Helper()
	stream, reqW, respR := newFakeStream()
	done := make(chan struct{})
	go func() { RelayStream(stream, opts); close(done) }()

	wrote := make(chan error, 1)
	go func() {
		_, err := io.WriteString(reqW, raw)
		wrote <- err
	}()

	br := bufio.NewReader(respR)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus || string(body) != wantBody {
		t.Fatalf("refusal = %d %q, want %d %q", resp.StatusCode, body, wantStatus, wantBody)
	}

	select {
	case err := <-wrote:
		if err != nil {
			t.Fatalf("writing the request body: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the refused request's body was never drained")
	}
	if rest, err := io.ReadAll(br); err != nil || len(rest) != 0 {
		t.Fatalf("after the response: %q, %v; want a clean CLOSE", rest, err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RelayStream never returned")
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if stream.resetCalled {
		t.Fatalf("stream was reset (%v %q), want CLOSE", stream.resetCode, stream.resetMsg)
	}
}

// TestRelayStreamUnsupportedTargetForm covers MCPWARP.md §4.3's last row:
// `*`, authority-form and `\mcp` answer 400 before the path check.
// http.ReadRequest itself rejects `\mcp` (the generic malformed-head 400).
func TestRelayStreamUnsupportedTargetForm(t *testing.T) {
	reg := registryWithService("svc", "id1", "https://svc.tunnel.example/mcp")
	fwd := NewForwarder()
	defer fwd.Close()
	opts := testOpts(t, reg, map[string]*url.URL{}, fwd)

	for _, line := range []string{"OPTIONS * HTTP/1.1", "CONNECT svc.tunnel.example:443 HTTP/1.1", "GET h.example:443 HTTP/1.1"} {
		status, _, body := relayOnce(t, opts, line+"\r\nHost: svc.tunnel.example\r\n\r\n")
		if status != 400 || body != `{"error":"unsupported request target form"}` {
			t.Errorf("%s = %d %q, want 400 unsupported request target form", line, status, body)
		}
	}
	if status, _, _ := relayOnce(t, opts, "GET \\mcp HTTP/1.1\r\nHost: svc.tunnel.example\r\n\r\n"); status != 400 {
		t.Errorf(`GET \mcp = %d, want 400`, status)
	}
}

func TestRelayStreamDisabledService(t *testing.T) {
	reg := registryWithService("svc", "id1", "https://svc.tunnel.example/mcp")
	reg.ApplyDisable("id1", "quota exceeded")
	fwd := NewForwarder()
	defer fwd.Close()
	opts := testOpts(t, reg, map[string]*url.URL{}, fwd)

	stream, reqW, respR := newFakeStream()
	go RelayStream(stream, opts)
	fmt.Fprintf(reqW, "GET /mcp HTTP/1.1\r\nHost: svc.tunnel.example\r\n\r\n")

	resp, err := http.ReadResponse(bufio.NewReader(respR), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != 503 {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}
}

// TestRelayStreamRelaysRejectedWhileReregistering: the tunnel decides
// routing, not the registry's rejection. It routes a name to this session as
// soon as a re-register (`e`, or a reconnect's batch) records it, before the
// "registered" reply clears a CONFLICT from an earlier take-over, so such a
// request must reach the local target.
func TestRelayStreamRelaysRejectedWhileReregistering(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
	}))
	defer srv.Close()
	target, _ := url.Parse(srv.URL + "/mcp")

	reg := registryWithService("svc", "id1", "https://svc.tunnel.example/mcp")
	reg.ApplyRegisterErrors([]appproto.ServiceError{{Name: "svc", Code: "CONFLICT", Message: "taken over"}}, nil)
	reg.MarkReregistering("svc", "http")
	fwd := NewForwarder()
	defer fwd.Close()
	opts := testOpts(t, reg, map[string]*url.URL{"svc": target}, fwd)

	stream, reqW, respR := newFakeStream()
	done := make(chan struct{})
	go func() { RelayStream(stream, opts); close(done) }()
	fmt.Fprintf(reqW, "GET /mcp HTTP/1.1\r\nHost: svc.tunnel.example\r\n\r\n")

	resp, err := http.ReadResponse(bufio.NewReader(respR), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	<-done
	if n := hits.Load(); n != 1 {
		t.Fatalf("the local target was hit %d times, want 1", n)
	}
}

func TestRelayStreamNoTarget(t *testing.T) {
	reg := registryWithService("svc", "id1", "https://svc.tunnel.example/mcp")
	fwd := NewForwarder()
	defer fwd.Close()
	opts := testOpts(t, reg, map[string]*url.URL{}, fwd)

	stream, reqW, respR := newFakeStream()
	go RelayStream(stream, opts)
	fmt.Fprintf(reqW, "GET /mcp HTTP/1.1\r\nHost: svc.tunnel.example\r\n\r\n")

	resp, err := http.ReadResponse(bufio.NewReader(respR), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != 502 {
		t.Fatalf("expected 502, got %d", resp.StatusCode)
	}
}

func TestRelayStreamUnreachableTarget(t *testing.T) {
	target, _ := url.Parse("http://127.0.0.1:1")
	reg := registryWithService("svc", "id1", "https://svc.tunnel.example/mcp")
	fwd := NewForwarder()
	defer fwd.Close()
	opts := testOpts(t, reg, map[string]*url.URL{"svc": target}, fwd)

	stream, reqW, respR := newFakeStream()
	go RelayStream(stream, opts)
	fmt.Fprintf(reqW, "GET /mcp HTTP/1.1\r\nHost: svc.tunnel.example\r\n\r\n")

	resp, err := http.ReadResponse(bufio.NewReader(respR), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != 502 {
		t.Fatalf("expected 502, got %d", resp.StatusCode)
	}
}

func TestRelayStreamMalformedRequest(t *testing.T) {
	reg := registry.New()
	fwd := NewForwarder()
	defer fwd.Close()
	opts := testOpts(t, reg, nil, fwd)

	stream, reqW, respR := newFakeStream()
	go RelayStream(stream, opts)
	fmt.Fprintf(reqW, "NOT A REQUEST\r\n\r\n")
	reqW.Close()

	resp, err := http.ReadResponse(bufio.NewReader(respR), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

// bufStream is a Stream backed by an already-fully-buffered request head (no
// live writer goroutine, so an oversized or truncated head can't ever block
// a test on a Write call) plus a real io.Pipe for the response/reset side —
// used by the two request-head-overflow-vs-torn-stream tests below.
type bufStream struct {
	*bytes.Reader
	out *io.PipeWriter

	mu          sync.Mutex
	resetCalled bool
	resetCode   wsmixer.ErrorCode
	resetMsg    string
}

func (b *bufStream) Write(p []byte) (int, error) { return b.out.Write(p) }
func (b *bufStream) CloseWrite() error           { return b.out.Close() }
func (b *bufStream) Reset(code wsmixer.ErrorCode, msg string) error {
	b.mu.Lock()
	b.resetCalled, b.resetCode, b.resetMsg = true, code, msg
	b.mu.Unlock()
	return b.out.CloseWithError(fmt.Errorf("reset: %s", msg))
}

func newBufStream(head []byte) (stream *bufStream, respR *io.PipeReader) {
	respR, respW := io.Pipe()
	return &bufStream{Reader: bytes.NewReader(head), out: respW}, respR
}

// TestRelayStreamOversizedHeadReturns431 covers blocker B2: a head that
// blows through maxHeadBytes without ever completing must be answered 431,
// not silently dropped as if the stream had simply torn.
func TestRelayStreamOversizedHeadReturns431(t *testing.T) {
	reg := registry.New()
	fwd := NewForwarder()
	defer fwd.Close()
	opts := testOpts(t, reg, nil, fwd)

	// A request line plus one header value alone bigger than maxHeadBytes,
	// with no terminating blank line — never a completeable head.
	head := "GET /mcp HTTP/1.1\r\nX-Big: " + strings.Repeat("a", maxHeadBytes+4096) + "\r\n"
	stream, respR := newBufStream([]byte(head))

	done := make(chan struct{})
	go func() { RelayStream(stream, opts); close(done) }()

	resp, err := http.ReadResponse(bufio.NewReader(respR), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != 431 {
		t.Fatalf("expected 431, got %d", resp.StatusCode)
	}
	// Drain the body — writeResponse's io.CopyN into the pipe blocks until
	// it's read, so RelayStream can't return until this happens.
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatalf("read body: %v", err)
	}
	resp.Body.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RelayStream never returned")
	}
}

// TestRelayStreamTornStreamResets covers blocker B2's other half: a head
// that ends (genuine EOF) before completing — well under maxHeadBytes, so
// not an oversized-head case — must Reset the stream (freeing the peer's
// stream slot) rather than silently hang it.
func TestRelayStreamTornStreamResets(t *testing.T) {
	reg := registry.New()
	fwd := NewForwarder()
	defer fwd.Close()
	opts := testOpts(t, reg, nil, fwd)

	// A request line and a partial header line, then nothing — the peer
	// vanished mid-head, well short of maxHeadBytes and short of the
	// terminating blank line.
	head := "GET /mcp HTTP/1.1\r\nHost: svc.example\r\n"
	stream, _ := newBufStream([]byte(head))

	done := make(chan struct{})
	go func() { RelayStream(stream, opts); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RelayStream never returned")
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()
	if !stream.resetCalled {
		t.Fatal("expected the stream to be Reset on a torn head, got no reset")
	}
	if stream.resetCode != wsmixer.CancelCode {
		t.Fatalf("expected CancelCode, got %v", stream.resetCode)
	}
}

// TestRelayStreamSSEIncremental verifies an SSE response is delivered chunk
// by chunk as the upstream flushes it, not buffered until the handler
// returns — mirrors mcpwarp-cli's relay.e2e.test.ts timing assertion.
func TestRelayStreamSSEIncremental(t *testing.T) {
	const delay = 150 * time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		w.WriteHeader(200)
		for i := 0; i < 3; i++ {
			fmt.Fprintf(w, "data: chunk-%d\n\n", i)
			flusher.Flush()
			if i < 2 {
				time.Sleep(delay)
			}
		}
	}))
	defer srv.Close()
	target, _ := url.Parse(srv.URL + "/mcp")

	reg := registryWithService("svc", "id1", "https://svc.tunnel.example/mcp")
	fwd := NewForwarder()
	defer fwd.Close()
	opts := testOpts(t, reg, map[string]*url.URL{"svc": target}, fwd)

	stream, reqW, respR := newFakeStream()
	go RelayStream(stream, opts)
	fmt.Fprintf(reqW, "GET /mcp HTTP/1.1\r\nHost: svc.tunnel.example\r\n\r\n")

	br := bufio.NewReader(respR)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	defer resp.Body.Close()

	type chunk struct {
		data string
		at   time.Time
	}
	chunks := make(chan chunk, 3)
	go func() {
		buf := make([]byte, 256)
		for i := 0; i < 3; i++ {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				chunks <- chunk{data: string(buf[:n]), at: time.Now()}
			}
			if err != nil {
				return
			}
		}
	}()

	var got []chunk
	for i := 0; i < 3; i++ {
		select {
		case c := <-chunks:
			got = append(got, c)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for chunk %d", i)
		}
	}
	// The gap between the first and last chunk must reflect the upstream's
	// own pacing (2 * delay), not have arrived all at once after buffering.
	gap := got[2].at.Sub(got[0].at)
	if gap < delay {
		t.Fatalf("expected chunks spread out over roughly %v, got a %v gap — response body looks buffered, not streamed", 2*delay, gap)
	}
}

// TestRelayStreamExactlyMaxHeadBytesSucceeds covers the maxHeadBytes
// boundary itself: a head landing at exactly the cap must parse cleanly,
// not be treated as oversized (that's the +N-over-budget case
// TestRelayStreamOversizedHeadReturns431 already covers).
func TestRelayStreamExactlyMaxHeadBytesSucceeds(t *testing.T) {
	reg := registry.New()
	fwd := NewForwarder()
	defer fwd.Close()
	opts := testOpts(t, reg, nil, fwd)

	prefix := "GET /mcp HTTP/1.1\r\nHost: nowhere.example\r\nX-Pad: "
	suffix := "\r\n\r\n"
	padLen := maxHeadBytes - len(prefix) - len(suffix)
	head := prefix + strings.Repeat("a", padLen) + suffix
	if len(head) != maxHeadBytes {
		t.Fatalf("test setup: head is %d bytes, want exactly %d", len(head), maxHeadBytes)
	}

	stream, respR := newBufStream([]byte(head))
	done := make(chan struct{})
	go func() { RelayStream(stream, opts); close(done) }()

	resp, err := http.ReadResponse(bufio.NewReader(respR), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	// Unknown host (never registered), but the head itself parsed cleanly —
	// 503, not 431: a head of exactly maxHeadBytes must not be treated as
	// oversized.
	if resp.StatusCode != 503 {
		t.Fatalf("expected 503 (head parsed, host unknown), got %d", resp.StatusCode)
	}
	// Drain the body — writeResponse's io.CopyN into the pipe blocks until
	// it's read, so RelayStream can't return until this happens.
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatalf("read body: %v", err)
	}
	resp.Body.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RelayStream never returned")
	}
}

// TestRelayStreamHeadDeadlineExceeded covers the 10s head deadline itself
// (shortened here via the requestHeadDeadline test seam): a stream that
// never finishes sending a head must be Reset once the deadline elapses,
// not left hanging forever.
func TestRelayStreamHeadDeadlineExceeded(t *testing.T) {
	orig := requestHeadDeadline
	requestHeadDeadline = 50 * time.Millisecond
	t.Cleanup(func() { requestHeadDeadline = orig })

	reg := registry.New()
	fwd := NewForwarder()
	defer fwd.Close()
	opts := testOpts(t, reg, nil, fwd)

	stream, _, _ := newFakeStream()
	done := make(chan struct{})
	go func() { RelayStream(stream, opts); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RelayStream never returned after the head deadline elapsed")
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()
	if !stream.resetCalled {
		t.Fatal("expected the stream to be Reset once the head deadline elapsed")
	}
	if stream.resetCode != wsmixer.CancelCode {
		t.Fatalf("expected CancelCode, got %v", stream.resetCode)
	}
}
