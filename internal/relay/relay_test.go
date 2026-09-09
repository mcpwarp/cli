package relay

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
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

func TestRelayStreamUnknownHost(t *testing.T) {
	reg := registry.New()
	fwd := NewForwarder()
	defer fwd.Close()
	opts := testOpts(t, reg, nil, fwd)

	stream, reqW, respR := newFakeStream()
	go RelayStream(stream, opts)
	fmt.Fprintf(reqW, "GET /mcp HTTP/1.1\r\nHost: nowhere.example\r\n\r\n")

	resp, err := http.ReadResponse(bufio.NewReader(respR), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != 404 {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
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
	// 404, not 431: a head of exactly maxHeadBytes must not be treated as
	// oversized.
	if resp.StatusCode != 404 {
		t.Fatalf("expected 404 (head parsed, host unknown), got %d", resp.StatusCode)
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
