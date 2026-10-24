// Package fakeedge is the in-process fake tunnel DESIGN.md §10 calls for: a
// real net/http test server upgrading to WebSocket and running ws-mixer's own
// server-side handshake (wsmixer.AcceptConn), scripted by the test that
// starts it. It speaks DESIGN.md §8's app protocol as raw JSON envelopes
// rather than through internal/appproto, so it outlives the CLI's own
// protocol packages and can drive whatever client sits on top of
// ws-mixer-go.
//
// Test-only by convention: it imports "testing", and nothing outside a
// _test.go file may import it, so it never reaches a production binary.
package fakeedge

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// DefaultWait bounds every Wait* helper: generous enough for a full-jitter
// first reconnect (base 1s) on a loaded CI runner, short enough that a
// missing event fails the test instead of hanging it.
const DefaultWait = 5 * time.Second

// Options scripts a Server.
type Options struct {
	// OnConn runs once per accepted connection, on that connection's own
	// serving goroutine, after the handshake and before Run — the place to
	// bind a per-connection handler with Conn.OnApp, so no frame can arrive
	// ahead of it.
	OnConn func(c *Conn)

	// OnApp handles every app message on a connection whose OnConn bound no
	// handler of its own. It runs on the connection's delivery goroutine
	// (wsmixer.Conn.OnApp's rules: send replies, never block).
	OnApp func(c *Conn, f Frame)

	// Authenticate, if set, decides each handshake: attempt counts every
	// handshake this server has run, from 1. Return wsmixer.Unauthorized to
	// refuse with error{UNAUTHORIZED} and close 4011, wsmixer.Unsupported
	// for 4010. The default accepts every handshake.
	Authenticate func(attempt int, h *wsmixer.Hello) error
}

// EventKind names what an Event records.
type EventKind string

const (
	// EventAccepted: a handshake succeeded and the connection is live.
	EventAccepted EventKind = "accepted"
	// EventRefused: Authenticate refused a handshake.
	EventRefused EventKind = "refused"
	// EventApp: an app message arrived (Event.Frame).
	EventApp EventKind = "app"
	// EventDrain: the client sent drain (Event.DrainReason), e.g.
	// client_requested on a graceful shutdown (DESIGN.md §3).
	EventDrain EventKind = "drain"
	// EventClosed: a connection ended (Event.CloseCode is the peer's WS
	// close code, -1 when it sent none). Kept for the ServiceGroup
	// adapter's upcoming tests.
	EventClosed EventKind = "closed"
)

// Event is one thing the edge observed, in arrival order across every
// connection.
type Event struct {
	Kind        EventKind
	Conn        int // the connection's Index; 0 for EventRefused
	At          time.Time
	Frame       Frame
	DrainReason string
	CloseCode   int
}

// Frame is one decoded app message: the envelope's op plus the service names
// a register/unregister carries (DESIGN.md §8), with the raw body kept for
// anything else a test wants to read.
type Frame struct {
	Op    string
	Names []string
	Raw   json.RawMessage
}

// ParseFrame decodes raw's {"mcpwarp":{"op":...,"services":[{"name":...}]}}
// envelope. A body without one yields a Frame with an empty Op.
func ParseFrame(raw json.RawMessage) Frame {
	var wrapper struct {
		Mcpwarp struct {
			Op       string `json:"op"`
			Services []struct {
				Name string `json:"name"`
			} `json:"services"`
		} `json:"mcpwarp"`
	}
	f := Frame{Raw: raw}
	if json.Unmarshal(raw, &wrapper) != nil {
		return f
	}
	f.Op = wrapper.Mcpwarp.Op
	if f.Op == "register" || f.Op == "unregister" {
		f.Names = make([]string, len(wrapper.Mcpwarp.Services))
		for i, s := range wrapper.Mcpwarp.Services {
			f.Names[i] = s.Name
		}
	}
	return f
}

// RegisterNames returns the service names of a "register" body, in order, or
// nil if raw isn't one.
func RegisterNames(raw json.RawMessage) []string {
	f := ParseFrame(raw)
	if f.Op != "register" {
		return nil
	}
	return f.Names
}

// Server is a running fake edge. URL is its ws:// address.
type Server struct {
	URL string

	t    testing.TB
	opts Options
	srv  *httptest.Server
	wg   sync.WaitGroup

	mu       sync.Mutex
	closed   bool              // set by close; a later upgrade is dropped
	raws     []*websocket.Conn // every upgraded socket, handshaken or not
	conns    []*Conn
	events   []Event
	attempts int
	// changed is closed and replaced on every new event, so a waiter
	// blocks on it instead of polling.
	changed chan struct{}
}

// Serve starts a Server whose every accepted connection runs onConn first
// (nil for none) — New with only Options.OnConn set.
func Serve(t testing.TB, onConn func(c *Conn)) *Server {
	t.Helper()
	return New(t, Options{OnConn: onConn})
}

// New starts a Server, torn down by t.Cleanup: every connection still open
// is closed and its serving goroutine waited for (bounded), so no handler
// outlives the test that started it.
func New(t testing.TB, opts Options) *Server {
	t.Helper()
	s := &Server{t: t, opts: opts, changed: make(chan struct{})}
	s.srv = httptest.NewServer(http.HandlerFunc(s.serve))
	s.URL = "ws" + strings.TrimPrefix(s.srv.URL, "http")
	t.Cleanup(s.close)
	return s
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	s.wg.Add(1)
	defer s.wg.Done()
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:    []string{wsmixer.Subprotocol},
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		return
	}
	defer ws.CloseNow()
	s.mu.Lock()
	if s.closed {
		// Upgraded after close took its snapshot of raws: nothing would
		// close this socket for us.
		s.mu.Unlock()
		return
	}
	s.raws = append(s.raws, ws)
	s.mu.Unlock()

	bearer := r.Header.Get("Authorization")
	if !strings.HasPrefix(bearer, "Bearer ") {
		s.t.Errorf("fakeedge: request missing a Bearer Authorization header, got %q", bearer)
		ws.Close(websocket.StatusPolicyViolation, "missing bearer token")
		return
	}
	bearer = strings.TrimPrefix(bearer, "Bearer ")

	wc, err := wsmixer.AcceptConn(r.Context(), ws, bearer, wsmixer.AcceptOptions{
		ServerInfo: &wsmixer.ServerInfo{Name: "fake-edge", Version: "test"},
		Authenticate: func(ctx context.Context, h *wsmixer.Hello) (wsmixer.WelcomeMeta, error) {
			s.mu.Lock()
			s.attempts++
			attempt := s.attempts
			s.mu.Unlock()
			if s.opts.Authenticate != nil {
				if err := s.opts.Authenticate(attempt, h); err != nil {
					s.record(Event{Kind: EventRefused})
					return wsmixer.WelcomeMeta{}, err
				}
			}
			return wsmixer.WelcomeMeta{}, nil
		},
		Request: r,
	})
	if err != nil {
		return
	}

	s.mu.Lock()
	c := &Conn{Conn: wc, Raw: ws, Index: len(s.conns) + 1, srv: s}
	s.conns = append(s.conns, c)
	s.mu.Unlock()
	s.record(Event{Kind: EventAccepted, Conn: c.Index})

	// Bound before OnConn and Run: every frame is recorded, then handed to
	// whichever handler the test picked.
	wc.OnApp(c.deliver)
	wc.OnDrain(func(d *wsmixer.DrainMsg) {
		s.record(Event{Kind: EventDrain, Conn: c.Index, DrainReason: d.Reason})
	})
	if s.opts.OnConn != nil {
		s.opts.OnConn(c)
	}
	wc.Run()
	<-wc.Done()
	s.record(Event{Kind: EventClosed, Conn: c.Index, CloseCode: wc.PeerCloseCode()})
}

func (s *Server) record(e Event) {
	e.At = time.Now()
	s.mu.Lock()
	s.events = append(s.events, e)
	close(s.changed)
	s.changed = make(chan struct{})
	s.mu.Unlock()
}

// close runs at t.Cleanup. httptest.Server.Close stops new upgrades but no
// longer tracks a hijacked (upgraded) connection, so each socket is closed
// here and its handler waited for explicitly — including one still in its
// handshake, which would otherwise sit out the hello timeout.
func (s *Server) close() {
	s.srv.Close()
	s.mu.Lock()
	s.closed = true
	raws := append([]*websocket.Conn(nil), s.raws...)
	s.mu.Unlock()
	for _, ws := range raws {
		_ = ws.CloseNow()
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(DefaultWait):
		s.t.Errorf("fakeedge: connection handlers still running %v after Close", DefaultWait)
	}
}

// ConnCount reports how many connections the edge has accepted so far,
// across every reconnect.
func (s *Server) ConnCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// Latest returns the most recently accepted connection, or nil if none yet.
func (s *Server) Latest() *Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.conns) == 0 {
		return nil
	}
	return s.conns[len(s.conns)-1]
}

// ConnAt returns the index'th accepted connection (1-based), or nil. Kept
// for the ServiceGroup adapter's upcoming tests.
func (s *Server) ConnAt(index int) *Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index < 1 || index > len(s.conns) {
		return nil
	}
	return s.conns[index-1]
}

// Attempts reports how many handshakes reached Authenticate, refused ones
// included. Kept for the ServiceGroup adapter's upcoming tests.
func (s *Server) Attempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

// Events returns a copy of everything observed so far, in arrival order.
func (s *Server) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event(nil), s.events...)
}

// Frames returns every app message with the given op (every one for ""), in
// arrival order across connections.
func (s *Server) Frames(op string) []Frame {
	var out []Frame
	for _, e := range s.Events() {
		if e.Kind == EventApp && (op == "" || e.Frame.Op == op) {
			out = append(out, e.Frame)
		}
	}
	return out
}

// RegisterBatches returns the names of every "register" received, one slice
// per frame, in arrival order.
func (s *Server) RegisterBatches() [][]string {
	frames := s.Frames("register")
	out := make([][]string, len(frames))
	for i, f := range frames {
		out[i] = f.Names
	}
	return out
}

// WaitUntil blocks until cond, re-checked after every event, holds — or
// fails the test after DefaultWait, naming what.
func (s *Server) WaitUntil(t testing.TB, what string, cond func() bool) {
	t.Helper()
	s.WaitUntilFor(t, what, DefaultWait, cond)
}

// WaitUntilFor is WaitUntil with its own timeout, for an event a policy
// delays past DefaultWait (e.g. a reconnect scheduled at random(0, cap)).
func (s *Server) WaitUntilFor(t testing.TB, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		s.mu.Lock()
		changed := s.changed
		s.mu.Unlock()
		if cond() {
			return
		}
		select {
		case <-changed:
		case <-deadline.C:
			t.Fatalf("fakeedge: timed out after %v waiting for %s; events: %s", timeout, what, s.describe())
		}
	}
}

// WaitEvent returns the first event, from the start of the run, that
// matches — waiting for one if none has arrived yet.
func (s *Server) WaitEvent(t testing.TB, what string, match func(Event) bool) Event {
	t.Helper()
	var found Event
	s.WaitUntil(t, what, func() bool {
		for _, e := range s.Events() {
			if match(e) {
				found = e
				return true
			}
		}
		return false
	})
	return found
}

// WaitConn waits for the index'th connection (1-based) and returns it. Kept
// for the ServiceGroup adapter's upcoming tests.
func (s *Server) WaitConn(t testing.TB, index int) *Conn {
	t.Helper()
	s.WaitUntil(t, fmt.Sprintf("connection #%d", index), func() bool { return s.ConnCount() >= index })
	return s.ConnAt(index)
}

// WaitRegister waits for the n'th "register" frame (1-based) and returns its
// names.
func (s *Server) WaitRegister(t testing.TB, n int) []string {
	t.Helper()
	s.WaitUntil(t, fmt.Sprintf("register #%d", n), func() bool { return len(s.Frames("register")) >= n })
	return s.Frames("register")[n-1].Names
}

func (s *Server) describe() string {
	var b strings.Builder
	for _, e := range s.Events() {
		fmt.Fprintf(&b, "\n  #%d %s", e.Conn, e.Kind)
		switch e.Kind {
		case EventApp:
			fmt.Fprintf(&b, " %s", e.Frame.Raw)
		case EventDrain:
			fmt.Fprintf(&b, " %s", e.DrainReason)
		case EventClosed:
			fmt.Fprintf(&b, " %d", e.CloseCode)
		}
	}
	return b.String()
}

// Conn is one accepted connection. It embeds the server-side *wsmixer.Conn,
// so SendApp/OpenStream/Close/Drain/Session work as on the SDK's own type;
// Raw is the coder/websocket conn beneath it. Never call the embedded
// Conn.Conn.OnApp or Conn.Conn.OnDrain: each takes a single handler and
// would unbind the edge's recording; use this type's OnApp instead.
type Conn struct {
	*wsmixer.Conn
	Raw   *websocket.Conn
	Index int // 1-based accept order

	srv   *Server
	mu    sync.Mutex
	onApp func(raw json.RawMessage)
}

// OnApp binds fn to this connection's app messages, in place of
// Options.OnApp. Unlike wsmixer.Conn.OnApp it doesn't unbind the edge's own
// recording, which runs first, so every frame is in Server.Events by the
// time fn sees it.
func (c *Conn) OnApp(fn func(raw json.RawMessage)) {
	c.mu.Lock()
	c.onApp = fn
	c.mu.Unlock()
}

func (c *Conn) deliver(raw json.RawMessage) {
	f := ParseFrame(raw)
	c.srv.record(Event{Kind: EventApp, Conn: c.Index, Frame: f})
	c.mu.Lock()
	fn := c.onApp
	c.mu.Unlock()
	switch {
	case fn != nil:
		fn(raw)
	case c.srv.opts.OnApp != nil:
		c.srv.opts.OnApp(c, f)
	}
}

// Service is one success in a "registered" reply.
type Service struct {
	Name    string
	ID      string
	URL     string
	Created bool
}

// RegisterError is one per-service refusal in a "registered" reply (e.g.
// QUOTA_EXCEEDED, CONFLICT, SERVER_DISABLED).
type RegisterError struct {
	Name    string
	Code    string
	Message string
}

// Accepted builds a success for each name at id "id-<name>" and URL
// "https://<name>.example/mcp".
func Accepted(names ...string) []Service {
	out := make([]Service, len(names))
	for i, n := range names {
		out[i] = Service{Name: n, ID: "id-" + n, URL: "https://" + n + ".example/mcp"}
	}
	return out
}

// SendOp sends {"mcpwarp":{"v":1,"op":op,...fields}}, for an op the
// helpers below don't cover. Kept for the ServiceGroup adapter's upcoming
// tests.
func (c *Conn) SendOp(op string, fields map[string]any) error {
	body := map[string]any{"v": 1, "op": op}
	for k, v := range fields {
		body[k] = v
	}
	return c.SendApp(context.Background(), map[string]any{"mcpwarp": body})
}

// Registered replies "registered" with services and errs; either may be nil
// and still goes out as an empty array.
func (c *Conn) Registered(services []Service, errs []RegisterError) error {
	svcs := make([]map[string]any, len(services))
	for i, s := range services {
		svcs[i] = map[string]any{"name": s.Name, "id": s.ID, "url": s.URL, "created": s.Created}
	}
	es := make([]map[string]any, len(errs))
	for i, e := range errs {
		es[i] = map[string]any{"name": e.Name, "code": e.Code, "message": e.Message}
	}
	return c.SendOp("registered", map[string]any{"services": svcs, "errors": es})
}

// ReplyRegisteredAll binds a handler that answers every "register" with a
// "registered" accepting each name it carried (see Accepted). seen, if
// non-nil, gets each batch's names first.
func (c *Conn) ReplyRegisteredAll(seen func(names []string)) {
	c.OnApp(func(raw json.RawMessage) {
		names := RegisterNames(raw)
		if names == nil {
			return
		}
		if seen != nil {
			seen(names)
		}
		_ = c.Registered(Accepted(names...), nil)
	})
}

// Disable sends a dashboard "disable" for id.
func (c *Conn) Disable(id, reason string) error {
	return c.SendOp("disable", map[string]any{"id": id, "reason": reason})
}

// Enable sends a dashboard "enable" for id/name.
func (c *Conn) Enable(id, name string) error {
	return c.SendOp("enable", map[string]any{"id": id, "name": name})
}

// Conflict sends a per-name take-over (DESIGN.md §8's "same name from two
// sessions").
func (c *Conn) Conflict(id, name, reason string) error {
	return c.SendOp("conflict", map[string]any{"id": id, "name": name, "reason": reason})
}

// SendError sends a connection-level "error" op (UNSUPPORTED_VERSION,
// CONNECTION_LIMIT, TOO_MANY_SERVICES, ...).
func (c *Conn) SendError(code, message string) error {
	return c.SendOp("error", map[string]any{"code": code, "message": message})
}

// Sentinel sends an op no client recognizes. A client handles app messages
// in order and logs-and-ignores an unknown op (DESIGN.md §8), so once that
// log line shows, everything sent before the sentinel has been applied.
func (c *Conn) Sentinel() error {
	return c.SendOp("sentinel", nil)
}

// StartDrain begins a server drain on its own goroutine (Drain blocks until
// its deadline or the streams empty, and must never run on the delivery
// goroutine) and returns Drain's result. Kept for the ServiceGroup adapter's
// upcoming tests.
func (c *Conn) StartDrain(reason string, deadline time.Duration) <-chan error {
	done := make(chan error, 1)
	go func() { done <- c.Drain(context.Background(), reason, wsmixer.DrainOptions{Deadline: deadline}) }()
	return done
}

// CloseBare closes the WebSocket directly with code and reason, with no
// preceding stream-0 error{} frame — unlike the embedded Close, which always
// sends one first.
func (c *Conn) CloseBare(code websocket.StatusCode, reason string) error {
	return c.Raw.Close(code, reason)
}
