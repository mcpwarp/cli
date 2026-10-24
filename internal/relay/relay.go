package relay

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mcpwarp/cli/internal/registry"
	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// maxHeadBytes bounds the HTTP request head (request line + headers) read
// off a stream — DESIGN.md §3/§7's 64 KiB cap.
const maxHeadBytes = 64 * 1024

// requestHeadDeadline bounds how long a request head may take to arrive in
// full, starting when readRequest is called — a peer that opens a stream and
// never finishes sending a head would otherwise tie it up forever. Matches
// Node request-parser.ts's HEAD_DEADLINE_MS. A var, not a const, so a test
// can shorten it rather than actually waiting out 10s.
var requestHeadDeadline = 10 * time.Second

// errRequestHeadTimeout is returned by readRequest when requestHeadDeadline
// elapses before a request head is fully parsed. By the time it's returned,
// the stream has already been Reset by the deadline timer itself.
var errRequestHeadTimeout = errors.New("relay: request head did not arrive within the deadline")

// unreachableWarnInterval bounds how often RelayStream's "local server
// unreachable" warn may repeat for the same service — Node forward/
// relay.ts's UNREACHABLE_WARN_INTERVAL_MS: a busy unreachable service can
// open many streams a second, and warning on every one of them is noise.
const unreachableWarnInterval = 60 * time.Second

// pathRefusedWarn is the first refused non-/mcp request's warn per service.
const pathRefusedWarn = "request refused: only /mcp is forwarded since mcpwarp v0.3.0; point the MCP client at the service's /mcp URL"

// UnreachableWarnLimiter rate-limits the "local server unreachable" warn to
// once per unreachableWarnInterval per service name, and the path-rule warn
// to the first refused non-/mcp request per service name. One instance is
// shared across every RelayStream call on the same tunnel connection (the
// caller owns it, via Options.UnreachableWarn); a nil Options.UnreachableWarn
// disables rate limiting (every call warns), which is what tests get by
// default.
type UnreachableWarnLimiter struct {
	mu          sync.Mutex
	last        map[string]time.Time
	pathRefused map[string]bool
}

// NewUnreachableWarnLimiter builds an empty UnreachableWarnLimiter.
func NewUnreachableWarnLimiter() *UnreachableWarnLimiter {
	return &UnreachableWarnLimiter{last: make(map[string]time.Time), pathRefused: make(map[string]bool)}
}

// FirstPathRefusal reports whether this is name's first refused non-/mcp
// request, recording it if so.
func (l *UnreachableWarnLimiter) FirstPathRefusal(name string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.pathRefused[name] {
		return false
	}
	l.pathRefused[name] = true
	return true
}

// Allow reports whether name's warn should fire right now, recording now as
// name's last warn time if so.
func (l *UnreachableWarnLimiter) Allow(name string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if last, ok := l.last[name]; ok && now.Sub(last) < unreachableWarnInterval {
		return false
	}
	l.last[name] = now
	return true
}

// Stream is the subset of *wsmixer.Stream this package needs, narrowed so
// tests can drive RelayStream against a plain net.Pipe half instead of a
// real ws-mixer connection.
type Stream interface {
	io.Reader
	io.Writer
	CloseWrite() error
	Reset(code wsmixer.ErrorCode, msg string) error
}

// Options configures RelayStream.
type Options struct {
	Registry  *registry.Registry
	Forwarder *Forwarder
	// Targets is the local forward target per configured service, keyed by
	// config name (not the tunnel-assigned id) — DESIGN.md §3: a bridge's
	// loopback URL for kind:"stdio", the configured URL for kind:"http".
	Targets map[string]*url.URL
	Log     *slog.Logger
	// Ctx bounds the Forward call's lifetime — the caller (internal/tunnel)
	// ties this to the stream's own goroutine, cancelled when the stream
	// dies or the tunnel is shutting down (Node forward/relay.ts's
	// AbortController). Defaults to context.Background() when nil, e.g. for
	// tests that don't need cancellation.
	Ctx context.Context
	// UnreachableWarn rate-limits the "local server unreachable" warn below
	// (once per service per unreachableWarnInterval) and the path-rule warn
	// (first refused non-/mcp request per service) — shared by the caller
	// across every RelayStream call on the same tunnel connection. nil (the
	// zero value, e.g. in tests) disables rate limiting.
	UnreachableWarn *UnreachableWarnLimiter
	// OnPathRefused, when set, is called with a known service's name, the
	// refused path and the service's public URL alongside the path-rule warn
	// (so under UnreachableWarn, once per service), after the 404 is
	// written — the caller (internal/tunnel) surfaces it on the bus, which
	// may block.
	OnPathRefused func(service, path, publicURL string)
}

func (o Options) log() *slog.Logger {
	if o.Log != nil {
		return o.Log
	}
	return slog.Default()
}

func (o Options) ctx() context.Context {
	if o.Ctx != nil {
		return o.Ctx
	}
	return context.Background()
}

// safeReset best-effort resets stream — the stream may already be gone by
// the time a failure path tries, in which case Reset itself errors; that's
// expected and not worth surfacing.
func safeReset(stream Stream, code wsmixer.ErrorCode, msg string) {
	_ = stream.Reset(code, msg)
}

// RelayStream handles exactly one stream end to end: parses the HTTP
// request off it, refuses any path but /mcp (MCPWARP.md §4.3), resolves the
// target service by the request's Host header, forwards, and streams the
// response back. Never throws: every failure path ends in either a
// best-effort HTTP-shaped error response or, if the stream itself is
// unusable, stream.Reset — RelayStream itself always returns.
func RelayStream(stream Stream, opts Options) {
	log := opts.log()

	req, headRemaining, headErr := readRequest(stream)
	if headErr != nil {
		if errors.Is(headErr, errRequestHeadTimeout) {
			// Already Reset by the deadline timer itself.
			log.Debug("request head did not arrive within the deadline")
			return
		}
		if headRemaining == 0 {
			// The LimitedReader's budget hit zero before a request could be
			// assembled: an oversized head, not a torn stream — reading past
			// maxHeadBytes surfaces as io.ErrUnexpectedEOF from
			// http.ReadRequest, indistinguishable from a genuine EOF by
			// error value alone, so this is checked first.
			log.Debug("request head exceeded max size", "err", headErr)
			if err := writeErrorResponse(stream, 431, map[string]string{"error": "request head too large"}, ""); err != nil {
				safeReset(stream, wsmixer.ProtocolErrorCode, "request head too large")
			}
			return
		}
		if errors.Is(headErr, io.EOF) || errors.Is(headErr, io.ErrUnexpectedEOF) {
			// The stream ended before a request could even be assembled
			// (peer reset, connection died) — reset it so the peer's stream
			// slot is freed instead of left hanging.
			log.Debug("stream ended before a request could be parsed")
			safeReset(stream, wsmixer.CancelCode, "stream ended before request head completed")
			return
		}
		log.Debug("malformed request head", "err", headErr)
		if err := writeErrorResponse(stream, 400, map[string]string{"error": "bad request"}, ""); err != nil {
			safeReset(stream, wsmixer.ProtocolErrorCode, "malformed request head")
		}
		return
	}

	if !supportedTargetForm(req.RequestURI) {
		if err := writeErrorResponse(stream, 400, map[string]string{"error": "unsupported request target form"}, req.Method); err != nil {
			safeReset(stream, wsmixer.InternalErrorCode, "unsupported request target form")
		}
		return
	}

	hostHeader := req.Host
	if hostHeader == "" {
		if err := writeErrorResponse(stream, 400, map[string]string{"error": "bad request"}, req.Method); err != nil {
			safeReset(stream, wsmixer.InternalErrorCode, "bad request")
		}
		return
	}

	// The refusals before Forward leave the request body unread; refusal
	// drains it before CLOSE (MCPWARP.md §4.4 "Ending a refused stream").
	refusal := drainBeforeClose{Stream: stream, body: req.Body}

	// MCPWARP.md §4.4 row 2: the path check precedes every service check, so
	// a non-/mcp path answers 404 even for an unknown or disabled service.
	if path := requestPath(req.RequestURI); path != "/mcp" {
		// Warn (and call OnPathRefused) once per known service, so a client
		// on a path other than /mcp is visible without --verbose; unknown
		// hosts stay at Debug. The lookup is for the log and hook only; the
		// 404 still precedes every service check.
		name, entry, known := opts.Registry.ResolveByHost(hostHeader)
		first := known && (opts.UnreachableWarn == nil || opts.UnreachableWarn.FirstPathRefusal(name))
		if first {
			log.Warn(pathRefusedWarn, "service", name, "path", path, "url", entry.URL)
		} else {
			log.Debug("request for a path other than /mcp", "host", hostHeader, "target", req.RequestURI)
		}
		if err := writeErrorResponse(refusal, 404, map[string]string{"error": "not_found"}, req.Method); err != nil {
			safeReset(stream, wsmixer.InternalErrorCode, "not found")
		}
		if first && opts.OnPathRefused != nil {
			opts.OnPathRefused(name, path, entry.URL)
		}
		return
	}

	name, entry, ok := opts.Registry.ResolveByHost(hostHeader)
	if !ok {
		// MCPWARP.md §4.8 row 3b: no service selected.
		log.Warn("request for an unknown service", "host", hostHeader)
		if err := writeErrorResponse(refusal, 503, map[string]string{"error": "service unavailable"}, req.Method); err != nil {
			safeReset(stream, wsmixer.InternalErrorCode, "unknown service")
		}
		return
	}
	if opts.Registry.IsDisabled(entry.ID) {
		log.Warn("request for a disabled service", "host", hostHeader)
		if err := writeErrorResponse(refusal, 503, map[string]string{"error": "service disabled"}, req.Method); err != nil {
			safeReset(stream, wsmixer.InternalErrorCode, "service disabled")
		}
		return
	}

	target := opts.Targets[name]
	if target == nil {
		log.Error("registered service has no configured forward target", "name", name)
		if err := writeErrorResponse(refusal, 502, map[string]string{"error": "local server not configured"}, req.Method); err != nil {
			safeReset(stream, wsmixer.InternalErrorCode, "local server not configured")
		}
		return
	}

	resp, err := opts.Forwarder.Forward(opts.ctx(), req, target)
	if err != nil {
		switch {
		case errors.Is(err, ErrTargetUnreachable):
			if opts.UnreachableWarn == nil || opts.UnreachableWarn.Allow(name, time.Now()) {
				log.Warn("local server unreachable", "target", target.String(), "err", err)
			}
			if werr := writeErrorResponse(stream, 502, map[string]string{"error": "local server unreachable"}, req.Method); werr != nil {
				safeReset(stream, wsmixer.InternalErrorCode, "local server unreachable")
			}
		case errors.Is(err, ErrTargetConnectionReset):
			log.Warn("connection to local server reset mid-request", "target", target.String())
			if werr := writeErrorResponse(stream, 502, map[string]string{"error": "connection reset"}, req.Method); werr != nil {
				safeReset(stream, wsmixer.InternalErrorCode, "connection reset")
			}
		case errors.Is(err, ErrTargetTimeout):
			log.Warn("local server timed out", "target", target.String())
			if werr := writeErrorResponse(stream, 504, map[string]string{"error": "local server timed out"}, req.Method); werr != nil {
				safeReset(stream, wsmixer.InternalErrorCode, "local server timed out")
			}
		default:
			log.Error("forward failed", "target", target.String(), "err", err)
			if werr := writeErrorResponse(stream, 502, map[string]string{"error": "bad gateway"}, req.Method); werr != nil {
				safeReset(stream, wsmixer.InternalErrorCode, "forward failed")
			}
		}
		return
	}
	defer resp.Body.Close()

	if err := writeResponse(stream, resp, req.Method); err != nil {
		log.Debug("failed writing response to stream", "err", err)
		safeReset(stream, wsmixer.InternalErrorCode, "internal error")
	}
}

// readRequest reads exactly one HTTP request head (capped at maxHeadBytes,
// and at requestHeadDeadline) off stream and returns the parsed
// *http.Request, whose Body then reads unbounded from the same underlying
// stream, plus the LimitedReader's remaining budget at the point of failure
// (0 means the cap, not a torn stream, caused the error).
func readRequest(stream Stream) (*http.Request, int64, error) {
	var timedOut atomic.Bool
	timer := time.AfterFunc(requestHeadDeadline, func() {
		timedOut.Store(true)
		safeReset(stream, wsmixer.CancelCode, "request head deadline exceeded")
	})

	lr := &io.LimitedReader{R: stream, N: maxHeadBytes}
	br := bufio.NewReader(lr)
	req, err := http.ReadRequest(br)
	stopped := timer.Stop()
	if err != nil {
		if timedOut.Load() {
			return nil, lr.N, errRequestHeadTimeout
		}
		return nil, lr.N, err
	}
	if !stopped && timedOut.Load() {
		// timer.Stop() returning false means the deadline fired (and
		// already Reset the stream) concurrently with http.ReadRequest
		// returning successfully off data that was already fully buffered
		// before the reset landed. The stream is no longer usable either
		// way, so this races against — and must still be treated as — the
		// timeout, not a genuine success.
		return nil, lr.N, errRequestHeadTimeout
	}
	// Headers are parsed; lift the cap so the body isn't truncated by the
	// head's own budget.
	lr.N = 1<<63 - 1
	return req, lr.N, nil
}

// supportedTargetForm reports whether target is origin-form (starts with
// "/") or absolute-form http(s)://… — MCPWARP.md §4.2's Target row; `*` and
// authority-form (which http.ReadRequest accepts, for OPTIONS and CONNECT)
// are refused with 400 before the §4.3 path check.
func supportedTargetForm(target string) bool {
	if strings.HasPrefix(target, "/") {
		return true
	}
	scheme, _, ok := strings.Cut(target, "://")
	return ok && (strings.EqualFold(scheme, "http") || strings.EqualFold(scheme, "https"))
}

// requestPath is MCPWARP.md §4.3's requestPath(target): the raw request-line
// target with any scheme://authority stripped, cut at the first '?' or '#'.
// Compared as received — never decoded or cleaned — so "/%6Dcp" and
// "/a/../mcp" are not "/mcp".
func requestPath(target string) string {
	if !strings.HasPrefix(target, "/") {
		if _, rest, ok := strings.Cut(target, "://"); ok {
			if i := strings.IndexAny(rest, "/?#"); i >= 0 {
				target = rest[i:]
			} else {
				target = ""
			}
		}
	}
	if i := strings.IndexAny(target, "?#"); i >= 0 {
		target = target[:i]
	}
	return target
}

// drainBeforeClose holds back a refusal's CLOSE until the request body still
// unread has been read and dropped — MCPWARP.md §4.4 "Ending a refused
// stream": the agent drains rather than RESETs, the edge's own body cap
// bounding how much.
type drainBeforeClose struct {
	Stream
	body io.Reader
}

func (d drainBeforeClose) CloseWrite() error {
	_, _ = io.Copy(io.Discard, d.body)
	return d.Stream.CloseWrite()
}

func writeErrorResponse(stream Stream, status int, payload map[string]string, method string) error {
	body, _ := json.Marshal(payload)
	resp := &http.Response{
		StatusCode:    status,
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        http.Header{"Content-Type": []string{"application/json"}},
		ContentLength: int64(len(body)),
		Body:          io.NopCloser(bytes.NewReader(body)),
	}
	return writeResponse(stream, resp, method)
}
