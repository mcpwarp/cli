package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mcpwarp/cli/internal/eventbus"
)

const (
	defaultRequestTimeout = 5 * time.Minute
	// maxBodyBytes matches stdio_child.go's stdout line cap — no reason the
	// inbound body should be allowed to grow unbounded either.
	maxBodyBytes = 16 * 1024 * 1024
	// maxPending: past this many simultaneously in-flight client requests
	// (regular + subscriptions), refuse new ones with 503.
	defaultMaxPending = 1024
	// abandonedCap bounds the set of ids abandoned to a request timeout, so
	// a late child response for one is recognized and dropped rather than
	// logged as a mystery.
	abandonedCap = 1000
	// closeBarrier: how long Close waits for in-flight request handlers to
	// finish writing their own response before force-closing sockets.
	closeBarrier = 2 * time.Second
	// maxHeaderBytes: the 64 KiB request head cap (DESIGN.md §7). Counts
	// the request line too, and net/http's bufio slop pushes the
	// effective cap to roughly 68 KiB; overflow gets a bare 431 from
	// net/http itself, no JSON body. This is unrelated to Node's own
	// bridge, which has no such cap — that 64 KiB figure belongs to the
	// M3 relay's parser — but keeping the setting here matches DESIGN.md.
	maxHeaderBytes = 64 * 1024
	// ensureChildTimeout bounds how long one request waits on
	// StartBridgeOptions.EnsureChild to bring a down child back up before
	// falling through to the usual "local server is not running" 502. A
	// fresh stdio child is normally up well within it; the bound is there
	// so a slow spawn can't hold the request hostage the way the 5 min
	// request timeout would (DESIGN.md §7).
	ensureChildTimeout = 5 * time.Second

	subscriptionIDMetaKey = "io.modelcontextprotocol/subscriptionId"
)

// jsonID is either a string or a JSON number (Go: float64, from
// encoding/json). isJSONID reports whether v is a valid JSON-RPC id shape.
func isJSONID(v any) bool {
	switch v.(type) {
	case string, float64, json.Number:
		return true
	}
	return false
}

func isRequestParams(msg map[string]any) (map[string]any, bool) {
	return isRecord(msg["params"])
}

func getMeta(container map[string]any) (map[string]any, bool) {
	if container == nil {
		return nil, false
	}
	return isRecord(container["_meta"])
}

// numericID converts a decoded JSON-RPC id (float64/json.Number) to an
// int64 internal id, if it is numeric and integral.
func numericID(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	}
	return 0, false
}

func internalProgressToken(id int64) string {
	return "p" + strconv.FormatInt(id, 10)
}

func internalIDFromProgressToken(token any) (int64, bool) {
	s, ok := token.(string)
	if !ok || len(s) < 2 || s[0] != 'p' {
		return 0, false
	}
	n, err := strconv.ParseInt(s[1:], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// originAllowed: no Origin header, the literal "null", or
// http://127.0.0.1*/http://localhost* — everything else 403. Literal, not
// scheme-agnostic (DESIGN.md §7).
func originAllowed(origin string, has bool) bool {
	if !has || origin == "null" {
		return true
	}
	return strings.HasPrefix(origin, "http://127.0.0.1") || strings.HasPrefix(origin, "http://localhost")
}

// SupervisedBridge is the subset of Bridge the supervisor drives.
type SupervisedBridge interface {
	ReplaceChild(child *StdioChild)
	MarkFailed(reason string)
}

// StartBridgeOptions configures StartBridge.
type StartBridgeOptions struct {
	Name           string
	SpawnSpec      SpawnSpec
	Log            *slog.Logger
	RequestTimeout time.Duration // 0 -> defaultRequestTimeout
	MaxPending     int           // 0 -> defaultMaxPending
	// ProbeTimeout bounds the lazy server/discover probe (0 ->
	// defaultProbeTimeout); HeadHoldTimeout bounds how long a modern
	// request's SSE head waits for the child's first message (0 ->
	// defaultHeadHold); InitializeHoldTimeout is the same for a 2025-style
	// initialize, which waits so Mcp-Session-Id can ride on the head (0 ->
	// defaultInitializeHold). MaxReprobeInterval caps the re-probe backoff
	// (0 -> reprobeBackoffFactor probe timeouts). Test seams, like
	// RequestTimeout.
	ProbeTimeout          time.Duration
	HeadHoldTimeout       time.Duration
	InitializeHoldTimeout time.Duration
	MaxReprobeInterval    time.Duration
	// Bus, if non-nil, is passed to NewStdioChildWithBus so this bridge's
	// initial child publishes its stderr lines as eventbus.LogLine
	// telemetry (DESIGN.md §3) — the same wiring a supervisor's own
	// respawned children get via supervisor.Deps.Spawn.
	Bus *eventbus.Bus
	// EnsureChild, if non-nil, is the lazy-respawn safety net (DESIGN.md
	// §7): when a POST carrying a method (a request, a notification or
	// subscriptions/listen) finds the current child has exited, the bridge
	// calls it once, with a context bounded by ensureChildTimeout (and by
	// the request's own context), before routing it. A bare client response
	// never triggers it: it answers a request from the dead child's own id
	// space, so a stray late one must not resurrect a disabled service.
	// Returning nil means the supervisor believes a child is live (normally
	// one just handed over via ReplaceChild), and the request proceeds
	// against the current child; a child that has in fact exited, but whose
	// exit the supervisor hasn't processed yet so it still reads healthy,
	// still yields the 502. Returning an error refuses the respawn, and the
	// request gets the same "local server is not running" 502 it would
	// without EnsureChild. The expected implementation is a
	// supervisor's EnsureRunning behind a guard: return an error, without
	// calling EnsureRunning, for a service the user disabled locally (`d`,
	// §9), so a stray request can never undo that. It runs on the request's
	// goroutine and must be safe for concurrent calls. Nil keeps the old
	// behaviour exactly.
	EnsureChild func(ctx context.Context) error
}

// pendingEntry is one in-flight client request. once guards every write to
// the client: resolveWith, rejectWith, resolveInitialized, respond and
// abandon are mutually exclusive, and abandon (run by the handler on every
// exit path) makes any later one a no-op, so nothing writes to the
// ResponseWriter after the handler returns.
type pendingEntry struct {
	originalID    any
	progressToken any           // nil if the request carried none
	sse           *sseResponder // non-nil iff Accept: text/event-stream
	plain         http.ResponseWriter
	extraHeaders  map[string]string
	timer         *time.Timer
	done          chan struct{}
	once          sync.Once
	// modern: a modern-shaped request to a modern child, whose final
	// errors get an HTTP status mapped from the JSON-RPC code.
	modern bool
	// raw, non-nil only for a 2025-style initialize, hands the child's
	// answer to the handler goroutine instead of writing it from the
	// child's pump (see completeInitialize).
	raw chan map[string]any
	// revoke releases a provisional session handed out with a held
	// initialize head; set only for initialize.
	revoke func()
	// era, set only for initialize, is the process the child's answer
	// reports on, whether or not the handler is still waiting for it.
	era *eraState
}

// releaseProvisional revokes the provisional session this response carries,
// if any: the initialize it answered was rejected, abandoned or failed.
func (e *pendingEntry) releaseProvisional() {
	if e.sse != nil && e.revoke != nil && e.sse.takeProvisional() {
		e.revoke()
	}
}

func (e *pendingEntry) resolveWith(msg map[string]any) {
	status := http.StatusOK
	if e.modern {
		status = modernErrorStatus(msg)
	}
	e.respond(status, msg)
}

// respond writes msg (its id rewritten to the client's) with status. In SSE
// form a non-200 status is only possible while the head is still held;
// otherwise the message goes out as a frame in the already-committed 200
// stream.
func (e *pendingEntry) respond(status int, msg map[string]any) {
	e.once.Do(func() {
		e.timer.Stop()
		if status != http.StatusOK {
			e.releaseProvisional()
		}
		rewritten := cloneMsg(msg)
		rewritten["id"] = e.originalID
		if e.sse != nil {
			if status == http.StatusOK || !e.sse.finalStatus(status, e.extraHeaders, rewritten) {
				e.sse.writeFrame(rewritten)
				e.sse.end()
			}
		} else {
			writeJSONHeaders(e.plain, status, e.extraHeaders, rewritten)
		}
		close(e.done)
	})
}

// resolveInitialized is resolveWith for a successful initialize: session
// supplies the Mcp-Session-Id header, and runs (so the session gets minted)
// only if the head can still carry it.
func (e *pendingEntry) resolveInitialized(msg map[string]any, session func() map[string]string) {
	e.once.Do(func() {
		e.timer.Stop()
		rewritten := cloneMsg(msg)
		rewritten["id"] = e.originalID
		if e.sse != nil {
			if !e.sse.setHeadersIfUncommitted(session) {
				session() // head already out with a provisional id: confirm it
				e.sse.takeProvisional()
			}
			e.sse.writeFrame(rewritten)
			e.sse.end()
		} else {
			writeJSONHeaders(e.plain, http.StatusOK, session(), rewritten)
		}
		close(e.done)
	})
}

func (e *pendingEntry) rejectWith(err error) {
	e.once.Do(func() {
		e.timer.Stop()
		e.releaseProvisional()
		if errors.Is(err, errChildRestarted) {
			body := map[string]any{
				"jsonrpc": "2.0",
				"id":      e.originalID,
				"error":   map[string]any{"code": -32000, "message": "local server restarted"},
			}
			if e.sse != nil {
				if !e.sse.finalStatus(502, nil, body) {
					e.sse.writeFrame(body)
					e.sse.end()
				}
			} else {
				writeJSONFlush(e.plain, 502, body)
			}
			close(e.done)
			return
		}
		status, body := 502, map[string]any{"error": "local server is not running"}
		if errors.Is(err, errUpstreamTimeout) {
			status, body = 504, map[string]any{"error": "upstream timeout"}
		}
		if e.sse != nil {
			if !e.sse.finalStatus(status, nil, body) {
				e.sse.end()
			}
		} else {
			writeJSONFlush(e.plain, status, body)
		}
		close(e.done)
	})
}

// abandon claims the entry without writing anything: the client is gone, or
// the handler is unwinding. It waits out a writer already inside once.
func (e *pendingEntry) abandon() {
	e.once.Do(func() {
		e.timer.Stop()
		e.releaseProvisional()
		if e.sse != nil {
			e.sse.detach()
		}
		close(e.done)
	})
}

type subscriptionEntry struct {
	originalID any
	res        *sseResponder
	done       chan struct{}
}

var (
	errUpstreamTimeout = errors.New("upstream timeout")
	errChildGone       = errors.New("bridge closing")
	errChildRestarted  = errors.New("stdio child exited")
)

// Bridge is one streamable-HTTP <-> stdio bridge: one net/http listener on
// 127.0.0.1:0 in front of one StdioChild. Ported from http-bridge.ts.
type Bridge struct {
	name string
	log  *slog.Logger

	timeout      time.Duration
	maxPending   int
	ensureChild  func(ctx context.Context) error
	probeTimeout time.Duration
	reprobeCap   time.Duration
	headHold     time.Duration
	initHold     time.Duration

	listener net.Listener
	server   *http.Server
	port     int

	session *session
	pushHub *pushStreamHub

	// mu guards everything below, including era (the current process's
	// eraState) and every eraState's fields.
	mu            sync.Mutex
	child         *StdioChild
	era           *eraState
	closing       bool
	failed        string
	hasFailed     bool
	pending       map[int64]*pendingEntry
	subscriptions map[int64]*subscriptionEntry
	abandoned     map[int64]struct{}
	abandonedFIFO []int64
	// abandonedInit maps an abandoned initialize's id to its process, so the
	// child's late answer still reaches the era state; its ids are a subset
	// of abandoned's and are evicted with them.
	abandonedInit map[int64]*eraState
	nextID        int64

	wg sync.WaitGroup
}

func cloneMsg(msg map[string]any) map[string]any {
	out := make(map[string]any, len(msg)+1)
	for k, v := range msg {
		out[k] = v
	}
	return out
}

// StartBridge spawns the stdio child and starts the loopback HTTP
// listener. On failure at any point the spawned child is closed before
// the error is returned.
func StartBridge(opts StartBridgeOptions) (*Bridge, error) {
	log := opts.Log
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	timeout := opts.RequestTimeout
	if timeout == 0 {
		timeout = defaultRequestTimeout
	}
	maxPending := opts.MaxPending
	if maxPending == 0 {
		maxPending = defaultMaxPending
	}
	probeTimeout := opts.ProbeTimeout
	if probeTimeout == 0 {
		probeTimeout = defaultProbeTimeout
	}
	reprobeCap := opts.MaxReprobeInterval
	if reprobeCap == 0 {
		reprobeCap = reprobeBackoffFactor * probeTimeout
	}
	headHold := opts.HeadHoldTimeout
	if headHold == 0 {
		headHold = defaultHeadHold
	}
	initHold := opts.InitializeHoldTimeout
	if initHold == 0 {
		initHold = defaultInitializeHold
	}

	child, err := NewStdioChildWithBus(opts.SpawnSpec, log.With("server", opts.Name), opts.Name, opts.Bus)
	if err != nil {
		return nil, err
	}

	b := &Bridge{
		name:          opts.Name,
		log:           log,
		timeout:       timeout,
		maxPending:    maxPending,
		ensureChild:   opts.EnsureChild,
		probeTimeout:  probeTimeout,
		reprobeCap:    reprobeCap,
		headHold:      headHold,
		initHold:      initHold,
		child:         child,
		session:       newSession(),
		pushHub:       newPushStreamHub(log.With("server", opts.Name)),
		pending:       make(map[int64]*pendingEntry),
		subscriptions: make(map[int64]*subscriptionEntry),
		abandoned:     make(map[int64]struct{}),
		abandonedInit: make(map[int64]*eraState),
		nextID:        1,
	}
	b.era = b.newEraState(child)
	b.wireChild(child, b.era)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = child.Close()
		return nil, err
	}
	b.listener = ln
	b.port = ln.Addr().(*net.TCPAddr).Port

	mux := http.NewServeMux()
	mux.HandleFunc("/", b.handle)
	b.server = &http.Server{
		Handler:        mux,
		MaxHeaderBytes: maxHeaderBytes,
	}
	go func() {
		_ = b.server.Serve(ln)
	}()

	return b, nil
}

// Port is the bound loopback port.
func (b *Bridge) Port() int { return b.port }

// URL is the bridge's http://127.0.0.1:<port>/mcp endpoint.
func (b *Bridge) URL() string { return fmt.Sprintf("http://127.0.0.1:%d/mcp", b.port) }

// GetCurrentChild returns the currently active StdioChild — supervisor hook.
func (b *Bridge) GetCurrentChild() *StdioChild {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.child
}

// ReplaceChild swaps in a freshly spawned child after a supervisor
// restart. The listener/port/session are untouched; only routing wiring
// moves to the new child, which starts with a fresh era state (the old
// process's discover result says nothing about it). Clears a
// previously-latched MarkFailed.
func (b *Bridge) ReplaceChild(child *StdioChild) {
	st := b.newEraState(child)
	b.mu.Lock()
	b.child = child
	b.endEraLocked(b.era)
	// The old process's late initialize answers can only reach its ended
	// state; drop them rather than pin it until FIFO eviction.
	for id, old := range b.abandonedInit {
		if old == b.era {
			delete(b.abandonedInit, id)
		}
	}
	b.era = st
	b.hasFailed = false
	b.failed = ""
	b.mu.Unlock()
	b.wireChild(child, st)
}

// MarkFailed latches a permanent-failure response for every new request
// until the next ReplaceChild or Close.
func (b *Bridge) MarkFailed(reason string) {
	b.mu.Lock()
	b.hasFailed = true
	b.failed = reason
	b.mu.Unlock()
	b.log.Error("stdio child supervisor gave up restarting; service is now failed", "server", b.name, "reason", reason)
}

func (b *Bridge) currentChild() *StdioChild {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.child
}

// ensureLiveChild runs StartBridgeOptions.EnsureChild when the current
// child has exited (DESIGN.md §7's lazy respawn). It reports nothing: the
// caller re-reads currentChild() afterwards, so a successful respawn is
// picked up through ReplaceChild, and a refused or failed one leaves the
// dead child in place to produce today's "not running" response.
func (b *Bridge) ensureLiveChild(parent context.Context) {
	if b.ensureChild == nil || !b.currentChild().HasExited() {
		return
	}
	ctx, cancel := context.WithTimeout(parent, ensureChildTimeout)
	defer cancel()
	if err := b.ensureChild(ctx); err != nil {
		b.log.Debug("stdio child is down and was not respawned for this request", "server", b.name, "err", err)
	}
}

func (b *Bridge) addAbandoned(id int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.abandoned[id]; !ok {
		b.abandoned[id] = struct{}{}
		b.abandonedFIFO = append(b.abandonedFIFO, id)
		if len(b.abandonedFIFO) > abandonedCap {
			oldest := b.abandonedFIFO[0]
			b.abandonedFIFO = b.abandonedFIFO[1:]
			delete(b.abandoned, oldest)
			delete(b.abandonedInit, oldest)
		}
	}
}

func (b *Bridge) isAbandoned(id int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.abandoned[id]
	return ok
}

// findInternalIDForOriginalID resolves a legacy client's own
// notifications/cancelled requestId (in the client's id space) back to the
// bridge-minted internal id. Picks the most recently issued match if more
// than one pending request happens to share the original id.
func (b *Bridge) findInternalIDForOriginalID(originalID any) (int64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	found := int64(0)
	ok := false
	for internalID, entry := range b.pending {
		if entry.originalID == originalID {
			if !ok || internalID > found {
				found = internalID
				ok = true
			}
		}
	}
	return found, ok
}

func (b *Bridge) handleChildResponse(id int64, msg map[string]any) {
	b.mu.Lock()
	sub, hasSub := b.subscriptions[id]
	if hasSub {
		delete(b.subscriptions, id)
	}
	var entry *pendingEntry
	if !hasSub {
		entry = b.pending[id]
		if entry != nil {
			delete(b.pending, id)
		}
	}
	abandoned := b.isAbandonedLocked(id)
	var initEra *eraState
	switch {
	case entry != nil:
		initEra = entry.era
	case !hasSub:
		initEra = b.abandonedInit[id]
		delete(b.abandonedInit, id)
	}
	b.mu.Unlock()

	// An initialize answer reaches the era state here, before any handler
	// sees it, so it counts even if nobody is waiting for it any more.
	if initEra != nil {
		b.noteInitializeAnswer(initEra, msg)
	}

	if hasSub {
		rewritten := cloneMsg(msg)
		rewritten["id"] = sub.originalID
		rewriteSubscriptionTag(rewritten, id, sub.originalID)
		sub.res.writeFrame(rewritten)
		sub.res.end()
		close(sub.done)
		return
	}
	if entry != nil {
		if entry.raw != nil {
			entry.raw <- msg // buffered 1, and a request has one answer
			return
		}
		entry.resolveWith(msg)
		return
	}
	if abandoned {
		b.log.Debug("late response for an abandoned (timed-out) request id, dropping", "server", b.name, "id", id)
		return
	}
	b.log.Debug("response with unrecognized id from stdio child, dropping", "server", b.name, "id", id)
}

func (b *Bridge) isAbandonedLocked(id int64) bool {
	_, ok := b.abandoned[id]
	return ok
}

// rewriteSubscriptionTag rewrites params/result._meta[subscriptionIdKey]
// from `from` to `to` in place, if present.
func rewriteSubscriptionTag(msg map[string]any, from, to any) {
	for _, key := range []string{"params", "result"} {
		container, ok := isRecord(msg[key])
		if !ok {
			continue
		}
		meta, ok := getMeta(container)
		if !ok {
			continue
		}
		if meta[subscriptionIDMetaKey] == from {
			meta[subscriptionIDMetaKey] = to
			return
		}
	}
}

func getSubscriptionIDTag(params map[string]any) (any, bool) {
	meta, ok := getMeta(params)
	if !ok {
		return nil, false
	}
	v, ok := meta[subscriptionIDMetaKey]
	if !ok || !isJSONID(v) {
		return nil, false
	}
	return v, true
}

func (b *Bridge) handleChildNotification(msg map[string]any) {
	if msg["method"] == "notifications/progress" {
		params, _ := isRequestParams(msg)
		var token any
		if params != nil {
			token = params["progressToken"]
		}
		if internalID, ok := internalIDFromProgressToken(token); ok {
			b.mu.Lock()
			entry := b.pending[internalID]
			b.mu.Unlock()
			if entry != nil {
				rewritten := cloneMsg(msg)
				if params != nil {
					p := cloneMsg(params)
					p["progressToken"] = entry.progressToken
					rewritten["params"] = p
				}
				if entry.sse != nil {
					entry.sse.writeFrame(rewritten)
				} else {
					b.log.Debug("dropping progress notification: client did not request an SSE response for the matching request", "server", b.name)
				}
				return
			}
		}
	}
	params, _ := isRequestParams(msg)
	if subID, ok := getSubscriptionIDTag(params); ok {
		b.mu.Lock()
		sub := b.subscriptions[mustInt64(subID)]
		b.mu.Unlock()
		if sub != nil {
			rewritten := cloneMsg(msg)
			rewriteSubscriptionTag(rewritten, subID, sub.originalID)
			sub.res.writeFrame(rewritten)
			return
		}
	}
	b.pushHub.Dispatch(msg)
}

// mustInt64 best-effort-converts a JSON-RPC id value to the int64 key
// space subscriptions are stored under; subscription ids are always
// bridge-minted, so this only ever sees a numeric value in practice.
func mustInt64(v any) int64 {
	n, _ := numericID(v)
	return n
}

func (b *Bridge) wireChild(c *StdioChild, st *eraState) {
	c.OnMessage(func(msg map[string]any) {
		idVal, hasIDRaw := msg["id"]
		hasID := hasIDRaw && isJSONID(idVal)
		methodVal, hasMethodRaw := msg["method"]
		_, isStr := methodVal.(string)
		hasMethod := hasMethodRaw && isStr

		switch {
		case hasID && !hasMethod:
			if s, isProbe := idVal.(string); isProbe && strings.HasPrefix(s, probeIDPrefix) {
				b.handleProbeResponse(st, s, msg)
				return
			}
			id, ok := numericID(idVal)
			if !ok {
				b.pushHub.Dispatch(msg)
				return
			}
			b.handleChildResponse(id, msg)
		case hasID && hasMethod:
			// Server-initiated request (legacy, <=2025-11-25 only) — shape
			// alone routes this to the legacy push stream, never the
			// pending/subscriptions maps, even on an id collision.
			b.pushHub.Dispatch(msg)
		case !hasID && hasMethod:
			b.handleChildNotification(msg)
		default:
			// no id, no method: not a valid JSON-RPC message.
		}
	})

	c.OnExit(func(_ ExitInfo) {
		b.log.Warn("stdio child exited; failing pending requests", "server", b.name)
		b.mu.Lock()
		pending := b.pending
		b.pending = make(map[int64]*pendingEntry)
		subs := b.subscriptions
		b.subscriptions = make(map[int64]*subscriptionEntry)
		b.endEraLocked(st)
		b.mu.Unlock()

		for _, entry := range pending {
			entry.rejectWith(errChildRestarted)
		}
		for _, sub := range subs {
			sub.res.end()
			close(sub.done)
		}
		b.pushHub.Close()
	})

	c.OnError(func(err error) {
		b.log.Error("stdio child error", "server", b.name, "err", err)
	})
}

// --- HTTP plumbing -----------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, body any) {
	text, err := json.Marshal(body)
	if err != nil {
		text = []byte(`{"error":"internal error"}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(text)))
	w.WriteHeader(status)
	_, _ = w.Write(text)
}

// writeJSONFlush is writeJSON for a final answer that may be followed at once
// by Close hard-closing the listener: flushing puts it on the wire before the
// handler's return, which is when net/http would otherwise do so.
func writeJSONFlush(w http.ResponseWriter, status int, body any) {
	writeJSON(w, status, body)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

func writeJSONHeaders(w http.ResponseWriter, status int, extra map[string]string, body any) {
	for k, v := range extra {
		w.Header().Set(k, v)
	}
	writeJSON(w, status, body)
}

func writeEmpty(w http.ResponseWriter, status int, extra map[string]string) {
	for k, v := range extra {
		w.Header().Set(k, v)
	}
	w.WriteHeader(status)
}

func jsonRPCError(w http.ResponseWriter, status int, code int, message string) {
	writeJSON(w, status, map[string]any{
		"jsonrpc": "2.0",
		"id":      nil,
		"error":   map[string]any{"code": code, "message": message},
	})
}

func openSSEHeaders(w http.ResponseWriter, sessionID string, hasSession bool) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	if hasSession {
		w.Header().Set("Mcp-Session-Id", sessionID)
	}
}

// sseResponder wraps an http.ResponseWriter opened as an SSE stream,
// serializing writes and guarding against writing to a finished response.
// It is also the single owner of the response head: with newHeldSSEResponder
// the head is not written until the first frame, an explicit commit, the
// hold deadline, or end — whichever comes first, all under mu — so a final
// error arriving while the head is held can still choose its HTTP status
// (finalStatus), and nothing can write once ended is set.
type sseResponder struct {
	mu          sync.Mutex
	w           http.ResponseWriter
	flusher     http.Flusher
	ended       bool
	headWritten bool
	extra       map[string]string
	holdTimer   *time.Timer
	// provisional: the head went out at the hold deadline carrying a
	// provisional session id that the caller must revoke if the answer
	// turns out to be a rejection.
	provisional bool
	// mintHead, set only for a held initialize, supplies the headers of a
	// provisional session; commitLocked runs it, so every way the head can
	// go out (hold deadline, a progress frame) carries a session id.
	mintHead func() (map[string]string, bool)
}

// newSSEResponder wraps a response whose 200 SSE head the caller has
// already written.
func newSSEResponder(w http.ResponseWriter) *sseResponder {
	f, _ := w.(http.Flusher)
	return &sseResponder{w: w, flusher: f, headWritten: true}
}

// newHeldSSEResponder wraps a response whose head is still unwritten; extra
// headers ride on it if it is committed as a 200 stream.
func newHeldSSEResponder(w http.ResponseWriter, extra map[string]string) *sseResponder {
	f, _ := w.(http.Flusher)
	return &sseResponder{w: w, flusher: f, extra: extra}
}

func (s *sseResponder) commitLocked() {
	if s.headWritten {
		return
	}
	s.headWritten = true
	if s.holdTimer != nil {
		s.holdTimer.Stop()
	}
	if s.mintHead != nil {
		h, tracked := s.mintHead()
		s.mintHead = nil
		s.provisional = tracked
		s.mergeExtraLocked(h)
	}
	for k, v := range s.extra {
		s.w.Header().Set(k, v)
	}
	openSSEHeaders(s.w, "", false)
	s.w.WriteHeader(200)
	if s.flusher != nil {
		s.flusher.Flush()
	}
}

// commit writes the 200 SSE head now, unless something already has.
func (s *sseResponder) commit() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ended {
		s.commitLocked()
	}
}

// holdFor arms the deadline that commits the head if nothing has by then.
func (s *sseResponder) holdFor(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.headWritten || s.ended {
		return
	}
	s.holdTimer = time.AfterFunc(d, s.commit)
}

func (s *sseResponder) writeFrame(msg any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	s.commitLocked()
	_, _ = io.WriteString(s.w, frameSSE(msg))
	if s.flusher != nil {
		s.flusher.Flush()
	}
}

// finalStatus answers with a plain JSON body and the given status instead
// of a stream, iff the head is still held; it then ends the response and
// reports true.
func (s *sseResponder) finalStatus(status int, headers map[string]string, body any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended || s.headWritten {
		return false
	}
	s.headWritten, s.ended = true, true
	if s.holdTimer != nil {
		s.holdTimer.Stop()
	}
	writeJSONHeaders(s.w, status, headers, body)
	if s.flusher != nil {
		s.flusher.Flush()
	}
	return true
}

// setHeadersIfUncommitted sets the headers f returns on the pending head,
// calling f only if the head can still carry them.
func (s *sseResponder) setHeadersIfUncommitted(f func() map[string]string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended || s.headWritten {
		return false
	}
	s.mintHead = nil
	s.mergeExtraLocked(f())
	return true
}

func (s *sseResponder) mergeExtraLocked(h map[string]string) {
	if s.extra == nil {
		s.extra = map[string]string{}
	}
	for k, v := range h {
		s.extra[k] = v
	}
}

// holdForInitialize is holdFor for an initialize, whose head must carry
// Mcp-Session-Id: when the head is committed, at the deadline or earlier,
// mint supplies the headers of a provisional session (and whether the caller
// must later revoke it).
func (s *sseResponder) holdForInitialize(d time.Duration, mint func() (map[string]string, bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.headWritten || s.ended {
		return
	}
	s.mintHead = mint
	s.holdTimer = time.AfterFunc(d, s.commit)
}

// takeProvisional reports, once, whether a provisional session is
// outstanding on this response. It also disarms a mint that has not run, so a
// hold deadline firing after the caller released the session cannot hand out
// one that nobody will revoke.
func (s *sseResponder) takeProvisional() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mintHead = nil
	p := s.provisional
	s.provisional = false
	return p
}

// end finishes the stream, writing the 200 head first if it was still held.
func (s *sseResponder) end() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ended {
		s.commitLocked()
	}
	s.ended = true
}

// detach finishes the response without writing anything: the handler is
// returning, so no later writer may touch w.
func (s *sseResponder) detach() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ended = true
	if s.holdTimer != nil {
		s.holdTimer.Stop()
	}
}

// pushSinkAdapter adapts an sseResponder plus a per-request done channel to
// the pushSink interface pushStreamHub expects. ended is closed exactly
// once, by End(), so handleGet — which owns the underlying HTTP handler
// goroutine — can select on it and return once the hub tears the stream
// down server-side (e.g. on child exit), rather than only ever unblocking
// on the client disconnecting first.
type pushSinkAdapter struct {
	res     *sseResponder
	done    <-chan struct{}
	ended   chan struct{}
	endOnce *sync.Once
}

func newPushSinkAdapter(res *sseResponder, done <-chan struct{}) pushSinkAdapter {
	return pushSinkAdapter{res: res, done: done, ended: make(chan struct{}), endOnce: &sync.Once{}}
}

func (p pushSinkAdapter) Write(chunk string) error {
	p.res.mu.Lock()
	defer p.res.mu.Unlock()
	if p.res.ended {
		return errChildGone
	}
	_, err := io.WriteString(p.res.w, chunk)
	if err == nil && p.res.flusher != nil {
		p.res.flusher.Flush()
	}
	return err
}

func (p pushSinkAdapter) End() {
	p.res.end()
	p.endOnce.Do(func() { close(p.ended) })
}

func (p pushSinkAdapter) OnClose(fn func()) {
	go func() {
		<-p.done
		fn()
	}()
}

func firstHeader(h http.Header, key string) (string, bool) {
	v := h.Get(key)
	return v, v != ""
}

// headerTrackingWriter notes whether a response has already started (a
// header or byte written) so the panic-recovery middleware below knows
// whether it's still safe to write a JSON error body.
type headerTrackingWriter struct {
	http.ResponseWriter
	started bool
}

func (h *headerTrackingWriter) WriteHeader(status int) {
	h.ResponseWriter.WriteHeader(status)
	h.started = true
}

func (h *headerTrackingWriter) Write(p []byte) (int, error) {
	n, err := h.ResponseWriter.Write(p)
	h.started = true
	return n, err
}

func (h *headerTrackingWriter) Flush() {
	if f, ok := h.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (b *Bridge) handle(w http.ResponseWriter, r *http.Request) {
	tw := &headerTrackingWriter{ResponseWriter: w}
	// Last-resort safety net mirroring http-bridge.ts:877-881's handlePost
	// .catch(): an unhandled panic answers 502 rather than taking down the
	// whole server or hanging the connection. http.ErrAbortHandler is
	// net/http's own sentinel for "abort this handler without logging" —
	// re-panicking it lets net/http do that rather than swallowing it into
	// a bogus 502.
	defer func() {
		if rec := recover(); rec != nil {
			if rec == http.ErrAbortHandler {
				panic(rec)
			}
			b.log.Error("panic handling bridge request", "server", b.name, "panic", rec, "stack", string(debug.Stack()))
			if !tw.started {
				writeJSON(tw, 502, map[string]any{"error": "internal error"})
			}
		}
	}()

	if r.URL.Path != "/mcp" {
		writeJSON(tw, 404, map[string]any{"error": "not found"})
		return
	}
	origin, hasOrigin := firstHeader(r.Header, "Origin")
	if !originAllowed(origin, hasOrigin) {
		writeJSON(tw, 403, map[string]any{"error": "origin not allowed"})
		return
	}

	b.mu.Lock()
	failed, hasFailed := b.failed, b.hasFailed
	b.mu.Unlock()
	if hasFailed && r.Method != http.MethodDelete {
		b.log.Debug("rejecting request against a permanently failed service", "server", b.name, "reason", failed)
		writeJSON(tw, 502, map[string]any{"error": fmt.Sprintf("local server '%s' is not running", b.name)})
		return
	}

	switch r.Method {
	case http.MethodPost:
		if !b.acquireRequestSlot() {
			writeJSON(tw, 503, map[string]any{"error": "bridge is closing"})
			return
		}
		defer b.wg.Done()
		b.handlePost(tw, r)
	case http.MethodGet:
		b.handleGet(tw, r)
	case http.MethodDelete:
		b.handleDelete(tw, r)
	default:
		writeJSON(tw, 405, map[string]any{"error": "method not allowed"})
	}
}

// acquireRequestSlot increments b.wg iff the bridge isn't closing —
// gating wg.Add(1) behind the same lock Close() sets its closing flag
// under, so Add can never race Close's wg.Wait() (sync.WaitGroup: "Add
// with a positive delta ... must happen before a Wait").
func (b *Bridge) acquireRequestSlot() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closing {
		return false
	}
	b.wg.Add(1)
	return true
}

// writeMethodNotAllowed answers GET/DELETE against a child known to be
// modern-only: the 2026-07-28 HTTP binding has no standalone stream and no
// sessions to terminate.
func writeMethodNotAllowed(w http.ResponseWriter) {
	w.Header().Set("Allow", "POST")
	writeJSON(w, 405, map[string]any{"error": "method not allowed"})
}

func (b *Bridge) handleGet(w http.ResponseWriter, r *http.Request) {
	if !strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		writeJSON(w, 405, map[string]any{"error": "method not allowed"})
		return
	}
	if b.refuseStatelessMethod(w) {
		return
	}
	headerSessionID, hasHeader := firstHeader(r.Header, "Mcp-Session-Id")
	check := b.session.check("", headerSessionID, hasHeader)
	switch check.kind {
	case sessionUnknown:
		writeJSON(w, 404, map[string]any{"error": "unknown session"})
		return
	case sessionMissing:
		jsonRPCError(w, 400, -32600, "invalid request: Mcp-Session-Id header is required once a session has been established")
		return
	}
	openSSEHeaders(w, check.sessionID, check.kind == sessionOK)
	w.WriteHeader(200)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	res := newSSEResponder(w)
	done := r.Context().Done()
	sink := newPushSinkAdapter(res, done)
	b.pushHub.Open(sink)
	// Unblock on whichever comes first: the client disconnecting, or the
	// hub ending this sink server-side (e.g. pushHub.Close() on child
	// exit) — without the latter, a legacy GET stream left open past a
	// child exit would never reach EOF client-side.
	select {
	case <-done:
	case <-sink.ended:
	}
	// Synchronously deregister and end the sink before this handler
	// returns, rather than leaving it to the async OnClose callback — a
	// concurrent Dispatch/keepalive must never still be writing to w once
	// the handler has returned it to net/http.
	b.pushHub.CloseCurrent(sink)
}

func (b *Bridge) handleDelete(w http.ResponseWriter, r *http.Request) {
	if b.refuseStatelessMethod(w) {
		return
	}
	headerSessionID, hasHeader := firstHeader(r.Header, "Mcp-Session-Id")
	newID, ok := b.session.rotate(headerSessionID, hasHeader)
	if !ok {
		writeJSON(w, 404, map[string]any{"error": "unknown session"})
		return
	}
	writeEmpty(w, 204, map[string]string{"Mcp-Session-Id": newID})
}

func (b *Bridge) handlePost(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	bodyBuf, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, 413, map[string]any{"error": "request body too large"})
		return
	}

	var parsedAny any
	if len(bodyBuf) == 0 {
		parsedAny = map[string]any{}
	} else if err := json.Unmarshal(bodyBuf, &parsedAny); err != nil {
		jsonRPCError(w, 400, -32700, "parse error: request body is not valid JSON")
		return
	}
	if _, isArr := parsedAny.([]any); isArr {
		jsonRPCError(w, 400, -32600, "batch requests are not supported")
		return
	}
	parsed, ok := isRecord(parsedAny)
	if !ok {
		jsonRPCError(w, 400, -32600, "invalid request: body must be a JSON object")
		return
	}

	methodVal, hasMethodRaw := parsed["method"]
	method, isStr := methodVal.(string)
	hasMethod := hasMethodRaw && isStr
	// hasID mirrors Node's `hasOwnProperty(parsed, "id") && parsed.id !==
	// undefined` (http-bridge.ts:750): the property merely being present
	// makes this a request, not a notification, even if its value is
	// null — JSON.parse/encoding-json never produce `undefined` for a
	// present key, so the "!== undefined" half is automatically satisfied
	// whenever hasOwnProperty is.
	_, hasID := parsed["id"]

	// A modern-shaped message is stateless: any Mcp-Session-Id on it is
	// ignored (no gate, no 404, never echoed). A 2025-style initialize
	// skips the gate too; its session is minted only once the child has
	// answered it successfully.
	modernShaped := hasMethod && isModernShaped(parsed)
	// A live child known to take only modern requests refuses 2025-style
	// ones before any session gate: there are no sessions to be unknown.
	if !modernShaped && (hasMethod || hasID) {
		if c, st := b.currentChildEra(); !c.HasExited() && b.refuseLegacyRequest(w, parsed, st) {
			return
		}
	}
	var sessionID string
	var hasSession bool
	if !modernShaped && method != "initialize" {
		headerSessionID, hasHeader := firstHeader(r.Header, "Mcp-Session-Id")
		check := b.session.check(method, headerSessionID, hasHeader)
		switch check.kind {
		case sessionUnknown:
			writeJSON(w, 404, map[string]any{"error": "unknown session"})
			return
		case sessionMissing:
			jsonRPCError(w, 400, -32600, "invalid request: Mcp-Session-Id header is required once a session has been established")
			return
		}
		hasSession = check.kind == sessionOK
		if hasSession {
			sessionID = check.sessionID
		}
	}

	// One lazy respawn here covers requests, notifications and
	// subscriptions/listen alike. The child is snapshotted right after:
	// regular requests use that snapshot (admitModern may move it to a newer
	// one), while notifications, client responses and subscriptions/listen
	// read currentChild() again. A bare client response (!hasMethod) is
	// left out: it answers the dead child's own request, which a fresh
	// child never issued, so respawning for it would only resurrect a
	// disabled service on a stray late reply.
	if hasMethod {
		b.ensureLiveChild(r.Context())
	}

	child, st := b.currentChildEra()
	// A child known to take only modern requests is given nothing 2025-style:
	// not initialize or any other request, nor a notification or a client
	// response (this revision defines no client notifications over HTTP and
	// forbids client responses). A dead child's verdict does not count: it
	// gets the "not running" answer below.
	if !modernShaped && (hasMethod || hasID) && !child.HasExited() && b.refuseLegacyRequest(w, parsed, st) {
		return
	}
	if modernShaped && hasID {
		var ok bool
		if child, st, ok = b.admitModern(w, r, parsed, child, st); !ok {
			return
		}
		if code, msg := modernRequestProblem(r, parsed); code != 0 {
			writeJSON(w, http.StatusBadRequest, jsonRPCErrorBody(parsed["id"], code, msg, nil))
			return
		}
	}

	if method == "subscriptions/listen" {
		b.handleSubscriptionsListen(w, r, parsed, sessionID, hasSession)
		return
	}

	if hasMethod && !hasID {
		// Notification.
		outgoing := parsed
		if method == "notifications/cancelled" {
			if cancelParams, ok := isRequestParams(parsed); ok {
				if reqID, ok := cancelParams["requestId"]; ok && isJSONID(reqID) {
					if internalID, ok := b.findInternalIDForOriginalID(reqID); ok {
						outgoing = cloneMsg(parsed)
						p := cloneMsg(cancelParams)
						p["requestId"] = internalID
						outgoing["params"] = p
					}
				}
			}
		}
		child := b.currentChild()
		if err := child.Send(outgoing); err != nil {
			b.log.Warn("failed to write notification to stdio child", "server", b.name, "err", err)
			writeJSON(w, 502, map[string]any{"error": "local server is not running"})
			return
		}
		writeEmpty(w, 202, nil)
		return
	}
	if !hasMethod && hasID {
		// Client answering a server-initiated request — its id is in the
		// child's own id space, never rewritten.
		child := b.currentChild()
		if err := child.Send(parsed); err != nil {
			b.log.Warn("failed to write response to stdio child", "server", b.name, "err", err)
			writeJSON(w, 502, map[string]any{"error": "local server is not running"})
			return
		}
		writeEmpty(w, 202, nil)
		return
	}
	if !hasMethod && !hasID {
		jsonRPCError(w, 400, -32600, "invalid request: body must have a method, an id, or both")
		return
	}

	// Request: has both id and method.
	kind := kindLegacy
	switch {
	case modernShaped:
		kind = kindModern
	case method == "initialize":
		kind = kindInitialize
	}
	b.handleRegularRequest(w, r, parsed, sessionID, hasSession, kind, child, st)
}

// admitAndMintID atomically checks the too-many-pending gate and mints an
// internal id in the same critical section — checking then minting under
// two separate lock acquisitions left a window where more callers than
// maxPending could all pass the check before any of them landed in
// b.pending/b.subscriptions.
func (b *Bridge) admitAndMintID() (int64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.pending)+len(b.subscriptions) >= b.maxPending {
		return 0, false
	}
	id := b.nextID
	b.nextID++
	return id, true
}

type requestKind int

const (
	kindLegacy     requestKind = iota // 2025-style request
	kindInitialize                    // 2025-style initialize
	kindModern                        // modern-shaped request, child already judged modern
)

func (b *Bridge) handleRegularRequest(w http.ResponseWriter, r *http.Request, parsed map[string]any, sessionID string, hasSession bool, kind requestKind, child *StdioChild, st *eraState) {
	internalID, ok := b.admitAndMintID()
	if !ok {
		writeJSON(w, 503, map[string]any{"error": "too many in-flight requests"})
		return
	}
	originalID := parsed["id"]

	var progressToken any
	if params, ok := isRequestParams(parsed); ok {
		if meta, ok := getMeta(params); ok {
			progressToken = meta["progressToken"]
		}
	}
	accept := r.Header.Get("Accept")
	wantsSSE := strings.Contains(accept, "text/event-stream")

	extraHeaders := map[string]string{}
	if hasSession {
		extraHeaders["Mcp-Session-Id"] = sessionID
	}

	// 2025-style requests open the SSE head at once, as ever. A modern
	// request holds it (briefly) for the child's first message so a final
	// error can still pick the HTTP status; initialize holds it so the
	// session header can ride on it.
	var sse *sseResponder
	if wantsSSE {
		sse = newHeldSSEResponder(w, extraHeaders)
		switch kind {
		case kindModern:
			sse.holdFor(b.headHold)
		case kindInitialize:
			sse.holdForInitialize(b.initHold, func() (map[string]string, bool) {
				id, tracked := b.session.provisionalMint()
				return map[string]string{"Mcp-Session-Id": id}, tracked
			})
		default:
			sse.commit()
		}
	}

	entry := &pendingEntry{
		originalID:    originalID,
		progressToken: progressToken,
		sse:           sse,
		plain:         w,
		extraHeaders:  extraHeaders,
		done:          make(chan struct{}),
		modern:        kind == kindModern,
	}
	if kind == kindInitialize {
		entry.raw = make(chan map[string]any, 1)
		entry.revoke = b.session.revoke
		entry.era = st
	}
	entry.timer = time.AfterFunc(b.timeout, func() {
		b.mu.Lock()
		b.dropPendingLocked(internalID, entry)
		b.mu.Unlock()
		b.addAbandoned(internalID)
		entry.rejectWith(errUpstreamTimeout)
	})

	b.mu.Lock()
	b.pending[internalID] = entry
	b.mu.Unlock()
	// Whatever way this handler exits (including a panic, which unwinds
	// through here before handle's recover), the entry is claimed first, so
	// no timer, notification or late answer writes to w afterwards.
	defer func() {
		b.mu.Lock()
		delete(b.pending, internalID)
		b.mu.Unlock()
		entry.abandon()
	}()

	outgoing := cloneMsg(parsed)
	outgoing["id"] = internalID
	if progressToken != nil {
		p := cloneMsg(outgoing["params"].(map[string]any))
		meta := cloneMsg(p["_meta"].(map[string]any))
		meta["progressToken"] = internalProgressToken(internalID)
		p["_meta"] = meta
		outgoing["params"] = p
	}

	if err := child.Send(outgoing); err != nil {
		b.mu.Lock()
		delete(b.pending, internalID)
		b.mu.Unlock()
		b.log.Warn("failed to write request to stdio child", "server", b.name, "err", err)
		entry.rejectWith(errChildGone)
		return
	}

	select {
	case <-entry.done:
	case raw := <-entry.raw: // nil unless initialize
		b.completeInitialize(entry, raw)
	case <-r.Context().Done():
		b.onRequestDisconnect(internalID, entry, child)
	}
}

// dropPendingLocked removes a request whose client will not be answered by
// its handler any more (timed out, or gone) from b.pending, reporting whether
// it was still there. For an initialize it remembers the process, so the
// child's late answer still reaches the era state.
func (b *Bridge) dropPendingLocked(internalID int64, entry *pendingEntry) bool {
	_, still := b.pending[internalID]
	delete(b.pending, internalID)
	if still && entry.era != nil {
		b.abandonedInit[internalID] = entry.era
	}
	return still
}

// onRequestDisconnect handles the client going away mid-request. abandon
// waits out a writer already inside the entry (a concurrent resolveWith or
// rejectWith removed it from b.pending first), or claims the entry so a
// racing timer or answer can no longer write to the ResponseWriter.
func (b *Bridge) onRequestDisconnect(internalID int64, entry *pendingEntry, child *StdioChild) {
	b.mu.Lock()
	still := b.dropPendingLocked(internalID, entry)
	b.mu.Unlock()
	entry.abandon()
	if still {
		b.addAbandoned(internalID)
		_ = child.Send(cancelledNotification(internalID))
	}
}

func (b *Bridge) handleSubscriptionsListen(w http.ResponseWriter, r *http.Request, parsed map[string]any, sessionID string, hasSession bool) {
	internalID, ok := b.admitAndMintID()
	if !ok {
		writeJSON(w, 503, map[string]any{"error": "too many in-flight requests"})
		return
	}
	child := b.currentChild()
	if child.HasExited() {
		writeJSON(w, 502, map[string]any{"error": "local server is not running"})
		return
	}
	originalID := parsed["id"]

	openSSEHeaders(w, sessionID, hasSession)
	w.WriteHeader(200)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	res := newSSEResponder(w)
	sub := &subscriptionEntry{originalID: originalID, res: res, done: make(chan struct{})}

	b.mu.Lock()
	b.subscriptions[internalID] = sub
	b.mu.Unlock()

	outgoing := cloneMsg(parsed)
	outgoing["id"] = internalID
	if err := child.Send(outgoing); err != nil {
		b.mu.Lock()
		_, still := b.subscriptions[internalID]
		delete(b.subscriptions, internalID)
		b.mu.Unlock()
		if still {
			b.log.Warn("failed to write subscriptions/listen to stdio child", "server", b.name, "err", err)
			res.end()
		}
		return
	}

	select {
	case <-sub.done:
	case <-r.Context().Done():
		b.mu.Lock()
		_, still := b.subscriptions[internalID]
		delete(b.subscriptions, internalID)
		b.mu.Unlock()
		if still {
			_ = child.Send(map[string]any{
				"jsonrpc": "2.0", "method": "notifications/cancelled",
				"params": map[string]any{"requestId": internalID},
			})
		} else {
			// Already removed by a concurrent handleChildResponse — wait
			// for it to finish writing to res before returning.
			<-sub.done
		}
	}
}

// Close ends push/subscription streams, waits up to closeBarrier for
// in-flight handlers, closes the listener, then closes the child.
// Equivalent to CloseContext(context.Background()).
func (b *Bridge) Close() error {
	return b.CloseContext(context.Background())
}

// CloseContext is Close, but returns as soon as ctx is done rather than
// waiting out the full in-flight-handler/child-close sequence — so a
// caller enforcing DESIGN.md §3's 5s shutdown deadline across several
// bridges isn't held hostage by one with a slow-to-drain handler. The
// listener/child close this triggers still runs in the background to
// completion (bounded by their own internal ~2s grace timeouts) even if
// CloseContext itself returns early.
func (b *Bridge) CloseContext(ctx context.Context) error {
	b.mu.Lock()
	b.closing = true
	pending := b.pending
	b.pending = make(map[int64]*pendingEntry)
	subs := b.subscriptions
	b.subscriptions = make(map[int64]*subscriptionEntry)
	b.endEraLocked(b.era)
	b.mu.Unlock()

	b.pushHub.Close()

	for _, entry := range pending {
		entry.rejectWith(errChildGone)
	}
	for _, sub := range subs {
		sub.res.end()
		close(sub.done)
	}

	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(closeBarrier):
	case <-ctx.Done():
	}

	_ = b.server.Close()

	closeErr := make(chan error, 1)
	go func() { closeErr <- b.currentChild().Close() }()
	select {
	case err := <-closeErr:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
