package bridge

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// defaultMaxListens caps open subscriptions/listen streams; they are
	// long-lived, so they do not share maxPending with ordinary requests.
	defaultMaxListens = 64
	// defaultListenBudget caps the bytes one listen may have queued for a
	// client that is not reading.
	defaultListenBudget = 1 << 20
	// defaultListenWriteGrace is how long a listen handler already inside
	// Write gets once its listen has ended without it: over budget (it then
	// delivers what is queued), or the bridge closing (queued frames are
	// dropped and the stream just ends).
	defaultListenWriteGrace = 5 * time.Second
	// defaultListenKeepalive is the period of the SSE comment line an idle
	// listen stream carries once its head is out.
	defaultListenKeepalive = 15 * time.Second

	cancelReasonDisconnect = "client disconnected"
	cancelReasonOverflow   = "subscriptions/listen client is not reading"

	listenNeedsSSE = "subscriptions/listen is answered only as an SSE stream; Accept must include text/event-stream."
)

// listenEnd is why a listen's stream is over, once it is.
type listenEnd int

const (
	listenOpen         listenEnd = iota
	listenEndedByChild           // its response, or a notifications/cancelled naming it
	listenOverflow               // the client did not keep up with its queue
	listenChildGone              // the child it was admitted to exited or was replaced
	listenClosing                // the bridge is closing
	listenClientGone             // the handler left: the client disconnected
)

// listenEntry is one open subscriptions/listen. The pump goroutine only
// enqueues into it and wakes the handler; the handler goroutine owns every
// write to the client. Its mutex is taken after Bridge.mu, and neither is held
// while writing to the client or calling child.Send.
type listenEntry struct {
	originalID    any
	progressToken any // the client's, nil if it sent none
	version       any // the protocol version the request carried
	child         *StdioChild
	budget        int
	grace         time.Duration // set by admitListen, before the entry is shared
	wake          chan struct{} // capacity 1, sent without blocking

	mu     sync.Mutex
	frames []string // pre-framed SSE events, oldest first
	queued int      // bytes in frames
	// final is the child's response when it ended the listen with one, with
	// the client's id and tag already in place.
	final  map[string]any
	reason listenEnd
	// rc reaches the response for the write deadline; nil once the handler
	// has returned and no longer owns it.
	rc *http.ResponseController
}

func newListenEntry(originalID, progressToken, version any, child *StdioChild, budget int, w http.ResponseWriter) *listenEntry {
	return &listenEntry{
		originalID:    originalID,
		progressToken: progressToken,
		version:       version,
		child:         child,
		budget:        budget,
		wake:          make(chan struct{}, 1),
		rc:            http.NewResponseController(w),
	}
}

func (l *listenEntry) signal() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// enqueue queues one frame for the handler and reports whether it pushed the
// listen over its budget, which ends the listen. Frames already queued are
// still delivered. A listen that has ended takes nothing more.
func (l *listenEntry) enqueue(frame string) (overflowed bool) {
	l.mu.Lock()
	if l.reason != listenOpen {
		l.mu.Unlock()
		return false
	}
	if l.queued+len(frame) > l.budget {
		l.reason = listenOverflow
		overflowed = true
	} else {
		l.frames = append(l.frames, frame)
		l.queued += len(frame)
	}
	l.mu.Unlock()
	l.signal()
	return overflowed
}

// end ends the listen for reason unless it has already ended.
func (l *listenEntry) end(reason listenEnd) {
	l.mu.Lock()
	if l.reason != listenOpen {
		l.mu.Unlock()
		return
	}
	l.reason = reason
	l.mu.Unlock()
	l.signal()
}

// finish ends the listen with the child's response.
func (l *listenEntry) finish(final map[string]any) {
	l.mu.Lock()
	if l.reason != listenOpen {
		l.mu.Unlock()
		return
	}
	l.final, l.reason = final, listenEndedByChild
	l.mu.Unlock()
	l.signal()
}

// drain takes everything queued, and reports the final response and how the
// listen ended, if it has.
func (l *listenEntry) drain() (frames []string, final map[string]any, reason listenEnd) {
	l.mu.Lock()
	defer l.mu.Unlock()
	frames, l.frames, l.queued = l.frames, nil, 0
	return frames, l.final, l.reason
}

// state is why the listen has ended, listenOpen if it has not.
func (l *listenEntry) state() listenEnd {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.reason
}

// armWriteDeadline bounds what is left of the handler's writing, so a client
// that holds its connection open without reading cannot keep it in Write.
// net/http clears the deadline once the request is finished.
func (l *listenEntry) armWriteDeadline() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.rc != nil {
		_ = l.rc.SetWriteDeadline(time.Now().Add(l.grace))
	}
}

// release is the handler's last act on the response: a later
// armWriteDeadline does nothing.
func (l *listenEntry) release() {
	l.mu.Lock()
	l.rc = nil
	l.mu.Unlock()
}

// takeListensLocked removes and returns the listens take selects. b.mu must be
// held.
func (b *Bridge) takeListensLocked(take func(*listenEntry) bool) []*listenEntry {
	var taken []*listenEntry
	for id, l := range b.listens {
		if take(l) {
			delete(b.listens, id)
			taken = append(taken, l)
		}
	}
	return taken
}

// bridgeCancel is the notifications/cancelled the bridge itself sends a child
// for the request or listen it knows by bridge id id. For a modern-shaped
// request, version is the protocol version that request carried, and the
// cancel carries it in the same envelope so the child reads it as a message of
// that era; nil leaves the envelope out, as a 2025-shaped message has none.
func bridgeCancel(id int64, reason string, version any) map[string]any {
	params := map[string]any{"requestId": id, "reason": reason}
	if version != nil {
		params["_meta"] = map[string]any{protocolVersionMetaKey: version}
	}
	return map[string]any{"jsonrpc": "2.0", "method": "notifications/cancelled", "params": params}
}

// requestProtocolVersion is the protocol version a request carries in _meta,
// nil if it has none.
func requestProtocolVersion(parsed map[string]any) any {
	if params, ok := isRequestParams(parsed); ok {
		if meta, ok := getMeta(params); ok {
			return meta[protocolVersionMetaKey]
		}
	}
	return nil
}

func endListens(ls []*listenEntry, reason listenEnd) {
	for _, l := range ls {
		l.end(reason)
	}
}

// requestProgressToken is the progressToken a request carries in _meta, nil if
// it has none.
func requestProgressToken(parsed map[string]any) any {
	if params, ok := isRequestParams(parsed); ok {
		if meta, ok := getMeta(params); ok {
			return meta["progressToken"]
		}
	}
	return nil
}

// forwardedRequest is the client's request as the child sees it: addressed by
// the bridge id, with a progress token (progressToken, non-nil) swapped for
// the bridge's own.
func forwardedRequest(parsed map[string]any, internalID int64, progressToken any) map[string]any {
	outgoing := cloneMsg(parsed)
	outgoing["id"] = internalID
	if progressToken != nil {
		p := cloneMsg(outgoing["params"].(map[string]any))
		meta := cloneMsg(p["_meta"].(map[string]any))
		meta["progressToken"] = internalProgressToken(internalID)
		p["_meta"] = meta
		outgoing["params"] = p
	}
	return outgoing
}

// acceptsEventStream reports whether an Accept header (possibly sent as
// several lines) names text/event-stream with a non-zero quality.
func acceptsEventStream(values []string) bool {
	for _, field := range strings.Split(strings.Join(values, ","), ",") {
		parts := strings.Split(field, ";")
		if !strings.EqualFold(strings.TrimSpace(parts[0]), "text/event-stream") {
			continue
		}
		q := 1.0
		for _, p := range parts[1:] {
			if k, v, ok := strings.Cut(strings.TrimSpace(p), "="); ok && strings.EqualFold(strings.TrimSpace(k), "q") {
				if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
					q = f
				}
			}
		}
		if q > 0 {
			return true
		}
	}
	return false
}

// admitListen mints an id for l and registers it, under one lock section so
// the cap holds. A non-empty refusal says why it did not.
func (b *Bridge) admitListen(l *listenEntry) (id int64, refusal string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case b.closing:
		return 0, "bridge is closing"
	case len(b.listens) >= b.maxListens:
		return 0, "too many open subscriptions"
	}
	id = b.mintIDLocked()
	l.grace = b.writeGrace
	b.listens[id] = l
	return id, ""
}

// handleSubscriptionsListen serves a modern-shaped subscriptions/listen on
// child, a v2 process. It is answered only as a stream, kept
// open until the child, the client or the bridge ends it.
func (b *Bridge) handleSubscriptionsListen(w http.ResponseWriter, r *http.Request, parsed map[string]any, child *StdioChild) {
	if !acceptsEventStream(r.Header.Values("Accept")) {
		writeJSON(w, http.StatusNotAcceptable, jsonRPCErrorBody(parsed["id"], -32600, listenNeedsSSE, nil))
		return
	}
	progressToken := requestProgressToken(parsed)
	l := newListenEntry(parsed["id"], progressToken, requestProtocolVersion(parsed), child, b.listenBudget, w)
	id, refusal := b.admitListen(l)
	if refusal != "" {
		writeJSON(w, 503, map[string]any{"error": refusal})
		return
	}
	res := newHeldSSEResponder(w, nil)
	defer b.finishListen(id, l, res)

	if err := child.Send(forwardedRequest(parsed, id, progressToken)); err != nil {
		b.mu.Lock()
		delete(b.listens, id)
		b.mu.Unlock()
		b.log.Warn("failed to write subscriptions/listen to stdio child", "server", b.name, "err", err)
		res.finalStatus(502, nil, map[string]any{"error": "local server is not running"})
		return
	}
	b.serveListen(r.Context(), res, l)
}

// serveListen is the listen handler's loop: the only writer to the response.
// The head is held until the first thing the child has to say (or the hold
// deadline), so a child that does not support listens can still answer with
// an HTTP error status.
func (b *Bridge) serveListen(ctx context.Context, res *sseResponder, l *listenEntry) {
	hold := time.NewTimer(b.headHold)
	defer hold.Stop()
	holdC := hold.C
	var keepalive *time.Ticker
	var keepaliveC <-chan time.Time
	defer func() {
		if keepalive != nil {
			keepalive.Stop()
		}
	}()
	committed := false
	commit := func() {
		committed, holdC = true, nil
		res.commit()
		keepalive = time.NewTicker(b.keepalive)
		keepaliveC = keepalive.C
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-holdC:
			commit()
		case <-keepaliveC:
			if res.writeRaw(": ping\n\n") != nil {
				return
			}
		case <-l.wake:
			frames, final, reason := l.drain()
			if reason == listenClosing {
				res.end()
				return
			}
			if !committed {
				if len(frames) == 0 && final != nil {
					if status := modernErrorStatus(final); status != http.StatusOK && res.finalStatus(status, nil, final) {
						return
					}
				}
				if len(frames) == 0 && final == nil {
					if reason == listenOpen {
						continue
					}
					res.end()
					return
				}
				commit()
			}
			for _, f := range frames {
				if res.writeRaw(f) != nil {
					return
				}
			}
			if reason == listenOpen {
				continue
			}
			if final != nil && res.writeRaw(frameSSE(final)) != nil {
				return
			}
			res.end()
			return
		}
	}
}

// finishListen runs however the handler exits. A listen still registered here
// was not ended by the child, and the child is told to stop; one already gone
// from b.listens (the child ended it, or the pump removed it for lack of
// budget and cancels it itself) gets nothing from here. Either way it takes no
// more frames.
func (b *Bridge) finishListen(id int64, l *listenEntry, res *sseResponder) {
	reason := cancelReasonDisconnect
	if l.state() == listenOverflow {
		reason = cancelReasonOverflow
	}
	b.mu.Lock()
	still := b.listens[id] == l
	if still {
		delete(b.listens, id)
		b.addAbandonedLocked(id)
	}
	b.mu.Unlock()
	l.end(listenClientGone)
	res.detach()
	l.release()
	if still {
		_ = l.child.Send(bridgeCancel(id, reason, l.version))
	}
}
