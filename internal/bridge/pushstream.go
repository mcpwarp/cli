package bridge

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// backlogCap is the drop-oldest backlog size for the legacy push stream
// (DESIGN.md §7), ported from push-stream.ts's BACKLOG_CAP.
const backlogCap = 256

const pushKeepaliveInterval = 15 * time.Second

// pushSink is the minimal surface pushStreamHub needs from a
// transport-specific sink (an http.ResponseWriter flusher in practice).
type pushSink interface {
	Write(chunk string) error
	End()
	// OnClose registers fn to run once, when the underlying connection
	// closes.
	OnClose(fn func())
}

func frameSSE(msg any) string {
	b, err := json.Marshal(msg)
	if err != nil {
		b = []byte("null")
	}
	return "event: message\ndata: " + string(b) + "\n\n"
}

// pushStreamHub is the registry of currently-open server->client push
// streams for one bridge. "Most recently opened wins": opening a new sink
// supersedes whatever was open before. With nothing open, messages queue
// in a bounded FIFO. Ported from push-stream.ts.
type pushStreamHub struct {
	log *slog.Logger

	mu         sync.Mutex
	current    pushSink
	stopKeep   chan struct{}
	backlog    []any
	warnedDrop bool
}

func newPushStreamHub(log *slog.Logger) *pushStreamHub {
	return &pushStreamHub{log: log}
}

// Open registers sink as the current push stream, then flushes any backlog
// into it.
func (h *pushStreamHub) Open(sink pushSink) {
	// Flush the backlog under the same lock section that installs sink as
	// current — otherwise a Dispatch racing this call could see current
	// already set and write straight through, landing before the backlog
	// it should have followed (Node does this flush synchronously too,
	// push-stream.ts:69-71).
	h.mu.Lock()
	h.stopKeepaliveLocked()
	h.current = sink
	stop := make(chan struct{})
	h.stopKeep = stop
	backlog := h.backlog
	h.backlog = nil
	for _, msg := range backlog {
		_ = sink.Write(frameSSE(msg))
	}
	h.mu.Unlock()

	go func() {
		ticker := time.NewTicker(pushKeepaliveInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if err := sink.Write(": ping\n\n"); err != nil {
					return
				}
			}
		}
	}()

	sink.OnClose(func() {
		h.mu.Lock()
		if h.current == sink {
			h.current = nil
			h.stopKeepaliveLocked()
		}
		h.mu.Unlock()
	})
}

// CloseCurrent synchronously deregisters sink, if it is still the current
// stream, and ends it. Unlike the OnClose-triggered path (async relative to
// the caller), this is for a caller (the GET handler) that must not return
// until the sink is fully torn down — no further Dispatch/keepalive write
// can land on it once this returns.
func (h *pushStreamHub) CloseCurrent(sink pushSink) {
	h.mu.Lock()
	if h.current == sink {
		h.current = nil
		h.stopKeepaliveLocked()
	}
	h.mu.Unlock()
	sink.End()
}

func (h *pushStreamHub) stopKeepaliveLocked() {
	if h.stopKeep != nil {
		close(h.stopKeep)
		h.stopKeep = nil
	}
}

// Dispatch delivers a server-initiated message to the current push
// stream, or buffers it (drop-oldest past backlogCap) if none is open.
func (h *pushStreamHub) Dispatch(msg any) {
	h.mu.Lock()
	sink := h.current
	if sink == nil {
		if len(h.backlog) >= backlogCap {
			h.backlog = h.backlog[1:]
			if !h.warnedDrop {
				h.log.Warn(fmt.Sprintf("push-stream backlog full (%d messages); dropping oldest buffered message", backlogCap))
				h.warnedDrop = true
			}
		}
		h.backlog = append(h.backlog, msg)
		h.mu.Unlock()
		return
	}
	h.mu.Unlock()
	_ = sink.Write(frameSSE(msg))
}

// HasOpenStream reports whether a push stream is currently open.
func (h *pushStreamHub) HasOpenStream() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.current != nil
}

// Close tears down the current stream and clears the backlog (a child
// exit must not let messages queued for the old child replay to whatever
// client opens the next push stream against the new one).
func (h *pushStreamHub) Close() {
	h.mu.Lock()
	h.stopKeepaliveLocked()
	sink := h.current
	h.current = nil
	h.backlog = nil
	h.warnedDrop = false
	h.mu.Unlock()
	if sink != nil {
		sink.End()
	}
}
