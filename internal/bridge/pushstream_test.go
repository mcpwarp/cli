package bridge

import (
	"sync"
	"testing"
)

type fakeSink struct {
	mu       sync.Mutex
	writes   []string
	ended    bool
	closeFns []func()
}

func (f *fakeSink) Write(chunk string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes = append(f.writes, chunk)
	return nil
}
func (f *fakeSink) End() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ended = true
}
func (f *fakeSink) OnClose(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeFns = append(f.closeFns, fn)
}
func (f *fakeSink) fireClose() {
	f.mu.Lock()
	fns := append([]func(){}, f.closeFns...)
	f.mu.Unlock()
	for _, fn := range fns {
		fn()
	}
}

func TestPushStreamHub_BuffersThenFlushesOnOpen(t *testing.T) {
	h := newPushStreamHub(testLogger())
	h.Dispatch(map[string]any{"method": "notify", "params": map[string]any{"n": 1}})
	h.Dispatch(map[string]any{"method": "notify", "params": map[string]any{"n": 2}})

	sink := &fakeSink{}
	h.Open(sink)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.writes) != 2 {
		t.Fatalf("want 2 buffered frames flushed, got %d", len(sink.writes))
	}
}

func TestPushStreamHub_DispatchGoesStraightToOpenSink(t *testing.T) {
	h := newPushStreamHub(testLogger())
	sink := &fakeSink{}
	h.Open(sink)
	h.Dispatch(map[string]any{"method": "notify"})

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.writes) != 1 {
		t.Fatalf("want 1 direct write, got %d", len(sink.writes))
	}
}

func TestPushStreamHub_MostRecentlyOpenedWins(t *testing.T) {
	h := newPushStreamHub(testLogger())
	first := &fakeSink{}
	second := &fakeSink{}
	h.Open(first)
	h.Open(second)
	h.Dispatch(map[string]any{"method": "notify"})

	first.mu.Lock()
	firstWrites := len(first.writes)
	first.mu.Unlock()
	second.mu.Lock()
	secondWrites := len(second.writes)
	second.mu.Unlock()

	if firstWrites != 0 || secondWrites != 1 {
		t.Fatalf("want only the most recent sink fed: first=%d second=%d", firstWrites, secondWrites)
	}
}

func TestPushStreamHub_DropOldestPastCap(t *testing.T) {
	h := newPushStreamHub(testLogger())
	for i := 0; i < backlogCap+10; i++ {
		h.Dispatch(map[string]any{"n": i})
	}
	sink := &fakeSink{}
	h.Open(sink)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.writes) != backlogCap {
		t.Fatalf("want exactly backlogCap=%d frames flushed, got %d", backlogCap, len(sink.writes))
	}
	// The oldest 10 should have been dropped, so the first flushed frame
	// should correspond to n=10, not n=0.
	if want := `"n":10`; !contains(sink.writes[0], want) {
		t.Fatalf("expected the oldest entries dropped; first frame = %q", sink.writes[0])
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}

func TestPushStreamHub_CloseEndsSinkAndClearsBacklog(t *testing.T) {
	h := newPushStreamHub(testLogger())
	h.Dispatch(map[string]any{"n": 1})
	sink := &fakeSink{}
	h.Open(sink)
	h.Close()

	sink.mu.Lock()
	ended := sink.ended
	sink.mu.Unlock()
	if !ended {
		t.Fatal("expected Close to end the current sink")
	}
	if h.HasOpenStream() {
		t.Fatal("expected no open stream after Close")
	}
}
