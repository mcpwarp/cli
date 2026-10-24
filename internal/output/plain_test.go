package output

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mcpwarp/cli/internal/eventbus"
)

// syncBuffer wraps bytes.Buffer with a mutex so a test goroutine can poll
// its content while RunPlain concurrently writes to it — without this,
// -race flags the plain read/write as a data race even though the test
// only ever reads a snapshot for a "has this appeared yet" check.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestRunPlainWritesControlLines(t *testing.T) {
	bus := eventbus.New(4)
	buf := &syncBuffer{}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- RunPlain(ctx, bus, buf) }()

	bus.Publish(eventbus.ConnStateChanged{State: "connected", Session: "sess-1"})
	bus.Publish(eventbus.ServerStateChanged{Name: "fs", State: "healthy", Restarts: 0})
	bus.Publish(eventbus.StreamOpened{ID: 1})
	bus.Publish(eventbus.StreamClosed{ID: 1})
	bus.Publish(eventbus.AppError{Code: "QUOTA_EXCEEDED", Message: "too many servers", Service: "fs", Hint: "upgrade your plan at https://example.test/settings to add more servers"})

	// Give RunPlain's goroutine a chance to drain what was just published
	// before asserting on the buffer.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if strings.Contains(buf.String(), "QUOTA_EXCEEDED") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for AppError line; got:\n%s", buf.String())
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	if err := <-done; err == nil {
		t.Fatalf("expected RunPlain to return ctx.Err() after cancel, got nil")
	}

	out := buf.String()
	// "healthy" prints as "active" (displayState): a healthy supervisor and
	// an http row's registry-derived "active" are the same fact.
	for _, want := range []string{"connected", "sess-1", "fs", "active", "stream 1 opened", "stream 1 closed", "QUOTA_EXCEEDED", "too many servers", "upgrade your plan at https://example.test/settings"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q; got:\n%s", want, out)
		}
	}
	if strings.Contains(out, "healthy") {
		t.Errorf("expected \"healthy\" to print as \"active\", got:\n%s", out)
	}
}

// TestRunPlainStopsOnClosedBus closes Telemetry and Control separately
// (rather than via bus.Close(), which closes both at once) to deterministically
// exercise the window where Telemetry is closed but Control isn't yet — a
// closed-Telemetry case that isn't nil'd out is always-ready and spins the
// select instead of blocking, and RunPlain must still be sitting in that
// select (not have returned early) once Control closes.
func TestRunPlainStopsOnClosedBus(t *testing.T) {
	bus := eventbus.New(1)
	var buf bytes.Buffer
	ctx := context.Background()

	done := make(chan error, 1)
	go func() { done <- RunPlain(ctx, bus, &buf) }()

	close(bus.Telemetry)
	time.Sleep(20 * time.Millisecond)

	select {
	case err := <-done:
		t.Fatalf("RunPlain returned (%v) before Control closed", err)
	default:
	}

	close(bus.Control)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil error on closed bus, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("RunPlain did not return after Control closed")
	}
}

func TestRunPlainNeverBlocksProducer(t *testing.T) {
	// A Control channel with zero consumer would deadlock Publish once
	// full; RunPlain must keep draining it even while nobody reads the
	// LogLine forwarding it does on the side.
	bus := eventbus.New(0)
	var buf bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go RunPlain(ctx, bus, &buf)

	publishDone := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			bus.Publish(eventbus.StreamOpened{ID: uint32(i)})
		}
		close(publishDone)
	}()

	select {
	case <-publishDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("Publish blocked — RunPlain stopped draining Control")
	}
}

// TestRunPlainPrintsServiceTransitions: each eventbus.ServiceChanged is one
// line in DESIGN.md §9's STATE vocabulary — the active line carries the
// public URL (plain mode's only place for it), a rejected/taken_over line
// the wire code, the message and the hint, and pending/disabled a bare
// state.
func TestRunPlainPrintsServiceTransitions(t *testing.T) {
	bus := eventbus.New(8)
	buf := &syncBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- RunPlain(ctx, bus, buf) }()

	bus.Publish(eventbus.ServiceChanged{Name: "anki", Kind: "http", State: "pending"})
	bus.Publish(eventbus.ServiceChanged{Name: "anki", Kind: "http", State: "active", URL: "https://anki-anatoly.tunnel.mcpwarp.io/mcp"})
	bus.Publish(eventbus.ServiceChanged{Name: "deepwiki", Kind: "http", State: "rejected", Code: "QUOTA_EXCEEDED", Message: "server limit reached", Hint: "upgrade your plan at https://web.mcpwarp.io/settings to add more servers"})
	bus.Publish(eventbus.ServiceChanged{Name: "notes", Kind: "stdio", State: "taken_over", Code: "TAKEN_OVER", Hint: "press e to reclaim it"})
	bus.Publish(eventbus.ServiceChanged{Name: "fs", Kind: "stdio", State: "disabled"})

	want := "· anki: pending\n" +
		"✓ anki: active https://anki-anatoly.tunnel.mcpwarp.io/mcp\n" +
		"✗ deepwiki: rejected (QUOTA_EXCEEDED): server limit reached · upgrade your plan at https://web.mcpwarp.io/settings to add more servers\n" +
		"✗ notes: taken_over (TAKEN_OVER) · press e to reclaim it\n" +
		"· fs: disabled\n"
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(buf.String(), "fs: disabled") {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for the last ServiceChanged line; got:\n%s", buf.String())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done

	if got := buf.String(); got != want {
		t.Fatalf("plain output =\n%s\nwant\n%s", got, want)
	}
}

// TestServiceLineOptionalPartsAndColor: the code, message and hint parts
// appear only when present (a hint without a code, a code without a hint,
// a message without a code), a hint's own em dash stays intact behind the
// " · " separator, and with color on, rejected/taken_over get the red ✗ and
// active the green ✓.
func TestServiceLineOptionalPartsAndColor(t *testing.T) {
	for _, tc := range []struct {
		evt   eventbus.ServiceChanged
		color bool
		want  string
	}{
		{eventbus.ServiceChanged{Name: "notes", State: "taken_over", Hint: "press e to reclaim it"}, false, "✗ notes: taken_over · press e to reclaim it\n"},
		{eventbus.ServiceChanged{Name: "notes", State: "taken_over", Message: "another session took it", Hint: "e reclaims it — no config change"}, false, "✗ notes: taken_over: another session took it · e reclaims it — no config change\n"},
		{eventbus.ServiceChanged{Name: "fs", State: "disabled", Code: "DISABLED"}, false, "· fs: disabled (DISABLED)\n"},
		{eventbus.ServiceChanged{Name: "fs", State: "active"}, false, "✓ fs: active\n"},
		{eventbus.ServiceChanged{Name: "fs", State: "weird"}, false, "! fs: weird\n"},
		{eventbus.ServiceChanged{Name: "notes", State: "taken_over"}, true, colorRed + "✗" + colorReset + " notes: taken_over\n"},
		{eventbus.ServiceChanged{Name: "fs", State: "active", URL: "https://x"}, true, colorGreen + "✓" + colorReset + " fs: active https://x\n"},
	} {
		var b bytes.Buffer
		writeControlLine(&b, tc.color, tc.evt)
		if got := b.String(); got != tc.want {
			t.Errorf("writeControlLine(%+v, color=%v) = %q, want %q", tc.evt, tc.color, got, tc.want)
		}
	}
}
