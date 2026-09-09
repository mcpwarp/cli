package cli

import (
	"bytes"
	"strings"
	"testing"
)

// TestSwapWriterRedirectsFutureWrites is S-6: a write before Swap must land
// on the original destination, and a write after Swap must land on the new
// one — not both, and not the old one after the swap.
func TestSwapWriterRedirectsFutureWrites(t *testing.T) {
	var before, after bytes.Buffer
	sw := NewSwapWriter(&before)

	if _, err := sw.Write([]byte("first")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	sw.Swap(&after)
	if _, err := sw.Write([]byte("second")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if before.String() != "first" {
		t.Fatalf("before-swap buffer = %q, want %q", before.String(), "first")
	}
	if after.String() != "second" {
		t.Fatalf("after-swap buffer = %q, want %q", after.String(), "second")
	}
}

// TestNewLoggerWithSwapRedirectsLoggerOutput is S-6: a log line emitted
// after Swap must reach the new destination, not the one the logger was
// built with — proving the *slog.Logger NewLoggerWithSwap returns keeps
// writing through the same SwapWriter rather than a fixed io.Writer.
func TestNewLoggerWithSwapRedirectsLoggerOutput(t *testing.T) {
	var original, redirected bytes.Buffer
	log, sw := NewLoggerWithSwap(false)
	sw.Swap(&original)

	log.Info("before redirect")
	sw.Swap(&redirected)
	log.Info("after redirect")

	if strings.Contains(original.String(), "after redirect") {
		t.Fatalf("expected the post-swap line not to reach the original writer, got %q", original.String())
	}
	if !strings.Contains(redirected.String(), "after redirect") {
		t.Fatalf("expected the post-swap line to reach the redirected writer, got %q", redirected.String())
	}
	if !strings.Contains(original.String(), "before redirect") {
		t.Fatalf("expected the pre-swap line to have reached the original writer, got %q", original.String())
	}
}
