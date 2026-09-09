package output

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/mcpwarp/cli/internal/shutdown"
)

func TestStartSpinnerNoTTYIsNoOp(t *testing.T) {
	var buf bytes.Buffer
	orig := Stdout
	Stdout = &buf
	defer func() { Stdout = orig }()

	stop := StartSpinner("waiting...")
	stop()
	stop() // must be safe to call twice

	if buf.Len() != 0 {
		t.Fatalf("expected no output on a non-TTY, got %q", buf.String())
	}
}

func TestStartSpinnerRegistersOneShutdownHandlerThatRestoresCursor(t *testing.T) {
	shutdown.ResetForTests()
	t.Cleanup(shutdown.ResetForTests)

	origStdout, origIsTerminal := Stdout, isTerminal
	t.Cleanup(func() { Stdout, isTerminal = origStdout, origIsTerminal })

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	Stdout = w
	isTerminal = func(*os.File) bool { return true } // os.Pipe() isn't a real tty

	stop := StartSpinner("waiting...")
	if got := shutdown.HandlerCount(); got != 1 {
		t.Fatalf("expected exactly one registered shutdown handler, got %d", got)
	}

	// Run the registered handler (as a real SIGINT/SIGTERM would, via
	// shutdown.Run) and check it actually restored the cursor.
	shutdown.RunHandlersForTests(context.Background())
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), showCursor) {
		t.Fatalf("expected the shutdown handler to write the show-cursor sequence, got %q", out)
	}

	stop()
	if got := shutdown.HandlerCount(); got != 0 {
		t.Fatalf("expected Stop to unregister the handler, got count %d", got)
	}
}
