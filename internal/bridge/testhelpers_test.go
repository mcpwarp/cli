package bridge

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newBareChild builds a StdioChild with no live process behind it, for
// tests that only exercise the NDJSON framing/parsing logic directly
// (pumpStdout/handleLine) without spawning anything.
func newBareChild() *StdioChild {
	return &StdioChild{log: testLogger(), exitCh: make(chan struct{})}
}

var fakeMCPPath string

// TestMain builds the fakemcp e2e fixture once for the whole package's
// test run (into a dir removed at the end, not tied to any single test's
// t.TempDir() lifecycle) rather than once per test.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "mcpwarp-fakemcp-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "fakemcp fixture: MkdirTemp: %v\n", err)
	} else {
		out := filepath.Join(dir, "fakemcp-bin")
		cmd := exec.Command("go", "build", "-o", out, "./testdata/fakemcp")
		if output, buildErr := cmd.CombinedOutput(); buildErr != nil {
			fmt.Fprintf(os.Stderr, "fakemcp fixture build failed: %v\n%s\n", buildErr, output)
		} else {
			fakeMCPPath = out
		}
	}
	code := m.Run()
	if dir != "" {
		_ = os.RemoveAll(dir)
	}
	os.Exit(code)
}

func buildFakeMCP(t *testing.T) string {
	t.Helper()
	if fakeMCPPath == "" {
		t.Fatal("fakemcp fixture failed to build in TestMain")
	}
	return fakeMCPPath
}
