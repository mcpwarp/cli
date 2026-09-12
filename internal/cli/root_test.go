package cli

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// captureStderr redirects the process's real os.Stderr for the duration of
// fn and returns everything written to it. Needed because
// SetFlagErrorFunc's handler writes straight to cmd.ErrOrStderr() (which
// defaults to os.Stderr), bypassing output.Stderr's redirectable var.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stderr
	os.Stderr = w
	defer func() { os.Stderr = orig }()

	fn()

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func TestExecuteExitCodes(t *testing.T) {
	t.Run("--version exits 0", func(t *testing.T) {
		if code := Execute("dev", []string{"--version"}); code != 0 {
			t.Errorf("got %d", code)
		}
	})

	t.Run("--help exits 0", func(t *testing.T) {
		if code := Execute("dev", []string{"--help"}); code != 0 {
			t.Errorf("got %d", code)
		}
	})

	t.Run("bare invocation with no subcommand exits 2", func(t *testing.T) {
		if code := Execute("dev", []string{}); code != 2 {
			t.Errorf("got %d", code)
		}
	})

	t.Run("unknown command exits 2 via our own exitError (Node parity, see TestUnknownCommandMatchesNodeWording)", func(t *testing.T) {
		if code := Execute("dev", []string{"does-not-exist"}); code != 2 {
			t.Errorf("got %d", code)
		}
	})

	t.Run("unknown flag exits 2", func(t *testing.T) {
		if code := Execute("dev", []string{"status", "--no-such-flag"}); code != 2 {
			t.Errorf("got %d", code)
		}
	})

	t.Run("-v is not wired to --version (only Node's -V/--version is) and exits 2", func(t *testing.T) {
		var code int
		stderr := captureStderr(t, func() {
			code = Execute("dev", []string{"-v"})
		})
		if code != 2 {
			t.Errorf("got %d", code)
		}
		if !strings.HasPrefix(stderr, "error: unknown option '-v'") {
			t.Errorf("got stderr %q", stderr)
		}
	})

	t.Run("an unrecognized long flag exits 2 and names the flag", func(t *testing.T) {
		var code int
		stderr := captureStderr(t, func() {
			code = Execute("dev", []string{"--nope"})
		})
		if code != 2 {
			t.Errorf("got %d", code)
		}
		if !strings.HasPrefix(stderr, "error: unknown option '--nope'") {
			t.Errorf("got stderr %q", stderr)
		}
	})

	t.Run("status against a missing config file exits 2", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "missing.json")
		if code := Execute("dev", []string{"status", "--config", path}); code != 2 {
			t.Errorf("got %d", code)
		}
	})

	t.Run("status exits 1 (a runtime failure, not usage/config) when the home directory can't be resolved", func(t *testing.T) {
		// config.ResolveConfigPath falls back to os.UserHomeDir() when
		// --config isn't given; with $HOME (or, on Windows, %USERPROFILE%)
		// unset that's a genuine runtime failure (#12), not a usage/config
		// error, so it's exit 1.
		t.Setenv("HOME", "")
		t.Setenv("USERPROFILE", "")
		if code := Execute("dev", []string{"status"}); code != 1 {
			t.Errorf("got %d", code)
		}
	})

	t.Run("status against a valid config exits 0", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.json")
		raw := `{"servers":[{"name":"notes","kind":"http","url":"http://127.0.0.1:8765/mcp"}]}`
		if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
			t.Fatal(err)
		}
		if code := Execute("dev", []string{"status", "--config", path}); code != 0 {
			t.Errorf("got %d", code)
		}
	})
}

// TestUnknownCommandMatchesNodeWording is the "unknown command" bullet: Node
// (program.ts, commander's default) prints `error: unknown command 'bogus'`
// followed by a "(Did you mean ...?)" hint when one applies, then usage, all
// on stderr, exit 2. The old Go behaviour was `✗ unknown command "bogus" for
// "mcpwarp"` with no usage printed at all (SilenceErrors/SilenceUsage
// suppressed cobra's own printing and runRoot's fallback used output.Error's
// "✗ " glyph instead).
func TestUnknownCommandMatchesNodeWording(t *testing.T) {
	t.Run("no close match: first line and usage, no suggestion", func(t *testing.T) {
		var code int
		stderr := captureStderr(t, func() {
			code = Execute("dev", []string{"bogus"})
		})
		if code != 2 {
			t.Fatalf("got exit code %d, want 2", code)
		}
		if !strings.HasPrefix(stderr, "error: unknown command 'bogus'\n") {
			t.Fatalf("got stderr %q", stderr)
		}
		if !strings.Contains(stderr, "Usage:") {
			t.Fatalf("expected usage to be printed, got stderr %q", stderr)
		}
		if strings.Contains(stderr, "Did you mean") {
			t.Fatalf("did not expect a suggestion for \"bogus\", got stderr %q", stderr)
		}
	})

	t.Run("close match: a Did-you-mean hint before usage", func(t *testing.T) {
		var code int
		stderr := captureStderr(t, func() {
			code = Execute("dev", []string{"statu"})
		})
		if code != 2 {
			t.Fatalf("got exit code %d, want 2", code)
		}
		want := "error: unknown command 'statu'\n(Did you mean status?)\n\nUsage:"
		if !strings.HasPrefix(stderr, want) {
			t.Fatalf("got stderr %q, want prefix %q", stderr, want)
		}
	})
}

// TestSignalDuringCommandOwnsExit and TestSignalShutdownTimeoutForcesExit
// (N1's self-signalling races) live in root_signal_posix_test.go —
// syscall.Kill has no Windows equivalent.
