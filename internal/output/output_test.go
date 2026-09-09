package output

import (
	"bytes"
	"strings"
	"testing"
)

// withCapturedStreams redirects Stdout/Stderr to buffers for the duration of
// fn, restoring the real streams afterward.
func withCapturedStreams(t *testing.T, fn func(stdout, stderr *bytes.Buffer)) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	origStdout, origStderr := Stdout, Stderr
	Stdout, Stderr = &stdout, &stderr
	t.Cleanup(func() { Stdout, Stderr = origStdout, origStderr })
	fn(&stdout, &stderr)
}

func TestStreamRouting(t *testing.T) {
	t.Run("Success, Warn, Info, and Dim write to Stdout, never Stderr", func(t *testing.T) {
		withCapturedStreams(t, func(stdout, stderr *bytes.Buffer) {
			Success("ok")
			Warn("careful")
			Info("plain")
			Dim("dim")
			if stderr.Len() != 0 {
				t.Errorf("expected nothing on stderr, got %q", stderr.String())
			}
			if stdout.Len() == 0 {
				t.Error("expected output on stdout")
			}
		})
	})

	t.Run("Error writes to Stderr, never Stdout", func(t *testing.T) {
		withCapturedStreams(t, func(stdout, stderr *bytes.Buffer) {
			Error("broke")
			if stdout.Len() != 0 {
				t.Errorf("expected nothing on stdout, got %q", stdout.String())
			}
			if !strings.Contains(stderr.String(), "broke") {
				t.Errorf("expected message on stderr, got %q", stderr.String())
			}
		})
	})
}

func TestGlyphs(t *testing.T) {
	cases := []struct {
		name  string
		fn    func(string)
		glyph string
	}{
		{"Success prefixes with a check mark", Success, "✓"},
		{"Warn prefixes with a bang", Warn, "!"},
		{"Error prefixes with a cross", Error, "✗"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			withCapturedStreams(t, func(stdout, stderr *bytes.Buffer) {
				c.fn("message")
				got := stdout.String() + stderr.String()
				if !strings.HasPrefix(got, c.glyph+" message") {
					t.Errorf("expected prefix %q, got %q", c.glyph+" message", got)
				}
			})
		})
	}

	t.Run("Info and Dim print no glyph", func(t *testing.T) {
		withCapturedStreams(t, func(stdout, stderr *bytes.Buffer) {
			Info("plain")
			if stdout.String() != "plain\n" {
				t.Errorf("got %q", stdout.String())
			}
		})
	})
}

func TestNoColorAndNonTTY(t *testing.T) {
	// Stdout/Stderr are *bytes.Buffer in tests, never *os.File, so
	// colorEnabled's isTTY check can never pass — no ANSI escape should
	// ever reach a redirected stream, NO_COLOR or not.
	t.Run("never emits ANSI escapes against a non-file writer", func(t *testing.T) {
		t.Setenv("NO_COLOR", "")
		withCapturedStreams(t, func(stdout, stderr *bytes.Buffer) {
			Success("s")
			Warn("w")
			Error("e")
			combined := stdout.String() + stderr.String()
			if strings.Contains(combined, "\x1b[") {
				t.Errorf("expected no ANSI escapes, got %q", combined)
			}
		})
	})

	t.Run("streamColorEnabled is false for a non-file writer regardless of NO_COLOR", func(t *testing.T) {
		t.Setenv("NO_COLOR", "1")
		if streamColorEnabled(Stdout) {
			t.Error("expected streamColorEnabled false for a non-*os.File writer")
		}
		t.Setenv("NO_COLOR", "")
		if streamColorEnabled(Stdout) {
			t.Error("expected streamColorEnabled false for a non-*os.File writer")
		}
	})
}

func TestColorEnabled(t *testing.T) {
	cases := []struct {
		name    string
		isTTY   bool
		noColor string
		want    bool
	}{
		{"a TTY with NO_COLOR unset enables color", true, "", true},
		{"a TTY with NO_COLOR set (any value) disables color", true, "1", false},
		{"a TTY with NO_COLOR set to any non-empty value still disables color", true, "0", false},
		{"a non-TTY disables color even with NO_COLOR unset", false, "", false},
		{"a non-TTY with NO_COLOR set disables color", false, "1", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := colorEnabled(tc.isTTY, tc.noColor); got != tc.want {
				t.Errorf("colorEnabled(%v, %q) = %v, want %v", tc.isTTY, tc.noColor, got, tc.want)
			}
		})
	}
}
