package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mcpwarp/cli/internal/output"
)

// withCapturedStdout redirects output.Stdout to a buffer for the duration of
// the test, restoring it afterward.
func withCapturedStdout(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	orig := output.Stdout
	output.Stdout = &buf
	t.Cleanup(func() { output.Stdout = orig })
	return &buf
}

func TestRunStatus(t *testing.T) {
	t.Run("prints the config path, server table, and not-logged-in status", func(t *testing.T) {
		dir := t.TempDir()
		configPath := filepath.Join(dir, "config.json")
		raw := `{"servers":[
			{"name":"blender","kind":"stdio","command":"uvx","args":["blender-mcp"]},
			{"name":"notes","kind":"http","url":"http://127.0.0.1:8765/mcp"}
		]}`
		if err := os.WriteFile(configPath, []byte(raw), 0o644); err != nil {
			t.Fatal(err)
		}

		buf := withCapturedStdout(t)
		// HomeDir points auth.CredentialsPaths at an empty t.TempDir(), the
		// #11 seam — without it this test would read the developer's real
		// ~/.mcpwarp/credentials.
		ctx := &Context{ConfigPath: configPath, HomeDir: dir, Log: NewLogger(false)}
		if err := runStatus(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got := buf.String()
		for _, want := range []string{
			"config: " + configPath,
			"2 servers configured",
			"NAME",
			"KIND",
			"TARGET",
			"blender",
			"uvx blender-mcp",
			"notes",
			"http://127.0.0.1:8765/mcp",
			"login: not logged in",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("expected output to contain %q, got:\n%s", want, got)
			}
		}
	})

	t.Run("singular server count", func(t *testing.T) {
		dir := t.TempDir()
		configPath := filepath.Join(dir, "config.json")
		raw := `{"servers":[{"name":"notes","kind":"http","url":"http://127.0.0.1:8765/mcp"}]}`
		if err := os.WriteFile(configPath, []byte(raw), 0o644); err != nil {
			t.Fatal(err)
		}

		buf := withCapturedStdout(t)
		ctx := &Context{ConfigPath: configPath, HomeDir: dir, Log: NewLogger(false)}
		if err := runStatus(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strings.Contains(buf.String(), "1 server configured") {
			t.Errorf("expected singular server count, got:\n%s", buf.String())
		}
	})
}
