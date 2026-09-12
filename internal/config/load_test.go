package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveConfigPath(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("uses the override path when given", func(t *testing.T) {
		override := "/some/other/config.json"
		got, err := ResolveConfigPath(override)
		if err != nil {
			t.Fatal(err)
		}
		want, err := filepath.Abs(override)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("got %q want %q", got, want)
		}
	})

	t.Run("defaults to ~/.mcpwarp/config.json", func(t *testing.T) {
		got, err := ResolveConfigPath("")
		if err != nil {
			t.Fatal(err)
		}
		want := filepath.Join(home, ".mcpwarp", "config.json")
		if got != want {
			t.Errorf("got %q want %q", got, want)
		}
	})

	t.Run("expands a bare ~ to the home directory", func(t *testing.T) {
		got, err := ResolveConfigPath("~")
		if err != nil {
			t.Fatal(err)
		}
		if got != home {
			t.Errorf("got %q want %q", got, home)
		}
	})

	t.Run("expands a ~/ prefixed path against the home directory", func(t *testing.T) {
		got, err := ResolveConfigPath("~/configs/mcpwarp.json")
		if err != nil {
			t.Fatal(err)
		}
		want := filepath.Join(home, "configs", "mcpwarp.json")
		if got != want {
			t.Errorf("got %q want %q", got, want)
		}
	})

	t.Run("resolves a relative override to an absolute path", func(t *testing.T) {
		got, err := ResolveConfigPath("relative/config.json")
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasSuffix(got, filepath.Join("relative", "config.json")) || !filepath.IsAbs(got) {
			t.Errorf("got %q", got)
		}
	})
}

func TestLoadConfig(t *testing.T) {
	t.Run("returns a ConfigError with exitCode 2 and a create-it hint when the file is missing", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "does-not-exist.json")

		_, err := LoadConfig(path)
		if err == nil {
			t.Fatal("expected an error")
		}
		ce, ok := err.(*ConfigError)
		if !ok {
			t.Fatalf("expected *ConfigError, got %T", err)
		}
		if ce.ExitCode() != 2 {
			t.Errorf("expected exit code 2, got %d", ce.ExitCode())
		}
		if !strings.Contains(ce.Error(), path) || !strings.Contains(ce.Error(), "servers") {
			t.Errorf("message missing path/example: %s", ce.Error())
		}
	})

	t.Run("returns a ConfigError for a trailing comma with a (line, column) hint", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.json")
		raw := "{\n  \"servers\": [\n    { \"name\": \"a\", \"kind\": \"stdio\", \"command\": \"x\", }\n  ]\n}"
		if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
			t.Fatal(err)
		}

		_, err := LoadConfig(path)
		if err == nil {
			t.Fatal("expected an error")
		}
		if !strings.Contains(err.Error(), "line") || !strings.Contains(err.Error(), "column") {
			t.Errorf("expected a line/column hint, got: %s", err.Error())
		}
	})

	t.Run("strips a leading BOM before parsing JSON", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.json")
		raw := "\ufeff" + `{"servers":[{"name":"notes","kind":"http","url":"http://127.0.0.1:8765/mcp"}]}`
		if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
			t.Fatal(err)
		}

		cfg, err := LoadConfig(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(cfg.Servers) != 1 || cfg.Servers[0].Name != "notes" {
			t.Errorf("got %+v", cfg)
		}
	})

	t.Run("returns a ConfigError listing each schema issue path + message", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.json")
		raw := `{"servers":[
			{"name":"Bad Name","kind":"stdio"},
			{"name":"notes","kind":"http","url":"not-a-url"}
		]}`
		if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
			t.Fatal(err)
		}

		_, err := LoadConfig(path)
		if err == nil {
			t.Fatal("expected an error")
		}
		msg := err.Error()
		for _, want := range []string{path, "servers[0].name", "servers[0].command", "servers[1].url"} {
			if !strings.Contains(msg, want) {
				t.Errorf("expected message to contain %q, got:\n%s", want, msg)
			}
		}
	})

	t.Run("returns the parsed config on success", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.json")
		raw := `{"servers":[{"name":"notes","kind":"http","url":"http://127.0.0.1:8765/mcp"}]}`
		if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
			t.Fatal(err)
		}

		cfg, err := LoadConfig(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(cfg.Servers) != 1 || cfg.Servers[0].Kind != KindHTTP {
			t.Errorf("got %+v", cfg)
		}
	})
}
