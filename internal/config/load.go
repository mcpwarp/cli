package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ConfigError is a config-loading failure. Every command that hits one
// prints its message to stderr and exits 2 (overview.md §8).
type ConfigError struct {
	message string
}

func (e *ConfigError) Error() string { return e.message }

// ExitCode matches Node's ConfigError (config/load.ts): config problems are
// always usage-shaped, exit 2.
func (e *ConfigError) ExitCode() int { return 2 }

// ExampleJSON is a minimal valid config, shown in the "file not found"
// error below and reused by the root command's --help text. One key per
// line so its widest line still fits under 80 columns once indented under
// the root help's numbered list.
const ExampleJSON = `{
  "servers": [
    {
      "name": "blender",
      "kind": "stdio",
      "command": "uvx",
      "args": ["blender-mcp"]
    },
    {
      "name": "notes",
      "kind": "http",
      "url": "http://127.0.0.1:8765/mcp"
    }
  ]
}`

// ResolveConfigPath mirrors config/load.ts's resolveConfigPath: `~` expansion
// done by hand, no XDG search path.
func ResolveConfigPath(override string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if override == "" {
		return filepath.Join(home, ".mcpwarp", "config.json"), nil
	}
	if override == "~" {
		return home, nil
	}
	if strings.HasPrefix(override, "~/") {
		return filepath.Join(home, override[2:]), nil
	}
	abs, err := filepath.Abs(override)
	if err != nil {
		return "", err
	}
	return abs, nil
}

// LoadConfig reads, parses and validates the config file at path, returning
// a *ConfigError for every failure mode (missing file, invalid JSON,
// validation errors) — same shape as Node's loadConfig.
func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &ConfigError{message: fmt.Sprintf(
				"config file not found: %s\n\nCreate one — for example:\n%s\n\nRun `mcpwarp status --config <path>` to check a different location.",
				path, ExampleJSON,
			)}
		}
		return nil, &ConfigError{message: fmt.Sprintf("failed to read config file %s: %s", path, err.Error())}
	}

	raw = bytes.TrimPrefix(raw, []byte{0xEF, 0xBB, 0xBF})

	if !json.Valid(raw) {
		return nil, &ConfigError{message: fmt.Sprintf("invalid JSON in %s: %s", path, describeJSONError(raw))}
	}

	cfg, issues := validateConfig(raw)
	if issues != nil {
		return nil, &ConfigError{message: fmt.Sprintf("invalid config at %s:\n%s", path, issues.Error())}
	}
	return cfg, nil
}

// describeJSONError re-runs json.Unmarshal to get a concrete *json.SyntaxError
// (json.Valid only reports validity, not the error) and renders a
// (line, column) position from its byte offset. Go's wording and position
// semantics aren't the same as V8's — describeJsonError in Node's
// config/load.ts parses a byte offset out of V8's own message text — this
// only produces the same "message (line X, column Y)" shape from Go's own
// SyntaxError, not matching text.
func describeJSONError(raw []byte) string {
	var v any
	err := json.Unmarshal(raw, &v)
	if err == nil {
		return "invalid JSON"
	}
	syntaxErr, ok := err.(*json.SyntaxError)
	if !ok {
		return err.Error()
	}
	// SyntaxError.Offset is one byte past where the error occurred, not the
	// erroring byte itself.
	offset := syntaxErr.Offset - 1
	if offset < 0 {
		offset = 0
	}
	if offset > int64(len(raw)) {
		offset = int64(len(raw))
	}
	upToOffset := raw[:offset]
	line := bytes.Count(upToOffset, []byte{'\n'}) + 1
	lastNewline := bytes.LastIndexByte(upToOffset, '\n')
	column := int(offset) - lastNewline
	return fmt.Sprintf("%s (line %d, column %d)", syntaxErr.Error(), line, column)
}
