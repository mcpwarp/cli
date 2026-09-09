package tui

import (
	"os"
	"path/filepath"
)

// LogFilePath returns where slog should write while the TUI owns the
// screen (DESIGN.md §9: "log/slog to a file; the log-tail pane reads the
// same stream via the event bus, not the file"). home is the user's home
// directory (e.g. os.UserHomeDir()) — kept as a parameter rather than
// looked up here so callers/tests can point it elsewhere without touching
// the environment.
func LogFilePath(home string) string {
	return filepath.Join(home, ".mcpwarp", "mcpwarp-up.log")
}

// OpenLogFile opens (creating and appending to) the file LogFilePath
// names, 0600, creating its parent directory (0700) if needed. The caller
// is responsible for closing the returned file and for routing slog's
// output to it — this milestone only provides the seam; wiring slog to it
// is M3B's job (DESIGN.md §9).
func OpenLogFile(home string) (*os.File, error) {
	dir := filepath.Join(home, ".mcpwarp")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return os.OpenFile(LogFilePath(home), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
}
