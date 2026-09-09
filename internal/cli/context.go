package cli

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"

	"github.com/mcpwarp/cli/internal/config"
)

// Context is the per-invocation state handed to every command — the Go
// analogue of Node's CommandContext (cli/context.ts).
type Context struct {
	Verbose            bool
	ConfigPath         string
	IssuerOverride     string
	ConnectURLOverride string
	Log                *slog.Logger

	// Ctx is the command's context, cancelled on SIGINT/SIGTERM (Execute
	// wires it via signal.NotifyContext) — so a poll loop like login's
	// PollForToken stops promptly on Ctrl-C instead of running to its own
	// deadline. Nil in tests that construct a Context directly; Context()
	// falls back to context.Background() in that case.
	Ctx context.Context

	// HomeDir overrides the home directory auth.CredentialsPaths resolves
	// the credentials file under. Empty means the real home directory; a
	// test seam so status_test.go doesn't touch the developer's real
	// ~/.mcpwarp/credentials.
	HomeDir string

	// LogWriter is the SwapWriter Log's handler actually writes through
	// (populated alongside Log by NewLoggerWithSwap) — lets a command
	// (`up`'s TTY path, DESIGN.md §9) redirect the same logger to a file
	// for the duration it owns the terminal, without reconstructing Log or
	// racing a concurrent write. Nil for a Context built without
	// NewLoggerWithSwap (e.g. a test constructing Context{Log: ...}
	// directly) — callers must check before using it.
	LogWriter *SwapWriter

	cachedConfig *config.Config
}

// Context returns c.Ctx, or context.Background() if it hasn't been set.
func (c *Context) Context() context.Context {
	if c.Ctx != nil {
		return c.Ctx
	}
	return context.Background()
}

// LoadConfig resolves --config against the default, loads and validates it,
// and caches the result for the lifetime of this context.
func (c *Context) LoadConfig() (*config.Config, error) {
	if c.cachedConfig != nil {
		return c.cachedConfig, nil
	}
	path, err := config.ResolveConfigPath(c.ConfigPath)
	if err != nil {
		return nil, err
	}
	cfg, err := config.LoadConfig(path)
	if err != nil {
		return nil, err
	}
	c.cachedConfig = cfg
	return cfg, nil
}

// NewLogger builds the stderr slog logger: level info, or debug under
// --verbose or MCPWARP_DEBUG (matching Node's createLogger verbose switch).
func NewLogger(verbose bool) *slog.Logger {
	log, _ := NewLoggerWithSwap(verbose)
	return log
}

// NewLoggerWithSwap is NewLogger, also returning the SwapWriter its handler
// writes through — so a caller (up's TTY path, DESIGN.md §9: "log/slog to
// a file while the TUI owns the screen") can later redirect the same
// logger's output (e.g. to tui.OpenLogFile's file) without reconstructing
// it or racing a concurrent write.
func NewLoggerWithSwap(verbose bool) (*slog.Logger, *SwapWriter) {
	level := slog.LevelInfo
	if verbose || os.Getenv("MCPWARP_DEBUG") != "" {
		level = slog.LevelDebug
	}
	sw := NewSwapWriter(os.Stderr)
	handler := slog.NewJSONHandler(sw, &slog.HandlerOptions{Level: level})
	return slog.New(handler), sw
}

// SwapWriter is an io.Writer whose destination can be redirected while in
// use, guarded by a mutex so a concurrent Write never races a Swap. Lets a
// logger already handed out (slog.Logger has no "change my output" method)
// keep working after its destination moves.
type SwapWriter struct {
	mu sync.Mutex
	w  io.Writer
}

// NewSwapWriter builds a SwapWriter initially writing to w.
func NewSwapWriter(w io.Writer) *SwapWriter { return &SwapWriter{w: w} }

// Write implements io.Writer, writing to the current destination.
func (s *SwapWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	w := s.w
	s.mu.Unlock()
	return w.Write(p)
}

// Swap redirects future writes to w.
func (s *SwapWriter) Swap(w io.Writer) {
	s.mu.Lock()
	s.w = w
	s.mu.Unlock()
}
