package cli

// ExitCoder is implemented by errors that carry their own process exit code
// (config.ConfigError, auth.SettingsError, and the generic exitError below).
// Any other error returned from a command's RunE is treated as a usage error
// (cobra flag/arg parsing failures) and mapped to exit 2, matching Node's
// commander exitOverride (program.ts).
type ExitCoder interface {
	error
	ExitCode() int
}

// exitError wraps an error already reported to the user (via output.Error)
// so Execute doesn't print it a second time; only its exit code matters to
// the caller.
type exitError struct {
	code int
}

func (e *exitError) Error() string { return "" }
func (e *exitError) ExitCode() int { return e.code }

// RuntimeError signals a command already printed its own failure message
// and should exit 1 (a runtime failure, not a usage/config problem).
func RuntimeError() error { return &exitError{code: 1} }
