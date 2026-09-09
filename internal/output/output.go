// Package output is the product-facing stdout/stderr surface: plain lines,
// ✓/!/✗ prefixes with colour only on a TTY when NO_COLOR is unset — the Go
// equivalent of the Node CLI's src/cli/output.ts.
package output

import (
	"fmt"
	"io"
	"os"

	"golang.org/x/term"
)

const (
	colorReset  = "\x1b[0m"
	colorGreen  = "\x1b[32m"
	colorRed    = "\x1b[31m"
	colorYellow = "\x1b[33m"
	colorDim    = "\x1b[2m"
)

// Stdout/Stderr are the streams product output is written to — vars so
// tests can redirect them.
var (
	Stdout io.Writer = os.Stdout
	Stderr io.Writer = os.Stderr
)

// isTerminal is term.IsTerminal behind a var, wrapped to take the *os.File
// itself rather than a bare fd — a test seam shared by isTTY and spinner.go
// so a test can force TTY-shaped behaviour (e.g. on an os.Pipe end, which
// term.IsTerminal would honestly report false for) without a real pty.
var isTerminal = func(f *os.File) bool { return term.IsTerminal(int(f.Fd())) }

func isTTY(f *os.File) bool {
	return isTerminal(f)
}

// streamColorEnabled reports whether w should get ANSI color: w must be a
// real *os.File (a TTY check on anything else is meaningless), and NO_COLOR
// must be unset.
func streamColorEnabled(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	return colorEnabled(isTTY(f), os.Getenv("NO_COLOR"))
}

// colorEnabled reports whether color output should be used, given whether
// the destination is a terminal and the current NO_COLOR value.
func colorEnabled(isTTY bool, noColor string) bool {
	return isTTY && noColor == ""
}

// Success prints "✓ message" to stdout, green on a TTY.
func Success(message string) {
	c := streamColorEnabled(Stdout)
	fmt.Fprintf(Stdout, "%s✓%s %s\n", ifColor(c, colorGreen), ifColor(c, colorReset), message)
}

// Warn prints "! message" to stdout, yellow on a TTY.
func Warn(message string) {
	c := streamColorEnabled(Stdout)
	fmt.Fprintf(Stdout, "%s!%s %s\n", ifColor(c, colorYellow), ifColor(c, colorReset), message)
}

// Info prints message as-is to stdout, no glyph.
func Info(message string) {
	fmt.Fprintln(Stdout, message)
}

// Dim prints message to stdout, dimmed on a TTY.
func Dim(message string) {
	c := streamColorEnabled(Stdout)
	if c {
		fmt.Fprintf(Stdout, "%s%s%s\n", colorDim, message, colorReset)
	} else {
		fmt.Fprintln(Stdout, message)
	}
}

// Error prints "✗ message" to stderr, red on a TTY.
func Error(message string) {
	c := streamColorEnabled(Stderr)
	fmt.Fprintf(Stderr, "%s✗%s %s\n", ifColor(c, colorRed), ifColor(c, colorReset), message)
}

func ifColor(enabled bool, code string) string {
	if enabled {
		return code
	}
	return ""
}
