package output

import (
	"context"
	"fmt"
	"io"

	"github.com/mcpwarp/cli/internal/eventbus"
)

// RunPlain is the --no-tui / non-TTY degrade path (DESIGN.md §9): one line
// per Control event, Node-style ✓/!/✗ prefixes where they fit, written to
// w. LogLines pass through to Stderr as they arrive; MetricSamples are
// dropped (DESIGN.md §9 — no bandwidth/latency display outside the TUI).
//
// RunPlain keeps draining bus.Control until ctx is done, even once w stops
// being interesting to anyone, because Control is the bus's lossless
// channel (DESIGN.md §3): a caller that stopped reading it would
// backpressure the bridge/supervisor/tunnel goroutines publishing to it.
// Telemetry and Metrics are drop-oldest and self-bounding without a reader
// (eventbus.Bus's PublishTelemetry/PublishMetric never block), so RunPlain
// reads Telemetry only because forwarding LogLines to stderr is useful,
// not because skipping it would risk blocking a producer; it doesn't read
// Metrics at all.
func RunPlain(ctx context.Context, bus *eventbus.Bus, w io.Writer) error {
	color := streamColorEnabled(w)
	telemetry := bus.Telemetry
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case evt, ok := <-bus.Control:
			if !ok {
				return nil
			}
			writeControlLine(w, color, evt)
		case line, ok := <-telemetry:
			if !ok {
				// A closed Telemetry with telemetry still in this select
				// would otherwise be an always-ready case: every future
				// iteration re-enters this branch on the zero LogLine
				// instead of blocking, spinning the loop until Control
				// also closes. Nil the local var so this case blocks
				// forever instead, leaving Control the only live case.
				telemetry = nil
				continue
			}
			writeLogLine(Stderr, line)
		}
	}
}

func writeControlLine(w io.Writer, color bool, evt any) {
	switch e := evt.(type) {
	case eventbus.ConnStateChanged:
		glyph, style := glyphFor(connStateSeverity(e.State))
		reason := ""
		if e.Reason != "" {
			reason = " reason=" + e.Reason
		}
		fmt.Fprintf(w, "%s connection %s session=%s%s\n", colorize(color, style, glyph), e.State, e.Session, reason)

	case eventbus.ServerStateChanged:
		glyph, style := glyphFor(serverStateSeverity(e.State))
		fmt.Fprintf(w, "%s %s: %s (restarts=%d)\n", colorize(color, style, glyph), e.Name, displayState(e.State), e.Restarts)

	case eventbus.StreamOpened:
		fmt.Fprintf(w, "%s stream %d opened\n", colorize(color, colorDim, "·"), e.ID)

	case eventbus.StreamClosed:
		if e.Err != "" {
			fmt.Fprintf(w, "%s stream %d closed: %s\n", colorize(color, colorYellow, "!"), e.ID, e.Err)
		} else {
			fmt.Fprintf(w, "%s stream %d closed\n", colorize(color, colorDim, "·"), e.ID)
		}

	case eventbus.AppError:
		service := ""
		if e.Service != "" {
			service = " service=" + e.Service
		}
		fmt.Fprintf(w, "%s %s: %s%s\n", colorize(color, colorRed, "✗"), e.Code, e.Message, service)

	default:
		fmt.Fprintf(w, "%s unknown event %#v\n", colorize(color, colorYellow, "!"), evt)
	}
}

// writeLogLine writes a forwarded LogLine to w. Callers pass the package's
// Stderr var explicitly (rather than this function reaching for the global
// itself) so the destination is visible at the call site and swappable in
// tests.
func writeLogLine(w io.Writer, line eventbus.LogLine) {
	prefix := line.Server
	if line.Level != "" {
		prefix += "/" + line.Level
	}
	fmt.Fprintf(w, "%s: %s\n", prefix, line.Text)
}

// severity is a three-way glyph/color selector shared by connection and
// server state lines: good (✓, green), warn (!, yellow), bad (✗, red).
type severity int

const (
	sevGood severity = iota
	sevWarn
	sevBad
)

func connStateSeverity(state string) severity {
	switch state {
	case "connected", "healthy":
		return sevGood
	case "closed", "error":
		return sevBad
	default:
		return sevWarn
	}
}

// displayState maps a raw supervisor state to what gets printed: healthy
// and an http row's "active" are the same fact from a user's point of
// view, so both print as "active" (mirrors internal/tui/view.go's
// displayState — kept separate since neither package imports the other).
func displayState(s string) string {
	if s == "healthy" {
		return "active"
	}
	return s
}

func serverStateSeverity(state string) severity {
	switch state {
	case "healthy":
		return sevGood
	case "failed":
		return sevBad
	default:
		return sevWarn
	}
}

func glyphFor(sev severity) (glyph string, color string) {
	switch sev {
	case sevGood:
		return "✓", colorGreen
	case sevBad:
		return "✗", colorRed
	default:
		return "!", colorYellow
	}
}

func colorize(enabled bool, code, s string) string {
	if !enabled {
		return s
	}
	return code + s + colorReset
}
