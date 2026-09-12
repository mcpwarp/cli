package tui

import (
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/mcpwarp/cli/internal/eventbus"
)

// renderAt is a small helper: apply a WindowSizeMsg then render, the same
// sequence bubbletea drives on a real terminal.
func renderAt(t *testing.T, m Model, w, h int) string {
	t.Helper()
	next, _ := m.Update(tea.WindowSizeMsg{Width: w, Height: h})
	m = next.(Model)
	view := m.View()
	return ansi.Strip(view.Content)
}

func TestViewSmoke80x24(t *testing.T) {
	m := newTestModel([]Server{
		{Name: "fs", Kind: "stdio", URL: "http://127.0.0.1:9001", State: "healthy", Restarts: 0},
		{Name: "git", Kind: "stdio", URL: "http://127.0.0.1:9002", State: "restarting", Restarts: 4},
	}, nil)
	next, _ := m.Update(controlEventMsg{evt: eventbus.ConnStateChanged{State: "connected", Session: "sess-1"}})
	m = next.(Model)
	next, _ = m.Update(telemetryEventMsg{line: eventbus.LogLine{Server: "fs", Level: "info", Text: "started"}})
	m = next.(Model)
	next, _ = m.Update(namedKey(t, "l")) // show the log pane too
	m = next.(Model)

	out := renderAt(t, m, 80, 24)
	for _, want := range []string{"NAME", "KIND", "STATE", "RESTARTS", "URL", "fs", "git", "127.0.0.1:9001", "active", "restarting", "CONNECTED", "q quit"} {
		if !strings.Contains(out, want) {
			t.Fatalf("view missing %q; got:\n%s", want, out)
		}
	}
	// "healthy" is the supervisor's internal state name (unchanged in the
	// model); the STATE column must show "active" instead, not both.
	if strings.Contains(out, "healthy") {
		t.Fatalf("view must display a healthy row's state as \"active\", not \"healthy\"; got:\n%s", out)
	}
}

// TestLastErrorHintRendered checks that an AppError's Hint reaches the
// view, not just the log pane (it used to be logged only).
func TestLastErrorHintRendered(t *testing.T) {
	m := newTestModel(nil, nil)
	next, _ := m.Update(controlEventMsg{evt: eventbus.AppError{
		Code: "SERVER_DISABLED", Message: "disabled", Service: "svc",
		Hint: "this server was disabled in the dashboard",
	}})
	m = next.(Model)

	out := renderAt(t, m, 80, 24)
	if !strings.Contains(out, "this server was disabled in the dashboard") {
		t.Fatalf("view missing last-error hint; got:\n%s", out)
	}
}

// TestLastErrorLinesTruncatedToWidth checks that the last-error and hint
// lines are width-truncated like every other line (renderTable) — untrimmed
// they wrap into extra physical rows a short terminal's height budget can't
// see, pushing the footer off the bottom.
func TestLastErrorLinesTruncatedToWidth(t *testing.T) {
	m := newTestModel([]Server{{Name: "fs", State: "healthy"}}, nil)
	next, _ := m.Update(namedKey(t, "l")) // show the log pane too
	m = next.(Model)
	next, _ = m.Update(controlEventMsg{evt: eventbus.AppError{
		Code: "INVALID_NAME", Message: "server name must match [a-z0-9]([a-z0-9-]*[a-z0-9])? (max 30 chars) and this message on its own runs well past eighty columns",
		Service: "a-very-long-service-name-that-also-helps-push-this-line-past-eighty-columns",
		Hint:    "fix the name in your config — it is the public URL slug and must match [a-z0-9]([a-z0-9-]*[a-z0-9])? (max 30 chars)",
	}})
	m = next.(Model)

	out := renderAt(t, m, 80, 7)
	lines := strings.Split(out, "\n")
	if len(lines) > 7 {
		t.Fatalf("rendered %d lines into a 7-line terminal:\n%s", len(lines), out)
	}
	for _, line := range lines {
		if w := lipgloss.Width(line); w > 80 {
			t.Fatalf("line exceeds terminal width 80 (display width %d): %q", w, line)
		}
	}
	if got := lines[len(lines)-1]; !strings.Contains(got, "q quit") {
		t.Fatalf("last rendered line = %q, want the footer", got)
	}
}

// TestStateHealthyDisplaysAsActive is the STATE-column vocabulary fix:
// "healthy" (a stdio supervisor's internal state) and "active" (an http
// row's registry-derived state, see cli.newTUIRenderer/pollTunnelForTUI)
// must render identically, while every other state passes through as-is.
func TestStateHealthyDisplaysAsActive(t *testing.T) {
	m := newTestModel([]Server{
		{Name: "fs", Kind: "stdio", State: "healthy"},
		{Name: "notes", Kind: "http", State: "active"},
		{Name: "git", Kind: "stdio", State: "restarting"},
	}, nil)
	out := renderAt(t, m, 80, 24)
	if strings.Contains(out, "healthy") {
		t.Fatalf("expected \"healthy\" not to appear in the rendered view; got:\n%s", out)
	}
	if !strings.Contains(out, "restarting") {
		t.Fatalf("expected \"restarting\" to render as-is; got:\n%s", out)
	}
	if m.servers[0].State != "healthy" {
		t.Fatalf("displayState must not mutate the model's stored State, got %q", m.servers[0].State)
	}
}

func TestViewSmoke40x10(t *testing.T) {
	m := newTestModel([]Server{
		{Name: "fs", Kind: "stdio", State: "healthy"},
	}, nil)
	out := renderAt(t, m, 40, 10)
	for _, want := range []string{"NAME", "fs", "q quit"} {
		if !strings.Contains(out, want) {
			t.Fatalf("view missing %q; got:\n%s", want, out)
		}
	}
}

func TestViewSmokeTiny(t *testing.T) {
	m := newTestModel([]Server{{Name: "fs"}}, nil)
	out := renderAt(t, m, 5, 3)
	if !strings.Contains(out, "too small") {
		t.Fatalf("expected a degrade message, got:\n%s", out)
	}
}

func TestViewSmokeHelpOverlay(t *testing.T) {
	m := newTestModel(nil, nil)
	next, _ := m.Update(namedKey(t, "?"))
	m = next.(Model)
	out := renderAt(t, m, 80, 24)
	for _, want := range []string{"keybindings", "restart selected server", "press any key to return"} {
		if !strings.Contains(out, want) {
			t.Fatalf("help overlay missing %q; got:\n%s", want, out)
		}
	}
}

func TestViewSmokeNoServers(t *testing.T) {
	m := newTestModel(nil, nil)
	out := renderAt(t, m, 80, 24)
	if !strings.Contains(out, "no servers configured") {
		t.Fatalf("expected the no-servers message; got:\n%s", out)
	}
}

// TestViewBeforeWindowSizeMsg exercises View() against the width/height New
// seeds (80x24) before any WindowSizeMsg arrives — bubbletea sends the
// initial WindowSizeMsg asynchronously, so View() can run before it lands.
func TestViewBeforeWindowSizeMsg(t *testing.T) {
	bus := eventbus.New(4)
	m := New(bus, []Server{{Name: "fs", State: "healthy"}}, nil)
	out := ansi.Strip(m.View().Content)
	if !strings.Contains(out, "fs") || !strings.Contains(out, "q quit") {
		t.Fatalf("expected a rendered dashboard before any WindowSizeMsg; got:\n%s", out)
	}
}

// TestAltScreenSetOnEveryBranch is blocker 2's named test: AltScreen must be
// true on the help, too-small and normal branches alike, or the renderer
// exits/re-enters the alt screen (and flickers) whenever the branch changes.
func TestAltScreenSetOnEveryBranch(t *testing.T) {
	cases := []struct {
		name string
		m    Model
		w, h int
	}{
		{"normal", newTestModel([]Server{{Name: "fs"}}, nil), 80, 24},
		{"tooSmall", newTestModel([]Server{{Name: "fs"}}, nil), 5, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			next, _ := tc.m.Update(tea.WindowSizeMsg{Width: tc.w, Height: tc.h})
			m := next.(Model)
			if view := m.View(); !view.AltScreen {
				t.Fatalf("AltScreen = false, want true")
			}
		})
	}

	t.Run("help", func(t *testing.T) {
		m := newTestModel(nil, nil)
		next, _ := m.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
		m = next.(Model)
		next, _ = m.Update(namedKey(t, "?"))
		m = next.(Model)
		if view := m.View(); !view.AltScreen {
			t.Fatalf("AltScreen = false, want true (help overlay)")
		}
	})
}

// TestTableTruncatesByDisplayWidthNotBytes is item 5's test: a unicode name
// and a long URL must be measured/truncated by terminal column, not UTF-8
// byte count — a byte-based truncateWidth would either cut a multi-byte
// rune in half or under-truncate a narrow-looking-but-long ASCII URL.
func TestTableTruncatesByDisplayWidthNotBytes(t *testing.T) {
	longURL := "http://127.0.0.1:9001/" + strings.Repeat("x", 200)
	m := newTestModel([]Server{
		{Name: "文件", Kind: "stdio", State: "healthy", URL: longURL}, // 2 wide runes, 6 bytes
	}, nil)
	next, _ := m.Update(tea.WindowSizeMsg{Width: 40, Height: 24})
	m = next.(Model)
	out := ansi.Strip(m.renderTable())

	for _, line := range strings.Split(out, "\n") {
		if lipgloss.Width(line) > 40 {
			t.Fatalf("line exceeds terminal width 40 (display width %d): %q", lipgloss.Width(line), line)
		}
	}
	if !strings.Contains(out, "文件") {
		t.Fatalf("expected the unicode name to render intact; got:\n%s", out)
	}
	if strings.Contains(out, strings.Repeat("x", 200)) {
		t.Fatalf("expected the long URL to be truncated; got:\n%s", out)
	}
}

// TestFooterNeverClippedByLogPane is item 10's test: with the log pane
// open, the footer must still be the last line rendered — the log pane
// gets whatever height remains, not a fixed size that can push the footer
// past the bottom of a short terminal.
func TestFooterNeverClippedByLogPane(t *testing.T) {
	m := newTestModel([]Server{{Name: "fs", State: "healthy"}}, nil)
	next, _ := m.Update(namedKey(t, "l"))
	m = next.(Model)
	for i := 0; i < 50; i++ {
		next, _ = m.Update(telemetryEventMsg{line: eventbus.LogLine{Server: "fs", Text: "line"}})
		m = next.(Model)
	}

	out := renderAt(t, m, 80, 12)
	lines := strings.Split(out, "\n")
	if got := lines[len(lines)-1]; !strings.Contains(got, "q quit") {
		t.Fatalf("last rendered line = %q, want the footer", got)
	}
	if len(lines) > 12 {
		t.Fatalf("rendered %d lines into a 12-line terminal", len(lines))
	}
}

// TestStreamsClampedAtZero is item 7's test: a StreamClosed racing ahead of
// its StreamOpened (or one arriving for a stream from a previous session)
// must not drive the header's streams count negative.
func TestStreamsClampedAtZero(t *testing.T) {
	m := newTestModel(nil, nil)
	next, _ := m.Update(controlEventMsg{evt: eventbus.StreamClosed{ID: 1}})
	m = next.(Model)
	out := renderAt(t, m, 80, 24)
	if !strings.Contains(out, "streams=0") {
		t.Fatalf("expected streams=0, got:\n%s", out)
	}
}
