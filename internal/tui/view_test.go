package tui

import (
	"fmt"
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

var errQuota = eventbus.AppError{
	Code: "QUOTA_EXCEEDED", Message: "server limit reached", Service: "deepwiki",
	Hint: "upgrade your plan at https://mcpwarp.io/settings to add more servers",
}

// TestRegistrationStatesRendered: an http row the tunnel rejected shows
// "rejected" (in the error style), a pending one "pending" — neither
// "active" — while the `last error` line still carries the detail. A stdio
// row shows the registration only over a healthy supervisor state; a
// restarting child stays "restarting" whatever the tunnel said.
func TestRegistrationStatesRendered(t *testing.T) {
	m := newTestModel([]Server{
		{Name: "deepwiki", Kind: "http", State: RegistrationRejected, Registration: RegistrationRejected},
		{Name: "notes", Kind: "http", State: RegistrationPending, Registration: RegistrationPending},
		{Name: "fs", Kind: "stdio", State: "healthy", Registration: RegistrationRejected},
		{Name: "git", Kind: "stdio", State: "healthy", Registration: RegistrationPending},
		{Name: "db", Kind: "stdio", State: "restarting", Registration: RegistrationRejected},
	}, nil)
	next, _ := m.Update(controlEventMsg{evt: errQuota})
	m = next.(Model)

	out := renderAt(t, m, 100, 24)
	want := map[string]string{"deepwiki": "rejected", "notes": "pending", "fs": "rejected", "git": "pending", "db": "restarting"}
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(strings.TrimPrefix(line, ">"))
		if len(fields) < 3 {
			continue
		}
		if state, ok := want[fields[0]]; ok {
			if fields[2] != state {
				t.Fatalf("%s STATE = %q, want %q; got:\n%s", fields[0], fields[2], state, out)
			}
			delete(want, fields[0])
		}
	}
	if len(want) != 0 {
		t.Fatalf("rows not found: %v; got:\n%s", want, out)
	}
	if strings.Contains(out, "active") {
		t.Fatalf("no row here is confirmed, so none may read \"active\"; got:\n%s", out)
	}
	if !strings.Contains(out, "last error: [QUOTA_EXCEEDED] server limit reached (service=deepwiki)") {
		t.Fatalf("expected the last-error line to stay; got:\n%s", out)
	}
	if raw := m.renderTable(); !strings.Contains(raw, styleBad.Render(padRight(RegistrationRejected, len("restarting")))) {
		t.Fatalf("expected the rejected STATE cell in the error style; got %q", raw)
	}
}

// TestFooterAndHelpListCopyKey: `c` is advertised in both the footer hint
// and the `?` overlay, and the footer still fits an 80-column terminal.
func TestFooterAndHelpListCopyKey(t *testing.T) {
	m := newTestModel([]Server{{Name: "fs", State: "healthy"}}, nil)
	out := renderAt(t, m, 80, 24)
	lines := strings.Split(out, "\n")
	footer := lines[len(lines)-1]
	if !strings.Contains(footer, "c copy URL") || !strings.Contains(footer, "e enable") {
		t.Fatalf("footer = %q, want it to list `c copy URL` and still end with `e enable`", footer)
	}
	if w := lipgloss.Width(footer); w > 80 {
		t.Fatalf("footer is %d columns, wider than 80", w)
	}

	next, _ := m.Update(namedKey(t, "?"))
	m = next.(Model)
	if out := renderAt(t, m, 80, 24); !strings.Contains(out, "c          copy selected server's public URL") {
		t.Fatalf("help overlay missing the c binding; got:\n%s", out)
	}
}

const (
	shortURL = "https://fs.example/mcp"
	// wrapURL is 59 columns: it fits beside the other columns at 120 wide
	// but not at 60.
	wrapURL = "https://anki-anatoly.tunnel.dev.mcpwarp.io/mcp/some-path-xy"
)

// tableLines renders just the table at w columns, ANSI stripped.
func tableLines(t *testing.T, m Model, w int) []string {
	t.Helper()
	next, _ := m.Update(tea.WindowSizeMsg{Width: w, Height: 24})
	m = next.(Model)
	return strings.Split(ansi.Strip(m.renderTable()), "\n")
}

// TestTableURLFitsStaysOnOneLine: on a wide terminal every row is one line
// with its full URL inline — the table looks as it always did.
func TestTableURLFitsStaysOnOneLine(t *testing.T) {
	m := newTestModel([]Server{
		{Name: "fs", Kind: "stdio", State: "healthy", URL: shortURL},
		{Name: "anki", Kind: "http", State: "active", URL: wrapURL},
	}, nil)
	lines := tableLines(t, m, 120)
	if len(lines) != 3 {
		t.Fatalf("expected header + 2 single-line rows, got %d lines:\n%s", len(lines), strings.Join(lines, "\n"))
	}
	if !strings.Contains(lines[1], "fs") || !strings.HasSuffix(lines[1], shortURL) {
		t.Fatalf("fs row = %q, want its URL inline", lines[1])
	}
	if !strings.Contains(lines[2], "anki") || !strings.HasSuffix(lines[2], wrapURL) {
		t.Fatalf("anki row = %q, want its URL inline", lines[2])
	}
}

// TestTableURLWrapsWhenNarrow: when a URL doesn't fit its column it moves,
// whole and un-ellipsized, to its own line under the row; no line exceeds
// the terminal width.
func TestTableURLWrapsWhenNarrow(t *testing.T) {
	m := newTestModel([]Server{{Name: "anki", Kind: "http", State: "active", URL: wrapURL}}, nil)
	lines := tableLines(t, m, 66)
	if len(lines) != 3 {
		t.Fatalf("expected header + row + URL line, got %d lines:\n%s", len(lines), strings.Join(lines, "\n"))
	}
	if !strings.Contains(lines[1], "anki") || strings.Contains(lines[1], "https://") {
		t.Fatalf("row line = %q, want the row without its URL", lines[1])
	}
	if got := strings.TrimSpace(lines[2]); got != wrapURL {
		t.Fatalf("URL line = %q, want the full URL %q", lines[2], wrapURL)
	}
	if strings.Contains(strings.Join(lines, "\n"), "…") {
		t.Fatalf("nothing should be truncated at this width:\n%s", strings.Join(lines, "\n"))
	}
	for _, l := range lines {
		if w := lipgloss.Width(l); w > 66 {
			t.Fatalf("line exceeds width 66 (%d): %q", w, l)
		}
	}
}

// TestTableURLWrapMixedRows: only the row whose URL doesn't fit gets the
// extra line; and the selected row's highlight covers its URL line too.
func TestTableURLWrapMixedRows(t *testing.T) {
	m := newTestModel([]Server{
		{Name: "fs", Kind: "stdio", State: "healthy", URL: shortURL},
		{Name: "anki", Kind: "http", State: "active", URL: wrapURL},
		{Name: "deepwiki", Kind: "http", State: RegistrationRejected},
	}, nil)
	next, _ := m.Update(namedKey(t, "j")) // select anki
	m = next.(Model)
	lines := tableLines(t, m, 66)
	if len(lines) != 5 {
		t.Fatalf("expected header + fs + anki + anki URL + deepwiki, got %d lines:\n%s", len(lines), strings.Join(lines, "\n"))
	}
	if !strings.HasSuffix(lines[1], shortURL) {
		t.Fatalf("fs row = %q, want its short URL inline", lines[1])
	}
	if !strings.HasPrefix(lines[2], "> anki") || strings.TrimSpace(lines[3]) != wrapURL {
		t.Fatalf("anki rows = %q / %q, want the selected row then its full URL", lines[2], lines[3])
	}
	if !strings.Contains(lines[4], "deepwiki") {
		t.Fatalf("deepwiki row = %q", lines[4])
	}

	next, _ = m.Update(tea.WindowSizeMsg{Width: 66, Height: 24})
	m = next.(Model)
	raw := strings.Split(m.renderTable(), "\n")
	if want := styleSelected.Render("  " + "  " + wrapURL); raw[3] != want {
		t.Fatalf("selected row's URL line = %q, want it highlighted like the row (%q)", raw[3], want)
	}
}

// TestTableURLTruncatedOnlyAsLastResort: narrower than even the URL's own
// line, the URL is cut with "…" rather than overflowing the terminal.
func TestTableURLTruncatedOnlyAsLastResort(t *testing.T) {
	m := newTestModel([]Server{{Name: "anki", Kind: "http", State: "active", URL: wrapURL}}, nil)
	lines := tableLines(t, m, 30)
	if len(lines) != 3 || !strings.HasSuffix(lines[2], "…") {
		t.Fatalf("expected a truncated URL line, got:\n%s", strings.Join(lines, "\n"))
	}
	for _, l := range lines {
		if w := lipgloss.Width(l); w > 30 {
			t.Fatalf("line exceeds width 30 (%d): %q", w, l)
		}
	}
}

// TestWrappedURLsKeepFooterOnScreen: the extra URL lines (and a notice)
// count against the log pane's budget, so the footer stays the last line
// and the frame never exceeds the terminal height.
func TestWrappedURLsKeepFooterOnScreen(t *testing.T) {
	m := newTestModel([]Server{
		{Name: "a", Kind: "http", State: "active", URL: wrapURL},
		{Name: "b", Kind: "http", State: "active", URL: wrapURL},
		{Name: "c", Kind: "http", State: "active", URL: wrapURL},
	}, nil)
	next, _ := m.Update(namedKey(t, "l"))
	m = next.(Model)
	for i := 0; i < 50; i++ {
		next, _ = m.Update(telemetryEventMsg{line: eventbus.LogLine{Server: "a", Text: "line"}})
		m = next.(Model)
	}
	next, _ = m.Update(copyResultMsg{url: wrapURL})
	m = next.(Model)

	out := renderAt(t, m, 66, 16)
	lines := strings.Split(out, "\n")
	if len(lines) > 16 {
		t.Fatalf("rendered %d lines into a 16-line terminal:\n%s", len(lines), out)
	}
	if got := lines[len(lines)-1]; !strings.Contains(got, "q quit") {
		t.Fatalf("last rendered line = %q, want the footer", got)
	}
	if n := strings.Count(out, wrapURL); n < 3 {
		t.Fatalf("expected all three full URLs on screen, found %d:\n%s", n, out)
	}
}

// TestLogTailVisibleAfterTableGrows: on a 66x20 terminal the table grows
// after the last resize (URLs arrive by snapshot and wrap onto their own
// lines, a notice line appears) while log lines keep coming — the newest
// must stay visible, and the frame must fill the terminal exactly, not 3
// lines short.
func TestLogTailVisibleAfterTableGrows(t *testing.T) {
	m := newTestModel([]Server{
		{Name: "anki", Kind: "http", State: RegistrationPending},
		{Name: "notes", Kind: "http", State: RegistrationPending},
	}, nil)
	next, _ := m.Update(tea.WindowSizeMsg{Width: 66, Height: 20})
	m = next.(Model)
	next, _ = m.Update(namedKey(t, "l"))
	m = next.(Model)
	for i := 0; i < 20; i++ {
		next, _ = m.Update(telemetryEventMsg{line: eventbus.LogLine{Server: "fs", Text: fmt.Sprintf("line-%d", i)}})
		m = next.(Model)
	}
	next, _ = m.Update(SnapshotMsg{Servers: []Server{
		{Name: "anki", Kind: "http", URL: wrapURL, State: RegistrationActive},
		{Name: "notes", Kind: "http", URL: wrapURL, State: RegistrationActive},
	}})
	m = next.(Model)
	for i := 20; i < 40; i++ {
		next, _ = m.Update(telemetryEventMsg{line: eventbus.LogLine{Server: "fs", Text: fmt.Sprintf("line-%d", i)}})
		m = next.(Model)
	}
	next, _ = m.Update(copyResultMsg{url: wrapURL})
	m = next.(Model)

	out := ansi.Strip(m.View().Content)
	lines := strings.Split(out, "\n")
	if len(lines) != 20 {
		t.Fatalf("rendered %d lines into a 20-line terminal, want exactly 20:\n%s", len(lines), out)
	}
	for _, want := range []string{"line-38", "line-39"} {
		if !strings.Contains(out, want) {
			t.Fatalf("newest log line %q not visible:\n%s", want, out)
		}
	}
	if got := lines[len(lines)-1]; !strings.Contains(got, "q quit") {
		t.Fatalf("last rendered line = %q, want the footer", got)
	}

	// A user scrolled back through the log keeps their place when the
	// notice later clears and the pane grows again.
	next, _ = m.Update(namedKey(t, "pgup"))
	m = next.(Model)
	offset := m.logsVP.YOffset()
	next, _ = m.Update(clearNoticeMsg{seq: m.noticeSeq})
	m = next.(Model)
	if m.logsVP.AtBottom() || m.logsVP.YOffset() != offset {
		t.Fatalf("scrolled-back pane moved: offset %d -> %d (atBottom=%v)", offset, m.logsVP.YOffset(), m.logsVP.AtBottom())
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
