package tui

import (
	"fmt"
	"strconv"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/mcpwarp/cli/internal/eventbus"
)

// Restrained palette (DESIGN.md §9: "no rainbow") — a handful of semantic
// colors, reused everywhere rather than one-off styles per widget.
var (
	colorGood = lipgloss.Color("2") // ANSI green
	colorWarn = lipgloss.Color("3") // ANSI yellow
	colorBad  = lipgloss.Color("1") // ANSI red
	colorAcc  = lipgloss.Color("6") // ANSI cyan, selection highlight

	styleBold     = lipgloss.NewStyle().Bold(true)
	styleGood     = lipgloss.NewStyle().Foreground(colorGood)
	styleWarn     = lipgloss.NewStyle().Foreground(colorWarn)
	styleBad      = lipgloss.NewStyle().Foreground(colorBad).Bold(true)
	styleHeader   = lipgloss.NewStyle().Bold(true).Underline(true)
	styleSelected = lipgloss.NewStyle().Foreground(colorAcc).Bold(true)
	stylePane     = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).Padding(0, 1)
)

// styles holds the background-dependent styles. Secondary text (the footer,
// the last-error hint, log-line prefixes) is deliberately not ANSI 8
// "bright black": the terminal theme defines that color, and it is
// near-invisible on a mid-gray background like #3c3d42 (~1.9:1) though fine
// on near-black. Instead the secondary gray is an explicit color picked per
// background from the terminal's answer to tea.RequestBackgroundColor
// (Init): #a8a8a8 on dark
// (about 4.5:1 on #3c3d42, 7:1 on #1a1c20), #626262 on light (about 6:1 on
// white). Both are exact xterm-256 grays (248/241), so a 256-color terminal
// gets them unchanged; a 16-color one is downsampled by lipgloss to ANSI 7/8.
// Footer keys don't use the gray at all: they're bold in the terminal's own
// default foreground, the one color guaranteed to read on its background.
// Until the reply arrives (or if the terminal never answers) the dark
// variant is used, matching what most terminals run.
type styles struct {
	dim     lipgloss.Style
	footKey lipgloss.Style
}

func newStyles(darkBG bool) styles {
	gray := lipgloss.LightDark(darkBG)(lipgloss.Color("#626262"), lipgloss.Color("#a8a8a8"))
	return styles{
		dim:     lipgloss.NewStyle().Foreground(gray),
		footKey: lipgloss.NewStyle().Bold(true),
	}
}

// View implements tea.Model. It degrades gracefully at small sizes: below
// minWidth/minHeight it renders only the header and a size warning rather
// than a garbled table.
const minWidth = 20
const minHeight = 6

// View implements tea.Model. AltScreen is set on every returned tea.View —
// bubbletea's renderer toggles alt-screen mode whenever a frame's AltScreen
// differs from the previous frame's (cursed_renderer.go), so leaving it
// unset on the help/too-small branches would exit the alt screen (and
// flicker back in on the next normal frame) every time help opens or the
// terminal gets small.
func (m Model) View() tea.View {
	var content string
	switch {
	case m.showHelp:
		content = m.renderHelp()
	case m.width < minWidth || m.height < minHeight:
		content = fmt.Sprintf("mcpwarp up\nterminal too small (%dx%d) — resize", m.width, m.height)
	default:
		content = m.renderDashboard()
	}
	view := tea.NewView(content)
	view.AltScreen = true
	return view
}

// renderDashboard lays out header/table/[logs]/footer, giving the log pane
// (when visible) whatever height remains after the other three sections so
// the footer is never pushed off the bottom of the terminal.
func (m Model) renderDashboard() string {
	header := m.renderHeader()
	table := m.renderTable()
	footer := m.renderFooter()

	sections := []string{header, table}
	if m.logsVisible {
		if avail := m.logPaneHeightBudget(); avail > 0 {
			// Update already fits the stored viewport to this height; this
			// only matters for a View before any Update (New's 80x24 seed).
			logs := m.logsVP
			fitLogPane(&logs, avail)
			sections = append(sections, logs.View())
		}
	}
	sections = append(sections, footer)
	return strings.Join(sections, "\n")
}

func countLines(s string) int {
	return strings.Count(s, "\n") + 1
}

// logPaneHeightBudget is the log pane's height once header/table/footer are
// accounted for, so the footer is never clipped. The "\n" renderDashboard
// joins sections with ends one section's last line rather than adding a
// line of its own, so the budget is the terminal height minus exactly those
// sections' line counts. It counts the rendered lines rather than rows, so
// a transient notice line or a row whose URL wrapped onto its own line
// (renderTable) shrinks the pane instead of pushing the footer off-screen.
// It returns 0 (not shown) rather than clamping up to a minimum once the
// terminal is too small to fit a useful pane — logPaneHeightBudget's own
// caller only shows the pane once this is >= 3.
func (m Model) logPaneHeightBudget() int {
	fixedLines := countLines(m.renderHeader()) + countLines(m.renderTable()) + countLines(m.renderFooter())
	avail := m.height - fixedLines
	if avail < 3 {
		return 0
	}
	return avail
}

func (m Model) renderHeader() string {
	stateStyle := m.styles.dim
	switch m.connState {
	case "connected", "healthy":
		stateStyle = styleGood
	case "disconnected", "reconnecting":
		stateStyle = styleWarn
	case "closed", "error":
		stateStyle = styleBad
	}
	state := m.connState
	if state == "" {
		state = "unknown"
	}

	var parts []string
	parts = append(parts, stateStyle.Render(strings.ToUpper(state)))
	if m.connSession != "" {
		parts = append(parts, "session="+m.connSession)
	}
	if m.connReason != "" {
		parts = append(parts, "reason="+m.connReason)
	}
	if m.haveLatency {
		parts = append(parts, fmt.Sprintf("latency=%dms", int64(m.latencyMs)))
	}
	streams := m.streamsOpened - m.streamsClosed
	if streams < 0 {
		streams = 0
	}
	parts = append(parts, fmt.Sprintf("streams=%d", streams))
	if m.haveBytes {
		parts = append(parts, fmt.Sprintf("bytes=%.0f", m.bytesTotal))
	}

	// Truncated like every other line: logPaneHeightBudget's exact line
	// count assumes no line wraps.
	lines := []string{truncateWidth(styleBold.Render("mcpwarp up")+"  "+strings.Join(parts, "  "), m.width)}

	if m.lastErr != "" {
		const label = "last error: "
		lines = append(lines, styleBad.Render(label)+truncateWidth(m.lastErr, m.width-lipgloss.Width(label)))
		if m.lastErrHint != "" {
			lines = append(lines, m.styles.dim.Render(truncateWidth(m.lastErrHint, m.width)))
		}
	}
	// The notice gets its own line rather than replacing the last-error
	// pair, so a `c` press never hides an error, even for a few seconds.
	if line := m.renderNotice(); line != "" {
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

// renderNotice is the transient `c` feedback line: a colored label (green
// "copied", yellow otherwise) and the rest in bold default foreground, so it
// stands out on any background instead of reading as secondary gray text.
func (m Model) renderNotice() string {
	if m.notice.text == "" {
		return ""
	}
	labelStyle := styleGood.Bold(true)
	if m.notice.tone == noticeWarn {
		labelStyle = styleWarn.Bold(true)
	}
	label := m.notice.label + " "
	return labelStyle.Render(label) + styleBold.Render(truncateWidth(m.notice.text, m.width-lipgloss.Width(label)))
}

var tableCols = []string{"NAME", "KIND", "STATE", "RESTARTS", "URL"}

// displayState maps a row to what the STATE column shows. The supervisor's
// "healthy" and an http row's registry-derived "active" are the same fact
// from a user's point of view, so both display as "active" — but only once
// the tunnel agrees: a healthy stdio row whose registration is still
// pending, was rejected, or is disabled shows that instead, since a healthy
// local process with no public URL isn't "active" to anyone. Any other
// supervisor state (restarting, failed, ...) wins over the registration,
// keeping a local problem visible. The model itself keeps whichever values
// it was given (tests that assert on stored state are unaffected).
func displayState(s Server) string {
	if s.State != "healthy" {
		return s.State
	}
	switch s.Registration {
	case "", RegistrationActive:
		return RegistrationActive
	default:
		return s.Registration
	}
}

// renderTable draws one line per server, plus a second line for any row
// whose URL doesn't fit in the URL column at the current width: that URL
// moves under the row, indented past the selection marker, so the whole
// link stays visible and mouse-selectable instead of ending in "…". Only
// such rows grow; at a width where every URL fits the table is one line per
// row. A URL wider than even its own line is truncated as a last resort.
func (m Model) renderTable() string {
	if len(m.servers) == 0 {
		return m.styles.dim.Render("(no servers configured)")
	}

	nameW, kindW, stateW, restartsW := lipgloss.Width(tableCols[0]), lipgloss.Width(tableCols[1]), lipgloss.Width(tableCols[2]), lipgloss.Width(tableCols[3])
	for _, s := range m.servers {
		nameW = maxInt(nameW, lipgloss.Width(s.Name))
		kindW = maxInt(kindW, lipgloss.Width(s.Kind))
		stateW = maxInt(stateW, lipgloss.Width(displayState(s)))
		restartsW = maxInt(restartsW, lipgloss.Width(strconv.Itoa(s.Restarts)))
	}

	var b strings.Builder
	header := formatRow(nameW, kindW, stateW, restartsW,
		tableCols[0], tableCols[1], tableCols[2], tableCols[3], tableCols[4])
	b.WriteString(styleHeader.Render(truncateWidth(header, m.width)))
	// Each row gets a 2-column "> "/"  " prefix, so the row itself has
	// m.width-2 columns to fit in.
	const rowPrefixWidth = 2
	// urlIndent sets a wrapped URL line off from the NAME column it sits
	// under, so it reads as belonging to the row above.
	const urlIndent = 2
	rowWidth := m.width - rowPrefixWidth
	for i, s := range m.servers {
		state := displayState(s)
		restarts := strconv.Itoa(s.Restarts)
		url := s.URL
		urlLine := ""
		if url != "" && lipgloss.Width(formatRow(nameW, kindW, stateW, restartsW, s.Name, s.Kind, state, restarts, url)) > rowWidth {
			urlLine = truncateWidth(strings.Repeat(" ", urlIndent)+url, rowWidth)
			url = ""
		}

		rowStyle, prefix := lipgloss.NewStyle(), "  "
		if i == m.cursor {
			rowStyle, prefix = styleSelected, "> "
		}
		// The STATE cell is rendered on its own so "rejected" can carry the
		// error style without its reset cutting a selected row's highlight
		// short for the cells after it.
		stateStyle := rowStyle
		if state == RegistrationRejected {
			stateStyle = styleBad
		}
		row := rowStyle.Render(prefix+padRight(s.Name, nameW)+"  "+padRight(s.Kind, kindW)+"  ") +
			stateStyle.Render(padRight(state, stateW)) +
			rowStyle.Render(strings.TrimRight("  "+padRight(restarts, restartsW)+"  "+url, " "))
		b.WriteString("\n")
		b.WriteString(truncateWidth(row, m.width))
		if urlLine != "" {
			b.WriteString("\n")
			b.WriteString(rowStyle.Render(strings.Repeat(" ", rowPrefixWidth) + urlLine))
		}
	}
	return b.String()
}

func formatRow(nameW, kindW, stateW, restartsW int, name, kind, state, restarts, url string) string {
	return padRight(name, nameW) + "  " + padRight(kind, kindW) + "  " +
		padRight(state, stateW) + "  " + padRight(restarts, restartsW) + "  " + url
}

// padRight and truncateWidth use lipgloss.Width/ansi.Truncate rather than
// len/byte-slicing so a unicode server name or URL is measured and cut by
// display column, not by UTF-8 byte count.
func padRight(s string, w int) string {
	sw := lipgloss.Width(s)
	if sw >= w {
		return s
	}
	return s + strings.Repeat(" ", w-sw)
}

func truncateWidth(s string, w int) string {
	if w <= 0 {
		return ""
	}
	if lipgloss.Width(s) <= w {
		return s
	}
	return ansi.Truncate(s, w, "…")
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// footerKeys is the footer hint, key then description; at 80 columns it
// fits exactly, and narrower terminals truncate it rather than wrap.
var footerKeys = [][2]string{
	{"q", "quit"},
	{"?", "help"},
	{"↑↓/jk", "select"},
	{"l", "logs"},
	{"c", "copy URL"},
	{"r", "restart"},
	{"d", "disable"},
	{"e", "enable"},
}

func (m Model) renderFooter() string {
	items := make([]string, len(footerKeys))
	for i, kv := range footerKeys {
		items[i] = m.styles.footKey.Render(kv[0]) + " " + m.styles.dim.Render(kv[1])
	}
	return truncateWidth(strings.Join(items, "  "), m.width)
}

func (m Model) renderHelp() string {
	lines := []string{
		styleBold.Render("mcpwarp up — keybindings"),
		"",
		"  q          quit",
		"  ?          toggle this help",
		"  ↑/k, ↓/j   move selection",
		"  l          toggle log pane",
		"  c          copy selected server's public URL",
		"  r          restart selected server",
		"  d          disable selected server",
		"  e          enable selected server",
		"",
		m.styles.dim.Render("press any key to return"),
	}
	return stylePane.Render(strings.Join(lines, "\n"))
}

func renderLogLines(lines []eventbus.LogLine, dim lipgloss.Style) string {
	var b strings.Builder
	for i, l := range lines {
		if i > 0 {
			b.WriteString("\n")
		}
		prefix := l.Server
		if l.Level != "" {
			prefix += "/" + l.Level
		}
		b.WriteString(dim.Render("["+prefix+"] ") + l.Text)
	}
	return b.String()
}

func logPaneWidth(width int) int {
	w := width - 4
	if w < 10 {
		w = 10
	}
	return w
}
