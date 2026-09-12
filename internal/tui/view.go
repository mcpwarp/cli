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
	colorDim  = lipgloss.Color("8") // ANSI bright black
	colorAcc  = lipgloss.Color("6") // ANSI cyan, selection highlight

	styleBold     = lipgloss.NewStyle().Bold(true)
	styleDim      = lipgloss.NewStyle().Foreground(colorDim)
	styleGood     = lipgloss.NewStyle().Foreground(colorGood)
	styleWarn     = lipgloss.NewStyle().Foreground(colorWarn)
	styleBad      = lipgloss.NewStyle().Foreground(colorBad).Bold(true)
	styleHeader   = lipgloss.NewStyle().Bold(true).Underline(true)
	styleSelected = lipgloss.NewStyle().Foreground(colorAcc).Bold(true)
	stylePane     = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).Padding(0, 1)
	styleFooter   = lipgloss.NewStyle().Foreground(colorDim)
)

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
	footer := renderFooter()

	sections := []string{header, table}
	if m.logsVisible {
		if avail := m.logPaneHeightBudget(); avail > 0 {
			logs := m.logsVP
			logs.SetHeight(avail)
			sections = append(sections, logs.View())
		}
	}
	sections = append(sections, footer)
	return strings.Join(sections, "\n")
}

func countLines(s string) int {
	return strings.Count(s, "\n") + 1
}

// logPaneHeightBudget is the log pane's height once header/table/footer and
// their joining newlines are accounted for, so the footer is never clipped.
// It returns 0 (not shown) rather than clamping up to a minimum once the
// terminal is too small to fit a useful pane — logPaneHeightBudget's own
// caller only shows the pane once this is >= 3.
func (m Model) logPaneHeightBudget() int {
	fixedLines := countLines(m.renderHeader()) + countLines(m.renderTable()) + countLines(renderFooter())
	const joins = 3 // header/table/logs/footer joined by "\n" between each
	avail := m.height - fixedLines - joins
	if avail < 3 {
		return 0
	}
	return avail
}

func (m Model) renderHeader() string {
	stateStyle := styleDim
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

	line1 := styleBold.Render("mcpwarp up") + "  " + strings.Join(parts, "  ")

	line2 := ""
	if m.lastErr != "" {
		line2 = styleBad.Render("last error: ") + m.lastErr
	}
	if line2 == "" {
		return line1
	}
	return line1 + "\n" + line2
}

var tableCols = []string{"NAME", "KIND", "STATE", "RESTARTS", "URL"}

// displayState maps a stored State value to what the STATE column shows:
// the supervisor's "healthy" and an http row's registry-derived "active"
// are the same fact from a user's point of view, so both display as "active" —
// the model itself keeps whichever value it was given (tests that assert
// on stored state are unaffected).
func displayState(s string) string {
	if s == "healthy" {
		return "active"
	}
	return s
}

func (m Model) renderTable() string {
	if len(m.servers) == 0 {
		return styleDim.Render("(no servers configured)")
	}

	nameW, kindW, stateW, restartsW := lipgloss.Width(tableCols[0]), lipgloss.Width(tableCols[1]), lipgloss.Width(tableCols[2]), lipgloss.Width(tableCols[3])
	for _, s := range m.servers {
		nameW = maxInt(nameW, lipgloss.Width(s.Name))
		kindW = maxInt(kindW, lipgloss.Width(s.Kind))
		stateW = maxInt(stateW, lipgloss.Width(displayState(s.State)))
		restartsW = maxInt(restartsW, lipgloss.Width(strconv.Itoa(s.Restarts)))
	}

	var b strings.Builder
	header := formatRow(nameW, kindW, stateW, restartsW,
		tableCols[0], tableCols[1], tableCols[2], tableCols[3], tableCols[4])
	b.WriteString(styleHeader.Render(truncateWidth(header, m.width)))
	// Each row gets a 2-column "> "/"  " prefix, so truncate to m.width-2 —
	// truncating to the full width first would let the prefix push the row
	// past m.width.
	const rowPrefixWidth = 2
	for i, s := range m.servers {
		row := formatRow(nameW, kindW, stateW, restartsW,
			s.Name, s.Kind, displayState(s.State), strconv.Itoa(s.Restarts), s.URL)
		row = truncateWidth(row, m.width-rowPrefixWidth)
		b.WriteString("\n")
		if i == m.cursor {
			b.WriteString(styleSelected.Render("> " + row))
		} else {
			b.WriteString("  " + row)
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

func renderFooter() string {
	hint := "q quit  ? help  ↑/↓ or j/k select  l logs  r restart  d disable  e enable"
	return styleFooter.Render(hint)
}

func (m Model) renderHelp() string {
	lines := []string{
		styleBold.Render("mcpwarp up — keybindings"),
		"",
		"  q          quit",
		"  ?          toggle this help",
		"  ↑/k, ↓/j   move selection",
		"  l          toggle log pane",
		"  r          restart selected server",
		"  d          disable selected server",
		"  e          enable selected server",
		"",
		styleDim.Render("press any key to return"),
	}
	return stylePane.Render(strings.Join(lines, "\n"))
}

func renderLogLines(lines []eventbus.LogLine) string {
	var b strings.Builder
	for i, l := range lines {
		if i > 0 {
			b.WriteString("\n")
		}
		prefix := l.Server
		if l.Level != "" {
			prefix += "/" + l.Level
		}
		b.WriteString(styleDim.Render("["+prefix+"] ") + l.Text)
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
