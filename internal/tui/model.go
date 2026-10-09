package tui

import (
	"context"
	"errors"
	"fmt"
	"time"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"

	"github.com/mcpwarp/cli/internal/eventbus"
)

// maxLogLines bounds the in-memory log tail ring (DESIGN.md §9: "scrollable
// log tail" — bounded so a chatty child can't grow the dashboard's memory
// without limit).
const maxLogLines = 500

// Model is the bubbletea v2 Model backing `mcpwarp up`'s dashboard. It is
// driven purely by eventbus events plus local key input: construct with
// New, then either hand it to Run or drive it directly in tests via
// Update/View.
type Model struct {
	bus  *eventbus.Bus
	ctrl Controller
	// ctx bounds the listen Cmds' channel receives; Run sets it to its own
	// context so a canceled Run doesn't leak a listener goroutine parked on
	// a bus channel forever (it would otherwise keep consuming events meant
	// for whatever reads that bus next). Defaults to context.Background()
	// so callers that construct a Model directly (tests, or a caller not
	// using Run) don't need to care.
	ctx context.Context

	servers []Server
	cursor  int

	connState   string
	connSession string
	connReason  string

	haveLatency bool
	latencyMs   float64

	haveBytes  bool
	bytesTotal float64

	streamsOpened int
	streamsClosed int

	lastErr     string
	lastErrHint string

	logs        []eventbus.LogLine
	logsVisible bool
	logsVP      viewport.Model

	showHelp bool

	// notice is the transient `c` feedback line (renderNotice); noticeSeq
	// tags each one so a clear tick armed for an older notice doesn't wipe
	// a newer one that replaced it before the tick fired.
	notice    notice
	noticeSeq int

	styles styles

	width  int
	height int

	quitting bool
}

type noticeTone int

const (
	noticeOK noticeTone = iota
	noticeWarn
)

type notice struct {
	tone  noticeTone
	label string
	text  string
}

// noticeDuration is how long a `c` notice stays up before clearNoticeMsg
// removes it. A var only so tests can run the real tick without waiting.
var noticeDuration = 4 * time.Second

type clearNoticeMsg struct{ seq int }

// New constructs a Model seeded with an initial server snapshot. bus must
// be non-nil for Run/Init to subscribe to; ctrl may be nil (r/d/e become
// no-ops), which is convenient for View-only tests.
func New(bus *eventbus.Bus, servers []Server, ctrl Controller) Model {
	rows := make([]Server, len(servers))
	copy(rows, servers)
	vp := viewport.New(viewport.WithWidth(80), viewport.WithHeight(10))
	return Model{
		bus:     bus,
		ctrl:    ctrl,
		ctx:     context.Background(),
		servers: rows,
		logsVP:  vp,
		styles:  newStyles(true),
		width:   80,
		height:  24,
	}
}

// Init subscribes to all three bus channels. Per DESIGN.md §3, Control is
// lossless (a slow consumer backpressures producers) while Telemetry/
// Metrics are drop-oldest — Init treats them identically here: one
// standing listen Cmd per channel, each re-armed after every message it
// delivers, so no producer ever waits on the TUI for longer than its own
// channel's send semantics already allow. It also asks the terminal for its
// background color (bubbletea doesn't on its own), which picks the
// secondary-text gray — see view.go's styles.
func (m Model) Init() tea.Cmd {
	if m.bus == nil {
		return nil
	}
	return tea.Batch(
		listenControl(m.ctx, m.bus),
		listenTelemetry(m.ctx, m.bus),
		listenMetrics(m.ctx, m.bus),
		tea.RequestBackgroundColor,
	)
}

// --- bus subscription messages/commands ---

type controlEventMsg struct{ evt any }
type controlClosedMsg struct{}
type telemetryEventMsg struct{ line eventbus.LogLine }
type telemetryClosedMsg struct{}
type metricEventMsg struct{ sample eventbus.MetricSample }
type metricClosedMsg struct{}

// listenControl, listenTelemetry and listenMetrics each select on ctx.Done()
// alongside the channel receive: without that, the Cmd's blocking receive
// outlives a canceled Run (bubbletea leaks the goroutine running a Cmd that
// hasn't returned yet), and the leaked goroutine keeps competing for events
// on bus with whatever reads it next. Returning the *ClosedMsg on ctx
// cancellation reuses the same "don't re-arm" handling Update already has
// for a genuinely closed bus.
func listenControl(ctx context.Context, bus *eventbus.Bus) tea.Cmd {
	return func() tea.Msg {
		select {
		case <-ctx.Done():
			return controlClosedMsg{}
		case evt, ok := <-bus.Control:
			if !ok {
				return controlClosedMsg{}
			}
			return controlEventMsg{evt: evt}
		}
	}
}

func listenTelemetry(ctx context.Context, bus *eventbus.Bus) tea.Cmd {
	return func() tea.Msg {
		select {
		case <-ctx.Done():
			return telemetryClosedMsg{}
		case line, ok := <-bus.Telemetry:
			if !ok {
				return telemetryClosedMsg{}
			}
			return telemetryEventMsg{line: line}
		}
	}
}

func listenMetrics(ctx context.Context, bus *eventbus.Bus) tea.Cmd {
	return func() tea.Msg {
		select {
		case <-ctx.Done():
			return metricClosedMsg{}
		case sample, ok := <-bus.Metrics:
			if !ok {
				return metricClosedMsg{}
			}
			return metricEventMsg{sample: sample}
		}
	}
}

// Update implements tea.Model.
//
// After every message the log pane's stored height is re-fit to what
// renderDashboard will actually draw: the header and table change height
// on their own (a notice or last-error line appearing, a URL wrapping onto
// its own line), not only on a resize, and a viewport that kept the old,
// taller height would GotoBottom against it and leave the newest log lines
// below the visible pane.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	next, cmd := m.update(msg)
	nm := next.(Model)
	fitLogPane(&nm.logsVP, nm.logPaneHeightBudget())
	return nm, cmd
}

// fitLogPane sets vp's height to h, keeping a pane that was following the
// tail at the bottom; one the user has scrolled back keeps its offset.
func fitLogPane(vp *viewport.Model, h int) {
	if vp.Height() == h {
		return
	}
	wasBottom := vp.AtBottom()
	vp.SetHeight(h)
	if wasBottom {
		vp.GotoBottom()
	}
}

func (m Model) update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.logsVP.SetWidth(logPaneWidth(m.width))
		m.logsVP.SetHeight(m.logPaneHeightBudget())
		m.refreshLogViewport()
		return m, nil

	case tea.KeyPressMsg:
		return m.handleKey(msg)

	case controlEventMsg:
		m.applyControl(msg.evt)
		return m, listenControl(m.ctx, m.bus)
	case controlClosedMsg:
		// Control only closes when up.go's shutdown handlers call Bus.Close (or
		// ctx is cancelled by a signal): either way a shutdown is under way, so
		// quit to restore the terminal and unblock that handler's wait on uiDone.
		// quitting stays false — the shutdown isn't ours to start.
		return m, tea.Quit

	case telemetryEventMsg:
		m.appendLog(msg.line)
		return m, listenTelemetry(m.ctx, m.bus)
	case telemetryClosedMsg:
		return m, nil

	case metricEventMsg:
		m.applyMetric(msg.sample)
		return m, listenMetrics(m.ctx, m.bus)
	case metricClosedMsg:
		return m, nil

	case SnapshotMsg:
		m.applySnapshot(msg.Servers)
		return m, nil

	case tea.BackgroundColorMsg:
		m.styles = newStyles(msg.IsDark())
		// Log-line prefixes are baked into the viewport's content when it's
		// set, so re-render it in the new gray — without GotoBottom, so a
		// user scrolled back through the log stays where they are.
		m.logsVP.SetContent(renderLogLines(m.logs, m.styles.dim))
		return m, nil

	case copyResultMsg:
		// OSC 52 went out regardless (copyURLCmd). No native tool at all is
		// the normal SSH case, where OSC 52 is the path that matters, so it
		// reads as a plain success naming that path; there's no way to know
		// whether the terminal honored it, hence "terminal clipboard". A
		// tool that exists but failed is worth a warning with its name.
		switch {
		case msg.err == nil:
			return m, m.setNotice(noticeOK, "copied", msg.url+" to clipboard")
		case errors.Is(msg.err, errNoNativeClipboard):
			return m, m.setNotice(noticeOK, "copied", msg.url+" via OSC 52 (terminal clipboard)")
		default:
			return m, m.setNotice(noticeWarn, "sent", fmt.Sprintf("%s via OSC 52 only (%v)", msg.url, msg.err))
		}

	case clearNoticeMsg:
		if msg.seq == m.noticeSeq {
			m.notice = notice{}
		}
		return m, nil
	}

	return m, nil
}

// setNotice shows a transient notice line and returns the tick that clears
// it after noticeDuration.
func (m *Model) setNotice(tone noticeTone, label, text string) tea.Cmd {
	m.noticeSeq++
	m.notice = notice{tone: tone, label: label, text: text}
	seq := m.noticeSeq
	return tea.Tick(noticeDuration, func(time.Time) tea.Msg { return clearNoticeMsg{seq: seq} })
}

// copySelectedURL is `c`: copy the selected row's full public URL (a very
// narrow terminal still cuts it short in the table), or say why there's
// nothing to copy. A row has a URL only once the tunnel has registered it;
// a disabled row keeps the URL it was given, since that link is stable and
// works again on `e`.
func (m *Model) copySelectedURL() tea.Cmd {
	if m.cursor < 0 || m.cursor >= len(m.servers) {
		return nil
	}
	s := m.servers[m.cursor]
	if s.URL == "" {
		why := "has no public URL yet"
		if displayState(s) == RegistrationRejected {
			why = "has no public URL: the tunnel rejected it"
		}
		return m.setNotice(noticeWarn, "nothing to copy:", s.Name+" "+why)
	}
	return copyURLCmd(s.URL)
}

func (m *Model) handleKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	key := msg.String()

	if m.showHelp {
		// Any key dismisses the help overlay (a simple full-screen
		// overlay rather than a compositing layer — DESIGN.md §9 doesn't
		// specify overlay mechanics beyond "?  help overlay").
		if key == "q" || key == "ctrl+c" {
			m.quitting = true
			return *m, tea.Quit
		}
		m.showHelp = false
		return *m, nil
	}

	switch key {
	case "q", "ctrl+c":
		m.quitting = true
		return *m, tea.Quit
	case "?":
		m.showHelp = true
	case "up", "k":
		if m.cursor > 0 {
			m.cursor--
		}
	case "down", "j":
		if m.cursor < len(m.servers)-1 {
			m.cursor++
		}
	case "l":
		m.logsVisible = !m.logsVisible
		m.refreshLogViewport()
	case "c":
		return *m, m.copySelectedURL()
	case "pgup":
		if m.logsVisible {
			m.logsVP.PageUp()
		}
	case "pgdown":
		if m.logsVisible {
			m.logsVP.PageDown()
		}
	case "ctrl+u":
		if m.logsVisible {
			m.logsVP.HalfPageUp()
		}
	case "ctrl+d":
		if m.logsVisible {
			m.logsVP.HalfPageDown()
		}
	case "r":
		if name, ok := m.selectedName(); ok && m.ctrl != nil {
			ctrl := m.ctrl
			return *m, func() tea.Msg { ctrl.Restart(name); return nil }
		}
	case "d":
		if name, ok := m.selectedName(); ok && m.ctrl != nil {
			ctrl := m.ctrl
			return *m, func() tea.Msg { ctrl.Disable(name); return nil }
		}
	case "e":
		if name, ok := m.selectedName(); ok && m.ctrl != nil {
			ctrl := m.ctrl
			return *m, func() tea.Msg { ctrl.Enable(name); return nil }
		}
	}
	return *m, nil
}

func (m *Model) selectedName() (string, bool) {
	if m.cursor < 0 || m.cursor >= len(m.servers) {
		return "", false
	}
	return m.servers[m.cursor].Name, true
}

func (m *Model) applyControl(evt any) {
	switch e := evt.(type) {
	case eventbus.ConnStateChanged:
		m.connState = e.State
		m.connSession = e.Session
		m.connReason = e.Reason
	case eventbus.ServerStateChanged:
		m.upsertServerState(e.Name, e.State, e.Restarts)
	case eventbus.StreamOpened:
		m.streamsOpened++
	case eventbus.StreamClosed:
		m.streamsClosed++
	case eventbus.AppError:
		if e.Service != "" {
			m.lastErr = fmt.Sprintf("[%s] %s (service=%s)", e.Code, e.Message, e.Service)
		} else {
			m.lastErr = fmt.Sprintf("[%s] %s", e.Code, e.Message)
		}
		m.lastErrHint = e.Hint
	}
}

func (m *Model) applyMetric(sample eventbus.MetricSample) {
	switch sample.Kind {
	case "ping_rtt":
		m.haveLatency = true
		m.latencyMs = sample.Value
	case "bytes_total":
		m.haveBytes = true
		m.bytesTotal = sample.Value
	}
}

func (m *Model) upsertServerState(name, state string, restarts int) {
	for i := range m.servers {
		if m.servers[i].Name == name {
			m.servers[i].State = state
			m.servers[i].Restarts = restarts
			return
		}
	}
	m.servers = append(m.servers, Server{Name: name, State: state, Restarts: restarts})
	m.clampCursor()
}

func (m *Model) applySnapshot(snap []Server) {
	byName := make(map[string]int, len(m.servers))
	for i, s := range m.servers {
		byName[s.Name] = i
	}
	for _, s := range snap {
		if i, ok := byName[s.Name]; ok {
			m.servers[i].Kind = s.Kind
			m.servers[i].URL = s.URL
			// Empty State means the snapshot has nothing to say about it
			// (e.g. a stdio row, whose state is owned by
			// eventbus.ServerStateChanged) — leave whatever's there.
			if s.State != "" {
				m.servers[i].State = s.State
			}
			if s.Registration != "" {
				m.servers[i].Registration = s.Registration
			}
		} else {
			m.servers = append(m.servers, s)
			byName[s.Name] = len(m.servers) - 1
		}
	}
	m.clampCursor()
}

func (m *Model) clampCursor() {
	if m.cursor >= len(m.servers) {
		m.cursor = len(m.servers) - 1
	}
	if m.cursor < 0 {
		m.cursor = 0
	}
}

func (m *Model) appendLog(line eventbus.LogLine) {
	m.logs = append(m.logs, line)
	if len(m.logs) > maxLogLines {
		m.logs = m.logs[len(m.logs)-maxLogLines:]
	}
	if m.logsVisible {
		m.refreshLogViewport()
	}
}

func (m *Model) refreshLogViewport() {
	m.logsVP.SetContent(renderLogLines(m.logs, m.styles.dim))
	m.logsVP.GotoBottom()
}
