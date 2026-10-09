package tui

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
)

// fakeClipboard replaces the clipboardWrite seam for one test, so no test
// ever touches the real clipboard, and records what it was asked to write.
type fakeClipboard struct {
	mu     sync.Mutex
	writes []string
	err    error
}

func useFakeClipboard(t *testing.T, err error) *fakeClipboard {
	t.Helper()
	f := &fakeClipboard{err: err}
	orig := clipboardWrite
	clipboardWrite = func(_ context.Context, text string) error {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.writes = append(f.writes, text)
		return f.err
	}
	t.Cleanup(func() { clipboardWrite = orig })
	return f
}

func (f *fakeClipboard) got() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.writes...)
}

// shortNotice makes the notice tick fire right away so a test can run the
// real tea.Tick Cmd instead of hand-building its message.
func shortNotice(t *testing.T) {
	t.Helper()
	orig := noticeDuration
	noticeDuration = time.Millisecond
	t.Cleanup(func() { noticeDuration = orig })
}

// runCmd executes cmd, flattening a tea.BatchMsg into its members' results.
func runCmd(t *testing.T, cmd tea.Cmd) []tea.Msg {
	t.Helper()
	if cmd == nil {
		return nil
	}
	msg := cmd()
	batch, ok := msg.(tea.BatchMsg)
	if !ok {
		return []tea.Msg{msg}
	}
	var msgs []tea.Msg
	for _, c := range batch {
		msgs = append(msgs, runCmd(t, c)...)
	}
	return msgs
}

// isOSC52 reports whether msg is bubbletea's own SetClipboard message for
// want. Its type is unexported (tea.setClipboardMsg, a string), so it's
// matched by type name and value.
func isOSC52(msg tea.Msg, want string) bool {
	return fmt.Sprintf("%T", msg) == "tea.setClipboardMsg" && fmt.Sprint(msg) == want
}

const longURL = "https://anki-anatoly.tunnel.dev.mcpwarp.io/mcp/with/a/path/that/does/not/fit/in/the/column"

// TestCopyKeyCopiesFullURLAndConfirms: `c` on a row with a URL sends both
// the OSC 52 write and the native write with the full (untruncated) URL,
// and the native result shows a high-contrast "copied ... to clipboard"
// notice that the tick then clears.
func TestCopyKeyCopiesFullURLAndConfirms(t *testing.T) {
	clip := useFakeClipboard(t, nil)
	shortNotice(t)
	m := newTestModel([]Server{
		{Name: "fs", Kind: "stdio", State: "healthy", URL: "https://fs.example/mcp"},
		{Name: "anki", Kind: "http", State: "active", URL: longURL},
	}, nil)
	next, _ := m.Update(namedKey(t, "j"))
	m = next.(Model)

	next, cmd := m.Update(namedKey(t, "c"))
	m = next.(Model)
	if cmd == nil {
		t.Fatal("c on a row with a URL returned no Cmd")
	}
	var sawOSC52 bool
	var result *copyResultMsg
	for _, msg := range runCmd(t, cmd) {
		switch msg := msg.(type) {
		case copyResultMsg:
			result = &msg
		default:
			if isOSC52(msg, longURL) {
				sawOSC52 = true
			}
		}
	}
	if !sawOSC52 {
		t.Fatalf("expected a tea.SetClipboard (OSC 52) message carrying the full URL")
	}
	if result == nil || result.url != longURL || result.err != nil {
		t.Fatalf("native copy result = %+v, want the full URL and no error", result)
	}
	if got := clip.got(); len(got) != 1 || got[0] != longURL {
		t.Fatalf("native clipboard writes = %q, want exactly [%q]", got, longURL)
	}

	next, tick := m.Update(*result)
	m = next.(Model)
	out := renderAt(t, m, 200, 24)
	if !strings.Contains(out, "copied "+longURL+" to clipboard") {
		t.Fatalf("expected the copied notice with the full URL; got:\n%s", out)
	}

	msgs := runCmd(t, tick)
	if len(msgs) != 1 {
		t.Fatalf("expected the notice tick to yield one message, got %v", msgs)
	}
	next, _ = m.Update(msgs[0])
	m = next.(Model)
	if out := renderAt(t, m, 200, 24); strings.Contains(out, "copied") {
		t.Fatalf("notice should be gone after its tick; got:\n%s", out)
	}
}

// TestCopyKeyWithoutURLCopiesNothing: `c` on a pending or rejected row
// shows "nothing to copy" and returns only the clearing tick — no OSC 52,
// no native write.
func TestCopyKeyWithoutURLCopiesNothing(t *testing.T) {
	for _, tc := range []struct {
		name, state, want string
	}{
		{"pending", RegistrationPending, "nothing to copy: deepwiki has no public URL yet"},
		{"rejected", RegistrationRejected, "nothing to copy: deepwiki has no public URL: the tunnel rejected it"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clip := useFakeClipboard(t, nil)
			shortNotice(t)
			m := newTestModel([]Server{{Name: "deepwiki", Kind: "http", State: tc.state}}, nil)

			next, cmd := m.Update(namedKey(t, "c"))
			m = next.(Model)
			if out := renderAt(t, m, 120, 24); !strings.Contains(out, tc.want) {
				t.Fatalf("expected %q; got:\n%s", tc.want, out)
			}
			msgs := runCmd(t, cmd)
			if len(msgs) != 1 {
				t.Fatalf("expected only the notice tick, got %d messages: %v", len(msgs), msgs)
			}
			if _, ok := msgs[0].(clearNoticeMsg); !ok {
				t.Fatalf("expected clearNoticeMsg, got %T", msgs[0])
			}
			if got := clip.got(); len(got) != 0 {
				t.Fatalf("nothing should reach the clipboard, got %q", got)
			}
			next, _ = m.Update(msgs[0])
			m = next.(Model)
			if out := renderAt(t, m, 120, 24); strings.Contains(out, "nothing to copy") {
				t.Fatalf("notice should be gone after its tick; got:\n%s", out)
			}
		})
	}
}

// TestCopyWithoutNativeToolSaysOSC52: no native tool at all (the usual SSH
// case) reads as a neutral success naming the OSC 52 path, with no error
// suffix; a tool that exists but fails is a warning naming that tool.
func TestCopyWithoutNativeToolSaysOSC52(t *testing.T) {
	for _, tc := range []struct {
		name, want, notWant string
		err                 error
	}{
		{"no tool", "copied https://fs.example/mcp via OSC 52 (terminal clipboard)", "no clipboard tool", errNoNativeClipboard},
		{"tool failed", "sent https://fs.example/mcp via OSC 52 only (xclip: exit status 1)", "copied", errors.New("xclip: exit status 1")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useFakeClipboard(t, tc.err)
			m := newTestModel([]Server{{Name: "fs", State: "healthy", URL: "https://fs.example/mcp"}}, nil)
			_, cmd := m.Update(namedKey(t, "c"))
			var result copyResultMsg
			for _, msg := range runCmd(t, cmd) {
				if r, ok := msg.(copyResultMsg); ok {
					result = r
				}
			}
			if !errors.Is(result.err, tc.err) {
				t.Fatalf("copy result err = %v, want %v", result.err, tc.err)
			}
			next, _ := m.Update(result)
			m = next.(Model)
			out := renderAt(t, m, 200, 24)
			if !strings.Contains(out, tc.want) || strings.Contains(out, tc.notWant) {
				t.Fatalf("want %q (and no %q); got:\n%s", tc.want, tc.notWant, out)
			}
		})
	}
}

// TestClipboardCommandsByEnvironment: on Linux the Wayland and X11 tools
// are offered only when their display variable is set, so an SSH session
// with neither spawns nothing; macOS and Windows ignore both.
func TestClipboardCommandsByEnvironment(t *testing.T) {
	names := func(cmds [][]string) []string {
		var out []string
		for _, c := range cmds {
			out = append(out, c[0])
		}
		return out
	}
	cases := []struct {
		name, goos, wayland, display string
		want                         []string
	}{
		{"linux wayland only", "linux", "wayland-0", "", []string{"wl-copy"}},
		{"linux x11 only", "linux", "", ":0", []string{"xclip", "xsel"}},
		{"linux both", "linux", "wayland-0", ":0", []string{"wl-copy", "xclip", "xsel"}},
		{"linux neither (ssh)", "linux", "", "", nil},
		{"darwin", "darwin", "wayland-0", ":0", []string{"pbcopy"}},
		{"windows", "windows", "", "", []string{"clip.exe"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("WAYLAND_DISPLAY", tc.wayland)
			t.Setenv("DISPLAY", tc.display)
			if got := names(clipboardCommands(tc.goos)); strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("clipboardCommands(%q) = %v, want %v", tc.goos, got, tc.want)
			}
		})
	}
}

// TestStaleNoticeTickKeepsNewerNotice: a second notice replaces the first
// before the first's tick fires; that stale tick must not clear it.
func TestStaleNoticeTickKeepsNewerNotice(t *testing.T) {
	m := newTestModel([]Server{{Name: "fs", State: "healthy", URL: "https://fs.example/mcp"}}, nil)
	next, _ := m.Update(copyResultMsg{url: "https://first.example"})
	m = next.(Model)
	stale := clearNoticeMsg{seq: m.noticeSeq}
	next, _ = m.Update(copyResultMsg{url: "https://second.example"})
	m = next.(Model)
	next, _ = m.Update(stale)
	m = next.(Model)
	if out := renderAt(t, m, 120, 24); !strings.Contains(out, "copied https://second.example to clipboard") {
		t.Fatalf("a stale tick cleared the newer notice; got:\n%s", out)
	}
}

// TestNoticeDoesNotHideLastError: the notice is an extra header line, so
// the last-error line and its hint stay visible while it's up.
func TestNoticeDoesNotHideLastError(t *testing.T) {
	m := newTestModel([]Server{{Name: "deepwiki", Kind: "http", State: RegistrationRejected}}, nil)
	next, _ := m.Update(controlEventMsg{evt: errQuota})
	m = next.(Model)
	next, _ = m.Update(namedKey(t, "c"))
	m = next.(Model)
	out := ansi.Strip(m.renderHeader())
	for _, want := range []string{"last error: [QUOTA_EXCEEDED]", "upgrade your plan", "nothing to copy: deepwiki"} {
		if !strings.Contains(out, want) {
			t.Fatalf("header missing %q; got:\n%s", want, out)
		}
	}
}
