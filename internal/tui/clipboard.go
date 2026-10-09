package tui

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
)

// `c` copies the selected row's public URL (DESIGN.md §9) two ways at once,
// because neither reaches every terminal on its own:
//
//   - OSC 52 (tea.SetClipboard) asks the terminal itself to set the
//     clipboard, which is the only path that works over SSH and inside tmux
//     (with set-clipboard on) — but macOS Terminal.app ignores it entirely.
//   - The native clipboard tool (pbcopy, clip.exe, wl-copy/xclip/xsel)
//     writes the local machine's clipboard directly, covering Terminal.app,
//     but on a remote host it either fails (no display) or fills the wrong
//     machine's clipboard.
//
// Sending both is harmless where both work: they carry the same text.
// github.com/atotto/clipboard was considered, but it isn't in this module's
// graph (only bubbles' own go.mod names it) and its exec calls can't be
// bounded — shelling out here with exec.CommandContext keeps a wedged tool
// from hanging the write indefinitely.

// clipboardTimeout bounds one native clipboard write: the context kills a
// tool still running after it, and the same value as cmd.WaitDelay bounds
// the wait for its I/O to close afterwards, so the worst case is about
// twice this (~4s). pbcopy and friends return in milliseconds; this only
// matters for a tool that hangs.
const clipboardTimeout = 2 * time.Second

// errNoNativeClipboard: no native clipboard tool is usable here (e.g. a
// Linux host over SSH with neither DISPLAY nor WAYLAND_DISPLAY set), so
// OSC 52 is all that was sent.
var errNoNativeClipboard = errors.New("no clipboard tool found")

// clipboardWrite is the native-clipboard seam: tests replace it so they
// never touch the real clipboard.
var clipboardWrite = writeNativeClipboard

// copyResultMsg reports how the native half of a `c` copy went; err is nil
// when the native clipboard now holds url.
type copyResultMsg struct {
	url string
	err error
}

// copyURLCmd batches the OSC 52 write with the bounded native write. Both
// run as Cmds, off Update, so a slow or missing tool never stalls the UI.
func copyURLCmd(url string) tea.Cmd {
	return tea.Batch(
		tea.SetClipboard(url),
		func() tea.Msg {
			ctx, cancel := context.WithTimeout(context.Background(), clipboardTimeout)
			defer cancel()
			return copyResultMsg{url: url, err: clipboardWrite(ctx, url)}
		},
	)
}

// writeNativeClipboard tries each candidate tool for this platform in turn
// until one succeeds. Stdout/stderr stay nil (/dev/null): xclip and wl-copy
// fork a child that keeps serving the selection, and an inherited pipe
// would hold Wait open until that child exits; WaitDelay is a second guard
// against the same. That rules out capturing stderr for the error, so a
// failure is reported by tool name instead ("xclip: exit status 1").
func writeNativeClipboard(ctx context.Context, text string) error {
	var lastErr error = errNoNativeClipboard
	for _, argv := range clipboardCommands(runtime.GOOS) {
		path, err := exec.LookPath(argv[0])
		if err != nil {
			continue
		}
		cmd := exec.CommandContext(ctx, path, argv[1:]...)
		cmd.Stdin = strings.NewReader(text)
		cmd.WaitDelay = clipboardTimeout
		if err := cmd.Run(); err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("%s: timed out", argv[0])
			}
			lastErr = fmt.Errorf("%s: %w", argv[0], err)
			continue
		}
		return nil
	}
	return lastErr
}

// clipboardCommands lists the native tools to try, in order, for goos. On
// Linux/BSD the Wayland and X11 tools are only offered when their display
// variable is set, so an SSH session doesn't spawn tools that can only fail.
func clipboardCommands(goos string) [][]string {
	switch goos {
	case "darwin":
		return [][]string{{"pbcopy"}}
	case "windows":
		return [][]string{{"clip.exe"}}
	}
	var cmds [][]string
	if os.Getenv("WAYLAND_DISPLAY") != "" {
		cmds = append(cmds, []string{"wl-copy"})
	}
	if os.Getenv("DISPLAY") != "" {
		cmds = append(cmds,
			[]string{"xclip", "-selection", "clipboard"},
			[]string{"xsel", "--clipboard", "--input"},
		)
	}
	return cmds
}
