package output

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/mcpwarp/cli/internal/shutdown"
)

const (
	hideCursor = "\x1b[?25l"
	showCursor = "\x1b[?25h"
)

var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

// StartSpinner starts a braille-frame spinner writing "\r<frame> message" to
// Stdout every 80ms, hiding the cursor for the duration. No-op (a no-op
// stop func) when Stdout isn't a TTY — nothing to animate, and every \r
// would just wrap the log to a new line. The returned stop func is safe to
// call more than once and clears the whole line on exit.
//
// Cursor restoration on SIGINT/SIGTERM (DESIGN §3) goes through
// shutdown.Register rather than the spinner handling signals itself —
// unregistered again once Stop runs normally.
func StartSpinner(message string) func() {
	f, ok := Stdout.(*os.File)
	if !ok || !isTerminal(f) {
		return func() {}
	}

	fmt.Fprint(Stdout, hideCursor)
	done := make(chan struct{})
	var once sync.Once
	restore := func() {
		once.Do(func() {
			close(done)
			fmt.Fprintf(Stdout, "\r\x1b[K%s", showCursor)
		})
	}
	unregister := shutdown.Register("spinner: restore cursor", func(context.Context) error {
		restore()
		return nil
	})
	stop := func() {
		restore()
		unregister()
	}

	go func() {
		ticker := time.NewTicker(80 * time.Millisecond)
		defer ticker.Stop()
		i := 0
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				fmt.Fprintf(Stdout, "\r%s %s", spinnerFrames[i], message)
				i = (i + 1) % len(spinnerFrames)
			}
		}
	}()

	return stop
}
