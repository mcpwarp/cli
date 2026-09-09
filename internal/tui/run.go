package tui

import (
	"context"
	"errors"

	tea "charm.land/bubbletea/v2"

	"github.com/mcpwarp/cli/internal/eventbus"
)

// Run starts the dashboard and blocks until the user quits (`q`), a SIGINT/
// SIGTERM arrives, or ctx is canceled. It never calls os.Exit or drives
// shutdown itself (DESIGN.md §9): the returned quit is true only when the
// user asked to quit, telling the caller (M3B's `up` command) to run its own
// shutdown sequence; false means don't — ctx was already canceled by a
// shutdown under way elsewhere, or the signal path already owns it.
//
// bus must be the same *eventbus.Bus the bridge/supervisor/tunnel goroutines
// publish to; Run only reads it, closing it remains the caller's job.
//
// updates, if non-nil, lets the caller push a fresh server snapshot (e.g.
// once the tunnel assigns a service its public URL) into the running
// Program as a SnapshotMsg, until it closes or ctx is done.
func Run(ctx context.Context, bus *eventbus.Bus, servers []Server, ctrl Controller, updates <-chan SnapshotMsg) (quit bool, err error) {
	m := New(bus, servers, ctrl)
	m.ctx = ctx
	p := tea.NewProgram(m, tea.WithContext(ctx))
	if updates != nil {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case snap, ok := <-updates:
					if !ok {
						return
					}
					p.Send(snap)
				}
			}
		}()
	}
	final, err := p.Run()
	if err != nil {
		// bubbletea wraps the external context's own error into
		// ErrProgramKilled when WithContext's ctx is canceled (tea.go's
		// Run: `err = fmt.Errorf("%w: %w", ErrProgramKilled, externalCtx.Err())`).
		// That's this function's own "shutdown already under way elsewhere"
		// case, not a real failure — report it as (false, nil) like any
		// other non-quit return.
		if errors.Is(err, tea.ErrProgramKilled) && ctx.Err() != nil {
			return false, nil
		}
		return false, err
	}
	if fm, ok := final.(Model); ok {
		return fm.quitting, nil
	}
	return false, nil
}
