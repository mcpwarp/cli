// Package tui is the bubbletea v2 program for `mcpwarp up`'s default,
// TTY-attached screen (DESIGN.md §9). It is a pure consumer of
// internal/eventbus: producers (bridge/supervisor/tunnel) never import this
// package, only the reverse. It does not import internal/supervisor either
// — the caller (M3B's `up` wiring) hands over a snapshot of servers plus a
// small Controller it implements, so this package stays decoupled from the
// concrete supervisor type.
package tui

// Server is a snapshot of one configured server for the dashboard table
// (NAME | KIND | STATE | RESTARTS | URL, with aggregate streams/bytes in the
// header — StreamOpened/StreamClosed carry a stream ID but no server name,
// so a per-server streams column isn't derivable from the bus). Name/Kind/
// URL come from the caller's initial snapshot (and any later SnapshotMsg);
// State/Restarts are normally kept current by eventbus.ServerStateChanged,
// falling back to the snapshot's own values until the first one arrives.
type Server struct {
	Name     string
	Kind     string
	URL      string
	State    string
	Restarts int
}

// Controller is the set of actions the dashboard can invoke on a selected
// server, implemented by the caller (M3B, on top of internal/supervisor) so
// this package never imports internal/supervisor directly. Restart/Disable/
// Enable mirror the supervisor methods of the same name (DESIGN.md §9): `r`
// restarts in place, `d` disables (stop + unregister), `e` enables (respawn
// + register).
type Controller interface {
	Restart(name string)
	Disable(name string)
	Enable(name string)
}

// SnapshotMsg lets the caller push an updated server snapshot (e.g. Kind/
// URL for a server discovered after the dashboard started) into a running
// Program via (*tea.Program).Send. Servers are merged by Name, keeping each
// row's live State/Restarts unless the model hasn't seen a
// ServerStateChanged for that name yet.
type SnapshotMsg struct {
	Servers []Server
}
