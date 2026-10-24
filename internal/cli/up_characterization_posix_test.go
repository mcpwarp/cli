//go:build !windows

package cli

import (
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mcpwarp/cli/internal/bridge"
	"github.com/mcpwarp/cli/internal/tui"
)

// TestUpCharacterization_DashboardDisableStopsStdioChild needs a stdio child
// that stays up, so it builds the fakemcp fixture the e2e test uses
// (POSIX-only, like that test).
func TestUpCharacterization_DashboardDisableStopsStdioChild(t *testing.T) {
	fakemcp := buildFakeMCPBinary(t)
	var pid atomic.Int64
	h := startCharacterization(t, charSetup{
		servers: fmt.Sprintf(`{"name":"fs","kind":"stdio","command":%q,"args":[]},
			{"name":"notes","kind":"http","url":"http://127.0.0.1:1/mcp"}`, fakemcp),
		onBridgeStarted: func(name string, br *bridge.Bridge) {
			if child := br.GetCurrentChild(); name == "fs" && child != nil {
				pid.Store(int64(child.Pid()))
			}
		},
	})
	childPid := int(pid.Load())
	if !pidAlive(childPid) {
		t.Fatalf("fs child pid %d not alive before the disable", childPid)
	}

	if r, _ := h.row("fs"); r.Kind != "stdio" || r.State != "healthy" || r.Registration != tui.RegistrationPending {
		t.Fatalf("seeded fs row = %+v, want a stdio row with its supervisor's state and pending registration", r)
	}
	h.waitRow("fs", "fs active", charActive)
	h.waitRow("notes", "notes active", charActive)

	_ = h.edge.Latest().Disable("id-fs", "paused in the dashboard")
	h.waitFor("the fs supervisor to report disabled", func() bool {
		return slices.Contains(h.serverStates("fs"), "disabled")
	})
	// The child is stopped, not merely marked: its process goes away (polled,
	// since process exit raises no event).
	for deadline := time.Now().Add(charWait); pidAlive(childPid); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("fs child pid %d still alive after the dashboard disable", childPid)
		}
	}
	r := h.waitRow("fs", "fs disabled", func(r tui.Server) bool { return r.Registration == tui.RegistrationDisabled })
	// stdio's State belongs to ServerStateChanged; a snapshot leaves it
	// empty, and the URL of a disabled (not rejected) row stays.
	if r.State != "" || r.URL != "https://fs.example/mcp" {
		t.Fatalf("disabled fs row = %+v, want no State from the snapshot and its URL kept", r)
	}
	h.waitPlain("! fs: disabled (restarts=0)")
	h.waitRow("notes", "notes untouched", charActive)
	h.expectNoFatal("a dashboard disable")
}
