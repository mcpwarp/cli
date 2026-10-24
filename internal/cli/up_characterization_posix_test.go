//go:build !windows

package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mcpwarp/cli/internal/bridge"
	"github.com/mcpwarp/cli/internal/supervisor"
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

	if r, _ := h.seededRow("fs"); r.Kind != "stdio" || r.State != "healthy" || r.Registration != tui.RegistrationPending {
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
	// stdio's State belongs to ServerStateChanged, which a ServiceChanged
	// doesn't overwrite, and the URL of a disabled (not rejected) row stays.
	if r.State != "disabled" || r.URL != "https://fs.example/mcp" {
		t.Fatalf("disabled fs row = %+v, want the supervisor's disabled State and its URL kept", r)
	}
	h.waitPlain("! fs: disabled (restarts=0)")
	h.waitRow("notes", "notes untouched", charActive)
	h.expectNoFatal("a dashboard disable")
}

// startStdioCharacterization runs one fakemcp stdio server "fs" and returns
// the harness with fs's bridge and supervisor, once fs is active.
func startStdioCharacterization(t *testing.T) (*charHarness, *bridge.Bridge, *supervisor.Supervisor) {
	t.Helper()
	fakemcp := buildFakeMCPBinary(t)
	var br atomic.Pointer[bridge.Bridge]
	var sv atomic.Pointer[supervisor.Supervisor]
	h := startCharacterization(t, charSetup{
		servers:         fmt.Sprintf(`{"name":"fs","kind":"stdio","command":%q,"args":[]}`, fakemcp),
		onBridgeStarted: func(_ string, b *bridge.Bridge) { br.Store(b) },
		newSupervisor: func(o supervisor.Options, d supervisor.Deps) *supervisor.Supervisor {
			s := supervisor.New(o, d)
			sv.Store(s)
			return s
		},
	})
	h.waitRow("fs", "fs active", charActive)
	return h, br.Load(), sv.Load()
}

const charPing = `{"jsonrpc":"2.0","id":1,"method":"ping"}`

// TestUpCharacterization_RequestRespawnsStoppedStdioChild: a request that
// reaches fs's bridge while its child is down and nothing has marked fs
// disabled brings the child back through the bridge's EnsureChild
// (DESIGN.md §7) and is answered by it. The supervisor is disabled directly,
// leaving the tunnel routing fs: the window in which a dashboard enable has
// reached the registry but not yet the supervisor, where nothing but the
// request's own lazy respawn starts the child.
func TestUpCharacterization_RequestRespawnsStoppedStdioChild(t *testing.T) {
	h, br, sv := startStdioCharacterization(t)
	oldPid := br.GetCurrentChild().Pid()
	sv.Disable()
	if !br.GetCurrentChild().HasExited() {
		t.Fatal("Disable left fs's child running")
	}

	resp := openHTTPStream(t, h.edge, "fs.example", "/mcp", []byte(charPing))
	defer resp.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode relayed response: %v", err)
	}
	if resp.StatusCode != http.StatusOK || body["result"] != "pong" {
		t.Fatalf("relayed ping = %d %+v, want 200 pong from a respawned child", resp.StatusCode, body)
	}
	child := br.GetCurrentChild()
	if child.Pid() == oldPid || !pidAlive(child.Pid()) {
		t.Fatalf("fs child pid = %d (alive %v), want a live respawn replacing pid %d", child.Pid(), pidAlive(child.Pid()), oldPid)
	}
	if got := sv.GetState(); got != supervisor.Healthy {
		t.Fatalf("fs supervisor state = %v after the lazy respawn, want healthy", got)
	}
	h.waitFor("fs's supervisor to report the respawn", func() bool {
		states := h.serverStates("fs")
		i := slices.Index(states, "disabled")
		return i >= 0 && slices.Contains(states[i:], "restarting") && slices.Contains(states[i:], "healthy")
	})
	h.expectNoFatal("a lazy respawn")
}

// TestUpCharacterization_LocalDisableNotRespawnedByRequest: after a local
// `d` (the Controller, which marks the registry before stopping the
// child), a request that reaches fs's bridge gets the usual 502 and the
// child stays down.
func TestUpCharacterization_LocalDisableNotRespawnedByRequest(t *testing.T) {
	h, br, sv := startStdioCharacterization(t)
	h.controller().Disable("fs")
	if got := sv.GetState(); got != supervisor.Disabled {
		t.Fatalf("fs supervisor state = %v after d, want disabled", got)
	}

	resp, err := http.Post(br.URL(), "application/json", strings.NewReader(charPing))
	if err != nil {
		t.Fatalf("POST to fs's bridge: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadGateway || strings.TrimSpace(string(b)) != `{"error":"local server is not running"}` {
		t.Fatalf("POST to fs's bridge = %d %q, want 502 local server is not running", resp.StatusCode, b)
	}
	if got := sv.GetState(); got != supervisor.Disabled {
		t.Fatalf("fs supervisor state = %v after the request, want still disabled", got)
	}
	if !br.GetCurrentChild().HasExited() {
		t.Fatal("a request respawned a locally disabled child")
	}
	h.expectNoFatal("a local disable")
}

// TestUpCharacterization_DashboardDisabledNotRespawnedByRequest: once the
// dashboard disables fs, no request brings its child back. Through the
// tunnel the relay refuses the disabled id (503) before the bridge sees
// it; a request that reaches the bridge anyway gets the usual 502 from
// EnsureChild's guard, which never calls EnsureRunning.
func TestUpCharacterization_DashboardDisabledNotRespawnedByRequest(t *testing.T) {
	h, br, sv := startStdioCharacterization(t)
	_ = h.edge.Latest().Disable("id-fs", "paused in the dashboard")
	h.waitFor("the fs supervisor to report disabled", func() bool {
		return slices.Contains(h.serverStates("fs"), "disabled")
	})
	h.waitRow("fs", "fs disabled", func(r tui.Server) bool { return r.Registration == tui.RegistrationDisabled })

	if status, body := h.relay("fs.example"); status != http.StatusServiceUnavailable || body != `{"error":"service disabled"}` {
		t.Fatalf("relayed GET = %d %q, want 503 service disabled", status, body)
	}
	resp, err := http.Post(br.URL(), "application/json", strings.NewReader(charPing))
	if err != nil {
		t.Fatalf("POST to fs's bridge: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadGateway || strings.TrimSpace(string(b)) != `{"error":"local server is not running"}` {
		t.Fatalf("POST to fs's bridge = %d %q, want 502 local server is not running", resp.StatusCode, b)
	}
	if got := sv.GetState(); got != supervisor.Disabled {
		t.Fatalf("fs supervisor state = %v after the request, want still disabled", got)
	}
	if !br.GetCurrentChild().HasExited() {
		t.Fatal("a request respawned a dashboard-disabled child")
	}
	h.expectNoFatal("a dashboard disable")
}
