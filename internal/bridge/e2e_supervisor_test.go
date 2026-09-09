package bridge_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mcpwarp/cli/internal/bridge"
	"github.com/mcpwarp/cli/internal/supervisor"
)

// buildFakeMCPBinary is a standalone copy of internal/bridge's own
// TestMain fixture builder — this file is package bridge_test (a
// separate, external test package) so it can import internal/supervisor
// without an import cycle (supervisor already imports bridge), and so it
// can't reach the internal package's unexported fakeMCPPath/buildFakeMCP.
func buildFakeMCPBinary(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	out := filepath.Join(dir, "fakemcp-bin")
	cmd := exec.Command("go", "build", "-o", out, "./testdata/fakemcp")
	if err := cmd.Run(); err != nil {
		t.Fatalf("build fakemcp fixture: %v", err)
	}
	return out
}

// TestBridgeSupervisor_CrashRecoveryThroughRealSupervisor wires a real
// supervisor.Supervisor to a real bridge.Bridge (rather than a manual
// bridge.ReplaceChild call, as internal/bridge's own tests do) and kills
// the live child directly — the supervisor must notice, restart, and
// hand the bridge a fresh child entirely on its own.
func TestBridgeSupervisor_CrashRecoveryThroughRealSupervisor(t *testing.T) {
	bin := buildFakeMCPBinary(t)
	spec := bridge.SpawnSpec{Command: bin}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	b, err := bridge.StartBridge(bridge.StartBridgeOptions{
		Name:      "fake",
		SpawnSpec: spec,
		Log:       log,
	})
	if err != nil {
		t.Fatalf("StartBridge: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	sup := supervisor.New(supervisor.Options{
		Name:      "fake",
		SpawnSpec: spec,
		Log:       log,
		Child:     b.GetCurrentChild(),
		Bridge:    b,
	}, supervisor.Deps{})
	t.Cleanup(sup.Stop)

	// Simulate a real crash: kill the child the bridge is currently
	// routing to. Nothing here calls bridge.ReplaceChild — that's the
	// supervisor's job once it observes the exit.
	original := b.GetCurrentChild()
	_ = original.Close()

	// The supervisor starts Healthy already, so wait for an actual
	// hand-off (a new child in the bridge) rather than just re-observing
	// the pre-crash Healthy state before the exit has even propagated.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && (b.GetCurrentChild() == original || sup.GetState() != supervisor.Healthy) {
		time.Sleep(10 * time.Millisecond)
	}
	if b.GetCurrentChild() == original || sup.GetState() != supervisor.Healthy {
		t.Fatalf("supervisor did not recover to healthy on its own, state=%v", sup.GetState())
	}

	resp, err := http.Post(b.URL(), "application/json", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["result"] != "pong" {
		t.Fatalf("unexpected result after supervisor-driven recovery: %+v", body)
	}
}
