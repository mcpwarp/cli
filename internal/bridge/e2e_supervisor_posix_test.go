//go:build !windows

package bridge_test

import (
	"io"
	"log/slog"
	"syscall"
	"testing"

	"github.com/mcpwarp/cli/internal/bridge"
	"github.com/mcpwarp/cli/internal/supervisor"
)

// TestBridgeSupervisor_ShutdownKillsRealChildPid ports
// bridge.e2e.test.ts's "overview.md §11(g): a shutdown ... kills the real
// child process": supervisor.Stop() then bridge.Close() — exactly what
// cmd/mcpwarp's registered shutdown handler runs for a stdio bridge — must
// leave the real OS process gone, not just internally marked exited. Uses
// syscall.Kill(pid, 0) as its liveness probe (same as internal/bridge's own
// processAlive on POSIX), hence the !windows build tag.
func TestBridgeSupervisor_ShutdownKillsRealChildPid(t *testing.T) {
	bin := buildFakeMCPBinary(t)
	spec := bridge.SpawnSpec{Command: bin}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	b, err := bridge.StartBridge(bridge.StartBridgeOptions{
		Name:      "fake-sigint",
		SpawnSpec: spec,
		Log:       log,
	})
	if err != nil {
		t.Fatalf("StartBridge: %v", err)
	}

	sup := supervisor.New(supervisor.Options{
		Name:      "fake-sigint",
		SpawnSpec: spec,
		Log:       log,
		Child:     b.GetCurrentChild(),
		Bridge:    b,
	}, supervisor.Deps{})

	pid := b.GetCurrentChild().Pid()
	if pid == 0 {
		t.Fatal("expected a nonzero pid")
	}
	// Sanity check: the process is genuinely alive before shutdown —
	// kill(pid, 0) sends no signal, just probes existence/permission.
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatalf("expected the freshly spawned process to be alive, got %v", err)
	}

	sup.Stop()
	_ = b.Close()

	if err := syscall.Kill(pid, 0); err == nil {
		t.Fatal("expected the real child process to be gone after supervisor.Stop()+bridge.Close()")
	}
}
