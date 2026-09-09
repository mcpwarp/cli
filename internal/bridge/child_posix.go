//go:build !windows

package bridge

import (
	"os"
	"os/exec"
	"syscall"
)

// platformHandle carries no extra state on POSIX: the process group itself
// (set up via Setpgid below) is enough to target the whole tree with a
// negative pid.
type platformHandle struct{}

// setSysProcAttr makes the child the leader of its own process group so a
// signal to -pid reaches it and any grandchildren it spawns (e.g. an
// npx-wrapped server) — ports stdio-child.ts's `detached: true`.
func setSysProcAttr(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func (platformHandle) afterStart(cmd *exec.Cmd) error { return nil }

func (platformHandle) close() {}

func (platformHandle) terminate(cmd *exec.Cmd) error {
	return killGroupSignal(cmd, syscall.SIGTERM)
}

func (platformHandle) kill(cmd *exec.Cmd) error {
	return killGroupSignal(cmd, syscall.SIGKILL)
}

// killGroupSignal signals the whole process group (negative pid), falling
// back to signalling just the direct child if that fails — mirrors
// stdio-child.ts's killChildGroup.
func killGroupSignal(cmd *exec.Cmd, sig syscall.Signal) error {
	if cmd.Process == nil {
		return nil
	}
	if err := syscall.Kill(-cmd.Process.Pid, sig); err == nil {
		return nil
	}
	return cmd.Process.Signal(sig)
}

// processAlive probes whether pid is still alive via signal 0 (sends no
// signal, only checks existence/permission) — a test/e2e seam for
// asserting a killed child's process is actually gone.
func processAlive(pid int) bool {
	return syscall.Kill(pid, 0) == nil
}

// exitInfo extracts the exit code / terminating signal from a finished
// process's state, POSIX flavor (a real syscall.WaitStatus).
func exitInfoFromState(state *os.ProcessState) (code int, signaled bool, signal string) {
	if state == nil {
		return -1, false, ""
	}
	ws, ok := state.Sys().(syscall.WaitStatus)
	if !ok {
		return state.ExitCode(), false, ""
	}
	if ws.Signaled() {
		return -1, true, ws.Signal().String()
	}
	return ws.ExitStatus(), false, ""
}
