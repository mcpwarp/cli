//go:build windows

package bridge

import (
	"os"
	"os/exec"
	"sync"
	"syscall"
	"unsafe"
)

// Windows process-tree kill via a Job Object with KILL_ON_JOB_CLOSE
// (DESIGN.md §7/§13 item 6), plus CREATE_NEW_PROCESS_GROUP so a graceful
// "terminate" step can send CTRL_BREAK_EVENT to the whole group before the
// hard TerminateJobObject/CloseHandle kill. NOT RUNTIME-TESTED — no Windows
// environment was available to verify this file; it is written to compile
// and follow the documented Win32 contracts, but has not been exercised
// against a real process tree.
//
// golang.org/x/sys is unavailable to this package, so the Win32 calls are
// made directly via syscall.NewLazyDLL/kernel32, and the job-object structs
// are laid out here by hand to match JOBOBJECT_EXTENDED_LIMIT_INFORMATION.
//
// Known race: the child runs unsuspended between Start() and afterStart()
// assigning it to the job below, so a grandchild spawned in that window
// (e.g. an npx-style wrapper re-execing immediately) can end up outside
// the job and survive a kill. Closing it would need CREATE_SUSPENDED plus
// resuming the main thread only after the job assignment succeeds.

const (
	createNewProcessGroup = 0x00000200

	jobObjectExtendedLimitInformationClass = 9
	jobObjectLimitKillOnJobClose           = 0x00002000

	processTerminate = 0x0001
	processSetQuota  = 0x0100

	ctrlBreakEvent = 1
)

var (
	kernel32                     = syscall.NewLazyDLL("kernel32.dll")
	procCreateJobObjectW         = kernel32.NewProc("CreateJobObjectW")
	procSetInformationJobObject  = kernel32.NewProc("SetInformationJobObject")
	procAssignProcessToJobObject = kernel32.NewProc("AssignProcessToJobObject")
	procTerminateJobObject       = kernel32.NewProc("TerminateJobObject")
	procGenerateConsoleCtrlEvent = kernel32.NewProc("GenerateConsoleCtrlEvent")
)

type jobObjectBasicLimitInformation struct {
	PerProcessUserTimeLimit int64
	PerJobUserTimeLimit     int64
	LimitFlags              uint32
	MinimumWorkingSetSize   uintptr
	MaximumWorkingSetSize   uintptr
	ActiveProcessLimit      uint32
	Affinity                uintptr
	PriorityClass           uint32
	SchedulingClass         uint32
}

type ioCounters struct {
	ReadOperationCount  uint64
	WriteOperationCount uint64
	OtherOperationCount uint64
	ReadTransferCount   uint64
	WriteTransferCount  uint64
	OtherTransferCount  uint64
}

type jobObjectExtendedLimitInformation struct {
	BasicLimitInformation jobObjectBasicLimitInformation
	IoInfo                ioCounters
	ProcessMemoryLimit    uintptr
	JobMemoryLimit        uintptr
	PeakProcessMemoryUsed uintptr
	PeakJobMemoryUsed     uintptr
}

// platformHandle holds the Job Object handle assigned to the child once it
// starts, guarded so afterStart/terminate/kill can race close() safely.
type platformHandle struct {
	mu  sync.Mutex
	job syscall.Handle
}

func setSysProcAttr(cmd *exec.Cmd) {
	// HideWindow (STARTF_USESHOWWINDOW), not CREATE_NO_WINDOW: the latter
	// also suppresses console creation in a way that would swallow
	// CTRL_BREAK_EVENT (terminate's graceful-stop signal below).
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: createNewProcessGroup, HideWindow: true}
}

// afterStart creates a Job Object with KILL_ON_JOB_CLOSE and assigns the
// just-started child to it, so the whole tree dies if this handle is ever
// closed without an explicit TerminateJobObject/kill (a crash-safety net,
// mirroring liveChildren's process-exit handler on the Node side).
func (h *platformHandle) afterStart(cmd *exec.Cmd) error {
	jobHandle, _, err := procCreateJobObjectW.Call(0, 0)
	if jobHandle == 0 {
		return err
	}
	job := syscall.Handle(jobHandle)

	info := jobObjectExtendedLimitInformation{
		BasicLimitInformation: jobObjectBasicLimitInformation{
			LimitFlags: jobObjectLimitKillOnJobClose,
		},
	}
	ret, _, err := procSetInformationJobObject.Call(
		uintptr(job),
		jobObjectExtendedLimitInformationClass,
		uintptr(unsafe.Pointer(&info)),
		unsafe.Sizeof(info),
	)
	if ret == 0 {
		syscall.CloseHandle(job)
		return err
	}

	procHandle, err := syscall.OpenProcess(processTerminate|processSetQuota, false, uint32(cmd.Process.Pid))
	if err != nil {
		syscall.CloseHandle(job)
		return err
	}
	defer syscall.CloseHandle(procHandle)

	ret, _, err = procAssignProcessToJobObject.Call(uintptr(job), uintptr(procHandle))
	if ret == 0 {
		syscall.CloseHandle(job)
		return err
	}

	h.mu.Lock()
	h.job = job
	h.mu.Unlock()
	return nil
}

// terminate sends CTRL_BREAK_EVENT to the child's process group (its own
// pid, since CREATE_NEW_PROCESS_GROUP made it the group leader) — the
// closest Windows equivalent to a graceful SIGTERM.
func (h *platformHandle) terminate(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	ret, _, err := procGenerateConsoleCtrlEvent.Call(ctrlBreakEvent, uintptr(cmd.Process.Pid))
	if ret == 0 {
		return err
	}
	return nil
}

// kill terminates the whole job (process tree) — the hard-kill step,
// equivalent to SIGKILL-ing the group on POSIX.
func (h *platformHandle) kill(cmd *exec.Cmd) error {
	h.mu.Lock()
	job := h.job
	h.job = 0
	h.mu.Unlock()
	if job != 0 {
		procTerminateJobObject.Call(uintptr(job), 1)
		syscall.CloseHandle(job)
		return nil
	}
	if cmd.Process != nil {
		return cmd.Process.Kill()
	}
	return nil
}

// close releases the Job Object handle on a normal exit (kill() already
// does this itself for the hard-kill path) — without it, every spawned
// child leaks one handle for the process's lifetime. Because the job was
// created with jobObjectLimitKillOnJobClose, this CloseHandle is not just
// a leak-avoidance no-op: if any grandchild the direct child spawned is
// still alive in the job when the last handle to it closes, Windows kills
// the whole job (grandchild included) right here — intentional, matching
// the process-group SIGKILL fallback on POSIX.
func (h *platformHandle) close() {
	h.mu.Lock()
	job := h.job
	h.job = 0
	h.mu.Unlock()
	if job != 0 {
		syscall.CloseHandle(job)
	}
}

// processAlive probes whether pid is still alive by attempting to open it
// — NOT RUNTIME-TESTED, see the file header.
func processAlive(pid int) bool {
	h, err := syscall.OpenProcess(syscall.PROCESS_QUERY_INFORMATION, false, uint32(pid))
	if err != nil {
		return false
	}
	defer syscall.CloseHandle(h)
	var code uint32
	if err := syscall.GetExitCodeProcess(h, &code); err != nil {
		return false
	}
	const stillActive = 259
	return code == stillActive
}

// exitInfo extracts the exit code from a finished process's state. Windows
// has no POSIX-style terminating-signal concept, so `signaled` is always
// false here.
func exitInfoFromState(state *os.ProcessState) (code int, signaled bool, signal string) {
	if state == nil {
		return -1, false, ""
	}
	return state.ExitCode(), false, ""
}
