package bridge

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/mcpwarp/cli/internal/eventbus"
)

// maxLineBytes is the 16 MiB per-line cap on the child's stdout (DESIGN.md
// §7) — a line (or an unbroken run of stdout with no newline yet) past
// this is discarded with a warning rather than buffered without bound.
const maxLineBytes = 16 * 1024 * 1024

const closeGraceTimeout = 2 * time.Second

// pumpDrainGrace: how long waitExit gives pumpStdout/pumpStderr to drain
// after cmd.Wait() returns, before giving up on them and warning. A
// grandchild that inherited the child's stdout/stderr fds (e.g. an
// unmanaged background job) can hold those pipes open well past the
// child's own exit — the exit itself must still be reported promptly.
const pumpDrainGrace = 500 * time.Millisecond

// ExitInfo is what a child's exit reports: either a POSIX exit code, or
// (POSIX only) the terminating signal.
type ExitInfo struct {
	Code     int
	Signaled bool
	Signal   string
}

var (
	liveChildrenMu sync.Mutex
	liveChildren   = map[*StdioChild]struct{}{}
)

// KillAllLiveChildren is the last-resort net for every currently-spawned
// child (DESIGN.md §7), mirroring stdio-child.ts's `process.on("exit")`
// handler. Go has no equivalent of Node's synchronous exit hook that runs
// after os.Exit, so this is exported for cmd/mcpwarp's signal/shutdown path
// to call explicitly rather than relying on a package-level hook.
// KillAllLiveChildren's HasExited()-then-kill check below is inherently a
// check-then-act race: a child can only leave liveChildren once
// waitExit's cmd.Wait() has already reaped it (see waitExit), so there is
// no window in which a live entry's pid is guaranteed to still be
// unreaped by the time the signal is sent. On a long-running system with
// heavy pid churn, the OS could in principle recycle that pid to an
// unrelated process in the gap between the check and the kill. There is
// no portable way to signal "only if this is still my child" without
// pidfd (Linux-only), so this re-checks HasExited() as late as possible
// to shrink the window and accepts the residual race as a known,
// documented limitation rather than solving it partially per-platform.
func KillAllLiveChildren() {
	liveChildrenMu.Lock()
	children := make([]*StdioChild, 0, len(liveChildren))
	for c := range liveChildren {
		children = append(children, c)
	}
	liveChildrenMu.Unlock()
	for _, c := range children {
		if c.HasExited() {
			continue
		}
		_ = c.plat.kill(c.cmd)
	}
}

// StdioChild is one spawned stdio MCP server plus its NDJSON stdin writer
// / stdout reader. Ported from stdio-child.ts; restart/backoff is out of
// scope here — see internal/supervisor.
type StdioChild struct {
	name string
	log  *slog.Logger
	bus  *eventbus.Bus

	cmd     *exec.Cmd
	stdin   io.WriteCloser
	writeMu sync.Mutex
	pumpWG  sync.WaitGroup

	plat platformHandle

	mu               sync.Mutex
	exited           bool
	exitInfo         ExitInfo
	exitCh           chan struct{}
	messageListeners []func(map[string]any)
	exitListeners    []func(ExitInfo)
	errorListeners   []func(error)
}

func isRecord(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	return m, ok
}

func mergeEnv(extra map[string]string) []string {
	base := os.Environ()
	if len(extra) == 0 {
		return base
	}
	merged := make(map[string]string, len(base)+len(extra))
	for _, kv := range base {
		if i := strings.IndexByte(kv, '='); i >= 0 {
			merged[kv[:i]] = kv[i+1:]
		}
	}
	// Config env wins on key collision (DESIGN.md §7).
	for k, v := range extra {
		merged[k] = v
	}
	out := make([]string, 0, len(merged))
	for k, v := range merged {
		out = append(out, k+"="+v)
	}
	return out
}

// NewStdioChild spawns spec with shell:false, no bus wiring for stderr
// telemetry.
func NewStdioChild(spec SpawnSpec, log *slog.Logger, name string) (*StdioChild, error) {
	return NewStdioChildWithBus(spec, log, name, nil)
}

// NewStdioChildWithBus is NewStdioChild, additionally publishing each
// stderr line to bus's telemetry channel (as well as logging it at
// debug), if bus is non-nil.
func NewStdioChildWithBus(spec SpawnSpec, log *slog.Logger, name string, bus *eventbus.Bus) (*StdioChild, error) {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	cmd := exec.Command(spec.Command, spec.Args...)
	cmd.Env = mergeEnv(spec.Env)
	cmd.Dir = spec.Cwd
	setSysProcAttr(cmd)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}

	// cmd.StdoutPipe()/StderrPipe() hand back a pipe whose read end Wait()
	// closes as soon as it sees the child exit (os/exec: "it is incorrect
	// to call Wait before all reads from the pipe have completed") — that
	// races pumpStdout/pumpStderr's own reads and can truncate output the
	// child wrote right before exiting. Own the pipes instead: dup our own
	// os.Pipe() onto the child's stdout/stderr, close our copy of the
	// write end once started (so EOF depends only on the child's fds), and
	// gate Wait() behind both pumps finishing.
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		stdoutR.Close()
		stdoutW.Close()
		return nil, err
	}
	cmd.Stdout = stdoutW
	cmd.Stderr = stderrW

	c := &StdioChild{
		name:   name,
		log:    log,
		bus:    bus,
		cmd:    cmd,
		stdin:  stdin,
		exitCh: make(chan struct{}),
	}

	// exec.Cmd.Start() looks up and forks/execs synchronously, so an ENOENT
	// (or any other spawn failure) surfaces here directly — no separate
	// "ready" race needed the way Node's async spawn() requires one.
	if err := cmd.Start(); err != nil {
		stdoutR.Close()
		stdoutW.Close()
		stderrR.Close()
		stderrW.Close()
		return nil, err
	}
	stdoutW.Close()
	stderrW.Close()

	liveChildrenMu.Lock()
	liveChildren[c] = struct{}{}
	liveChildrenMu.Unlock()

	if err := c.plat.afterStart(cmd); err != nil {
		log.Warn("failed to set up process-group/job isolation for stdio child", "name", name, "err", err)
	}

	c.pumpWG.Add(2)
	go func() {
		defer c.pumpWG.Done()
		defer stdoutR.Close()
		c.pumpStdout(stdoutR)
	}()
	go func() {
		defer c.pumpWG.Done()
		defer stderrR.Close()
		c.pumpStderr(stderrR)
	}()
	go c.waitExit()

	return c, nil
}

func (c *StdioChild) waitExit() {
	// cmd.Stdout/cmd.Stderr are our own *os.File pipe ends (see
	// NewStdioChildWithBus), not the io.Writer os/exec would otherwise
	// spawn a copying goroutine for — so cmd.Wait() only waits on the
	// child process itself, never on the pipes draining. Call it directly,
	// rather than gating it behind pumpWG.Wait(): a grandchild that
	// inherited the write end (e.g. a detached background job) can hold
	// the read side open long after the child we're tracking has exited,
	// and that must not delay HasExited()/OnExit.
	_ = c.cmd.Wait()
	c.plat.close()

	pumpsDone := make(chan struct{})
	go func() {
		c.pumpWG.Wait()
		close(pumpsDone)
	}()
	select {
	case <-pumpsDone:
	case <-time.After(pumpDrainGrace):
		c.log.Warn("stdio child exited but its stdout/stderr pipe is still open (likely held by a grandchild); giving up waiting for it to drain", "name", c.name)
	}
	code, signaled, sig := exitInfoFromState(c.cmd.ProcessState)
	info := ExitInfo{Code: code, Signaled: signaled, Signal: sig}

	// Removed from liveChildren before exited is set/exitCh closes, so
	// KillAllLiveChildren's HasExited() check never wins a race against a
	// pid the OS has already reaped (and could have recycled).
	liveChildrenMu.Lock()
	delete(liveChildren, c)
	liveChildrenMu.Unlock()

	c.mu.Lock()
	c.exited = true
	c.exitInfo = info
	listeners := append([]func(ExitInfo){}, c.exitListeners...)
	c.mu.Unlock()

	close(c.exitCh)

	for _, fn := range listeners {
		fn(info)
	}
}

// OnMessage registers a listener for each parsed NDJSON stdout object.
func (c *StdioChild) OnMessage(fn func(map[string]any)) {
	c.mu.Lock()
	c.messageListeners = append(c.messageListeners, fn)
	c.mu.Unlock()
}

// OnExit registers a listener fired once the child exits. If the child has
// already exited by the time this is called, fn fires immediately (with
// the exit info) instead of being queued for an event that already
// happened — closes the same race stdio-child.ts's getExitInfo() exists to
// let callers recover from.
func (c *StdioChild) OnExit(fn func(ExitInfo)) {
	c.mu.Lock()
	if c.exited {
		info := c.exitInfo
		c.mu.Unlock()
		fn(info)
		return
	}
	c.exitListeners = append(c.exitListeners, fn)
	c.mu.Unlock()
}

// OnError registers a listener for asynchronous errors (e.g. a stdin write
// failing after the child has gone away).
func (c *StdioChild) OnError(fn func(error)) {
	c.mu.Lock()
	c.errorListeners = append(c.errorListeners, fn)
	c.mu.Unlock()
}

func (c *StdioChild) emitError(err error) {
	c.mu.Lock()
	listeners := append([]func(error){}, c.errorListeners...)
	c.mu.Unlock()
	for _, fn := range listeners {
		fn(err)
	}
}

// HasExited reports whether the child's exit has already happened.
func (c *StdioChild) HasExited() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.exited
}

// GetExitInfo returns the child's exit info and true, or false if it
// hasn't exited yet.
func (c *StdioChild) GetExitInfo() (ExitInfo, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.exitInfo, c.exited
}

// Pid is the underlying OS pid — a test/e2e seam for asserting a killed
// child's process is actually gone.
func (c *StdioChild) Pid() int {
	if c.cmd.Process == nil {
		return 0
	}
	return c.cmd.Process.Pid
}

// Send writes one JSON-RPC message as a newline-delimited NDJSON line.
// Writing to the pipe blocks (providing backpressure) until the kernel
// buffer has room — the direct equivalent of stdio-child.ts's awaited
// drain, without needing an explicit drain race in Go.
func (c *StdioChild) Send(msg any) error {
	c.mu.Lock()
	exited := c.exited
	c.mu.Unlock()
	if exited {
		return errors.New("stdio child has exited")
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	c.writeMu.Lock()
	_, err = c.stdin.Write(data)
	c.writeMu.Unlock()
	if err != nil {
		c.emitError(err)
	}
	return err
}

func (c *StdioChild) pumpStdout(r io.Reader) {
	buf := make([]byte, 0, 64*1024)
	chunk := make([]byte, 64*1024)
	// scanned is how much of buf's front has already been searched for a
	// newline and found none — each byte gets IndexByte'd at most once
	// (rather than rescanning the whole buffer on every 64 KiB read once
	// past the 16 MiB cap).
	scanned := 0
	for {
		n, err := r.Read(chunk)
		if n > 0 {
			buf = append(buf, chunk[:n]...)
			for {
				idx := bytes.IndexByte(buf[scanned:], '\n')
				if idx == -1 {
					scanned = len(buf)
					break
				}
				idx += scanned
				line := buf[:idx]
				buf = buf[idx+1:]
				scanned = 0
				if len(line) > 0 && line[len(line)-1] == '\r' {
					line = line[:len(line)-1]
				}
				c.handleLine(line)
			}
			if len(buf) > maxLineBytes {
				c.log.Warn("stdio child stdout line exceeded the 16 MiB cap with no newline yet; discarding buffered data", "name", c.name)
				buf = buf[:0]
				scanned = 0
			}
		}
		if err != nil {
			return
		}
	}
}

func (c *StdioChild) handleLine(line []byte) {
	if len(bytes.TrimSpace(line)) == 0 {
		return
	}
	if len(line) > maxLineBytes {
		c.log.Warn("stdio child stdout line exceeded the 16 MiB cap, skipping", "name", c.name, "length", len(line))
		return
	}
	var parsed any
	if err := json.Unmarshal(line, &parsed); err != nil {
		c.log.Debug("non-JSON stdout line from stdio child, skipping", "name", c.name)
		return
	}
	obj, ok := isRecord(parsed)
	if !ok {
		c.log.Debug("stdio child stdout line parsed to a non-object JSON value, skipping", "name", c.name)
		return
	}
	c.mu.Lock()
	listeners := append([]func(map[string]any){}, c.messageListeners...)
	c.mu.Unlock()
	for _, fn := range listeners {
		fn(obj)
	}
}

func (c *StdioChild) pumpStderr(r io.Reader) {
	br := bufio.NewReaderSize(r, 4096)
	for {
		line, err := br.ReadString('\n')
		trimmed := strings.TrimRight(line, "\r\n")
		if trimmed != "" {
			// Shown under --verbose (raises the logger's level to debug);
			// stays at debug either way, [name]-prefixed same as pino's
			// pretty/NDJSON output.
			c.log.Debug("["+c.name+"] "+trimmed, "server", c.name, "stderr", trimmed)
			if c.bus != nil {
				c.bus.PublishTelemetry(eventbus.LogLine{Server: c.name, Level: "debug", Text: trimmed})
			}
		}
		if err != nil {
			return
		}
	}
}

// Close sends SIGTERM (or the platform equivalent) to the whole process
// group, then SIGKILL after a 2s grace period if the child is still
// alive. A no-op if the child already exited.
func (c *StdioChild) Close() error {
	c.mu.Lock()
	exited := c.exited
	c.mu.Unlock()
	if exited {
		return nil
	}
	_ = c.plat.terminate(c.cmd)
	timer := time.NewTimer(closeGraceTimeout)
	defer timer.Stop()
	select {
	case <-c.exitCh:
		return nil
	case <-timer.C:
		_ = c.plat.kill(c.cmd)
		<-c.exitCh
		return nil
	}
}
