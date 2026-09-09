package bridge

import (
	"bytes"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestPumpStdout_ParsesNDJSONAndSkipsInvalidLines(t *testing.T) {
	c := newBareChild()
	var got []map[string]any
	c.OnMessage(func(m map[string]any) { got = append(got, m) })

	input := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"result":"ok"}`,
		``,                // blank line, skipped
		`not json at all`, // non-JSON, skipped at debug
		`[1,2,3]`,         // valid JSON but not an object, skipped
		`{"jsonrpc":"2.0","method":"ping"}`,
		``,
	}, "\n")

	c.pumpStdout(strings.NewReader(input))

	if len(got) != 2 {
		t.Fatalf("want 2 messages, got %d: %+v", len(got), got)
	}
	if got[0]["id"] != float64(1) {
		t.Errorf("first message id = %v", got[0]["id"])
	}
	if got[1]["method"] != "ping" {
		t.Errorf("second message method = %v", got[1]["method"])
	}
}

func TestPumpStdout_CarriageReturnStripped(t *testing.T) {
	c := newBareChild()
	var got []map[string]any
	c.OnMessage(func(m map[string]any) { got = append(got, m) })

	c.pumpStdout(strings.NewReader("{\"jsonrpc\":\"2.0\",\"method\":\"ping\"}\r\n"))

	if len(got) != 1 || got[0]["method"] != "ping" {
		t.Fatalf("got %+v", got)
	}
}

func TestPumpStdout_DiscardsUnbrokenLineOverCap(t *testing.T) {
	c := newBareChild()
	var got int
	c.OnMessage(func(m map[string]any) { got++ })

	huge := strings.Repeat("a", maxLineBytes+1000)
	r := io.MultiReader(strings.NewReader(huge), strings.NewReader("\n"+`{"jsonrpc":"2.0","method":"ping"}`+"\n"))
	c.pumpStdout(r)

	if got != 1 {
		t.Fatalf("want exactly 1 valid message surviving the discard, got %d", got)
	}
}

func TestHandleLine_SkipsOversizedSingleLine(t *testing.T) {
	c := newBareChild()
	var got int
	c.OnMessage(func(m map[string]any) { got++ })

	c.handleLine(bytes.Repeat([]byte("a"), maxLineBytes+10))

	if got != 0 {
		t.Fatalf("expected the oversized line to be skipped, got %d messages", got)
	}
}

func TestNewStdioChild_SpawnErrorSurfacesFromNew(t *testing.T) {
	_, err := NewStdioChild(SpawnSpec{Command: "/no/such/binary-mcpwarp-test"}, testLogger(), "bad")
	if err == nil {
		t.Fatal("expected an error for a nonexistent command")
	}
}

func TestStdioChild_SendAndReceiveRoundTrip(t *testing.T) {
	bin := buildFakeMCP(t)
	c, err := NewStdioChild(SpawnSpec{Command: bin}, testLogger(), "fake")
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	msgs := make(chan map[string]any, 4)
	c.OnMessage(func(m map[string]any) { msgs <- m })

	if err := c.Send(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "ping"}); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case m := <-msgs:
		if m["result"] != "pong" {
			t.Fatalf("unexpected reply: %+v", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for pong")
	}
}

// TestStdioChild_ReplyThenImmediateExit_NoTruncation reproduces the
// os/exec StdoutPipe/Wait race (StdoutPipe docs: "it is incorrect to call
// Wait before all reads from the pipe have completed") — the child writes
// its reply and exits in the same instant, and the reply must still
// arrive intact rather than being truncated by Wait() closing the pipe
// out from under pumpStdout. Run with -count=20 to catch the race.
func TestStdioChild_ReplyThenImmediateExit_NoTruncation(t *testing.T) {
	bin := buildFakeMCP(t)
	c, err := NewStdioChild(SpawnSpec{Command: bin}, testLogger(), "fake")
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	msgs := make(chan map[string]any, 4)
	c.OnMessage(func(m map[string]any) { msgs <- m })

	if err := c.Send(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "debug/replyThenExit"}); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case m := <-msgs:
		result, _ := m["result"].(map[string]any)
		if result == nil || result["ok"] != true {
			t.Fatalf("unexpected reply: %+v", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reply from a child that wrote it then exited immediately was lost")
	}
}

// TestStdioChild_GrandchildHoldingStdout_DetectsExitPromptly reproduces the
// os/exec Wait-vs-pipe-drain race the other way around from
// TestStdioChild_ReplyThenImmediateExit_NoTruncation: a grandchild that
// inherits the write end of the child's stdout pipe (e.g. an npx-style
// wrapper backgrounding a task) must not keep HasExited() false — waitExit
// must not gate cmd.Wait() behind the stdout/stderr pumps draining, since
// those only drain once every holder of the write end (including the
// grandchild) closes it.
func TestStdioChild_GrandchildHoldingStdout_DetectsExitPromptly(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("probe uses /bin/sh's job control; not applicable on windows")
	}
	c, err := NewStdioChild(SpawnSpec{Command: "/bin/sh", Args: []string{"-c", "sleep 30 & exit 0"}}, testLogger(), "grandchild-holder")
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	// c.Close() alone is a no-op once the shell itself has exited (its
	// exited flag is already set), which is exactly the case this test
	// forces — leaving the backgrounded "sleep 30" grandchild orphaned.
	// Reach past that and kill the whole process group directly.
	t.Cleanup(func() { _ = c.plat.kill(c.cmd) })

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.HasExited() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("HasExited() never became true — a grandchild holding stdout open blocked exit detection")
}

func TestStdioChild_CloseKillsProcess(t *testing.T) {
	bin := buildFakeMCP(t)
	c, err := NewStdioChild(SpawnSpec{Command: bin}, testLogger(), "fake")
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}

	pid := c.Pid()
	if pid == 0 {
		t.Fatal("expected a nonzero pid")
	}
	if !processAlive(pid) {
		t.Fatal("expected the freshly spawned process to be alive")
	}

	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !c.HasExited() {
		t.Fatal("expected HasExited() to be true after Close")
	}
	if processAlive(pid) {
		t.Fatal("expected the process to be gone after Close")
	}
}
