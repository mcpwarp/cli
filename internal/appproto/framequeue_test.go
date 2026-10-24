package appproto

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

func wantFrame(t *testing.T, got *Frame, op string, names ...string) {
	t.Helper()
	if got == nil {
		t.Fatalf("frame = nil, want %s %v", op, names)
	}
	if got.Op != op || !slices.Equal(got.Names, names) {
		t.Fatalf("frame = %s %v, want %s %v", got.Op, got.Names, op, names)
	}
}

func TestResendDelay(t *testing.T) {
	cases := map[int]time.Duration{
		0: time.Second, 1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 4: 8 * time.Second,
		5: 16 * time.Second, 6: 30 * time.Second, 7: 30 * time.Second, 40: 30 * time.Second,
	}
	for n, want := range cases {
		if got := ResendDelay(n); got != want {
			t.Errorf("ResendDelay(%d) = %v, want %v", n, got, want)
		}
	}
}

func TestFrameQueueOneInFlight(t *testing.T) {
	q := NewFrameQueue()
	q.Push(OpRegister, "a", "b")
	f := q.Next()
	wantFrame(t, f, OpRegister, "a", "b")
	q.Push(OpUnregister, "c")
	if got := q.Next(); got != nil {
		t.Fatalf("Next with a frame outstanding = %+v, want nil", got)
	}
	if got := q.Reply(OpUnregister, []string{"a"}); got != nil {
		t.Fatalf("an unregistered settled an outstanding register: %+v", got)
	}
	if got := q.Reply(OpRegister, []string{"a", "b"}); got != f {
		t.Fatalf("Reply = %+v, want the outstanding frame", got)
	}
	wantFrame(t, q.Next(), OpUnregister, "c")
	if q.Reply(OpUnregister, nil) == nil {
		t.Fatal("an empty unregistered did not settle the outstanding unregister")
	}
	if !q.Idle() {
		t.Fatal("queue not idle after every frame was answered")
	}
}

func TestFrameQueueLaterIntentSupersedes(t *testing.T) {
	q := NewFrameQueue()
	q.Push(OpRegister, "x")
	q.Next()

	q.Push(OpRegister, "a")
	q.Push(OpUnregister, "a") // register then unregister: unregistered
	q.Push(OpUnregister, "b")
	q.Push(OpRegister, "b") // unregister then register: registered
	q.Push(OpUnregister, "c")
	q.Push(OpRegister, "d")
	q.Push(OpRegister, "e")
	q.Push(OpUnregister, "e")

	q.Reply(OpRegister, []string{"x"})
	wantFrame(t, q.Next(), OpUnregister, "a")
	q.Reply(OpUnregister, []string{"a"})
	wantFrame(t, q.Next(), OpRegister, "b")
	q.Reply(OpRegister, []string{"b"})
	wantFrame(t, q.Next(), OpUnregister, "c")
	q.Reply(OpUnregister, []string{"c"})
	wantFrame(t, q.Next(), OpRegister, "d")
	q.Reply(OpRegister, []string{"d"})
	wantFrame(t, q.Next(), OpUnregister, "e")
	q.Reply(OpUnregister, []string{"e"})
	if !q.Idle() {
		t.Fatalf("queue not idle: %+v", q.queued)
	}
}

func TestFrameQueueBatchCap(t *testing.T) {
	names := make([]string, 250)
	for i := range names {
		names[i] = fmt.Sprintf("s%03d", i)
	}
	q := NewFrameQueue()
	q.Push(OpRegister, names...)
	for _, chunk := range [][]string{names[:100], names[100:200], names[200:]} {
		f := q.Next()
		wantFrame(t, f, OpRegister, chunk...)
		if q.Next() != nil {
			t.Fatal("a second frame went out with one outstanding")
		}
		q.Reply(OpRegister, chunk)
	}
	if !q.Idle() {
		t.Fatal("queue not idle after the last batch")
	}
}

func TestFrameQueueOverloadedBackoff(t *testing.T) {
	q := NewFrameQueue()
	if _, ok := q.Overloaded(); ok {
		t.Fatal("OVERLOADED with nothing outstanding scheduled a re-send")
	}
	q.Push(OpRegister, "a")
	f := q.Next()
	for _, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second} {
		d, ok := q.Overloaded()
		if !ok || d != want {
			t.Fatalf("Overloaded = %v, %v, want %v, true", d, ok, want)
		}
		if _, ok := q.Overloaded(); ok {
			t.Fatal("a second OVERLOADED during the pause scheduled another re-send")
		}
		if q.Next() != nil {
			t.Fatal("the slot was released during the pause")
		}
		if got := q.Resend(); got != f {
			t.Fatalf("Resend = %+v, want the dropped frame", got)
		}
	}
	q.Reply(OpRegister, []string{"a"})

	q.Push(OpRegister, "b")
	q.Next()
	if d, _ := q.Overloaded(); d != time.Second {
		t.Fatalf("a new frame's first OVERLOADED waits %v, want 1s", d)
	}
}

func TestFrameQueuePauseAnsweredOnlyByNamingReply(t *testing.T) {
	q := NewFrameQueue()
	q.Push(OpRegister, "a", "b")
	f := q.Next()
	q.Overloaded()

	if q.FrameError() != nil {
		t.Fatal("an error op during the pause answered the dropped frame")
	}
	if q.Reply(OpRegister, nil) != nil {
		t.Fatal("an empty registered during the pause answered the dropped frame")
	}
	if q.Reply(OpRegister, []string{"other"}) != nil {
		t.Fatal("a registered naming none of the frame's services answered it")
	}
	if !q.Paused() || q.Outstanding() != f {
		t.Fatal("the pause ended without a reply naming the frame")
	}
	q.Resend()
	if d, _ := q.Overloaded(); d != 2*time.Second {
		t.Fatalf("the next OVERLOADED waits %v, want 2s (counter unchanged)", d)
	}
	if got := q.Reply(OpRegister, []string{"b"}); got != f {
		t.Fatalf("a registered naming b during the pause = %+v, want it to settle the frame", got)
	}
	if q.Paused() || q.Outstanding() != nil {
		t.Fatal("the frame is still held after a reply settled it")
	}
}

func TestFrameQueueFrameErrorAnswersOutstanding(t *testing.T) {
	q := NewFrameQueue()
	q.Push(OpRegister, "a")
	f := q.Next()
	q.Overloaded()
	q.Resend()
	if got := q.FrameError(); got != f {
		t.Fatalf("FrameError = %+v, want the outstanding frame", got)
	}
	q.Push(OpRegister, "b")
	q.Next()
	if d, _ := q.Overloaded(); d != time.Second {
		t.Fatalf("OVERLOADED after a whole-frame error waits %v, want 1s", d)
	}
}

func TestFrameQueueFoldIntoPausedRegister(t *testing.T) {
	q := NewFrameQueue()
	q.Push(OpRegister, "a", "b")
	q.Next()
	q.Overloaded()

	q.Push(OpRegister, "c")   // folds into the re-send
	q.Push(OpUnregister, "a") // leaves the re-send, unregister queued
	q.Push(OpRegister, "b")   // already in the re-send
	q.Push(OpUnregister, "d") // queued
	q.Push(OpRegister, "d")   // owed unregister cancelled, folds in
	q.Push(OpUnregister, "e")

	wantFrame(t, q.Resend(), OpRegister, "b", "c", "d")
	if q.Next() != nil {
		t.Fatal("a second frame went out with the re-send outstanding")
	}
	q.Reply(OpRegister, []string{"b", "c", "d"})
	wantFrame(t, q.Next(), OpUnregister, "a", "e")
}

func TestFrameQueuePausedUnregisterFoldsNothing(t *testing.T) {
	q := NewFrameQueue()
	q.Push(OpUnregister, "a")
	q.Next()
	q.Overloaded()
	q.Push(OpRegister, "b")
	wantFrame(t, q.Resend(), OpUnregister, "a")
	q.Reply(OpUnregister, []string{"a"})
	wantFrame(t, q.Next(), OpRegister, "b")
}

func TestFrameQueueEmptiedResendFreesSlot(t *testing.T) {
	q := NewFrameQueue()
	q.Push(OpRegister, "a")
	q.Next()
	q.Overloaded()
	q.Push(OpUnregister, "a")
	if got := q.Resend(); got != nil {
		t.Fatalf("Resend of a frame with no names left = %+v, want nil", got)
	}
	if q.Outstanding() != nil {
		t.Fatal("the slot stayed reserved for an empty re-send")
	}
	wantFrame(t, q.Next(), OpUnregister, "a")
}

func TestFrameQueueRestart(t *testing.T) {
	q := NewFrameQueue()
	q.Push(OpRegister, "x")
	q.Next()
	q.Push(OpUnregister, "b")
	q.Push(OpRegister, "a", "z")
	q.Overloaded()

	q.Restart([]string{"a", "c"})
	if q.Paused() || q.Outstanding() != nil {
		t.Fatal("Restart kept the previous connection's outstanding frame")
	}
	wantFrame(t, q.Next(), OpRegister, "a", "c")
	q.Reply(OpRegister, []string{"a", "c"})
	// The queued unregister is dropped: the new connection holds nothing.
	wantFrame(t, q.Next(), OpRegister, "z")
	q.Reply(OpRegister, []string{"z"})
	if !q.Idle() {
		t.Fatalf("queue not idle: %+v", q.queued)
	}
}

func TestFrameQueueCloseBesideOutstandingRegister(t *testing.T) {
	q := NewFrameQueue()
	q.Push(OpRegister, "a", "b")
	reg := q.Next()
	q.Push(OpUnregister, "c")
	q.Push(OpRegister, "d")

	unreg := q.Close("a", "b", "c")
	wantFrame(t, unreg, OpUnregister, "a", "b", "c")
	if q.Next() != nil {
		t.Fatal("Next handed out a third frame")
	}
	if got := q.Reply(OpRegister, []string{"a", "b"}); got != reg {
		t.Fatalf("the late registered settled %+v, want the register", got)
	}
	if q.Outstanding() != unreg {
		t.Fatal("the closing unregister did not take the slot once the register was answered")
	}
	if got := q.Reply(OpUnregister, []string{"a"}); got != unreg {
		t.Fatalf("unregistered settled %+v, want the closing unregister", got)
	}
	if !q.Idle() {
		t.Fatalf("queue not idle: the queued register should be gone, %+v", q.queued)
	}
}

func TestFrameQueueCloseUnregisteredBeforeRegistered(t *testing.T) {
	q := NewFrameQueue()
	q.Push(OpRegister, "a")
	reg := q.Next()
	unreg := q.Close("a")
	if got := q.Reply(OpUnregister, []string{"a"}); got != unreg {
		t.Fatalf("unregistered settled %+v, want the closing unregister", got)
	}
	if got := q.Reply(OpRegister, []string{"a"}); got != reg {
		t.Fatalf("registered settled %+v, want the register", got)
	}
	if !q.Idle() {
		t.Fatal("queue not idle")
	}
}

func TestFrameQueueClosingAbandonsOverloadedRegister(t *testing.T) {
	q := NewFrameQueue()
	q.Push(OpRegister, "a")
	q.Next()
	unreg := q.Close("a")
	if _, ok := q.Overloaded(); ok {
		t.Fatal("OVERLOADED while closing scheduled a register re-send")
	}
	if q.Outstanding() != unreg || q.Paused() {
		t.Fatal("the abandoned register's slot did not pass to the closing unregister")
	}
	// The unregister itself is re-sent after OVERLOADED.
	if d, ok := q.Overloaded(); !ok || d != time.Second {
		t.Fatalf("OVERLOADED on the closing unregister = %v, %v, want 1s, true", d, ok)
	}
	wantFrame(t, q.Resend(), OpUnregister, "a")
}

func TestFrameQueueCloseWithNothingOutstanding(t *testing.T) {
	q := NewFrameQueue()
	if f := q.Close("a"); f != nil {
		t.Fatalf("Close with the slot free = %+v, want nil (Next sends it)", f)
	}
	wantFrame(t, q.Next(), OpUnregister, "a")
}

func TestFrameQueueFail(t *testing.T) {
	q := NewFrameQueue()
	q.Push(OpUnregister, "a")
	f := q.Next()
	q.Push(OpRegister, "b")
	if !q.Fail(f) {
		t.Fatal("Fail of the outstanding frame = false")
	}
	if q.Fail(f) {
		t.Fatal("a second Fail of the same frame = true")
	}
	wantFrame(t, q.Next(), OpRegister, "b")
}

func TestFrameQueueRestartBatches(t *testing.T) {
	names := make([]string, 150)
	for i := range names {
		names[i] = fmt.Sprintf("s%03d", i)
	}
	q := NewFrameQueue()
	q.Restart(names)
	wantFrame(t, q.Next(), OpRegister, names[:100]...)
	q.Reply(OpRegister, names[:100])
	wantFrame(t, q.Next(), OpRegister, names[100:]...)
	q.Restart(nil)
	if !q.Idle() {
		t.Fatal("Restart with no names queued something")
	}
}

func TestFrameQueueDropRegisters(t *testing.T) {
	q := NewFrameQueue()
	q.Push(OpRegister, "a")
	q.Next()
	q.Push(OpUnregister, "b")
	q.Push(OpRegister, "c")
	if q.DropRegisters() {
		t.Fatal("DropRegisters freed a register that is on the wire")
	}
	q.Overloaded()
	if !q.DropRegisters() {
		t.Fatal("DropRegisters kept a register paused by OVERLOADED")
	}
	wantFrame(t, q.Next(), OpUnregister, "b")
	q.Reply(OpUnregister, []string{"b"})
	if !q.Idle() {
		t.Fatalf("a queued register survived DropRegisters: %+v", q.queued)
	}
}
