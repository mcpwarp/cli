package appproto

import (
	"slices"
	"time"
)

// MaxServicesPerFrame is the most names one register/unregister carries:
// ws-mixer-spec MCPWARP.md §2.1 Batch's MAX_SERVICES_PER_MESSAGE
// (mcpwarp.MaxServicesPerMessage; internal/tunnel's tests check the two
// agree).
const MaxServicesPerFrame = 100

// maxResendDelay caps ResendDelay (ws-mixer-spec MCPWARP.md §2.6, §3.2).
const maxResendDelay = 30 * time.Second

// ResendDelay is the backoff ws-mixer-spec MCPWARP.md §2.6's OVERLOADED
// re-send and §3.2's transient retry share, each with its own counter:
// min(1000 × 2^(n−1), 30000) ms for the n-th consecutive failure (1s, 2s,
// 4s … 30s). An n below 1 counts as 1.
func ResendDelay(n int) time.Duration {
	d := time.Second
	for i := 1; i < n && d < maxResendDelay; i++ {
		d *= 2
	}
	return min(d, maxResendDelay)
}

// Frame is one register or unregister on its way to the tunnel.
type Frame struct {
	Op    string // OpRegister or OpUnregister
	Names []string
}

// FrameQueue is ws-mixer-spec MCPWARP.md §2.6's Frames in flight discipline
// for one connection's register/unregister frames: at most one outstanding
// (on the wire, or waiting out an OVERLOADED pause), everything else queued
// behind it in order, at most MaxServicesPerFrame names per frame (§2.1
// Batch). Intent for a name supersedes any earlier intent for it that
// hasn't gone out yet. It does no I/O and keeps no clock: the caller writes
// what Next, Resend and Close return and runs the delay Overloaded returns.
// Not safe for concurrent use.
//
// The one exception to one in flight is the closing unregister (§3.2
// Unregister: only close() MAY send its unregister while a register is
// outstanding): Close hands it out at once beside a register on the wire,
// held as behind. Replies still settle by frame order: the register's reply
// (or an error op, or OVERLOADED, which abandons a register while closing)
// settles the register and makes behind the outstanding frame; an
// "unregistered" settles behind directly.
type FrameQueue struct {
	outstanding *Frame
	// paused: OVERLOADED dropped outstanding unprocessed; it stays reserved
	// for the re-send (§2.6 One in flight), so nothing is on the wire.
	paused bool
	// overloaded counts consecutive OVERLOADED errors for outstanding, the
	// n of its re-send delay; reset whenever the slot is freed.
	overloaded int
	// behind is the closing unregister on the wire behind an outstanding
	// register, nil otherwise.
	behind *Frame
	// closing is set by Close: a register dropped by OVERLOADED is
	// abandoned, never re-sent.
	closing bool
	queued  []*Frame
}

// NewFrameQueue returns an empty FrameQueue.
func NewFrameQueue() *FrameQueue { return &FrameQueue{} }

// Push records op (OpRegister or OpUnregister) for each name, in order. A
// name's earlier queued intent is dropped first, so register-then-unregister
// before either goes out leaves only the unregister, and the reverse only
// the register. During an OVERLOADED pause of a register (§2.6 One in
// flight, §6A.7) a register folds into its re-send, after the names already
// there, while it stays within the batch cap; an unregister takes its name
// out of the re-send and queues behind it. A dropped unregister folds
// nothing in. Anything else joins the last queued frame when that has the
// same op and room, or starts a new one, so distinct frames keep their
// order.
func (q *FrameQueue) Push(op string, names ...string) {
	for _, name := range names {
		q.push(op, name)
	}
}

func (q *FrameQueue) push(op, name string) {
	q.queued = withoutName(q.queued, name, "")
	if q.paused && q.outstanding.Op == OpRegister {
		switch {
		case op == OpUnregister:
			q.outstanding.Names = slices.DeleteFunc(q.outstanding.Names, func(n string) bool { return n == name })
		case slices.Contains(q.outstanding.Names, name):
			return
		case len(q.outstanding.Names) < MaxServicesPerFrame:
			q.outstanding.Names = append(q.outstanding.Names, name)
			return
		}
	}
	if n := len(q.queued); n > 0 {
		if last := q.queued[n-1]; last.Op == op && len(last.Names) < MaxServicesPerFrame {
			last.Names = append(last.Names, name)
			return
		}
	}
	q.queued = append(q.queued, &Frame{Op: op, Names: []string{name}})
}

// withoutName removes name from every frame (of op only, unless op is "")
// and drops the frames it empties.
func withoutName(frames []*Frame, name, op string) []*Frame {
	return slices.DeleteFunc(frames, func(f *Frame) bool {
		if op == "" || f.Op == op {
			f.Names = slices.DeleteFunc(f.Names, func(n string) bool { return n == name })
		}
		return len(f.Names) == 0
	})
}

// Next takes the first queued frame as the outstanding one and returns it
// for the caller to write, or returns nil while a frame is outstanding or
// nothing is queued.
func (q *FrameQueue) Next() *Frame {
	if q.outstanding != nil || len(q.queued) == 0 {
		return nil
	}
	q.outstanding = q.queued[0]
	q.queued = q.queued[1:]
	return q.outstanding
}

// Overloaded records an OVERLOADED error op: it drops the outstanding frame
// unprocessed and returns how long to wait before Resend (§2.6 OVERLOADED
// re-send). With nothing on the wire (no frame outstanding, or one already
// paused) it answers nothing and returns false. While closing, a dropped
// register is abandoned instead (§3.2 Unregister): the slot moves on and it
// returns false.
func (q *FrameQueue) Overloaded() (time.Duration, bool) {
	if q.outstanding == nil || q.paused {
		return 0, false
	}
	if q.closing && q.outstanding.Op == OpRegister {
		q.settle()
		return 0, false
	}
	q.overloaded++
	q.paused = true
	return ResendDelay(q.overloaded), true
}

// Resend ends an OVERLOADED pause: it returns the paused frame, as Push may
// have changed it, for the caller to write again, still outstanding. A
// frame with no names left is not sent: the slot is freed and Resend
// returns nil (§2.6 One in flight). Nil too when nothing is paused.
func (q *FrameQueue) Resend() *Frame {
	if !q.paused {
		return nil
	}
	q.paused = false
	if len(q.outstanding.Names) == 0 {
		q.settle()
		return nil
	}
	return q.outstanding
}

// Reply records a "registered" (op OpRegister) or "unregistered" (op
// OpUnregister) reply listing names, and returns the frame it settles, or
// nil. On the wire any reply of the frame's op settles it; once OVERLOADED
// has paused it, only one naming one of its services does (§2.6 OVERLOADED
// re-send), and an empty reply answers nothing. An "unregistered" with a
// register still outstanding settles the closing unregister behind it.
func (q *FrameQueue) Reply(op string, names []string) *Frame {
	f := q.outstanding
	if f != nil && f.Op != op && q.behind != nil && q.behind.Op == op {
		b := q.behind
		q.behind = nil
		return b
	}
	if f == nil || f.Op != op {
		return nil
	}
	if q.paused && !slices.ContainsFunc(names, func(n string) bool { return slices.Contains(f.Names, n) }) {
		return nil
	}
	q.settle()
	return f
}

// FrameError records a whole-frame error op with any code other than
// OVERLOADED and CONNECTION_LIMIT, and returns the outstanding frame it
// answers (§2.6 One in flight). During an OVERLOADED pause nothing is on
// the wire, so it answers nothing and returns nil.
func (q *FrameQueue) FrameError() *Frame {
	f := q.outstanding
	if f == nil || q.paused {
		return nil
	}
	q.settle()
	return f
}

// Fail records that writing f failed, so no reply can follow: f stops
// holding its slot. It reports false when f no longer holds one (a stale
// failure: f was settled, or OVERLOADED has paused it since).
func (q *FrameQueue) Fail(f *Frame) bool {
	switch {
	case f == q.behind:
		q.behind = nil
	case f == q.outstanding && !q.paused:
		q.settle()
	default:
		return false
	}
	return true
}

// Abandon frees the slot without a reply: the outstanding frame's
// connection ended. Queued intent stays.
func (q *FrameQueue) Abandon() {
	q.behind = nil
	q.settle()
}

// Restart begins a new connection (§3.2 Register on every welcome): the
// outstanding frame is abandoned, names folded into it included, so names
// must list every name still wanted; their register frames, in order and in
// batches, go ahead of the queued intent, from which a register of any of
// those names is dropped as already covered, and every unregister, since a
// new connection holds nothing (§6A.2 may hold).
func (q *FrameQueue) Restart(names []string) {
	q.Abandon()
	q.queued = slices.DeleteFunc(q.queued, func(f *Frame) bool { return f.Op == OpUnregister })
	for _, name := range names {
		q.queued = withoutName(q.queued, name, OpRegister)
	}
	var front []*Frame
	for chunk := range slices.Chunk(names, MaxServicesPerFrame) {
		front = append(front, &Frame{Op: OpRegister, Names: slices.Clone(chunk)})
	}
	q.queued = append(front, q.queued...)
}

// DropRegisters removes every register not on the wire, queued or paused by
// OVERLOADED: while closing a dropped register is abandoned, never re-sent
// (§3.2 Unregister). It reports whether it freed the slot.
func (q *FrameQueue) DropRegisters() bool {
	q.queued = slices.DeleteFunc(q.queued, func(f *Frame) bool { return f.Op == OpRegister })
	if q.paused && q.outstanding.Op == OpRegister {
		q.settle()
		return true
	}
	return false
}

// Close starts closing (§3.2 Unregister): registers not on the wire are
// dropped (DropRegisters; the caller cancels a paused one's re-send) and
// an unregister for names is due. With a register on the wire it returns
// that unregister (its first batch) to write at once, held as behind;
// otherwise it queues it for Next and returns nil.
func (q *FrameQueue) Close(names ...string) *Frame {
	q.closing = true
	q.DropRegisters()
	if len(names) == 0 {
		return nil
	}
	if q.outstanding == nil || q.outstanding.Op != OpRegister || q.behind != nil {
		q.Push(OpUnregister, names...)
		return nil
	}
	first := names[:min(len(names), MaxServicesPerFrame)]
	for _, name := range first {
		q.queued = withoutName(q.queued, name, "")
	}
	q.behind = &Frame{Op: OpUnregister, Names: slices.Clone(first)}
	q.Push(OpUnregister, names[len(first):]...)
	return q.behind
}

// Outstanding returns the frame holding the slot, or nil.
func (q *FrameQueue) Outstanding() *Frame { return q.outstanding }

// Paused reports whether the outstanding frame is waiting out an OVERLOADED
// pause.
func (q *FrameQueue) Paused() bool { return q.paused }

// Idle reports whether no frame is outstanding or queued.
func (q *FrameQueue) Idle() bool { return q.outstanding == nil && len(q.queued) == 0 }

// settle frees the outstanding frame's slot; a closing unregister behind it
// takes the slot, still on the wire.
func (q *FrameQueue) settle() {
	q.outstanding = q.behind
	q.behind = nil
	q.paused = false
	q.overloaded = 0
}
