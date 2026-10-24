package tunnel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mcpwarp/cli/internal/appproto"
	"github.com/mcpwarp/cli/internal/eventbus"
	"github.com/mcpwarp/cli/internal/fakeedge"
	"github.com/mcpwarp/cli/internal/registry"
	"github.com/mcpwarp/ws-mixer-go/mcpwarp"
	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// These tests pin ws-mixer-spec MCPWARP.md §2.6's frames in flight and §3.2's
// transient retry (§11 rows 49 and 50): every register/unregister the client
// sends is read off scriptedEdge.frames and answered by the test, and every
// re-send delay is a fakeClock timer the test fires, so nothing waits on
// wall-clock time.

const sentinelLog = "ignoring unrecognized mcpwarp app message"

// fakeClock is Config.afterFunc stepped by hand: each scheduled func lands on
// armed and runs only when the test fires it.
type fakeClock struct{ armed chan *fakeTimer }

func newFakeClock() *fakeClock { return &fakeClock{armed: make(chan *fakeTimer, 64)} }

func (c *fakeClock) afterFunc(d time.Duration, f func()) func() bool {
	ft := &fakeTimer{d: d, f: f, stopped: make(chan struct{})}
	c.armed <- ft
	return ft.stop
}

// next returns the next timer scheduled, waiting for it.
func (c *fakeClock) next(t *testing.T) *fakeTimer {
	t.Helper()
	select {
	case ft := <-c.armed:
		return ft
	case <-time.After(fakeedge.DefaultWait):
		t.Fatal("no re-send was scheduled")
		return nil
	}
}

// none fails if a timer was scheduled and not yet taken by next.
func (c *fakeClock) none(t *testing.T) {
	t.Helper()
	select {
	case ft := <-c.armed:
		t.Fatalf("unexpected re-send scheduled after %v", ft.d)
	default:
	}
}

type fakeTimer struct {
	d       time.Duration
	f       func()
	once    sync.Once
	stopped chan struct{}
}

func (ft *fakeTimer) stop() bool {
	first := false
	ft.once.Do(func() {
		close(ft.stopped)
		first = true
	})
	return first
}

func (ft *fakeTimer) isStopped() bool {
	select {
	case <-ft.stopped:
		return true
	default:
		return false
	}
}

func (ft *fakeTimer) waitStopped(t *testing.T, why string) {
	t.Helper()
	select {
	case <-ft.stopped:
	case <-time.After(fakeedge.DefaultWait):
		t.Fatalf("the %v timer was never cancelled: %s", ft.d, why)
	}
}

// fire runs the timer's func even if it was stopped, as a timer that had
// already fired when stop was called would have.
func (ft *fakeTimer) fire() { ft.f() }

// logWatch is a slog.Handler a test can wait on for a message.
type logWatch struct {
	mu      sync.Mutex
	msgs    []string
	changed chan struct{}
}

func newLogWatch() *logWatch { return &logWatch{changed: make(chan struct{})} }

func (l *logWatch) Enabled(context.Context, slog.Level) bool { return true }
func (l *logWatch) WithAttrs([]slog.Attr) slog.Handler       { return l }
func (l *logWatch) WithGroup(string) slog.Handler            { return l }
func (l *logWatch) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	l.msgs = append(l.msgs, r.Message)
	close(l.changed)
	l.changed = make(chan struct{})
	l.mu.Unlock()
	return nil
}

func (l *logWatch) count(msg string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, m := range l.msgs {
		if m == msg {
			n++
		}
	}
	return n
}

func (l *logWatch) wait(t *testing.T, msg string, n int) {
	t.Helper()
	deadline := time.After(fakeedge.DefaultWait)
	for {
		l.mu.Lock()
		changed := l.changed
		l.mu.Unlock()
		if l.count(msg) >= n {
			return
		}
		select {
		case <-changed:
		case <-deadline:
			t.Fatalf("log %q seen %d times, want %d", msg, l.count(msg), n)
		}
	}
}

// scriptedEdge hands every register/unregister to the test on frames, which
// answers it. replies counts the answers the test has sent, so a frame can
// tell how many it arrived after.
type scriptedEdge struct {
	*fakeedge.Server
	frames  chan edgeFrame
	replies atomic.Int32
}

type edgeFrame struct {
	c       *fakeedge.Conn
	f       fakeedge.Frame
	replies int32
}

func newScriptedEdge(t *testing.T) *scriptedEdge {
	e := &scriptedEdge{frames: make(chan edgeFrame, 64)}
	e.Server = fakeedge.New(t, fakeedge.Options{OnApp: func(c *fakeedge.Conn, f fakeedge.Frame) {
		if f.Op == "register" || f.Op == "unregister" {
			e.frames <- edgeFrame{c: c, f: f, replies: e.replies.Load()}
		}
	}})
	return e
}

// next takes the next frame, which must be op for exactly names.
func (e *scriptedEdge) next(t *testing.T, op string, names ...string) edgeFrame {
	t.Helper()
	select {
	case ef := <-e.frames:
		if ef.f.Op != op || !slices.Equal(ef.f.Names, names) {
			t.Fatalf("frame = %s %v, want %s %v", ef.f.Op, ef.f.Names, op, names)
		}
		return ef
	case <-time.After(fakeedge.DefaultWait):
		t.Fatalf("no %s %v arrived", op, names)
		return edgeFrame{}
	}
}

// none fails if a frame arrived that next hasn't taken.
func (e *scriptedEdge) none(t *testing.T) {
	t.Helper()
	select {
	case ef := <-e.frames:
		t.Fatalf("unexpected %s %v", ef.f.Op, ef.f.Names)
	default:
	}
}

func (e *scriptedEdge) registered(t *testing.T, c *fakeedge.Conn, services []fakeedge.Service, errs []fakeedge.RegisterError) {
	t.Helper()
	e.replies.Add(1)
	if err := c.Registered(services, errs); err != nil {
		t.Fatalf("Registered: %v", err)
	}
}

func (e *scriptedEdge) unregistered(t *testing.T, c *fakeedge.Conn, names ...string) {
	t.Helper()
	e.replies.Add(1)
	if err := c.Unregistered(names...); err != nil {
		t.Fatalf("Unregistered: %v", err)
	}
}

func sendError(t *testing.T, c *fakeedge.Conn, code string) {
	t.Helper()
	if err := c.SendError(code, "edge says "+code); err != nil {
		t.Fatalf("SendError: %v", err)
	}
}

// settle has the edge send a sentinel and waits until the client logged it:
// everything sent before it has been handled.
func settle(t *testing.T, c *fakeedge.Conn, logs *logWatch) {
	t.Helper()
	want := logs.count(sentinelLog) + 1
	if err := c.Sentinel(); err != nil {
		t.Fatalf("Sentinel: %v", err)
	}
	logs.wait(t, sentinelLog, want)
}

type framesHarness struct {
	edge  *scriptedEdge
	clock *fakeClock
	logs  *logWatch
	exits chan int
	tun   *Tunnel
}

func startFramesHarness(t *testing.T, names ...string) *framesHarness {
	t.Helper()
	return startFramesHarnessOn(t, newTestBus(t), names...)
}

// startFramesHarnessOn is startFramesHarness publishing onto bus.
func startFramesHarnessOn(t *testing.T, bus *eventbus.Bus, names ...string) *framesHarness {
	t.Helper()
	h := &framesHarness{edge: newScriptedEdge(t), clock: newFakeClock(), logs: newLogWatch(), exits: make(chan int, 4)}
	services := make([]appproto.RegisterService, len(names))
	for i, n := range names {
		services[i] = appproto.RegisterService{Name: n, Kind: "http"}
	}
	tun, err := Start(context.Background(), Config{
		URL:         h.edge.URL,
		TokenSource: staticToken{token: "t"},
		Services:    services,
		Bus:         bus,
		Log:         slog.New(h.logs),
		FatalExit:   func(code int) { h.exits <- code },
		Reconnect:   wsmixer.ReconnectOptions{Base: 10 * time.Millisecond, Cap: 50 * time.Millisecond},
		afterFunc:   h.clock.afterFunc,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = tun.Close(context.Background()) })
	h.tun = tun
	return h
}

func (h *framesHarness) row(name string) registry.Row {
	for _, r := range h.tun.Registry().Rows() {
		if r.Name == name {
			return r
		}
	}
	return registry.Row{Name: name}
}

func (h *framesHarness) noFatal(t *testing.T, why string) {
	t.Helper()
	if n := h.logs.count("No service could be registered."); n != 0 {
		t.Fatalf("G5 fired: %s", why)
	}
	select {
	case code := <-h.exits:
		t.Fatalf("FatalExit(%d): %s", code, why)
	default:
	}
}

func (h *framesHarness) waitFatal(t *testing.T) int {
	t.Helper()
	select {
	case code := <-h.exits:
		return code
	case <-time.After(fakeedge.DefaultWait):
		t.Fatal("FatalExit was never called")
		return 0
	}
}

// TestFramesOneInFlightFoldsIntent: with the welcome register outstanding,
// local d/e intent queues behind it and later intent for a name supersedes
// earlier: unregister+register of a leaves a registered, register+unregister
// of c leaves c unregistered. Distinct frames keep their order and each goes
// out only once the one before it was answered.
func TestFramesOneInFlightFoldsIntent(t *testing.T) {
	h := startFramesHarness(t, "a", "b", "c")
	first := h.edge.next(t, "register", "a", "b", "c")

	_ = h.tun.UnregisterService("a")
	h.tun.RegisterService("a")
	_ = h.tun.UnregisterService("b")
	h.tun.RegisterService("c")
	_ = h.tun.UnregisterService("c")
	h.edge.registered(t, first.c, fakeedge.Accepted("a", "b", "c"), nil)

	reg := h.edge.next(t, "register", "a")
	if reg.replies != 1 {
		t.Fatalf("register [a] went out after %d replies, want 1 (after the welcome register's)", reg.replies)
	}
	h.edge.registered(t, reg.c, fakeedge.Accepted("a"), nil)
	unreg := h.edge.next(t, "unregister", "b", "c")
	if unreg.replies != 2 {
		t.Fatalf("unregister [b c] went out after %d replies, want 2 (one frame in flight)", unreg.replies)
	}
	h.edge.unregistered(t, unreg.c, "b", "c")
	settle(t, unreg.c, h.logs)
	h.edge.none(t)
}

// TestFramesBatchesOfAtMost100: a list over MaxServicesPerMessage goes out
// as consecutive frames, the next only after the previous was answered.
func TestFramesBatchesOfAtMost100(t *testing.T) {
	names := make([]string, 150)
	for i := range names {
		names[i] = fmt.Sprintf("s%03d", i)
	}
	h := startFramesHarness(t, names...)
	first := h.edge.next(t, "register", names[:100]...)
	h.edge.registered(t, first.c, fakeedge.Accepted(names[:100]...), nil)
	second := h.edge.next(t, "register", names[100:]...)
	if second.replies != 1 {
		t.Fatalf("the second batch went out after %d replies, want 1", second.replies)
	}
}

// TestFramesOverloadedResendBackoff: each OVERLOADED re-sends the dropped
// frame after 1s, 2s, 4s … capped at 30s (§2.6 OVERLOADED re-send), never
// dropping it.
func TestFramesOverloadedResendBackoff(t *testing.T) {
	h := startFramesHarness(t, "svc")
	ef := h.edge.next(t, "register", "svc")
	for _, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second} {
		sendError(t, ef.c, "OVERLOADED")
		ft := h.clock.next(t)
		if ft.d != want {
			t.Fatalf("re-send delay = %v, want %v", ft.d, want)
		}
		h.edge.none(t)
		ft.fire()
		ef = h.edge.next(t, "register", "svc")
	}
	h.edge.registered(t, ef.c, fakeedge.Accepted("svc"), nil)
	settle(t, ef.c, h.logs)
	if r := h.row("svc"); r.URL == "" || r.Rejected != "" {
		t.Fatalf("svc row = %+v, want active once the re-send was answered", r)
	}
	h.clock.none(t)
}

// TestFramesRegisteredDuringPauseSettles: a "registered" naming the dropped
// frame's services during the pause settles it: the re-send is cancelled (a
// timer that fires anyway sends nothing) and the OVERLOADED counter resets.
func TestFramesRegisteredDuringPauseSettles(t *testing.T) {
	h := startFramesHarness(t, "a", "b")
	ef := h.edge.next(t, "register", "a", "b")
	sendError(t, ef.c, "OVERLOADED")
	first := h.clock.next(t)
	sendError(t, ef.c, "OVERLOADED")
	settle(t, ef.c, h.logs)
	h.clock.none(t)

	h.edge.registered(t, ef.c, fakeedge.Accepted("a", "b"), nil)
	first.waitStopped(t, "a registered naming the frame's services arrived during the pause")
	first.fire()
	h.tun.RegisterService("a")
	ef = h.edge.next(t, "register", "a")

	sendError(t, ef.c, "OVERLOADED")
	if ft := h.clock.next(t); ft.d != time.Second {
		t.Fatalf("first OVERLOADED after the settled pause waits %v, want 1s (counter reset)", ft.d)
	}
}

// TestFramesErrorOrEmptyReplyDuringPauseAnswersNothing: during the pause
// nothing is on the wire, so an error op or an empty "registered" leaves the
// re-send scheduled and the OVERLOADED counter unchanged.
func TestFramesErrorOrEmptyReplyDuringPauseAnswersNothing(t *testing.T) {
	h := startFramesHarness(t, "svc")
	ef := h.edge.next(t, "register", "svc")
	sendError(t, ef.c, "OVERLOADED")
	first := h.clock.next(t)

	sendError(t, ef.c, "BAD_REQUEST")
	h.edge.registered(t, ef.c, nil, nil)
	settle(t, ef.c, h.logs)
	if first.isStopped() {
		t.Fatal("an error op or an empty reply during the pause cancelled the re-send")
	}
	h.clock.none(t)
	h.edge.none(t)

	first.fire()
	ef = h.edge.next(t, "register", "svc")
	sendError(t, ef.c, "OVERLOADED")
	if ft := h.clock.next(t); ft.d != 2*time.Second {
		t.Fatalf("second OVERLOADED waits %v, want 2s (counter unchanged by the pause's error op)", ft.d)
	}
}

// TestFramesPauseFoldsIntent: intent during the pause folds into the dropped
// register's re-send, still one frame in flight: an enable joins it, a local
// d takes its name out and queues an unregister behind it.
func TestFramesPauseFoldsIntent(t *testing.T) {
	h := startFramesHarness(t, "a", "b", "c")
	ef := h.edge.next(t, "register", "a", "b", "c")
	h.edge.registered(t, ef.c, fakeedge.Accepted("a", "b"), []fakeedge.RegisterError{{Name: "c", Code: "QUOTA_EXCEEDED", Message: "full"}})
	settle(t, ef.c, h.logs)

	h.tun.RegisterService("a")
	ef = h.edge.next(t, "register", "a")
	sendError(t, ef.c, "OVERLOADED")
	ft := h.clock.next(t)
	h.tun.RegisterService("c")
	_ = h.tun.UnregisterService("a")
	h.tun.RegisterService("b")
	settle(t, ef.c, h.logs)
	h.edge.none(t)

	ft.fire()
	ef = h.edge.next(t, "register", "c", "b")
	h.edge.registered(t, ef.c, fakeedge.Accepted("c", "b"), nil)
	ef = h.edge.next(t, "unregister", "a")
	h.edge.unregistered(t, ef.c, "a")
}

// TestFramesWholeFrameErrorAnswersFrame: a whole-frame error op other than
// OVERLOADED and CONNECTION_LIMIT answers the outstanding frame (§6A.6 G7):
// exactly that frame's names read rejected with the code, nothing is
// re-sent, and queued intent goes out next.
func TestFramesWholeFrameErrorAnswersFrame(t *testing.T) {
	names := make([]string, 150)
	for i := range names {
		names[i] = fmt.Sprintf("s%03d", i)
	}
	h := startFramesHarness(t, names...)
	ef := h.edge.next(t, "register", names[:100]...)
	h.edge.registered(t, ef.c, fakeedge.Accepted(names[:100]...), nil)
	ef = h.edge.next(t, "register", names[100:]...)
	_ = h.tun.UnregisterService("s000")
	sendError(t, ef.c, "CONNECTION_LIMIT")
	settle(t, ef.c, h.logs)
	h.edge.none(t)

	sendError(t, ef.c, "BAD_REQUEST")
	h.edge.next(t, "unregister", "s000")
	for _, name := range names[1:100] {
		if r := h.row(name); r.Rejected != "" || r.URL == "" {
			t.Fatalf("first-batch %s row = %+v, want still active", name, r)
		}
	}
	for _, name := range names[100:] {
		if r := h.row(name); r.Rejected != "BAD_REQUEST" || r.Reregistering {
			t.Fatalf("second-batch %s row = %+v, want rejected BAD_REQUEST", name, r)
		}
	}
	h.clock.none(t)
	h.noFatal(t, "a whole-frame BAD_REQUEST after a registration")
}

// TestFramesReconnectRestartsWithFullList: a reconnect drops the in-flight
// frame and its scheduled re-send; the new connection gets the current list
// first, and no queued unregister, since it holds nothing (§6A.2 may hold).
func TestFramesReconnectRestartsWithFullList(t *testing.T) {
	h := startFramesHarness(t, "a", "b", "c")
	ef := h.edge.next(t, "register", "a", "b", "c")
	h.edge.registered(t, ef.c, fakeedge.Accepted("a", "b", "c"), nil)
	settle(t, ef.c, h.logs)

	h.tun.RegisterService("a")
	ef = h.edge.next(t, "register", "a")
	_ = h.tun.UnregisterService("b")
	sendError(t, ef.c, "OVERLOADED")
	ft := h.clock.next(t)
	settle(t, ef.c, h.logs)

	_ = ef.c.Close(uint32(wsmixer.InternalErrorCode), "forcing reconnect for test")
	ft.waitStopped(t, "the connection ended")
	next := h.edge.next(t, "register", "a", "c")
	if next.c == ef.c {
		t.Fatal("the restart's register went out on the old connection")
	}
	h.edge.registered(t, next.c, fakeedge.Accepted("a", "c"), nil)
	ft.fire()
	settle(t, next.c, h.logs)
	h.edge.none(t)
}

// TestFramesTransientRetriedPerName: a transient code (§2.5) is not a
// refusal: the name stays pending and is re-sent alone with §3.2's backoff,
// counted over consecutive all-transient replies, until it gets a final
// answer. The reply's other names keep their outcome.
func TestFramesTransientRetriedPerName(t *testing.T) {
	h := startFramesHarness(t, "a", "b")
	ef := h.edge.next(t, "register", "a", "b")
	h.edge.registered(t, ef.c, fakeedge.Accepted("a"), []fakeedge.RegisterError{{Name: "b", Code: "INTERNAL", Message: "resolve deadline"}})

	steps := []struct {
		code string
		want time.Duration
	}{
		{"", time.Second}, // a mixed reply resets the counter: n = 1
		{"TIMEOUT", time.Second},
		{"UNAVAILABLE", 2 * time.Second},
		{"SERVICE_ERROR", 4 * time.Second},
	}
	for i, s := range steps {
		if i > 0 {
			h.edge.registered(t, ef.c, nil, []fakeedge.RegisterError{{Name: "b", Code: s.code, Message: "try again"}})
		}
		ft := h.clock.next(t)
		if ft.d != s.want {
			t.Fatalf("retry %d waits %v, want %v", i+1, ft.d, s.want)
		}
		if r := h.row("b"); r.Rejected != "" || !r.Reregistering {
			t.Fatalf("b row while its retry is scheduled = %+v, want pending, not rejected", r)
		}
		if r := h.row("a"); r.URL == "" || r.Rejected != "" {
			t.Fatalf("a row = %+v, want active", r)
		}
		ft.fire()
		ef = h.edge.next(t, "register", "b")
	}
	h.edge.registered(t, ef.c, fakeedge.Accepted("b"), nil)
	settle(t, ef.c, h.logs)
	if r := h.row("b"); r.URL == "" || r.Rejected != "" || r.Reregistering {
		t.Fatalf("b row after its retry succeeded = %+v, want active", r)
	}
	if n := h.logs.count("register error"); n != 1 {
		t.Fatalf("\"register error\" logged %d times, want once for the whole retry series", n)
	}
	if n := h.logs.count("register error; still retrying"); n != 3 {
		t.Fatalf("\"register error; still retrying\" logged %d times, want 3", n)
	}
	h.clock.none(t)
	h.noFatal(t, "transient errors")
}

// TestFramesReplyListingNoneOfFrameIsAllTransient: a "registered" that lists
// none of the answered frame's names counts as all-transient (§6A.7
// Correlation), even when it lists other names.
func TestFramesReplyListingNoneOfFrameIsAllTransient(t *testing.T) {
	h := startFramesHarness(t, "a", "b")
	ef := h.edge.next(t, "register", "a", "b")
	h.edge.registered(t, ef.c, fakeedge.Accepted("a"), []fakeedge.RegisterError{{Name: "b", Code: "INTERNAL", Message: "resolve deadline"}})
	for _, want := range []time.Duration{time.Second, time.Second, 2 * time.Second} {
		ft := h.clock.next(t)
		if ft.d != want {
			t.Fatalf("retry waits %v, want %v", ft.d, want)
		}
		ft.fire()
		ef = h.edge.next(t, "register", "b")
		h.edge.registered(t, ef.c, fakeedge.Accepted("other"), nil)
	}
}

// TestFramesUnlistedNameRetried: a name of the answered register that the
// reply leaves out is a transient failure (§6A.7 Correlation).
func TestFramesUnlistedNameRetried(t *testing.T) {
	h := startFramesHarness(t, "a", "b")
	ef := h.edge.next(t, "register", "a", "b")
	h.edge.registered(t, ef.c, fakeedge.Accepted("a"), nil)
	ft := h.clock.next(t)
	if ft.d != time.Second {
		t.Fatalf("retry waits %v, want 1s", ft.d)
	}
	ft.fire()
	h.edge.next(t, "register", "b")
}

// TestFramesFatalOnlyOnceEveryServiceRefused: G5 is judged on the overall
// state (§6A.6): a refusal next to a transient error isn't fatal while the
// transient name is retried; once it is refused too, with nothing ever
// registered, it is.
func TestFramesFatalOnlyOnceEveryServiceRefused(t *testing.T) {
	h := startFramesHarness(t, "a", "b")
	ef := h.edge.next(t, "register", "a", "b")
	h.edge.registered(t, ef.c, nil, []fakeedge.RegisterError{
		{Name: "a", Code: "QUOTA_EXCEEDED", Message: "full"},
		{Name: "b", Code: "INTERNAL", Message: "resolve deadline"},
	})
	ft := h.clock.next(t)
	settle(t, ef.c, h.logs)
	h.noFatal(t, "b is still being retried")
	if r := h.row("a"); r.Rejected != "QUOTA_EXCEEDED" {
		t.Fatalf("a row = %+v, want rejected QUOTA_EXCEEDED", r)
	}

	ft.fire()
	ef = h.edge.next(t, "register", "b")
	h.edge.registered(t, ef.c, nil, []fakeedge.RegisterError{{Name: "b", Code: "CONFLICT", Message: "name in use"}})
	if code := h.waitFatal(t); code != 1 {
		t.Fatalf("FatalExit(%d), want 1", code)
	}
	if n := h.logs.count("No service could be registered."); n != 1 {
		t.Fatalf("G5 logged %d times, want once", n)
	}
}

// TestFramesServerDisabledKeepsConnection: a SERVER_DISABLED service keeps
// the process up whatever the others end in, and its dashboard enable then
// arrives on this connection and registers it.
func TestFramesServerDisabledKeepsConnection(t *testing.T) {
	h := startFramesHarness(t, "a", "b")
	ef := h.edge.next(t, "register", "a", "b")
	h.edge.registered(t, ef.c, nil, []fakeedge.RegisterError{
		{Name: "a", Code: "SERVER_DISABLED", Message: "disabled"},
		{Name: "b", Code: "NO_RESPONDERS", Message: "no responders"},
	})
	ft := h.clock.next(t)
	ft.fire()
	ef = h.edge.next(t, "register", "b")
	h.edge.registered(t, ef.c, nil, []fakeedge.RegisterError{{Name: "b", Code: "QUOTA_EXCEEDED", Message: "full"}})
	settle(t, ef.c, h.logs)
	h.noFatal(t, "a is SERVER_DISABLED")

	if err := ef.c.Enable("id-a", "a"); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	ef = h.edge.next(t, "register", "a")
	h.edge.registered(t, ef.c, fakeedge.Accepted("a"), nil)
	settle(t, ef.c, h.logs)
	if r := h.row("a"); r.URL == "" || r.Rejected != "" {
		t.Fatalf("a row after the enable = %+v, want active", r)
	}
	h.noFatal(t, "a registered")
}

// TestFramesUnregisterAbandonsPausedRegister: the shutdown unregister goes
// through the same queue; a register paused by OVERLOADED is abandoned,
// never re-sent, and nothing registers after it (§3.2 Unregister).
func TestFramesUnregisterAbandonsPausedRegister(t *testing.T) {
	h := startFramesHarness(t, "svc")
	ef := h.edge.next(t, "register", "svc")
	sendError(t, ef.c, "OVERLOADED")
	ft := h.clock.next(t)

	done := make(chan error, 1)
	go func() { done <- h.tun.Unregister(context.Background()) }()
	ef = h.edge.next(t, "unregister", "svc")
	ft.waitStopped(t, "closing abandons the paused register")
	ft.fire()
	h.tun.RegisterService("svc")
	h.edge.unregistered(t, ef.c, "svc")
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Unregister: %v", err)
		}
	case <-time.After(unregisterBudget):
		t.Fatal("Unregister did not return once its unregister was answered")
	}
	settle(t, ef.c, h.logs)
	h.edge.none(t)
}

func waitUnregister(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * unregisterBudget):
		t.Fatal("Unregister never returned")
		return nil
	}
}

// TestFramesUnregisterBesideOutstandingRegister: the shutdown unregister is
// written at once beside a register still on the wire (§2.6 One in flight:
// only close() may), and the register's late reply still settles it.
func TestFramesUnregisterBesideOutstandingRegister(t *testing.T) {
	h := startFramesHarness(t, "a", "b")
	reg := h.edge.next(t, "register", "a", "b")

	done := make(chan error, 1)
	go func() { done <- h.tun.Unregister(context.Background()) }()
	unreg := h.edge.next(t, "unregister", "a", "b")
	if unreg.replies != 0 {
		t.Fatalf("the shutdown unregister waited for %d replies, want none", unreg.replies)
	}
	h.edge.registered(t, reg.c, fakeedge.Accepted("a", "b"), nil)
	settle(t, reg.c, h.logs)
	select {
	case err := <-done:
		t.Fatalf("Unregister returned %v before its unregister was answered", err)
	default:
	}
	h.edge.unregistered(t, unreg.c, "a", "b")
	if err := waitUnregister(t, done); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	settle(t, unreg.c, h.logs)
	h.edge.none(t)
	h.clock.none(t)
}

// TestFramesClosingAbandonsOverloadedRegister: a register on the wire when
// the shutdown starts and then dropped by OVERLOADED is abandoned, never
// re-sent (§3.2 Unregister), and nothing registers after the shutdown
// began.
func TestFramesClosingAbandonsOverloadedRegister(t *testing.T) {
	h := startFramesHarness(t, "svc")
	reg := h.edge.next(t, "register", "svc")

	done := make(chan error, 1)
	go func() { done <- h.tun.Unregister(context.Background()) }()
	unreg := h.edge.next(t, "unregister", "svc")
	sendError(t, reg.c, "OVERLOADED")
	h.tun.RegisterService("svc")
	settle(t, reg.c, h.logs)
	h.clock.none(t)
	h.edge.none(t)

	h.edge.unregistered(t, unreg.c, "svc")
	if err := waitUnregister(t, done); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	settle(t, unreg.c, h.logs)
	h.edge.none(t)
}

// TestFramesUnregisterSkipsTakenOver: neither the shutdown unregister nor
// anything else unregisters a taken-over name (§3.3 Per name).
func TestFramesUnregisterSkipsTakenOver(t *testing.T) {
	h := startFramesHarness(t, "a", "b")
	ef := h.edge.next(t, "register", "a", "b")
	h.edge.registered(t, ef.c, fakeedge.Accepted("a", "b"), nil)
	if err := ef.c.Conflict("id-b", "b", "taken_over"); err != nil {
		t.Fatalf("Conflict: %v", err)
	}
	settle(t, ef.c, h.logs)

	done := make(chan error, 1)
	go func() { done <- h.tun.Unregister(context.Background()) }()
	ef = h.edge.next(t, "unregister", "a")
	h.edge.unregistered(t, ef.c, "a")
	if err := waitUnregister(t, done); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
}

// TestFramesUnregisterReturnsOnCtx: with no reply, Unregister gives up when
// its ctx does.
func TestFramesUnregisterReturnsOnCtx(t *testing.T) {
	h := startFramesHarness(t, "svc")
	ef := h.edge.next(t, "register", "svc")
	h.edge.registered(t, ef.c, fakeedge.Accepted("svc"), nil)
	settle(t, ef.c, h.logs)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- h.tun.Unregister(ctx) }()
	h.edge.next(t, "unregister", "svc")
	if err := waitUnregister(t, done); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Unregister = %v, want context.DeadlineExceeded", err)
	}
}

// TestFramesUnregisterResentAfterOverloaded: a dropped unregister is
// re-sent after the pause like a register.
func TestFramesUnregisterResentAfterOverloaded(t *testing.T) {
	h := startFramesHarness(t, "a", "b")
	ef := h.edge.next(t, "register", "a", "b")
	h.edge.registered(t, ef.c, fakeedge.Accepted("a", "b"), nil)
	settle(t, ef.c, h.logs)

	_ = h.tun.UnregisterService("a")
	ef = h.edge.next(t, "unregister", "a")
	sendError(t, ef.c, "OVERLOADED")
	ft := h.clock.next(t)
	if ft.d != time.Second {
		t.Fatalf("re-send delay = %v, want 1s", ft.d)
	}
	ft.fire()
	ef = h.edge.next(t, "unregister", "a")
	h.edge.unregistered(t, ef.c, "a")
	settle(t, ef.c, h.logs)
	h.edge.none(t)
}

// TestFramesWriteFailureFreesSlot: a frame whose write failed gets no reply,
// so it stops holding the slot and the next queued frame goes out; a stale
// failure report for it changes nothing.
func TestFramesWriteFailureFreesSlot(t *testing.T) {
	h := startFramesHarness(t, "a", "b")
	ef := h.edge.next(t, "register", "a", "b")
	h.edge.registered(t, ef.c, fakeedge.Accepted("a", "b"), nil)
	settle(t, ef.c, h.logs)

	_ = h.tun.UnregisterService("a")
	h.edge.next(t, "unregister", "a")
	h.tun.RegisterService("a")
	failed := make(chan struct{})
	h.tun.disp.enqueue(func() {
		f := h.tun.frames.Outstanding()
		h.tun.frameWriteFailed(h.tun.framesConn, f, errors.New("write failed"))
		h.tun.frameWriteFailed(h.tun.framesConn, f, errors.New("write failed again"))
		close(failed)
	})
	<-failed
	ef = h.edge.next(t, "register", "a")
	h.edge.registered(t, ef.c, fakeedge.Accepted("a"), nil)
	settle(t, ef.c, h.logs)
	h.edge.none(t)
	if n := h.logs.count("failed to send unregister"); n != 2 {
		t.Fatalf("\"failed to send unregister\" logged %d times, want 2", n)
	}
}

// TestFramesTransientReportedOncePerSeries: the first transient error a name
// gets is reported (Warn + a pending ServiceChanged carrying its code), even
// when its series began with the name left out of a reply, which reports
// nothing; repeats are Debug only; a local d then e starts a new series that
// is reported again.
func TestFramesTransientReportedOncePerSeries(t *testing.T) {
	const reported = "register error"
	h := startFramesHarness(t, "a", "b")
	ef := h.edge.next(t, "register", "a", "b")
	h.edge.registered(t, ef.c, fakeedge.Accepted("a"), nil)
	h.clock.next(t).fire()
	ef = h.edge.next(t, "register", "b")
	h.edge.registered(t, ef.c, nil, []fakeedge.RegisterError{{Name: "b", Code: "INTERNAL", Message: "resolve deadline"}})
	h.clock.next(t).fire()
	ef = h.edge.next(t, "register", "b")
	h.edge.registered(t, ef.c, nil, []fakeedge.RegisterError{{Name: "b", Code: "INTERNAL", Message: "resolve deadline"}})
	settle(t, ef.c, h.logs)
	if n := h.logs.count(reported); n != 1 {
		t.Fatalf("%q logged %d times, want once for the series", reported, n)
	}

	_ = h.tun.UnregisterService("b")
	ef = h.edge.next(t, "unregister", "b")
	h.tun.RegisterService("b")
	h.edge.unregistered(t, ef.c, "b")
	ef = h.edge.next(t, "register", "b")
	h.edge.registered(t, ef.c, nil, []fakeedge.RegisterError{{Name: "b", Code: "INTERNAL", Message: "resolve deadline"}})
	settle(t, ef.c, h.logs)
	if n := h.logs.count(reported); n != 2 {
		t.Fatalf("%q logged %d times, want a second report after d then e", reported, n)
	}
}

// TestFramesStoppedIgnoresLateConnect: an onConnect the SDK had queued
// before Close stopped frames writes nothing, so nothing can add to sendWG
// beside Close's Wait.
func TestFramesStoppedIgnoresLateConnect(t *testing.T) {
	h := startFramesHarness(t, "svc")
	ef := h.edge.next(t, "register", "svc")
	h.edge.registered(t, ef.c, fakeedge.Accepted("svc"), nil)
	settle(t, ef.c, h.logs)

	conn := h.tun.client.Conn()
	if conn == nil {
		t.Fatal("no live connection")
	}
	ran := make(chan *wsmixer.Conn, 1)
	h.tun.disp.enqueue(func() {
		h.tun.stopFrames()
		h.tun.startFrames(conn)
		h.tun.queueFrame(appproto.OpUnregister, "svc")
		ran <- h.tun.framesConn
	})
	if got := <-ran; got != nil {
		t.Fatalf("framesConn = %p after a stopped startFrames, want nil", got)
	}
	settle(t, ef.c, h.logs)
	h.edge.none(t)
}

// serviceRecorder drains a bus's Control, keeping the ServiceChanged events
// and any AppError (which no registration outcome may produce).
type serviceRecorder struct {
	mu      sync.Mutex
	events  []eventbus.ServiceChanged
	appErrs []eventbus.AppError
	seen    int
	changed chan struct{}
}

// newServiceRecorder returns an unbuffered bus, so a Publish returns only
// once the recorder holds the event, and its recorder.
func newServiceRecorder(t *testing.T) (*eventbus.Bus, *serviceRecorder) {
	t.Helper()
	bus := eventbus.New(0)
	r := &serviceRecorder{changed: make(chan struct{})}
	go func() {
		for range bus.Telemetry {
		}
	}()
	go func() {
		for evt := range bus.Control {
			r.mu.Lock()
			switch e := evt.(type) {
			case eventbus.ServiceChanged:
				r.events = append(r.events, e)
			case eventbus.AppError:
				r.appErrs = append(r.appErrs, e)
			}
			close(r.changed)
			r.changed = make(chan struct{})
			r.mu.Unlock()
		}
	}()
	t.Cleanup(bus.Close)
	return bus, r
}

// expect waits for the next len(want) ServiceChanged events and fails unless
// they are exactly want.
func (r *serviceRecorder) expect(t *testing.T, want ...eventbus.ServiceChanged) {
	t.Helper()
	deadline := time.After(fakeedge.DefaultWait)
	for {
		r.mu.Lock()
		n, changed := len(r.events), r.changed
		r.mu.Unlock()
		if n >= r.seen+len(want) {
			break
		}
		select {
		case <-changed:
		case <-deadline:
			r.mu.Lock()
			defer r.mu.Unlock()
			t.Fatalf("got ServiceChanged %+v, want %+v next", r.events[r.seen:], want)
		}
	}
	r.mu.Lock()
	got := slices.Clone(r.events[r.seen : r.seen+len(want)])
	r.seen += len(want)
	r.mu.Unlock()
	if !slices.Equal(got, want) {
		t.Fatalf("ServiceChanged = %+v\nwant %+v", got, want)
	}
}

// none fails if a ServiceChanged arrived that expect hasn't taken, or any
// AppError did; call it after settle.
func (r *serviceRecorder) none(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.events) > r.seen {
		t.Fatalf("unexpected ServiceChanged %+v", r.events[r.seen:])
	}
	if len(r.appErrs) > 0 {
		t.Fatalf("unexpected AppError %+v", r.appErrs)
	}
}

// TestServiceChangedTransitions: every registration transition is published
// once, as one ServiceChanged per changed name and never as an AppError
// (eventbus.ServiceChanged's contract): a welcome's register keeps the
// seeded pending; a success is active with its URL; a transient series is
// pending with its first code, once; a refusal is rejected with code, the
// edge's message and the hint; a dashboard disable and enable, a local d and
// e, a take-over sticky across a dashboard enable and a reconnect, and the e
// that reclaims it.
func TestServiceChangedTransitions(t *testing.T) {
	bus, rec := newServiceRecorder(t)
	h := startFramesHarnessOn(t, bus, "a", "b")
	sc := func(name, state, code, message, url, hint string) eventbus.ServiceChanged {
		return eventbus.ServiceChanged{Name: name, Kind: "http", State: state, Code: code, Message: message, URL: url, Hint: hint}
	}
	active := func(name string) eventbus.ServiceChanged {
		return sc(name, "active", "", "", "https://"+name+".example/mcp", "")
	}
	pending := func(name string) eventbus.ServiceChanged { return sc(name, "pending", "", "", "", "") }

	ef := h.edge.next(t, "register", "a", "b")
	settle(t, ef.c, h.logs)
	rec.none(t)
	h.edge.registered(t, ef.c, fakeedge.Accepted("a"), []fakeedge.RegisterError{{Name: "b", Code: "INTERNAL", Message: "resolve deadline"}})
	rec.expect(t, active("a"), sc("b", "pending", "INTERNAL", "resolve deadline", "", ""))

	h.clock.next(t).fire()
	ef = h.edge.next(t, "register", "b")
	h.edge.registered(t, ef.c, nil, []fakeedge.RegisterError{{Name: "b", Code: "TIMEOUT", Message: "try again"}})
	settle(t, ef.c, h.logs)
	rec.none(t)
	h.clock.next(t).fire()
	ef = h.edge.next(t, "register", "b")
	h.edge.registered(t, ef.c, nil, []fakeedge.RegisterError{{Name: "b", Code: "QUOTA_EXCEEDED", Message: "full"}})
	rec.expect(t, sc("b", "rejected", "QUOTA_EXCEEDED", "full", "", registerErrorHint("QUOTA_EXCEEDED", "")))

	if err := ef.c.Disable("id-a", "paused in the dashboard"); err != nil {
		t.Fatalf("Disable: %v", err)
	}
	rec.expect(t, sc("a", "disabled", "DISABLED", "paused in the dashboard", "", registerErrorHint("SERVER_DISABLED", "")))
	if err := ef.c.Enable("id-a", "a"); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	rec.expect(t, pending("a"))
	ef = h.edge.next(t, "register", "a")
	h.edge.registered(t, ef.c, fakeedge.Accepted("a"), nil)
	rec.expect(t, active("a"))

	_ = h.tun.UnregisterService("b")
	rec.expect(t, sc("b", "disabled", "", "", "", ""))
	ef = h.edge.next(t, "unregister", "b")
	h.edge.unregistered(t, ef.c, "b")
	h.tun.RegisterService("b")
	rec.expect(t, pending("b"))
	ef = h.edge.next(t, "register", "b")
	h.edge.registered(t, ef.c, fakeedge.Accepted("b"), nil)
	rec.expect(t, active("b"))

	if err := ef.c.Conflict("id-a", "a", "taken_over"); err != nil {
		t.Fatalf("Conflict: %v", err)
	}
	rec.expect(t, sc("a", "taken_over", "TAKEN_OVER", "taken_over", "", takenOverHint))
	if err := ef.c.Enable("id-a", "a"); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	settle(t, ef.c, h.logs)
	rec.none(t)

	_ = ef.c.Close(uint32(wsmixer.InternalErrorCode), "forcing reconnect for test")
	ef = h.edge.next(t, "register", "b")
	rec.expect(t, pending("b"))
	h.edge.registered(t, ef.c, fakeedge.Accepted("b"), nil)
	rec.expect(t, active("b"))

	h.tun.RegisterService("a")
	rec.expect(t, pending("a"))
	ef = h.edge.next(t, "register", "a")
	h.edge.registered(t, ef.c, fakeedge.Accepted("a"), nil)
	rec.expect(t, active("a"))
	settle(t, ef.c, h.logs)
	rec.none(t)
	h.noFatal(t, "every service registered at some point")
}

// TestServiceChangedUnlistedName: a name the reply to its register leaves
// out starts a retry series shown as pending INTERNAL "unlisted in reply"
// (§6A.7) without a log; the series' first real transient error replaces
// that on the row (republished pending with its code) and is logged once,
// its repeats change neither, and its success makes it active.
func TestServiceChangedUnlistedName(t *testing.T) {
	bus, rec := newServiceRecorder(t)
	h := startFramesHarnessOn(t, bus, "a", "b")
	ef := h.edge.next(t, "register", "a", "b")
	h.edge.registered(t, ef.c, fakeedge.Accepted("a"), nil)
	rec.expect(t,
		eventbus.ServiceChanged{Name: "a", Kind: "http", State: "active", URL: "https://a.example/mcp"},
		eventbus.ServiceChanged{Name: "b", Kind: "http", State: "pending", Code: "INTERNAL", Message: "unlisted in reply"},
	)

	h.clock.next(t).fire()
	ef = h.edge.next(t, "register", "b")
	h.edge.registered(t, ef.c, nil, []fakeedge.RegisterError{{Name: "b", Code: "TIMEOUT", Message: "resolve deadline"}})
	rec.expect(t, eventbus.ServiceChanged{Name: "b", Kind: "http", State: "pending", Code: "TIMEOUT", Message: "resolve deadline"})

	h.clock.next(t).fire()
	ef = h.edge.next(t, "register", "b")
	h.edge.registered(t, ef.c, nil, []fakeedge.RegisterError{{Name: "b", Code: "UNAVAILABLE", Message: "try again"}})
	settle(t, ef.c, h.logs)
	rec.none(t)
	if n := h.logs.count("register error"); n != 1 {
		t.Fatalf("\"register error\" logged %d times, want once for the series' first transient error", n)
	}

	h.clock.next(t).fire()
	ef = h.edge.next(t, "register", "b")
	h.edge.registered(t, ef.c, fakeedge.Accepted("b"), nil)
	rec.expect(t, eventbus.ServiceChanged{Name: "b", Kind: "http", State: "active", URL: "https://b.example/mcp"})
	settle(t, ef.c, h.logs)
	rec.none(t)
}

func (r *serviceRecorder) appErrors() []eventbus.AppError {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.appErrs)
}

// TestOverloadedReportedOncePerFrame: only a frame's first OVERLOADED is
// published as an AppError, with the busy hint; its repeats only log at
// Debug; once the reply settles the frame, the next frame's first OVERLOADED
// is reported again.
func TestOverloadedReportedOncePerFrame(t *testing.T) {
	bus, rec := newServiceRecorder(t)
	h := startFramesHarnessOn(t, bus, "svc")
	want := eventbus.AppError{Code: "OVERLOADED", Message: "edge says OVERLOADED", Hint: overloadedHint}

	ef := h.edge.next(t, "register", "svc")
	for i := 0; i < 3; i++ {
		sendError(t, ef.c, "OVERLOADED")
		h.clock.next(t).fire()
		ef = h.edge.next(t, "register", "svc")
	}
	settle(t, ef.c, h.logs)
	if got := rec.appErrors(); len(got) != 1 || got[0] != want {
		t.Fatalf("AppErrors after three OVERLOADEDs of one frame = %+v, want just %+v", got, want)
	}

	h.edge.registered(t, ef.c, fakeedge.Accepted("svc"), nil)
	h.tun.RegisterService("svc")
	ef = h.edge.next(t, "register", "svc")
	sendError(t, ef.c, "OVERLOADED")
	h.clock.next(t)
	settle(t, ef.c, h.logs)
	if got := rec.appErrors(); len(got) != 2 || got[1] != want {
		t.Fatalf("AppErrors after the next frame's OVERLOADED = %+v, want a second %+v", got, want)
	}
}

// TestFramesConflictOnlyForHeldNames: a conflict counts only for a name this
// connection holds — one a "registered" on it carried a services[] entry
// for (ws-mixer-spec MCPWARP.md §3.3, §2.7). With the register still on the
// wire a conflict for one of its names is ignored (that register's reply
// decides), and so is one for a name the reply refused; after the reply a
// conflict for a held name takes it over, once.
func TestFramesConflictOnlyForHeldNames(t *testing.T) {
	bus, rec := newServiceRecorder(t)
	h := startFramesHarnessOn(t, bus, "a", "b")
	ef := h.edge.next(t, "register", "a", "b")

	if err := ef.c.Conflict("id-a", "a", "taken_over"); err != nil {
		t.Fatalf("Conflict: %v", err)
	}
	settle(t, ef.c, h.logs)
	rec.none(t)
	if n := h.logs.count("conflict for a name only in an outstanding register; its reply decides"); n != 1 {
		t.Fatalf("outstanding-register conflict logged %d times, want once", n)
	}

	h.edge.registered(t, ef.c, fakeedge.Accepted("a"), []fakeedge.RegisterError{{Name: "b", Code: "QUOTA_EXCEEDED", Message: "full"}})
	rec.expect(t,
		eventbus.ServiceChanged{Name: "a", Kind: "http", State: "active", URL: "https://a.example/mcp"},
		eventbus.ServiceChanged{Name: "b", Kind: "http", State: "rejected", Code: "QUOTA_EXCEEDED", Message: "full", Hint: registerErrorHint("QUOTA_EXCEEDED", "")},
	)
	if err := ef.c.Conflict("id-b", "b", "taken_over"); err != nil {
		t.Fatalf("Conflict: %v", err)
	}
	settle(t, ef.c, h.logs)
	rec.none(t)

	if err := ef.c.Conflict("id-a", "a", "taken_over"); err != nil {
		t.Fatalf("Conflict: %v", err)
	}
	rec.expect(t, eventbus.ServiceChanged{Name: "a", Kind: "http", State: "taken_over", Code: "TAKEN_OVER", Message: "taken_over", Hint: takenOverHint})
	if err := ef.c.Conflict("id-a", "a", "taken_over"); err != nil {
		t.Fatalf("Conflict: %v", err)
	}
	settle(t, ef.c, h.logs)
	rec.none(t)
	h.noFatal(t, "a was registered")
}

// TestFramesEndedConnectionIgnoresStaleOps: a registered, unregistered,
// disable or enable that a connection still delivers after its end was
// reported is ignored, while a conflict from it still applies
// (ws-mixer-spec MCPWARP.md §2.7 Ended connection). The end and the late
// frames are fed to onDisconnect/onApp as the SDK would hand them over, on
// a socket the test keeps open, so its sentinel still fences them.
func TestFramesEndedConnectionIgnoresStaleOps(t *testing.T) {
	bus, rec := newServiceRecorder(t)
	h := startFramesHarnessOn(t, bus, "a", "b")
	ef := h.edge.next(t, "register", "a", "b")
	h.edge.registered(t, ef.c, fakeedge.Accepted("a"), []fakeedge.RegisterError{{Name: "b", Code: "QUOTA_EXCEEDED", Message: "full"}})
	rec.expect(t,
		eventbus.ServiceChanged{Name: "a", Kind: "http", State: "active", URL: "https://a.example/mcp"},
		eventbus.ServiceChanged{Name: "b", Kind: "http", State: "rejected", Code: "QUOTA_EXCEEDED", Message: "full", Hint: registerErrorHint("QUOTA_EXCEEDED", "")},
	)
	settle(t, ef.c, h.logs)

	ended := h.tun.liveConn.Load()
	op := func(fields string) json.RawMessage {
		return json.RawMessage(`{"mcpwarp":{"v":1,` + fields + `}}`)
	}
	h.tun.onDisconnect(wsmixer.DisconnectReason{Phase: wsmixer.PhaseConnected, WSCode: 1006, Message: "connection lost"})
	h.tun.onApp(ended, op(`"op":"registered","services":[{"name":"b","id":"id-b","url":"https://b.example/mcp","created":false}],"errors":[]`))
	h.tun.onApp(ended, op(`"op":"disable","id":"id-a","reason":"disabled"`))
	h.tun.onApp(ended, op(`"op":"enable","id":"id-b","name":"b"`))
	h.tun.onApp(ended, op(`"op":"unregistered","services":[{"name":"a","id":"id-a"}]`))
	settle(t, ef.c, h.logs)
	rec.none(t)
	if _, ok := h.tun.Registry().Get("b"); ok {
		t.Fatal("a registered from the ended connection added b")
	}
	if r := h.row("a"); r.Disabled || r.URL == "" {
		t.Fatalf("a row = %+v, want still active after a stale disable", r)
	}

	h.tun.onApp(ended, op(`"op":"conflict","id":"id-a","name":"a","reason":"taken_over"`))
	rec.expect(t, eventbus.ServiceChanged{Name: "a", Kind: "http", State: "taken_over", Code: "TAKEN_OVER", Message: "taken_over", Hint: takenOverHint})
}

// TestFramesClosedConnDropsLateStaleOps: a registered delivered after its
// connection's socket closed is dropped on that connection's own Done, even
// when nothing has marked the connection ended yet — OnApp runs unordered
// against the disconnect report. The edge refuses every later handshake, so
// no replacement makes the connection superseded, and the test clears the
// ended marker once the disconnect has been handled, so Done is the only
// thing that can drop the message.
func TestFramesClosedConnDropsLateStaleOps(t *testing.T) {
	bus, rec := newServiceRecorder(t)
	edge := fakeedge.New(t, fakeedge.Options{
		OnApp: func(c *fakeedge.Conn, f fakeedge.Frame) {
			if f.Op == "register" {
				_ = c.Registered(fakeedge.Accepted("a"), []fakeedge.RegisterError{{Name: "b", Code: "QUOTA_EXCEEDED", Message: "full"}})
			}
		},
		Authenticate: func(attempt int, _ *wsmixer.Hello) error {
			if attempt > 1 {
				return wsmixer.Unsupported("no second connection in this test")
			}
			return nil
		},
	})
	tun, err := Start(context.Background(), Config{
		URL:         edge.URL,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "a", Kind: "http"}, {Name: "b", Kind: "http"}},
		Bus:         bus,
		Log:         slog.New(newLogWatch()),
		FatalExit:   func(int) {},
		Reconnect:   wsmixer.ReconnectOptions{Base: 10 * time.Millisecond, Cap: 50 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = tun.Close(context.Background()) })
	rec.expect(t,
		eventbus.ServiceChanged{Name: "a", Kind: "http", State: "active", URL: "https://a.example/mcp"},
		eventbus.ServiceChanged{Name: "b", Kind: "http", State: "rejected", Code: "QUOTA_EXCEEDED", Message: "full", Hint: registerErrorHint("QUOTA_EXCEEDED", "")},
	)

	ended := tun.liveConn.Load()
	_ = edge.Latest().Close(uint32(wsmixer.InternalErrorCode), "ending the connection for test")
	select {
	case <-ended.Done():
	case <-time.After(fakeedge.DefaultWait):
		t.Fatal("the connection never ended")
	}
	onDispatcher := func(fn func()) {
		done := make(chan struct{})
		tun.disp.enqueue(func() { fn(); close(done) })
		select {
		case <-done:
		case <-time.After(fakeedge.DefaultWait):
			t.Fatal("the dispatcher never ran the step")
		}
	}
	for marked := false; !marked; {
		onDispatcher(func() { marked = tun.endedConn == ended })
	}
	onDispatcher(func() { tun.endedConn = nil })

	tun.onApp(ended, json.RawMessage(`{"mcpwarp":{"v":1,"op":"registered","services":[{"name":"b","id":"id-b","url":"https://b.example/mcp","created":false}],"errors":[]}}`))
	onDispatcher(func() {})
	rec.none(t)
	if _, ok := tun.Registry().Get("b"); ok {
		t.Fatal("a registered delivered after the socket closed added b")
	}
}

func TestMaxServicesPerFrameMatchesSDK(t *testing.T) {
	if appproto.MaxServicesPerFrame != mcpwarp.MaxServicesPerMessage {
		t.Fatalf("appproto.MaxServicesPerFrame = %d, mcpwarp.MaxServicesPerMessage = %d", appproto.MaxServicesPerFrame, mcpwarp.MaxServicesPerMessage)
	}
}
