package bridge

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// How a listen ends: a tagged cancel, a client that stops reading, a handler
// that panics, and the cancels the child is sent for each.

func childCancelCount(t *testing.T, b *Bridge, requestID any) int {
	t.Helper()
	n, _ := debugCall(t, b, "debug/cancelCount", map[string]any{"requestId": requestID})["count"].(float64)
	return int(n)
}

func taggedCancelLine(t *testing.T, requestID int64, tag any) string {
	return jsonLine(t, map[string]any{"jsonrpc": "2.0", "method": "notifications/cancelled", "params": map[string]any{
		"requestId": requestID,
		"_meta":     map[string]any{subscriptionIDMetaKey: tag},
	}})
}

func listenIDs(b *Bridge) []int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	var ids []int64
	for id := range b.listens {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func TestListen_TaggedChildCancelEndsTheStreamAndLeaksNothing(t *testing.T) {
	b := startEraBridge(t, nil, listenFakeDual, "--listen")
	get := openGet(t, b)
	c, s, id := openListen(t, b, "L1")
	rawLines(t, b, taggedCancelLine(t, id, id), plainNotification(t, "x/marker"))
	if rest := s.drain(t); len(rest) != 0 {
		t.Fatalf("the cancel was queued as a frame: %q", rest)
	}
	get.waitFor(t, "x/marker")
	if get.sawAny("notifications/cancelled") {
		t.Fatal("the tagged cancel reached the GET stream")
	}
	if s.sawAny(strconv.FormatInt(id, 10)) {
		t.Fatalf("the bridge id reached the client: %q", s.seen)
	}
	if listenCount(b) != 0 {
		t.Fatal("the listen is still registered")
	}
	c.cancel() // a disconnect now has nothing left to cancel
	if n := childCancelCount(t, b, id); n != 0 {
		t.Fatalf("the bridge cancelled a listen the child had ended %d times", n)
	}
}

func TestListen_TaggedCancelAfterTheResultIsSwallowed(t *testing.T) {
	b := startEraBridge(t, nil, listenFakeDual, "--listen")
	get := openGet(t, b)
	_, s, id := openListen(t, b, "L1")
	rawLines(t, b,
		childResultLine(t, id, map[string]any{"resultType": "complete"}),
		taggedCancelLine(t, id, id),
		plainNotification(t, "x/marker"),
	)
	rest := s.drain(t)
	if len(rest) != 2 || strings.Contains(strings.Join(rest, "\n"), "notifications/cancelled") {
		t.Fatalf("stream after the result: %q", rest)
	}
	get.waitFor(t, "x/marker")
	if get.sawAny("notifications/cancelled") {
		t.Fatal("the later tagged cancel leaked to the GET stream")
	}
	if n := childCancelCount(t, b, id); n != 0 {
		t.Fatalf("the bridge cancelled a listen the child had answered %d times", n)
	}
}

// Overflow and the handler's exit both want to cancel the listen at the
// child; whichever order they land in, it gets exactly one.
func TestListen_OverflowAndHandlerExitSendOneCancel(t *testing.T) {
	for _, handlerFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("handlerFirst=%v", handlerFirst), func(t *testing.T) {
			b := startEraBridge(t, nil, listenFake, "--listen")
			rec := httptest.NewRecorder()
			l := newListenEntry("L1", nil, eraTestVersion, b.GetCurrentChild(), 10, rec)
			id, refusal := b.admitListen(l)
			if refusal != "" {
				t.Fatal(refusal)
			}
			res := newHeldSSEResponder(rec, nil)
			overflow := func() {
				b.enqueueListen(id, l, map[string]any{"jsonrpc": "2.0", "method": "x/big", "params": map[string]any{"p": strings.Repeat("x", 100)}})
			}
			if handlerFirst {
				b.finishListen(id, l, res)
				overflow() // the entry takes nothing more
			} else {
				overflow()
				waitFor(t, "the overflow cancel", func() bool { return childCancelCount(t, b, id) == 1 })
				b.finishListen(id, l, res)
			}
			time.Sleep(100 * time.Millisecond) // room for a wrong second cancel
			if n := childCancelCount(t, b, id); n != 1 {
				t.Fatalf("the child got %d cancels, want 1", n)
			}
			if listenCount(b) != 0 {
				t.Fatal("the entry is still registered")
			}
		})
	}
}

func TestListen_KeepaliveStartsWithTheHead(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) {
		o.HeadHoldTimeout = time.Hour
		o.ListenKeepalive = 20 * time.Millisecond
	}, listenFake, "--listen-no-ack")
	c := startListen(t, b, "L1", nil)
	id := waitListenID(t, b, "L1")
	waitFor(t, "the child to see the listen", func() bool { return len(childListens(t, b)) == 1 })
	time.Sleep(200 * time.Millisecond) // ten periods with the head still held
	select {
	case <-c.head:
		t.Fatal("something was written before the head was committed")
	default:
	}
	rawLines(t, b, taggedLine(t, "x/event", id, nil))
	s := c.stream(t)
	s.waitFor(t, "x/event")
	s.waitFor(t, ": ping")
	s.waitFor(t, ": ping")
	if listenCount(b) != 1 {
		t.Fatal("the idle listen was closed")
	}
}

// stallListen opens a listen over a raw connection that reads the head and
// the acknowledgement and then nothing, and floods it until the handler is
// blocked in Write with a queue behind it. The grace is short so a test need
// not wait the default out.
func stallListen(t *testing.T, b *Bridge) (int64, *listenEntry) {
	t.Helper()
	b.mu.Lock()
	b.writeGrace = 200 * time.Millisecond
	b.mu.Unlock()

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", b.Port()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	body := jsonLine(t, modernBody("stall", "subscriptions/listen", nil, nil))
	fmt.Fprintf(conn, "POST /mcp HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nAccept: %s\r\n"+
		"MCP-Protocol-Version: %s\r\nMcp-Method: subscriptions/listen\r\nContent-Length: %d\r\n\r\n%s",
		acceptSSE, eraTestVersion, len(body), body)
	_ = conn.SetReadDeadline(time.Now().Add(listenWait))
	br := bufio.NewReader(conn)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("no acknowledgement: %v", err)
		}
		if strings.Contains(line, "acknowledged") {
			break
		}
	}
	_ = conn.SetReadDeadline(time.Time{})

	id := waitListenID(t, b, "stall")
	b.mu.Lock()
	l := b.listens[id]
	b.mu.Unlock()
	queued := func() int {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.queued
	}
	for round := 0; round < 4; round++ {
		debugCall(t, b, "debug/flood", map[string]any{"subscriptionId": id, "count": 6000, "size": 4096})
		first := queued()
		time.Sleep(300 * time.Millisecond)
		if second := queued(); first > 0 && second >= first {
			return id, l // nothing is draining: the handler is stuck in Write
		}
	}
	t.Skip("the socket buffers absorbed everything the test could flood")
	return 0, nil
}

func TestListen_StuckClientOverflowFreesTheHandler(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.ListenBudget = 512 << 20 }, listenFake, "--listen")
	id, l := stallListen(t, b)

	l.mu.Lock()
	l.budget = 1
	l.mu.Unlock()
	rawLines(t, b, taggedLine(t, "x/event", id, nil)) // over budget

	drained := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(listenWait):
		t.Fatal("the handler is still blocked in Write")
	}
	if listenCount(b) != 0 {
		t.Fatal("the entry is still registered")
	}
	waitFor(t, "the cancel", func() bool { return childCancelCount(t, b, id) >= 1 })
	if n := childCancelCount(t, b, id); n != 1 {
		t.Fatalf("the child got %d cancels, want 1", n)
	}
	start := time.Now()
	_ = b.Close()
	if elapsed := time.Since(start); elapsed >= closeBarrier {
		t.Fatalf("Close took %v", elapsed)
	}
}

func TestListen_CloseWhileTheClientIsStuck(t *testing.T) {
	b := startEraBridge(t, func(o *StartBridgeOptions) { o.ListenBudget = 512 << 20 }, listenFake, "--listen")
	stallListen(t, b)

	start := time.Now()
	_ = b.Close()
	// The handler gave up on its own write deadline; the barrier's hard close
	// of the sockets did not have to.
	if elapsed := time.Since(start); elapsed >= closeBarrier {
		t.Fatalf("Close took %v: the barrier ran out", elapsed)
	}
	if listenCount(b) != 0 {
		t.Fatal("the entry is still registered")
	}
}

func TestListen_PanicInTheHandlerCleansUp(t *testing.T) {
	b := startEraBridge(t, nil, listenFake, "--listen")
	b.mu.Lock()
	id := b.nextID // the listen below is the next thing minted
	b.mu.Unlock()

	body := jsonLine(t, modernBody("P1", "subscriptions/listen", nil, nil))
	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(body))
	req.Header.Set("Accept", acceptSSE)
	req.Header.Set("MCP-Protocol-Version", eraTestVersion)
	req.Header.Set("Mcp-Method", "subscriptions/listen")
	// The head goes out on the acknowledgement, and that panics.
	rec := &panicOnFirstWriteHeader{ResponseRecorder: httptest.NewRecorder()}
	done := make(chan struct{})
	go func() {
		b.handle(rec, req)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(listenWait):
		t.Fatal("the handler did not return")
	}
	if rec.Code != 502 {
		t.Fatalf("status %d, want the panic handler's 502", rec.Code)
	}
	written := rec.Body.String()
	if listenCount(b) != 0 {
		t.Fatal("the entry is still registered")
	}
	waitFor(t, "the cancel", func() bool { return childCancelCount(t, b, id) == 1 })
	rawLines(t, b, taggedLine(t, "x/late", id, nil))
	debugCall(t, b, "debug/listens", nil)
	if rec.Body.String() != written {
		t.Fatalf("written after the panic response: %q", rec.Body.String()[len(written):])
	}
}

func TestListen_SameClientIDOnTwoClientsIsIsolated(t *testing.T) {
	b := startEraBridge(t, nil, listenFake, "--listen")
	first := startListen(t, b, "same", nil)
	a := first.stream(t)
	a.waitFor(t, "acknowledged")
	waitFor(t, "the first listen", func() bool { return listenCount(b) == 1 })
	second := startListen(t, b, "same", nil)
	c := second.stream(t)
	c.waitFor(t, "acknowledged")
	waitFor(t, "the second listen", func() bool { return listenCount(b) == 2 })
	ids := listenIDs(b)

	rawLines(t, b,
		taggedLine(t, "x/forFirst", ids[0], nil),
		taggedLine(t, "x/forSecond", ids[1], nil),
		childResultLine(t, ids[0], map[string]any{"resultType": "complete"}),
	)
	a.waitFor(t, "x/forFirst")
	c.waitFor(t, "x/forSecond")
	a.drain(t) // the first ends with its result
	if a.sawAny("x/forSecond") || c.sawAny("x/forFirst") {
		t.Fatalf("notifications crossed: %q / %q", a.seen, c.seen)
	}
	if !a.sawAny("resultType") {
		t.Fatal("the first listen did not get its result")
	}
	if listenCount(b) != 1 {
		t.Fatal("the second listen ended with the first")
	}
	rawLines(t, b, taggedLine(t, "x/more", ids[1], nil))
	c.waitFor(t, "x/more")
	if c.sawAny("resultType") {
		t.Fatal("the first listen's result reached the second client")
	}
}
