package bridge

import (
	"context"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// What the bridge keeps from the client and what it says to the child on the
// client's behalf: bridge ids and tokens never reach a client, and the
// cancels the bridge writes carry the era of the request they cancel.

// holdModernRequest sends a modern request the fake never answers on its own
// and returns the cancel that drops the client, and the bridge id.
func holdModernRequest(t *testing.T, b *Bridge) (context.CancelFunc, int64) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		if resp, _, err := eraSend(ctx, b.URL(), acceptJSON, modernBody(1, "debug/heldError", map[string]any{"code": -32000}, nil), nil); err == nil {
			resp.Body.Close()
		}
	}()
	return cancel, firstPendingID(t, b)
}

func TestListen_ChildCancelNamingABridgeIDIsNeverRelayed(t *testing.T) {
	b := startEraBridge(t, nil, listenFakeDual, "--listen")
	get := openGet(t, b)
	_, s, id := openListen(t, b, "L1")
	_, plainID := holdModernRequest(t, b)
	unknown := id + 5000

	rawLines(t, b,
		childCancelLine(t, plainID),      // a pending ordinary request
		childCancelLine(t, unknown),      // a bridge id naming nothing
		taggedCancelLine(t, plainID, id), // tagged for the open listen, naming the request
		taggedCancelLine(t, 5, id),       // tagged, naming no bridge id
		plainNotification(t, "x/marker"),
	)
	get.waitFor(t, "x/marker")
	for _, leaked := range []string{"notifications/cancelled", strconv.FormatInt(plainID, 10), strconv.FormatInt(unknown, 10)} {
		if get.sawAny(leaked) {
			t.Errorf("%q reached the GET stream: %q", leaked, get.seen)
		}
	}
	rawLines(t, b, taggedLine(t, "x/event", id, nil))
	s.waitFor(t, "x/event")
	if s.sawAny("requestId") || s.sawAny(strconv.FormatInt(plainID, 10)) {
		t.Errorf("a cancel was queued on the listen: %q", s.seen)
	}
	if listenCount(b) != 1 {
		t.Fatal("a cancel for another id ended the listen")
	}
}

func TestListen_LateProgressForABridgeIDIsDropped(t *testing.T) {
	b := startEraBridge(t, nil, listenFakeDual, "--listen")
	get := openGet(t, b)
	_, _, id := openListen(t, b, "L1")
	rawLines(t, b, childResultLine(t, id, map[string]any{"resultType": "complete"}))
	waitFor(t, "the listen to end", func() bool { return listenCount(b) == 0 })

	progress := func(token string) string {
		return jsonLine(t, map[string]any{"jsonrpc": "2.0", "method": "notifications/progress",
			"params": map[string]any{"progressToken": token, "progress": 1}})
	}
	rawLines(t, b,
		progress(internalProgressToken(id)),      // the listen that just ended
		progress(internalProgressToken(id+7000)), // nothing ever open
		progress("p5"), // the child's own token, outside the range
		plainNotification(t, "x/marker"),
	)
	get.waitFor(t, "x/marker")
	if get.sawAny(internalProgressToken(id)) || get.sawAny(internalProgressToken(id+7000)) {
		t.Errorf("a bridge token reached the GET stream: %q", get.seen)
	}
	if !get.sawAny(`"progressToken":"p5"`) {
		t.Errorf("the child's own progress was dropped: %q", get.seen)
	}
}

func cancelMeta(t *testing.T, b *Bridge, requestID int64) (bool, map[string]any) {
	t.Helper()
	r := debugCall(t, b, "debug/cancelMeta", map[string]any{"requestId": requestID})
	has, _ := r["hasMeta"].(bool)
	meta, _ := r["meta"].(map[string]any)
	return has, meta
}

func TestCancel_ModernRequestDisconnectCarriesItsVersion(t *testing.T) {
	b := startEraBridge(t, nil, listenFake)
	cancel, id := holdModernRequest(t, b)
	cancel()
	waitFor(t, "the cancel", func() bool { return childCancelCount(t, b, id) == 1 })
	has, meta := cancelMeta(t, b, id)
	if !has || meta[protocolVersionMetaKey] != eraTestVersion {
		t.Fatalf("cancel _meta: %v %v", has, meta)
	}
}

func TestCancel_LegacyRequestDisconnectHasNoEnvelope(t *testing.T) {
	b := startEraBridge(t, nil, listenFakeDual)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		body := map[string]any{"jsonrpc": "2.0", "id": 1, "method": "debug/heldError", "params": map[string]any{"code": -32000}}
		if resp, _, err := eraSend(ctx, b.URL(), acceptJSON, body, nil); err == nil {
			resp.Body.Close()
		}
	}()
	id := firstPendingID(t, b)
	cancel()
	waitFor(t, "the cancel", func() bool { return childCancelCount(t, b, id) == 1 })
	if has, meta := cancelMeta(t, b, id); has {
		t.Fatalf("a 2025-shaped request's cancel carries _meta: %v", meta)
	}
}

func TestCancel_ListenDisconnectCarriesItsVersion(t *testing.T) {
	b := startEraBridge(t, nil, listenFake, "--listen")
	c, _, id := openListen(t, b, "L1")
	c.cancel()
	waitFor(t, "the cancel", func() bool { return childCancelCount(t, b, id) == 1 })
	has, meta := cancelMeta(t, b, id)
	if !has || meta[protocolVersionMetaKey] != eraTestVersion {
		t.Fatalf("cancel _meta: %v %v", has, meta)
	}
}

func TestCancel_ListenOverflowCarriesItsVersion(t *testing.T) {
	b := startEraBridge(t, nil, listenFake, "--listen")
	rec := httptest.NewRecorder()
	l := newListenEntry("L1", nil, eraTestVersion, b.GetCurrentChild(), 10, rec)
	id, refusal := b.admitListen(l)
	if refusal != "" {
		t.Fatal(refusal)
	}
	b.enqueueListen(id, l, map[string]any{"jsonrpc": "2.0", "method": "x/big", "params": map[string]any{"p": strings.Repeat("x", 100)}})
	waitFor(t, "the cancel", func() bool { return childCancelCount(t, b, id) == 1 })
	has, meta := cancelMeta(t, b, id)
	if !has || meta[protocolVersionMetaKey] != eraTestVersion {
		t.Fatalf("cancel _meta: %v %v", has, meta)
	}
}

// A forwarded server/discover is dropped without a word to the child.
func TestCancel_ForwardedDiscoverIsNeverCancelled(t *testing.T) {
	b := startEraBridge(t, nil, listenFake, "--discover-hold")
	first := make(chan int, 1)
	go func() {
		status := 0
		if resp, _, err := eraSend(context.Background(), b.URL(), acceptJSON, modernCall(1), nil); err == nil {
			status = resp.StatusCode
			resp.Body.Close()
		}
		first <- status
	}()
	waitProbeInFlight(t, b)
	releaseHeld(t, b) // the probe's answer: the child is modern
	select {
	case status := <-first:
		if status != 200 {
			t.Fatalf("setup: %d", status)
		}
	case <-time.After(listenWait):
		t.Fatal("setup: the first request was not answered")
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		if resp, _, err := eraSend(ctx, b.URL(), acceptJSON, modernBody(9, "server/discover", nil, nil), nil); err == nil {
			resp.Body.Close()
		}
	}()
	id := firstPendingID(t, b) // held by the fake, like every discover answer
	cancel()
	drained := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(listenWait):
		t.Fatal("the request handler did not return")
	}
	if n := childCancelCount(t, b, id); n != 0 {
		t.Fatalf("the child was sent %d cancels for a forwarded server/discover", n)
	}
	if !b.isAbandoned(id) {
		t.Fatal("the dropped discover's id is not remembered")
	}
}
