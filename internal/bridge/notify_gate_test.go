package bridge

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The notification gate and cancel routing beyond the basic cases.

func modernCancel(requestID any) map[string]any {
	return map[string]any{"jsonrpc": "2.0", "method": "notifications/cancelled",
		"params": map[string]any{"requestId": requestID, "_meta": eraMeta(eraTestVersion)}}
}

func bareCancel(requestID any) map[string]any {
	return map[string]any{"jsonrpc": "2.0", "method": "notifications/cancelled",
		"params": map[string]any{"requestId": requestID}}
}

func TestNotify_FlippedProcessAdmitsAModernNotification(t *testing.T) {
	// The child's discover answer is held past the timeout, and it rejects a
	// forwarded initialize naming only 2026 versions: the process flips to v2.
	b := startTimedOut(t, nil, "--era=modern", "--discover-hold")
	if r := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil); r.status != 400 || versionOf(b) != versionV2 {
		t.Fatalf("setup: initialize %d %s", r.status, r.body)
	}
	if r := eraPost(t, b, acceptJSON, modernNotification(), nil); r.status != 202 {
		t.Fatalf("%d %s", r.status, r.body)
	}
	if indexOf(childReceived(t, b), probeNote) < 0 {
		t.Fatal("not forwarded")
	}
}

func TestNotify_ModernCancelToALegacyChildIsRefused(t *testing.T) {
	b := startEraBridge(t, nil, "--era=legacy")
	r := eraPost(t, b, acceptJSON, modernCancel("x"), nil)
	var m map[string]any
	_ = json.Unmarshal([]byte(r.body), &m)
	e, _ := m["error"].(map[string]any)
	if _, hasID := m["id"]; r.status != 400 || e == nil || e["code"] != float64(-32600) || hasID {
		t.Fatalf("%d %s", r.status, r.body)
	}
	if indexOf(childReceived(t, b), "notifications/cancelled") >= 0 {
		t.Fatal("the cancel reached a legacy child")
	}
}

// pendingModernOnModernChild holds a modern request with client id 1 on a v2
// process and returns its bridge id.
func pendingModernOnModernChild(t *testing.T) (*Bridge, int64) {
	t.Helper()
	b := startEraBridge(t, nil, listenFake)
	_, id := holdModernRequest(t, b)
	return b, id
}

func TestNotify_ModernCancelIsForwardedRewrittenWithItsEnvelope(t *testing.T) {
	b, id := pendingModernOnModernChild(t)
	if r := eraPost(t, b, acceptJSON, modernCancel(float64(1)), nil); r.status != 202 {
		t.Fatalf("%d %s", r.status, r.body)
	}
	waitFor(t, "the cancel", func() bool { return childCancelCount(t, b, id) == 1 })
	has, meta := cancelMeta(t, b, id)
	if !has || meta[protocolVersionMetaKey] != eraTestVersion {
		t.Fatalf("the client's envelope was lost: %v %v", has, meta)
	}
}

// A 2025-shaped cancel on a v2 process is refused; a 2026 client cancels by
// closing its stream.
func TestNotify_BareCancelOnV2IsRefused(t *testing.T) {
	b, id := pendingModernOnModernChild(t)
	r := eraPost(t, b, acceptJSON, bareCancel(float64(1)), nil)
	var m map[string]any
	_ = json.Unmarshal([]byte(r.body), &m)
	if _, hasID := m["id"]; r.status != 400 || hasID || !strings.Contains(r.body, "does not accept older MCP v1 requests") {
		t.Fatalf("%d %s", r.status, r.body)
	}
	if n := childCancelCount(t, b, id); n != 0 {
		t.Fatalf("the child got %d cancels", n)
	}
}

// An enveloped notifications/initialized is answered without waiting for
// the verdict, and never forwarded.
func TestNotify_EnvelopedInitializedIsAnsweredWithoutWaiting(t *testing.T) {
	b := startEraBridgeCold(t, nil, listenFake, "--discover-hold")
	body := map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized", "params": map[string]any{"_meta": eraMeta(eraTestVersion)}}
	if r := eraPost(t, b, acceptJSON, body, nil); r.status != 202 || r.elapsed > 5*time.Second {
		t.Fatalf("%d after %s: %s", r.status, r.elapsed, r.body)
	}
	if versionOf(b) != versionUnknown {
		t.Fatal("setup: the verdict came before the notification was answered")
	}
	releaseDirect(t, b)
	waitForVerdict(t, b)
	if got := childReceived(t, b); indexOf(got, "notifications/initialized") >= 0 {
		t.Fatalf("the child received %q", got)
	}
}

func TestNotify_ErrorAnswerRefusesAModernNotification(t *testing.T) {
	for _, code := range []string{"-32602", "-32603"} {
		b := startEraBridge(t, nil, "--era=modern", "--discover-error="+code)
		if r := eraPost(t, b, acceptJSON, modernNotification(), nil); r.status != 400 {
			t.Fatalf("discover error %s: %d %s", code, r.status, r.body)
		}
		if indexOf(childReceived(t, b), probeNote) >= 0 {
			t.Fatalf("discover error %s: forwarded", code)
		}
	}
}

// A request whose process has exited without being replaced: the cancel is
// dropped and answered 202, and nothing is written.
func TestCancel_ClientCancelForAnExitedProcessIsDropped(t *testing.T) {
	b := startEraBridge(t, nil, "--era=legacy")
	live := b.GetCurrentChild()
	dead := newBareChild()
	dead.exited = true
	e := &pendingEntry{originalID: "x", child: dead, plain: httptest.NewRecorder(), timer: time.NewTimer(time.Hour), done: make(chan struct{})}
	b.mu.Lock()
	b.child = dead // the current process, exited and not yet replaced
	b.pending[firstBridgeID+300] = e
	b.mu.Unlock()
	t.Cleanup(func() {
		b.mu.Lock()
		b.child = live
		delete(b.pending, firstBridgeID+300)
		b.mu.Unlock()
	})
	r := eraPost(t, b, acceptJSON, bareCancel("x"), nil)
	if r.status != 202 {
		t.Fatalf("%d %s", r.status, r.body)
	}
}
