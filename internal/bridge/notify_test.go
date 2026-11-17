package bridge

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Client notifications: an enveloped one waits for the verdict and reaches
// only a v2 child, and a cancel goes only to the process its request was sent
// to.

const probeNote = "notifications/whatever"

func modernNotification() map[string]any {
	return map[string]any{"jsonrpc": "2.0", "method": probeNote, "params": map[string]any{"_meta": eraMeta(eraTestVersion)}}
}

// childReceived is every method the fake has read, in order.
func childReceived(t *testing.T, b *Bridge) []string {
	t.Helper()
	raw, _ := debugCall(t, b, "debug/received", nil)["methods"].([]any)
	methods := make([]string, len(raw))
	for i, m := range raw {
		methods[i], _ = m.(string)
	}
	return methods
}

func indexOf(list []string, want string) int {
	for i, m := range list {
		if m == want {
			return i
		}
	}
	return -1
}

func TestNotify_ModernShapedToALegacyChildIsRefused(t *testing.T) {
	b := startEraBridge(t, nil, "--era=legacy")
	r := eraPost(t, b, acceptJSON, modernNotification(), nil)
	var m map[string]any
	_ = json.Unmarshal([]byte(r.body), &m)
	e, _ := m["error"].(map[string]any)
	if _, hasID := m["id"]; r.status != 400 || e == nil || e["code"] != float64(-32600) || hasID {
		t.Fatalf("%d %s", r.status, r.body)
	}
	if indexOf(childReceived(t, b), probeNote) >= 0 {
		t.Fatal("the notification reached a legacy child")
	}
}

func TestNotify_WaitsForTheVerdictThenFollowsTheProbe(t *testing.T) {
	b := startEraBridgeCold(t, func(o *StartBridgeOptions) {
		o.MaxPending = 1
		o.MaxListens = 1
	}, listenFake, "--discover-hold")
	b.mu.Lock()
	idBefore := b.nextID
	b.mu.Unlock()
	status := make(chan int, 1)
	go func() {
		code := 0
		if resp, _, err := eraSend(t.Context(), b.URL(), acceptJSON, modernNotification(), nil); err == nil {
			code = resp.StatusCode
			resp.Body.Close()
		}
		status <- code
	}()
	time.Sleep(200 * time.Millisecond)
	select {
	case code := <-status:
		t.Fatalf("answered %d while the probe was out", code)
	default:
	}
	// The waiting notification holds no slot and has no bridge id.
	b.mu.Lock()
	idWaiting, pending := b.nextID, len(b.pending)
	b.mu.Unlock()
	if idWaiting != idBefore || pending != 0 || listenCount(b) != 0 {
		t.Fatalf("the waiting notification took state: next id %d -> %d, %d pending", idBefore, idWaiting, pending)
	}
	releaseDirect(t, b)
	select {
	case code := <-status:
		if code != 202 {
			t.Fatalf("status %d after the probe", code)
		}
	case <-time.After(listenWait):
		t.Fatal("the notification was not answered after the probe")
	}
	got := childReceived(t, b)
	if d, n := indexOf(got, "server/discover"), indexOf(got, probeNote); d < 0 || n < 0 || d > n {
		t.Fatalf("order on the child's stdin: %q", got)
	}
}

func TestNotify_ModernChildWithAVerdictForwardsAtOnce(t *testing.T) {
	b := startEraBridge(t, nil, listenFake)
	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 200 {
		t.Fatalf("setup: %d %s", r.status, r.body)
	}
	if r := eraPost(t, b, acceptJSON, modernNotification(), nil); r.status != 202 {
		t.Fatalf("%d %s", r.status, r.body)
	}
	if indexOf(childReceived(t, b), probeNote) < 0 {
		t.Fatal("not forwarded")
	}
}

func TestNotify_SilentLegacyChildRefusesAfterTheProbeTimeout(t *testing.T) {
	b := startEraBridge(t, shortProbe, "--era=legacy-silent")
	if r := eraPost(t, b, acceptJSON, modernNotification(), nil); r.status != 400 {
		t.Fatalf("%d %s", r.status, r.body)
	}
	if indexOf(childReceived(t, b), probeNote) >= 0 {
		t.Fatal("forwarded to a child that never answered the probe")
	}
}

func TestNotify_DeadChildIs502(t *testing.T) {
	b := startEraBridge(t, nil, listenFake)
	killCurrentChild(t, b)
	r := eraPost(t, b, acceptJSON, modernNotification(), nil)
	if r.status != 502 || !strings.Contains(r.body, "local server is not running") {
		t.Fatalf("%d %s", r.status, r.body)
	}
}

func TestNotify_ModernShapedSkipsHeaderValidation(t *testing.T) {
	b := startEraBridge(t, nil, listenFake)
	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 200 {
		t.Fatalf("setup: %d %s", r.status, r.body)
	}
	for name, hdr := range map[string]map[string]string{
		"mismatching": {"MCP-Protocol-Version": "1999-01-01", "Mcp-Method": "notifications/other"},
		"missing":     {"MCP-Protocol-Version": "", "Mcp-Method": ""},
	} {
		if r := eraPost(t, b, acceptJSON, modernNotification(), hdr); r.status != 202 {
			t.Fatalf("%s headers: %d %s", name, r.status, r.body)
		}
	}
	if indexOf(childReceived(t, b), probeNote) < 0 {
		t.Fatal("not forwarded")
	}
}

func TestNotify_2025ShapedIsForwardedToAV1Child(t *testing.T) {
	b := startEraBridge(t, nil, "--era=legacy")
	body := map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"}
	if r := eraPost(t, b, acceptJSON, body, nil); r.status != 202 {
		t.Fatalf("%d %s", r.status, r.body)
	}
	if indexOf(childReceived(t, b), "notifications/initialized") < 0 {
		t.Fatal("not forwarded")
	}
}

// An enveloped notifications/initialized is never forwarded; the 2025-shaped
// one that follows a 2025 handshake still is.
func TestNotify_EnvelopedInitializedIsNeverForwarded(t *testing.T) {
	b := startEraBridge(t, nil, "--era=legacy")
	session := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil).header.Get("Mcp-Session-Id")
	hdr := map[string]string{"Mcp-Session-Id": session}

	enveloped := map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized", "params": map[string]any{"_meta": eraMeta(eraTestVersion)}}
	if r := eraPost(t, b, acceptJSON, enveloped, hdr); r.status != 202 {
		t.Fatalf("enveloped: %d %s", r.status, r.body)
	}
	if indexOf(childReceived(t, b), "notifications/initialized") >= 0 {
		t.Fatal("the enveloped notifications/initialized reached the child")
	}
	plain := map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"}
	if r := eraPost(t, b, acceptJSON, plain, hdr); r.status != 202 {
		t.Fatalf("2025-shaped: %d %s", r.status, r.body)
	}
	if indexOf(childReceived(t, b), "notifications/initialized") < 0 {
		t.Fatal("the 2025-shaped notifications/initialized was not forwarded")
	}
}

// A client's cancel is written only to the process its request was sent to.
func TestCancel_ClientCancelGoesOnlyToTheRequestsOwnProcess(t *testing.T) {
	b := startEraBridge(t, nil, "--era=legacy")
	old := b.GetCurrentChild()
	t.Cleanup(func() { _ = old.Close() })
	next := newEraChild(t, "--era=legacy")

	addPending := func(id int64, clientID string, child *StdioChild) {
		e := &pendingEntry{originalID: clientID, child: child, plain: httptest.NewRecorder(), timer: time.NewTimer(time.Hour), done: make(chan struct{})}
		b.mu.Lock()
		b.pending[id] = e
		b.mu.Unlock()
		t.Cleanup(func() {
			b.mu.Lock()
			delete(b.pending, id)
			b.mu.Unlock()
		})
	}
	const onOld, onNext = firstBridgeID + 100, firstBridgeID + 200
	addPending(onOld, "old-request", old)
	b.ReplaceChild(next)
	addPending(onNext, "new-request", next)

	cancel := func(clientID string) int {
		return eraPost(t, b, acceptJSON, map[string]any{
			"jsonrpc": "2.0", "method": "notifications/cancelled", "params": map[string]any{"requestId": clientID},
		}, nil).status
	}
	if status := cancel("old-request"); status != 202 {
		t.Fatalf("cancel of a request on the replaced process: %d", status)
	}
	if status := cancel("new-request"); status != 202 {
		t.Fatalf("cancel of a request on the current process: %d", status)
	}
	// The child reads in order: the answer to this follows any cancel written.
	if n := childCancelCount(t, b, int64(onOld)); n != 0 {
		t.Fatalf("the replacement process got %d cancels for the old process's request", n)
	}
	// The old process is no longer the bridge's, but it still runs: ask it
	// directly, over its own pipe.
	answer := make(chan map[string]any, 1)
	old.OnMessage(func(m map[string]any) {
		if m["id"] == "old-count" {
			answer <- m
		}
	})
	if err := old.Send(map[string]any{"jsonrpc": "2.0", "id": "old-count", "method": "debug/cancelCount", "params": map[string]any{"requestId": float64(onOld)}}); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-answer:
		if res, _ := m["result"].(map[string]any); res == nil || res["count"] != float64(0) {
			t.Fatalf("the old process was sent a cancel: %v", m)
		}
	case <-time.After(listenWait):
		t.Fatal("the old process did not answer")
	}
	if n := childCancelCount(t, b, int64(onNext)); n != 1 {
		t.Fatalf("the current process got %d cancels for its own request, want 1", n)
	}
}
