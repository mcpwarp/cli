//go:build !windows

package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/mcpwarp/cli/internal/bridge"
	"github.com/mcpwarp/cli/internal/shutdown"
	"github.com/mcpwarp/cli/internal/tunnel"
	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// buildFakeMCPBinary builds internal/bridge/testdata/fakemcp's fixture, the
// same one internal/bridge's own e2e tests use — this package can't reach
// its unexported buildFakeMCP helper, so it's built fresh here by full
// import path.
func buildFakeMCPBinary(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	out := filepath.Join(dir, "fakemcp-bin")
	cmd := exec.Command("go", "build", "-o", out, "github.com/mcpwarp/cli/internal/bridge/testdata/fakemcp")
	cmd.Env = append(os.Environ(), "GOFLAGS=")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build fakemcp fixture: %v\n%s", err, output)
	}
	return out
}

// fakeTunnelServer is an in-process stand-in for the SaaS tunnel: a real
// net/http server upgrading to WebSocket and running wsmixer's own
// server-side handshake (AcceptConn) — the same pattern internal/tunnel's
// own tests use (tunnel_test.go), reimplemented here since that package's
// version is unexported.
type fakeTunnelServer struct {
	srv *httptest.Server
	url string

	registerCh   chan struct{}
	unregisterCh chan struct{}
	conn         atomic.Pointer[wsmixer.Conn]

	publicHost string
	serviceID  string

	// unregisterSeenAt is set the instant the "unregister" frame arrives,
	// for TestUpE2E_RegisterRelayShutdownUnregister to compare against the
	// child's actual death time (S-5) — a channel send alone can't be
	// timestamped from the receiving side without racing whatever the
	// test does before it gets around to reading it.
	mu               sync.Mutex
	unregisterSeenAt time.Time
}

func newFakeTunnelServer(t *testing.T, publicHost, serviceID string) *fakeTunnelServer {
	t.Helper()
	f := &fakeTunnelServer{
		registerCh:   make(chan struct{}, 8),
		unregisterCh: make(chan struct{}, 8),
		publicHost:   publicHost,
		serviceID:    serviceID,
	}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols:    []string{wsmixer.Subprotocol},
			CompressionMode: websocket.CompressionDisabled,
		})
		if err != nil {
			return
		}
		defer ws.CloseNow()

		bearer := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		c, err := wsmixer.AcceptConn(r.Context(), ws, bearer, wsmixer.AcceptOptions{
			ServerInfo: &wsmixer.ServerInfo{Name: "fake-tunnel", Version: "test"},
			Authenticate: func(ctx context.Context, h *wsmixer.Hello) (wsmixer.WelcomeMeta, error) {
				return wsmixer.WelcomeMeta{}, nil
			},
			Request: r,
		})
		if err != nil {
			return
		}
		f.conn.Store(c)
		c.OnApp(func(raw json.RawMessage) {
			var wrapper struct {
				Mcpwarp struct {
					Op string `json:"op"`
				} `json:"mcpwarp"`
			}
			if json.Unmarshal(raw, &wrapper) != nil {
				return
			}
			switch wrapper.Mcpwarp.Op {
			case "register":
				_ = c.SendApp(context.Background(), map[string]any{
					"mcpwarp": map[string]any{
						"v":  1,
						"op": "registered",
						"services": []map[string]any{
							{"name": "echo", "id": f.serviceID, "url": "https://" + f.publicHost + "/mcp", "created": true},
						},
						"errors": []any{},
					},
				})
				select {
				case f.registerCh <- struct{}{}:
				default:
				}
			case "unregister":
				f.mu.Lock()
				f.unregisterSeenAt = time.Now()
				f.mu.Unlock()
				select {
				case f.unregisterCh <- struct{}{}:
				default:
				}
			}
		})
		c.Run()
		<-c.Done()
	}))
	f.url = "ws" + f.srv.URL[len("http"):]
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeTunnelServer) waitRegistered(t *testing.T) {
	t.Helper()
	select {
	case <-f.registerCh:
	case <-time.After(5 * time.Second):
		t.Fatal("fake tunnel never received a register")
	}
}

func (f *fakeTunnelServer) waitUnregistered(t *testing.T) {
	t.Helper()
	select {
	case <-f.unregisterCh:
	case <-time.After(5 * time.Second):
		t.Fatal("fake tunnel never received an unregister")
	}
}

func (f *fakeTunnelServer) unregisterAt() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unregisterSeenAt
}

// openHTTPStream opens a server-initiated stream (mirroring a public
// request arriving at the SaaS tunnel) carrying a raw HTTP request with
// Host set to f.publicHost — DESIGN.md §3's "OPEN carries no metadata,
// routed by Host header" — and returns the parsed response.
func (f *fakeTunnelServer) openHTTPStream(t *testing.T, path string, body []byte) *http.Response {
	t.Helper()
	c := f.conn.Load()
	if c == nil {
		t.Fatal("no server-side connection yet")
	}
	stream, err := c.OpenStream(context.Background())
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, "http://"+f.publicHost+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Host = f.publicHost
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(body))
	if err := req.Write(stream); err != nil {
		t.Fatalf("write request: %v", err)
	}
	_ = stream.CloseWrite()

	resp, err := http.ReadResponse(bufio.NewReader(stream), req)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp
}

// waitForRegistryActive polls th's registry until host resolves — the
// fake tunnel's registerCh fires the instant it *sends* "registered" over
// the wire, which races the real client's dispatcher goroutine actually
// applying that reply into its registry (registry.ApplyRegistered). A
// stream opened before that application lands finds nothing in
// ResolveByHost and 404s/503s, so callers must wait here first.
func waitForRegistryActive(t *testing.T, th tunnelHandle, host string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if reg := th.Registry(); reg != nil {
			if _, _, ok := reg.ResolveByHost(host); ok {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("registry never resolved host %q as active", host)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// pidAlive reports whether pid still names a live process — POSIX-only
// (syscall.Kill(pid, 0)), matching this repo's other pid-liveness checks
// (internal/bridge's own e2e tests use the same signal-0 probe).
func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	return syscall.Kill(pid, 0) == nil
}

// TestUpE2E_RegisterRelayShutdownUnregister runs `mcpwarp up` in-process
// end to end (M3B): a real stdio bridge+supervisor fronting the fakemcp
// fixture, a real internal/tunnel.Tunnel dialing an in-process fake tunnel
// server (net/http + wsmixer.AcceptConn, mirroring internal/tunnel's own
// test pattern), asserting register reaches the fake tunnel, an HTTP
// request opened as a stream reaches the bridge and gets a real response,
// then a shutdown sequence unregisters and kills the child.
func TestUpE2E_RegisterRelayShutdownUnregister(t *testing.T) {
	t.Cleanup(bridge.KillAllLiveChildren) // last-resort net, in case anything below leaks

	fakemcp := buildFakeMCPBinary(t)

	dir := t.TempDir()
	t.Setenv("MCPWARP_TOKEN", "mcpwarp_pat_e2e")

	configRaw := fmt.Sprintf(`{"servers": [
		{"name":"echo","kind":"stdio","command":%q,"args":[]}
	]}`, fakemcp)
	configPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(configPath, []byte(configRaw), 0o644); err != nil {
		t.Fatal(err)
	}

	fake := newFakeTunnelServer(t, "echo.e2e.mcpwarp.test", "e2e-service-1")

	shutdown.ResetForTests()
	t.Cleanup(shutdown.ResetForTests)

	var bridgeMu sync.Mutex
	var startedBridge *bridge.Bridge
	var tunnelMu sync.Mutex
	var startedTunnel tunnelHandle
	upCtx, cancelUp := context.WithCancel(context.Background())
	t.Cleanup(cancelUp)

	upDone := make(chan error, 1)
	ctx := &Context{
		ConfigPath:         configPath,
		HomeDir:            dir,
		ConnectURLOverride: fake.url,
		Log:                NewLogger(false),
		Ctx:                upCtx,
	}
	go func() {
		upDone <- runUp(ctx, true, upDeps{
			BlockForever: func(c context.Context) { <-c.Done() },
			OnBridgeStarted: func(name string, br *bridge.Bridge) {
				bridgeMu.Lock()
				startedBridge = br
				bridgeMu.Unlock()
			},
			StartTunnel: func(c context.Context, cfg tunnel.Config) (tunnelHandle, error) {
				th, err := tunnel.Start(c, cfg)
				if err != nil {
					return nil, err
				}
				tunnelMu.Lock()
				startedTunnel = th
				tunnelMu.Unlock()
				return th, nil
			},
		})
	}()
	t.Cleanup(func() {
		cancelUp()
		select {
		case <-upDone:
		case <-time.After(5 * time.Second):
			t.Error("runUp goroutine did not exit after cancellation")
		}
	})

	fake.waitRegistered(t)

	bridgeMu.Lock()
	br := startedBridge
	bridgeMu.Unlock()
	if br == nil {
		t.Fatal("OnBridgeStarted was never called")
	}
	child := br.GetCurrentChild()
	if child == nil {
		t.Fatal("bridge has no current child")
	}
	pid := child.Pid()
	if !pidAlive(pid) {
		t.Fatalf("expected child pid %d to be alive before shutdown", pid)
	}

	tunnelMu.Lock()
	th := startedTunnel
	tunnelMu.Unlock()
	if th == nil {
		t.Fatal("StartTunnel seam was never called")
	}
	// The fake tunnel's registerCh fires as soon as it *sends* "registered",
	// which races the real client applying that reply into its own
	// registry — wait for the registry itself before relying on it below.
	waitForRegistryActive(t, th, "echo.e2e.mcpwarp.test")

	// A "ping" request opened as a stream (mirroring a public HTTP request
	// hitting the tunnel) should reach the bridge, which reaches the real
	// fakemcp child, and come back with a real "pong" — the full
	// tunnel -> registry -> relay -> bridge -> stdio path.
	resp := fake.openHTTPStream(t, "/mcp", []byte(`{"jsonrpc":"2.0","id":7,"method":"ping"}`))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("relayed request status = %d", resp.StatusCode)
	}
	var decoded map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode relayed response: %v", err)
	}
	if decoded["result"] != "pong" {
		t.Fatalf("unexpected relayed result: %+v", decoded)
	}

	// --- shutdown: run the same bounded sequence shutdown.Run/FatalExit
	// would, without exiting the test binary, then assert its effects.
	// The pidAlive watcher starts before RunHandlersForTests (not after
	// waitUnregistered returns) so childDeadAt reflects when the child
	// actually died, not merely when this goroutine got scheduled to look
	// — otherwise the ordering assertion below is tautological (S-5).
	childDead := make(chan time.Time, 1)
	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for pidAlive(pid) {
			if time.Now().After(deadline) {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		childDead <- time.Now()
	}()

	shutdown.RunHandlersForTests(context.Background())

	fake.waitUnregistered(t)
	unregisterSeenAt := fake.unregisterAt()

	var childDeadAt time.Time
	select {
	case childDeadAt = <-childDead:
	case <-time.After(5 * time.Second):
		t.Fatalf("child pid %d still alive after shutdown", pid)
	}

	// DESIGN.md §3's shutdown ordering: unregister reaches the tunnel
	// before the child is killed, not merely before the test happens to
	// observe it dead.
	if !unregisterSeenAt.Before(childDeadAt) {
		t.Fatalf("unregister seen at %v, want strictly before child death at %v", unregisterSeenAt, childDeadAt)
	}

	// runUp's own exit (cancelUp + drain upDone) is handled by the
	// t.Cleanup registered above, so every process/goroutine this test
	// started is torn down there regardless of where this function returns.
}
