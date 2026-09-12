package tunnel

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/mcpwarp/cli/internal/appproto"
	"github.com/mcpwarp/cli/internal/eventbus"
	"github.com/mcpwarp/cli/internal/registry"
	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// staticToken is the test TokenSource: never fails, no ctx work.
type staticToken struct{ token string }

func (s staticToken) Token(context.Context) (string, error) { return s.token, nil }

// fakeTunnelServer is a scripted in-process stand-in for the SaaS tunnel:
// a real net/http server upgrading to WebSocket and running wsmixer's own
// server-side handshake (AcceptConn) — the "in-process fake tunnel" the
// task calls for, as opposed to a subprocess conformance harness.
type fakeTunnelServer struct {
	srv *httptest.Server
	url string

	mu     sync.Mutex
	conns  []*wsmixer.Conn
	onConn func(c *wsmixer.Conn) // called once per accepted connection, on its own goroutine
}

func newFakeTunnelServer(t *testing.T, onConn func(c *wsmixer.Conn)) *fakeTunnelServer {
	t.Helper()
	f := &fakeTunnelServer{onConn: onConn}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols:    []string{wsmixer.Subprotocol},
			CompressionMode: websocket.CompressionDisabled,
		})
		if err != nil {
			return
		}
		defer ws.CloseNow()

		bearer := r.Header.Get("Authorization")
		if !strings.HasPrefix(bearer, "Bearer ") {
			// A malformed/missing Authorization header used to panic here
			// (slicing past the header's length) instead of failing the
			// test cleanly.
			t.Errorf("fake tunnel: request missing a Bearer Authorization header, got %q", bearer)
			ws.Close(websocket.StatusPolicyViolation, "missing bearer token")
			return
		}
		bearer = strings.TrimPrefix(bearer, "Bearer ")
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
		f.mu.Lock()
		f.conns = append(f.conns, c)
		f.mu.Unlock()
		if f.onConn != nil {
			f.onConn(c)
		}
		c.Run()
		<-c.Done()
	}))
	f.url = "ws" + f.srv.URL[len("http"):]
	t.Cleanup(f.srv.Close)
	return f
}

// connCount reports how many connections this fake server has accepted so
// far (across every reconnect).
func (f *fakeTunnelServer) connCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.conns)
}

// latestConn returns the most recently accepted connection, or nil if none
// yet — unlike a CompareAndSwap-guarded atomic.Pointer, this always reflects
// the current connection across any number of reconnects.
func (f *fakeTunnelServer) latestConn() *wsmixer.Conn {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.conns) == 0 {
		return nil
	}
	return f.conns[len(f.conns)-1]
}

func newTestBus(t *testing.T) *eventbus.Bus {
	t.Helper()
	bus := eventbus.New(64)
	// Drain both channels in the background so Publish/PublishTelemetry
	// never blocks the test; t.Cleanup's Close makes both range loops (and
	// so these goroutines) exit once the test is done.
	go func() {
		for range bus.Control {
		}
	}()
	go func() {
		for range bus.Telemetry {
		}
	}()
	t.Cleanup(bus.Close)
	return bus
}

func TestRegisterOnConnect(t *testing.T) {
	var registerSeen atomic.Bool
	srv := newFakeTunnelServer(t, func(c *wsmixer.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			var wrapper struct {
				Mcpwarp struct {
					Op       string `json:"op"`
					Services []struct {
						Name string `json:"name"`
					} `json:"services"`
				} `json:"mcpwarp"`
			}
			if json.Unmarshal(raw, &wrapper) == nil && wrapper.Mcpwarp.Op == "register" {
				if len(wrapper.Mcpwarp.Services) == 1 && wrapper.Mcpwarp.Services[0].Name == "svc" {
					registerSeen.Store(true)
				}
				_ = c.SendApp(context.Background(), map[string]any{
					"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": []map[string]any{
						{"name": "svc", "id": "id1", "url": "https://svc.example/mcp", "created": true},
					}, "errors": []any{}},
				})
			}
		})
	})

	tun, err := Start(context.Background(), Config{
		URL:         srv.url,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Bus:         newTestBus(t),
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(int) {},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for !registerSeen.Load() {
		if time.Now().After(deadline) {
			t.Fatal("register was never sent on connect")
		}
		time.Sleep(time.Millisecond)
	}

	// The "registered" reply above should have populated the registry.
	deadline = time.Now().Add(2 * time.Second)
	for {
		if _, ok := tun.Registry().Get("svc"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("registry was never updated from the registered reply")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestUnregisterOnClose(t *testing.T) {
	unregisterCh := make(chan struct{}, 1)
	srv := newFakeTunnelServer(t, func(c *wsmixer.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			var wrapper struct {
				Mcpwarp struct {
					Op string `json:"op"`
				} `json:"mcpwarp"`
			}
			if json.Unmarshal(raw, &wrapper) == nil {
				switch wrapper.Mcpwarp.Op {
				case "register":
					_ = c.SendApp(context.Background(), map[string]any{
						"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": []map[string]any{
							{"name": "svc", "id": "id1", "url": "https://svc.example/mcp", "created": true},
						}, "errors": []any{}},
					})
				case "unregister":
					select {
					case unregisterCh <- struct{}{}:
					default:
					}
				}
			}
		})
	})

	tun, err := Start(context.Background(), Config{
		URL:         srv.url,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Bus:         newTestBus(t),
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(int) {},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := tun.Unregister(context.Background()); err != nil {
		t.Fatalf("Unregister: %v", err)
	}

	select {
	case <-unregisterCh:
	case <-time.After(2 * time.Second):
		t.Fatal("server never received unregister")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tun.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestCloseBoundedByCancelledCtx proves Close's relay/send waits are bounded
// by the passed ctx, not only by relayShutdownBudget/appSendTimeout: with a
// stuck relay goroutine (relayWG never reaches zero) and an already-
// cancelled ctx, Close must return almost immediately. On the old
// bare-time.After behaviour this would instead block for
// relayShutdownBudget (5s).
func TestCloseBoundedByCancelledCtx(t *testing.T) {
	srv := newFakeTunnelServer(t, nil)
	tun, err := Start(context.Background(), Config{
		URL:         srv.url,
		TokenSource: staticToken{token: "t"},
		Bus:         newTestBus(t),
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(int) {},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Simulate a relay goroutine that never notices relayCancel — Close's
	// relayWG.Wait() would otherwise hang until relayShutdownBudget.
	tun.relayWG.Add(1)
	t.Cleanup(tun.relayWG.Done)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_ = tun.Close(ctx)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Close took %v with an already-cancelled ctx, want it bounded well under relayShutdownBudget=%v", elapsed, relayShutdownBudget)
	}
}

func TestDisableAndEnableCallbacks(t *testing.T) {
	var disabledName, enabledName atomic.Value
	disabledName.Store("")
	enabledName.Store("")

	var serverConn atomic.Pointer[wsmixer.Conn]
	srv := newFakeTunnelServer(t, func(c *wsmixer.Conn) {
		serverConn.Store(c)
		c.OnApp(func(raw json.RawMessage) {
			var wrapper struct {
				Mcpwarp struct{ Op string } `json:"mcpwarp"`
			}
			_ = json.Unmarshal(raw, &wrapper)
			if wrapper.Mcpwarp.Op == "register" {
				_ = c.SendApp(context.Background(), map[string]any{
					"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": []map[string]any{
						{"name": "svc", "id": "id1", "url": "https://svc.example/mcp", "created": true},
					}, "errors": []any{}},
				})
			}
		})
	})

	tun, err := Start(context.Background(), Config{
		URL:         srv.url,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Bus:         newTestBus(t),
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(int) {},
		OnDisable: func(name, id, reason string) {
			disabledName.Store(name)
		},
		OnEnable: func(name string) {
			enabledName.Store(name)
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	waitFor := func(v *atomic.Value, want string) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for v.Load().(string) != want {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %q, got %q", want, v.Load())
			}
			time.Sleep(time.Millisecond)
		}
	}

	// Wait for registration to land before disabling.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := tun.Registry().Get("svc"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("registry never populated")
		}
		time.Sleep(time.Millisecond)
	}

	c := serverConn.Load()
	_ = c.SendApp(context.Background(), map[string]any{
		"mcpwarp": map[string]any{"v": 1, "op": "disable", "id": "id1", "reason": "quota"},
	})
	waitFor(&disabledName, "svc")

	_ = c.SendApp(context.Background(), map[string]any{
		"mcpwarp": map[string]any{"v": 1, "op": "enable", "id": "id1", "name": "svc"},
	})
	waitFor(&enabledName, "svc")
}

func TestUnsupportedVersionIsFatal(t *testing.T) {
	srv := newFakeTunnelServer(t, func(c *wsmixer.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			_ = c.SendApp(context.Background(), map[string]any{
				"mcpwarp": map[string]any{"v": 1, "op": "error", "code": "UNSUPPORTED_VERSION", "message": "upgrade"},
			})
		})
	})

	var exitCode atomic.Int32
	exitCode.Store(-1)
	tun, err := Start(context.Background(), Config{
		URL:         srv.url,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Bus:         newTestBus(t),
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(code int) { exitCode.Store(int32(code)) },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for exitCode.Load() == -1 {
		if time.Now().After(deadline) {
			t.Fatal("FatalExit was never called for UNSUPPORTED_VERSION")
		}
		time.Sleep(time.Millisecond)
	}
	if exitCode.Load() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode.Load())
	}
}

// TestRegisteredZeroSuccessesFirstConnectQuotaExceededIsFatal covers
// blocker B3: a "registered" reply with zero services and a non-
// SERVER_DISABLED error, on a connection that has never registered
// anything successfully, must exit — Node client.ts's
// "!sawFirstRegistered" fatal path.
func TestRegisteredZeroSuccessesFirstConnectQuotaExceededIsFatal(t *testing.T) {
	srv := newFakeTunnelServer(t, func(c *wsmixer.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			var wrapper struct {
				Mcpwarp struct{ Op string } `json:"mcpwarp"`
			}
			_ = json.Unmarshal(raw, &wrapper)
			if wrapper.Mcpwarp.Op == "register" {
				_ = c.SendApp(context.Background(), map[string]any{
					"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": []any{}, "errors": []map[string]any{
						{"name": "svc", "code": "QUOTA_EXCEEDED", "message": "over quota"},
					}},
				})
			}
		})
	})

	var exitCode atomic.Int32
	exitCode.Store(-1)
	tun, err := Start(context.Background(), Config{
		URL:         srv.url,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Bus:         newTestBus(t),
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(code int) { exitCode.Store(int32(code)) },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for exitCode.Load() == -1 {
		if time.Now().After(deadline) {
			t.Fatal("FatalExit was never called for a first-connect QUOTA_EXCEEDED-only registered reply")
		}
		time.Sleep(time.Millisecond)
	}
	if exitCode.Load() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode.Load())
	}
}

// TestRegisteredZeroSuccessesFirstConnectInvalidNameIsFatal pins that
// INVALID_NAME is not folded into the allServerDisabled loop alongside
// SERVER_DISABLED: a "registered" reply with zero services and only an
// INVALID_NAME error, on a connection that has never registered anything
// successfully, must still exit — same fatal path as
// TestRegisteredZeroSuccessesFirstConnectQuotaExceededIsFatal above.
func TestRegisteredZeroSuccessesFirstConnectInvalidNameIsFatal(t *testing.T) {
	srv := newFakeTunnelServer(t, func(c *wsmixer.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			var wrapper struct {
				Mcpwarp struct{ Op string } `json:"mcpwarp"`
			}
			_ = json.Unmarshal(raw, &wrapper)
			if wrapper.Mcpwarp.Op == "register" {
				_ = c.SendApp(context.Background(), map[string]any{
					"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": []any{}, "errors": []map[string]any{
						{"name": "bad-name", "code": "INVALID_NAME", "message": "server name must match [a-z0-9]([a-z0-9-]*[a-z0-9])? (max 30 chars)"},
					}},
				})
			}
		})
	})

	var exitCode atomic.Int32
	exitCode.Store(-1)
	tun, err := Start(context.Background(), Config{
		URL:         srv.url,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "bad-name", Kind: "http"}},
		Bus:         newTestBus(t),
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(code int) { exitCode.Store(int32(code)) },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for exitCode.Load() == -1 {
		if time.Now().After(deadline) {
			t.Fatal("FatalExit was never called for a first-connect INVALID_NAME-only registered reply")
		}
		time.Sleep(time.Millisecond)
	}
	if exitCode.Load() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode.Load())
	}
}

// TestRegisteredAllServerDisabledIsNotFatal covers blocker B3's other half:
// a "registered" reply whose only error is SERVER_DISABLED must not exit,
// even on the very first connect — it's resumable via a later "enable".
func TestRegisteredAllServerDisabledIsNotFatal(t *testing.T) {
	srv := newFakeTunnelServer(t, func(c *wsmixer.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			var wrapper struct {
				Mcpwarp struct{ Op string } `json:"mcpwarp"`
			}
			_ = json.Unmarshal(raw, &wrapper)
			if wrapper.Mcpwarp.Op == "register" {
				_ = c.SendApp(context.Background(), map[string]any{
					"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": []any{}, "errors": []map[string]any{
						{"name": "svc", "code": "SERVER_DISABLED", "message": "disabled in dashboard"},
					}},
				})
			}
		})
	})

	// Bus.Control is read directly below (not drained in the background by
	// newTestBus) so the test can wait for the deterministic signal that
	// handleApp has actually finished processing the reply — the
	// SERVER_DISABLED error's own AppError publish — rather than a fixed
	// sleep and a hope that it was long enough.
	bus := eventbus.New(64)
	go func() {
		for range bus.Telemetry {
		}
	}()
	t.Cleanup(bus.Close)

	var exitCode atomic.Int32
	exitCode.Store(-1)
	tun, err := Start(context.Background(), Config{
		URL:         srv.url,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Bus:         bus,
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(code int) { exitCode.Store(int32(code)) },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	deadline := time.After(2 * time.Second)
	for {
		select {
		case evt := <-bus.Control:
			if ae, ok := evt.(eventbus.AppError); ok && ae.Code == "SERVER_DISABLED" {
				goto processed
			}
		case <-deadline:
			t.Fatal("expected an AppError for the SERVER_DISABLED registered error, got none")
		}
	}
processed:
	if got := exitCode.Load(); got != -1 {
		t.Fatalf("expected no FatalExit for a SERVER_DISABLED-only registered reply, got exit code %d", got)
	}
}

// TestRegisteredInvalidNameAndUsernameRequiredHints covers the two per-
// service codes for the name-as-URL-slug rollout: INVALID_NAME and
// USERNAME_REQUIRED. Paired with a successful "good" service so
// zeroSuccesses is false, neither is fatal — this asserts the connection
// stays up and each error's AppError event is published with its code,
// mirroring TestRegisteredAllServerDisabledIsNotFatal's approach.
func TestRegisteredInvalidNameAndUsernameRequiredHints(t *testing.T) {
	srv := newFakeTunnelServer(t, func(c *wsmixer.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			var wrapper struct {
				Mcpwarp struct{ Op string } `json:"mcpwarp"`
			}
			_ = json.Unmarshal(raw, &wrapper)
			if wrapper.Mcpwarp.Op == "register" {
				_ = c.SendApp(context.Background(), map[string]any{
					"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": []any{
						map[string]any{"name": "good", "id": "svc-good", "url": "https://good.example/mcp", "created": true},
					}, "errors": []map[string]any{
						{"name": "bad-name", "code": "INVALID_NAME", "message": "server name must match [a-z0-9]([a-z0-9-]*[a-z0-9])? (max 30 chars)"},
						{"name": "no-username", "code": "USERNAME_REQUIRED", "message": "sign in to the dashboard once to choose a username"},
					}},
				})
			}
		})
	})

	bus := eventbus.New(64)
	go func() {
		for range bus.Telemetry {
		}
	}()
	t.Cleanup(bus.Close)

	var exitCode atomic.Int32
	exitCode.Store(-1)
	tun, err := Start(context.Background(), Config{
		URL:         srv.url,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "good", Kind: "http"}},
		Bus:         bus,
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(code int) { exitCode.Store(int32(code)) },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	seen := map[string]bool{}
	deadline := time.After(2 * time.Second)
	for len(seen) < 2 {
		select {
		case evt := <-bus.Control:
			if ae, ok := evt.(eventbus.AppError); ok {
				seen[ae.Code] = true
			}
		case <-deadline:
			t.Fatalf("expected AppError events for INVALID_NAME and USERNAME_REQUIRED, got %v", seen)
		}
	}
	if !seen["INVALID_NAME"] || !seen["USERNAME_REQUIRED"] {
		t.Fatalf("expected both INVALID_NAME and USERNAME_REQUIRED AppError events, got %v", seen)
	}
	if got := exitCode.Load(); got != -1 {
		t.Fatalf("expected no FatalExit alongside a successful registration, got exit code %d", got)
	}

	// The "good" service in the same reply should still have landed in the
	// registry — the two per-service errors above must not block it.
	regDeadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := tun.Registry().Get("good"); ok {
			break
		}
		if time.Now().After(regDeadline) {
			t.Fatal("registry was never updated with the successful \"good\" service")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestReconnectsAndReregisters(t *testing.T) {
	var registerCount atomic.Int32
	var firstConn atomic.Pointer[wsmixer.Conn]
	srv := newFakeTunnelServer(t, func(c *wsmixer.Conn) {
		firstConn.CompareAndSwap(nil, c)
		c.OnApp(func(raw json.RawMessage) {
			var wrapper struct {
				Mcpwarp struct{ Op string } `json:"mcpwarp"`
			}
			_ = json.Unmarshal(raw, &wrapper)
			if wrapper.Mcpwarp.Op == "register" {
				registerCount.Add(1)
			}
		})
	})

	tun, err := Start(context.Background(), Config{
		URL:         srv.url,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Bus:         newTestBus(t),
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(int) {},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for registerCount.Load() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("first register never arrived")
		}
		time.Sleep(time.Millisecond)
	}

	// Force a non-fatal disconnect: INTERNAL_ERROR isn't in the client's
	// fatal set, so ws-mixer-go's own reconnect state machine should redial
	// and this package's OnConnect should register again automatically.
	_ = firstConn.Load().Close(uint32(wsmixer.InternalErrorCode), "forcing reconnect for test")

	deadline = time.Now().Add(5 * time.Second)
	for registerCount.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("expected a second register after reconnect, got %d", registerCount.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

// TestReconnectsOnGoingAway covers the SDK's other reconnect path
// (client_reconnect.go): a 4012 (wsmixer.GoingAwayCode) close reconnects via
// its own short jitter(0,2s) schedule rather than the full backoff a plain
// INTERNAL_ERROR (4002, TestReconnectsAndReregisters) takes — both must
// still result in this package's OnConnect registering again.
func TestReconnectsOnGoingAway(t *testing.T) {
	var registerCount atomic.Int32
	var firstConn atomic.Pointer[wsmixer.Conn]
	srv := newFakeTunnelServer(t, func(c *wsmixer.Conn) {
		firstConn.CompareAndSwap(nil, c)
		c.OnApp(func(raw json.RawMessage) {
			var wrapper struct {
				Mcpwarp struct{ Op string } `json:"mcpwarp"`
			}
			_ = json.Unmarshal(raw, &wrapper)
			if wrapper.Mcpwarp.Op == "register" {
				registerCount.Add(1)
			}
		})
	})

	tun, err := Start(context.Background(), Config{
		URL:         srv.url,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Bus:         newTestBus(t),
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(int) {},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for registerCount.Load() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("first register never arrived")
		}
		time.Sleep(time.Millisecond)
	}

	// GoingAwayCode (0x0c) closes at WS code 4012 — the SDK's jitter(0,2s)
	// reconnect path, not the full-backoff one 4002/INTERNAL_ERROR takes.
	_ = firstConn.Load().Close(uint32(wsmixer.GoingAwayCode), "forcing a 4012 reconnect for test")

	// Generous deadline: full-jitter backoff (this package sets Cap to 30s,
	// S3) can occasionally land near its cap even on the first attempt, and
	// the 4012 path's own jitter is bounded at 2s regardless.
	deadline = time.Now().Add(10 * time.Second)
	for registerCount.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("expected a second register after a 4012 reconnect, got %d", registerCount.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

// TestUnauthorizedCloseIsFatalWithSessionExpiredMessage covers S4:
// classifyFatalMessage's 4011 (wsmixer.UnauthorizedCode) branch. Post-first-
// connect, ws-mixer-go's own client_reconnect.go sends 4010/4011 straight to
// goFatal (no retry), so the fake tunnel closing with UnauthorizedCode here
// must surface as FatalExit(1) with Node client.ts's exact wording.
func TestUnauthorizedCloseIsFatalWithSessionExpiredMessage(t *testing.T) {
	var firstConn atomic.Pointer[wsmixer.Conn]
	srv := newFakeTunnelServer(t, func(c *wsmixer.Conn) {
		firstConn.CompareAndSwap(nil, c)
	})

	var exitCode atomic.Int32
	exitCode.Store(-1)
	tun, err := Start(context.Background(), Config{
		URL:         srv.url,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Bus:         newTestBus(t),
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(code int) { exitCode.Store(int32(code)) },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for firstConn.Load() == nil {
		if time.Now().After(deadline) {
			t.Fatal("fake tunnel never accepted a connection")
		}
		time.Sleep(time.Millisecond)
	}

	_ = firstConn.Load().Close(uint32(wsmixer.UnauthorizedCode), "token no longer valid")

	deadline = time.Now().Add(2 * time.Second)
	for exitCode.Load() == -1 {
		if time.Now().After(deadline) {
			t.Fatal("FatalExit was never called for a post-connect 4011 close")
		}
		time.Sleep(time.Millisecond)
	}
	if exitCode.Load() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode.Load())
	}

	if got := classifyFatalMessage(srv.url, wsmixer.DisconnectReason{Fatal: true, WSCode: 4011}); got != "session expired, run `mcpwarp login`" {
		t.Fatalf("unexpected classifyFatalMessage for wsCode 4011: %q", got)
	}
}

// TestOnStreamNeverBlocksOnApp is the never-blocks contract: a stream
// relayed to a slow target must not delay a subsequent app message from
// being processed.
func TestOnStreamNeverBlocksOnApp(t *testing.T) {
	slowTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		w.Write([]byte("late"))
	}))
	t.Cleanup(slowTarget.Close)
	targetURL, _ := url.Parse(slowTarget.URL)

	var disabledAt atomic.Value
	disabledAt.Store(time.Time{})

	var serverConn atomic.Pointer[wsmixer.Conn]
	srv := newFakeTunnelServer(t, func(c *wsmixer.Conn) {
		serverConn.Store(c)
		c.OnApp(func(raw json.RawMessage) {
			var wrapper struct {
				Mcpwarp struct{ Op string } `json:"mcpwarp"`
			}
			_ = json.Unmarshal(raw, &wrapper)
			if wrapper.Mcpwarp.Op == "register" {
				_ = c.SendApp(context.Background(), map[string]any{
					"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": []map[string]any{
						{"name": "svc", "id": "id1", "url": "https://svc.example/mcp", "created": true},
					}, "errors": []any{}},
				})
			}
		})
	})

	// A custom bus, not newTestBus: Control's capacity (1) covers exactly
	// the one legitimate dispatcher-goroutine publish this test produces
	// before the assertion below (onConnect's ConnStateChanged{"connected"})
	// and no more, and — unlike newTestBus — nothing drains it until
	// openControl is closed at the very end of this test. Publish blocks
	// until either a consumer reads or the bus closes, so if
	// StreamOpened/StreamClosed were ever regressed back onto this
	// package's single dispatcher goroutine (instead of onStream's own
	// per-stream goroutine, which is unaffected by Control being full),
	// that publish would find the buffer already occupied and wedge the
	// dispatcher solid for the rest of the test — the disable below would
	// then never arrive. newTestBus's always-on background drain would
	// silently absorb that same bug, which is why it isn't used here.
	bus := eventbus.New(1)
	openControl := make(chan struct{})
	go func() {
		<-openControl
		for range bus.Control {
		}
	}()
	go func() {
		for range bus.Telemetry {
		}
	}()
	t.Cleanup(bus.Close)

	tun, err := Start(context.Background(), Config{
		URL:         srv.url,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Targets:     map[string]*url.URL{"svc": targetURL},
		Bus:         bus,
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(int) {},
		OnDisable: func(name, id, reason string) {
			disabledAt.Store(time.Now())
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := tun.Registry().Get("svc"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("registry never populated")
		}
		time.Sleep(time.Millisecond)
	}

	c := serverConn.Load()
	stream, err := c.OpenStream(context.Background())
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	fmt.Fprintf(stream, "GET /mcp HTTP/1.1\r\nHost: svc.example\r\n\r\n")
	_ = stream.CloseWrite()

	start := time.Now()
	_ = c.SendApp(context.Background(), map[string]any{
		"mcpwarp": map[string]any{"v": 1, "op": "disable", "id": "id1", "reason": "test"},
	})

	deadline = time.Now().Add(1 * time.Second)
	for disabledAt.Load().(time.Time).IsZero() {
		if time.Now().After(deadline) {
			t.Fatal("disable callback never fired — OnStream may have blocked the app-message goroutine")
		}
		time.Sleep(time.Millisecond)
	}
	if elapsed := disabledAt.Load().(time.Time).Sub(start); elapsed > time.Second {
		t.Fatalf("disable took %v to process — the slow relay stream appears to have blocked it", elapsed)
	}

	// Assertions done — let Control drain freely so tun.Close()'s own
	// ConnStateChanged{"closed"} publish (and the still-in-flight slow
	// relay's eventual StreamOpened/StreamClosed) doesn't hang the test.
	close(openControl)
}

// capturingHandler is a minimal slog.Handler that records every log record
// it's given, so a test can assert on a specific message having been
// logged instead of just on the resulting side effects.
type capturingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r)
	h.mu.Unlock()
	return nil
}
func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler      { return h }

func (h *capturingHandler) hasMessage(want string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Message == want {
			return true
		}
	}
	return false
}

// TestUnknownOpWarnsWithoutFatal covers handleApp's OpUnknown case
// (DESIGN.md §8's "ignored-with-log"): an unrecognized op must warn-log and
// otherwise be ignored — never treated as fatal.
func TestUnknownOpWarnsWithoutFatal(t *testing.T) {
	srv := newFakeTunnelServer(t, func(c *wsmixer.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			var wrapper struct {
				Mcpwarp struct{ Op string } `json:"mcpwarp"`
			}
			_ = json.Unmarshal(raw, &wrapper)
			if wrapper.Mcpwarp.Op == "register" {
				_ = c.SendApp(context.Background(), map[string]any{
					"mcpwarp": map[string]any{"v": 1, "op": "totally-unrecognized-op"},
				})
			}
		})
	})

	handler := &capturingHandler{}
	var exitCode atomic.Int32
	exitCode.Store(-1)
	tun, err := Start(context.Background(), Config{
		URL:         srv.url,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Bus:         newTestBus(t),
		Log:         slog.New(handler),
		FatalExit:   func(code int) { exitCode.Store(int32(code)) },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for !handler.hasMessage("ignoring unrecognized mcpwarp app message") {
		if time.Now().After(deadline) {
			t.Fatal("expected a warn log for the unrecognized op, got none")
		}
		time.Sleep(time.Millisecond)
	}
	if got := exitCode.Load(); got != -1 {
		t.Fatalf("expected no FatalExit for an unrecognized op, got exit code %d", got)
	}
}

// TestEnableClearsPendingDisableEvenWhenNameNotConfigured covers Node
// client.ts's item 4: a live "enable" must cancel any deferred disable for
// the same id regardless of whether this connection's own name-gate
// (hasConfiguredService) accepts the enable itself — otherwise a later
// "registered" batch that reuses that id could resolve a now-stale disable.
func TestEnableClearsPendingDisableEvenWhenNameNotConfigured(t *testing.T) {
	var serverConn atomic.Pointer[wsmixer.Conn]
	srv := newFakeTunnelServer(t, func(c *wsmixer.Conn) {
		serverConn.Store(c)
	})

	var disabledName atomic.Value
	disabledName.Store("")

	tun, err := Start(context.Background(), Config{
		URL:         srv.url,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Bus:         newTestBus(t),
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(int) {},
		OnDisable: func(name, id, reason string) {
			disabledName.Store(name)
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for serverConn.Load() == nil {
		if time.Now().After(deadline) {
			t.Fatal("fake tunnel never accepted a connection")
		}
		time.Sleep(time.Millisecond)
	}
	c := serverConn.Load()

	// A disable for id1 arrives before any "registered" has ever named it
	// — deferred in pendingDisables.
	_ = c.SendApp(context.Background(), map[string]any{
		"mcpwarp": map[string]any{"v": 1, "op": "disable", "id": "id1", "reason": "quota"},
	})

	// An enable for id1 names a service this connection doesn't hold — the
	// name-gate below rejects it, but the deferred disable above must still
	// be cancelled first.
	_ = c.SendApp(context.Background(), map[string]any{
		"mcpwarp": map[string]any{"v": 1, "op": "enable", "id": "id1", "name": "not-my-service"},
	})

	// id1 now resolves to the configured "svc" — if the enable above hadn't
	// cleared the deferred disable, this "registered" reply would
	// erroneously resolve and fire a stale OnDisable for svc.
	_ = c.SendApp(context.Background(), map[string]any{
		"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": []map[string]any{
			{"name": "svc", "id": "id1", "url": "https://svc.example/mcp", "created": true},
		}, "errors": []any{}},
	})

	deadline = time.Now().Add(2 * time.Second)
	for {
		if _, ok := tun.Registry().Get("svc"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("registry never populated")
		}
		time.Sleep(time.Millisecond)
	}

	// handleApp's "registered" case already ran ResolvePendingDisables
	// synchronously above the registry-populated check; OnDisable itself
	// now fires on its own goroutine (N3), so give it a bounded window to
	// have run if it were (erroneously) going to.
	deadline = time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := disabledName.Load().(string); got != "" {
			t.Fatalf("expected the deferred disable to have been cleared by the enable, but OnDisable fired for %q", got)
		}
		time.Sleep(time.Millisecond)
	}
}

// registeredNames extracts the sorted "register" batch's service names from
// a raw app message, or nil if raw isn't a "register".
func registeredNames(raw json.RawMessage) []string {
	var wrapper struct {
		Mcpwarp struct {
			Op       string `json:"op"`
			Services []struct {
				Name string `json:"name"`
			} `json:"services"`
		} `json:"mcpwarp"`
	}
	if json.Unmarshal(raw, &wrapper) != nil || wrapper.Mcpwarp.Op != "register" {
		return nil
	}
	names := make([]string, len(wrapper.Mcpwarp.Services))
	for i, s := range wrapper.Mcpwarp.Services {
		names[i] = s.Name
	}
	return names
}

// TestLocalDisablePersistsAcrossReconnect covers the bug fix: a local `d`
// (UnregisterService) must survive a reconnect instead of the next
// welcome's register batch silently resurrecting the name, and a following
// `e` (RegisterService) must bring it back on the reconnect after that.
func TestLocalDisablePersistsAcrossReconnect(t *testing.T) {
	var mu sync.Mutex
	var batches [][]string

	srv := newFakeTunnelServer(t, func(c *wsmixer.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			if names := registeredNames(raw); names != nil {
				mu.Lock()
				batches = append(batches, names)
				mu.Unlock()
			}
		})
	})

	tun, err := Start(context.Background(), Config{
		URL:         srv.url,
		TokenSource: staticToken{token: "t"},
		Services: []appproto.RegisterService{
			{Name: "svc-a", Kind: "http"},
			{Name: "svc-b", Kind: "http"},
		},
		Bus:       newTestBus(t),
		Log:       slog.New(slog.DiscardHandler),
		FatalExit: func(int) {},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	batchCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(batches)
	}
	batchAt := func(i int) []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), batches[i]...)
	}
	waitForBatch := func(n int) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for batchCount() < n {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for register batch #%d, got %d so far", n, batchCount())
			}
			time.Sleep(time.Millisecond)
		}
	}

	// Batch 1: the very first connect, unaffected by localUnregistered
	// (empty) — both names present.
	waitForBatch(1)
	if got := batchAt(0); len(got) != 2 {
		t.Fatalf("first register batch = %v, want both services", got)
	}

	waitForConn := func(n int) *wsmixer.Conn {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for srv.connCount() < n {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for the fake tunnel to accept connection #%d", n)
			}
			time.Sleep(time.Millisecond)
		}
		return srv.latestConn()
	}

	// A local `d` on svc-a, then force a reconnect.
	if err := tun.UnregisterService("svc-a"); err != nil {
		t.Fatalf("UnregisterService: %v", err)
	}
	_ = waitForConn(1).Close(uint32(wsmixer.InternalErrorCode), "forcing reconnect for test")

	// Batch 2: the reconnect's register must omit svc-a.
	waitForBatch(2)
	got := batchAt(1)
	for _, name := range got {
		if name == "svc-a" {
			t.Fatalf("register batch after local disable = %v, want svc-a omitted", got)
		}
	}
	if len(got) != 1 || got[0] != "svc-b" {
		t.Fatalf("register batch after local disable = %v, want [svc-b]", got)
	}

	// A local `e` on svc-a: RegisterService itself immediately re-sends just
	// svc-a over the still-live connection (sendRegisterFor, batch 3) before
	// the forced reconnect below produces the batch this test actually
	// wants to inspect (batch 4, once localUnregistered is empty again).
	tun.RegisterService("svc-a")
	waitForBatch(3)
	_ = waitForConn(2).Close(uint32(wsmixer.InternalErrorCode), "forcing a second reconnect for test")

	waitForBatch(4)
	got = batchAt(3)
	if len(got) != 2 {
		t.Fatalf("register batch after re-enable = %v, want both services back", got)
	}
}

// TestLocalDisableOfOnlyServiceSendsNoRegisterOnReconnect covers
// currentRegisterBody's empty-subset case: with a single configured
// service, a local `d` followed by a forced reconnect must produce no
// "register" op at all on the new connection — neither this CLI nor Node's
// ever sends a register with an empty services list, and the fake tunnel
// must see nothing to unregister-and-resurrect race against.
func TestLocalDisableOfOnlyServiceSendsNoRegisterOnReconnect(t *testing.T) {
	var mu sync.Mutex
	var batches [][]string

	srv := newFakeTunnelServer(t, func(c *wsmixer.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			if names := registeredNames(raw); names != nil {
				mu.Lock()
				batches = append(batches, names)
				mu.Unlock()
			}
		})
	})

	tun, err := Start(context.Background(), Config{
		URL:         srv.url,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Bus:         newTestBus(t),
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(int) {},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	batchCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(batches)
	}
	waitForBatch := func(n int) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for batchCount() < n {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for register batch #%d, got %d so far", n, batchCount())
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitForConn := func(n int) *wsmixer.Conn {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for srv.connCount() < n {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for the fake tunnel to accept connection #%d", n)
			}
			time.Sleep(time.Millisecond)
		}
		return srv.latestConn()
	}

	waitForBatch(1)

	if err := tun.UnregisterService("svc"); err != nil {
		t.Fatalf("UnregisterService: %v", err)
	}
	_ = waitForConn(1).Close(uint32(wsmixer.InternalErrorCode), "forcing reconnect for test")
	waitForConn(2)

	// Give the (absent) register a brief window to have arrived if it were
	// (erroneously) going to.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if batchCount() > 1 {
			t.Fatalf("got a register op on the reconnect with only one, locally-disabled, service: %v", batches[1])
		}
		time.Sleep(time.Millisecond)
	}
}

// TestServerDisablePersistsAcrossReconnect covers item 3: a dashboard-driven
// OpDisable (unlike a local `d`) must NOT be treated as a local
// unregister — the name must still be sent in the next reconnect's register
// batch so the server can reply SERVER_DISABLED and the resume-on-enable
// flow (OpEnable) keeps working.
func TestServerDisablePersistsAcrossReconnect(t *testing.T) {
	var mu sync.Mutex
	var batches [][]string
	var conn atomic.Pointer[wsmixer.Conn]

	srv := newFakeTunnelServer(t, func(c *wsmixer.Conn) {
		conn.Store(c)
		c.OnApp(func(raw json.RawMessage) {
			if names := registeredNames(raw); names != nil {
				mu.Lock()
				batches = append(batches, names)
				mu.Unlock()
				_ = c.SendApp(context.Background(), map[string]any{
					"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": []map[string]any{
						{"name": "svc", "id": "id1", "url": "https://svc.example/mcp", "created": true},
					}, "errors": []any{}},
				})
			}
		})
	})

	var disabledName atomic.Value
	disabledName.Store("")

	tun, err := Start(context.Background(), Config{
		URL:         srv.url,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Bus:         newTestBus(t),
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(int) {},
		OnDisable: func(name, id, reason string) {
			disabledName.Store(name)
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	batchCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(batches)
	}
	waitForBatch := func(n int) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for batchCount() < n {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for register batch #%d, got %d so far", n, batchCount())
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitForBatch(1)

	c := conn.Load()
	_ = c.SendApp(context.Background(), map[string]any{
		"mcpwarp": map[string]any{"v": 1, "op": "disable", "id": "id1", "reason": "quota"},
	})
	deadline := time.Now().Add(2 * time.Second)
	for disabledName.Load().(string) != "svc" {
		if time.Now().After(deadline) {
			t.Fatal("OnDisable never fired for svc")
		}
		time.Sleep(time.Millisecond)
	}

	_ = c.Close(uint32(wsmixer.InternalErrorCode), "forcing reconnect for test")

	waitForBatch(2)
	mu.Lock()
	got := append([]string(nil), batches[1]...)
	mu.Unlock()
	if len(got) != 1 || got[0] != "svc" {
		t.Fatalf("register batch after server-side disable + reconnect = %v, want [svc] (still re-registered)", got)
	}
}

// TestServerEnableClearsLocalDisable covers item 2: a dashboard-driven
// OpEnable for a name the user had also locally `d`-disabled must clear
// localUnregistered (handleApp's OpEnable now goes through RegisterService,
// not sendRegisterFor directly), or the very next reconnect would silently
// drop the service again despite the dashboard having just re-enabled it.
func TestServerEnableClearsLocalDisable(t *testing.T) {
	var mu sync.Mutex
	var batches [][]string
	var conn atomic.Pointer[wsmixer.Conn]

	srv := newFakeTunnelServer(t, func(c *wsmixer.Conn) {
		conn.Store(c)
		c.OnApp(func(raw json.RawMessage) {
			if names := registeredNames(raw); names != nil {
				mu.Lock()
				batches = append(batches, names)
				mu.Unlock()
				_ = c.SendApp(context.Background(), map[string]any{
					"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": []map[string]any{
						{"name": "svc", "id": "id1", "url": "https://svc.example/mcp", "created": true},
					}, "errors": []any{}},
				})
			}
		})
	})

	tun, err := Start(context.Background(), Config{
		URL:         srv.url,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Bus:         newTestBus(t),
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(int) {},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	batchCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(batches)
	}
	batchAt := func(i int) []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), batches[i]...)
	}
	waitForBatch := func(n int) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for batchCount() < n {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for register batch #%d, got %d so far", n, batchCount())
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitForBatch(1)

	// Wait for the registry to know svc's id before disabling — the fake
	// server's "registered" reply above races with this.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := tun.Registry().Get("svc"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("registry never populated")
		}
		time.Sleep(time.Millisecond)
	}

	// A local `d` on svc, then a dashboard-driven "enable" for it.
	if err := tun.UnregisterService("svc"); err != nil {
		t.Fatalf("UnregisterService: %v", err)
	}
	c := conn.Load()
	_ = c.SendApp(context.Background(), map[string]any{
		"mcpwarp": map[string]any{"v": 1, "op": "enable", "id": "id1", "name": "svc"},
	})

	// The enable's own RegisterService call re-sends svc immediately over
	// the still-live connection (batch 2) before the forced reconnect below
	// produces the batch this test actually wants to inspect (batch 3).
	waitForBatch(2)
	_ = c.Close(uint32(wsmixer.InternalErrorCode), "forcing reconnect for test")

	waitForBatch(3)
	if got := batchAt(2); len(got) != 1 || got[0] != "svc" {
		t.Fatalf("register batch after server-side enable + reconnect = %v, want [svc] (local disable cleared)", got)
	}
}

// TestRegisterServiceWhileDisconnectedClearsLocalFlag covers edge case 4:
// RegisterService (the `e` keypress) must clear the local-unregistered flag
// even with no live connection, so the next welcome's register batch
// includes the name again — exercised directly against a Tunnel built
// around a wsmixer.Client that has never dialed (Conn() reports nil, same
// as a genuinely dropped connection).
func TestRegisterServiceWhileDisconnectedClearsLocalFlag(t *testing.T) {
	services := []appproto.RegisterService{{Name: "svc", Kind: "http"}}
	// Only the fields UnregisterService/RegisterService/currentRegisterBody
	// touch: cfg (Services), log, registry, client (for Conn()), registerBody,
	// localUnregistered. disp is deliberately left nil — nothing here goes
	// through the dispatcher.
	tun := &Tunnel{
		cfg:               Config{Services: services},
		log:               slog.New(slog.DiscardHandler),
		registry:          registry.New(),
		client:            wsmixer.NewClient("ws://unused.invalid", wsmixer.StaticToken("t"), wsmixer.ClientConfig{}),
		registerBody:      appproto.EncodeRegister(services),
		localUnregistered: make(map[string]bool),
	}

	if err := tun.UnregisterService("svc"); err != nil {
		t.Fatalf("UnregisterService: %v", err)
	}
	if got := tun.currentRegisterBody(); got != nil {
		t.Fatalf("register body after local disable = %v, want nil (svc was the only configured service)", got)
	}

	// `e` while still disconnected (tun.client.Conn() is nil — never dialed).
	tun.RegisterService("svc")

	got := tun.currentRegisterBody()
	svcs := got["mcpwarp"].(map[string]any)["services"].([]map[string]any)
	if len(svcs) != 1 || svcs[0]["name"] != "svc" {
		t.Fatalf("register body after RegisterService while disconnected = %v, want [svc] restored", got)
	}
}
