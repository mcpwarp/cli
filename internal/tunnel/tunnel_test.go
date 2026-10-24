package tunnel

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"github.com/mcpwarp/cli/internal/fakeedge"
	"github.com/mcpwarp/cli/internal/registry"
	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// staticToken is the test TokenSource: never fails, no ctx work.
type staticToken struct{ token string }

func (s staticToken) Token(context.Context) (string, error) { return s.token, nil }

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
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
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
		URL:         srv.URL,
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
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
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
		URL:         srv.URL,
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
	srv := fakeedge.Serve(t, nil)
	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
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

	var serverConn atomic.Pointer[fakeedge.Conn]
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
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
		URL:         srv.URL,
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
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			_ = c.SendApp(context.Background(), map[string]any{
				"mcpwarp": map[string]any{"v": 1, "op": "error", "code": "UNSUPPORTED_VERSION", "message": "upgrade"},
			})
		})
	})

	var exitCode atomic.Int32
	exitCode.Store(-1)
	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
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
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
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
		URL:         srv.URL,
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
// INVALID_NAME is not folded into the anyServerDisabled loop alongside
// SERVER_DISABLED: a "registered" reply with zero services and only an
// INVALID_NAME error, on a connection that has never registered anything
// successfully, must still exit — same fatal path as
// TestRegisteredZeroSuccessesFirstConnectQuotaExceededIsFatal above.
func TestRegisteredZeroSuccessesFirstConnectInvalidNameIsFatal(t *testing.T) {
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
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
		URL:         srv.URL,
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
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
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
		URL:         srv.URL,
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
				if ae.Hint == "" {
					t.Fatal("expected a non-empty Hint on the SERVER_DISABLED AppError event")
				}
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

// TestRegisteredFirstReplyDisabledAndRefusedNotFatal is ws-mixer-spec
// fixture tunnel/ms_11_first_reply_disabled_and_refused_not_fatal
// (MCPWARP.md §6A.6 G5): a first "registered" with no successes that mixes
// SERVER_DISABLED with another refusal must not exit, since the disabled
// name isn't refused and its dashboard "enable" arrives on this connection.
// svc-a reads SERVER_DISABLED (cli.registrationOf shows it disabled) and
// svc-b QUOTA_EXCEEDED (rejected); the later enable re-registers svc-a and
// its success flips it to active while svc-b stays rejected.
func TestRegisteredFirstReplyDisabledAndRefusedNotFatal(t *testing.T) {
	var registers atomic.Int32
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			names := fakeedge.RegisterNames(raw)
			if names == nil {
				return
			}
			if registers.Add(1) == 1 {
				_ = c.SendApp(context.Background(), map[string]any{
					"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": []any{}, "errors": []map[string]any{
						{"name": "svc-a", "code": "SERVER_DISABLED", "message": "disabled in dashboard"},
						{"name": "svc-b", "code": "QUOTA_EXCEEDED", "message": "server limit reached"},
					}},
				})
				return
			}
			if len(names) != 1 || names[0] != "svc-a" {
				t.Errorf("expected the enable to re-register only svc-a, got %v", names)
				return
			}
			_ = c.SendApp(context.Background(), map[string]any{
				"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": []any{
					map[string]any{"name": "svc-a", "id": "id-a", "url": "https://svc-a.example/mcp", "created": true},
				}, "errors": []any{}},
			})
		})
	})

	var exitCode atomic.Int32
	exitCode.Store(-1)
	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc-a", Kind: "http"}, {Name: "svc-b", Kind: "http"}},
		Bus:         newTestBus(t),
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(code int) { exitCode.Store(int32(code)) },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	rowFor := func(name string) (registry.Row, bool) {
		for _, r := range tun.Registry().Rows() {
			if r.Name == name {
				return r, true
			}
		}
		return registry.Row{}, false
	}
	waitFor := func(what string, ok func() bool) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for !ok() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s; rows: %+v", what, tun.Registry().Rows())
			}
			time.Sleep(time.Millisecond)
		}
	}

	waitFor("the first reply's per-name outcomes", func() bool {
		a, okA := rowFor("svc-a")
		b, okB := rowFor("svc-b")
		return okA && okB && a.Rejected == "SERVER_DISABLED" && b.Rejected == "QUOTA_EXCEEDED"
	})
	if a, _ := rowFor("svc-a"); a.URL != "" {
		t.Fatalf("disabled svc-a row = %+v, want no URL", a)
	}

	_ = srv.Latest().SendApp(context.Background(), map[string]any{
		"mcpwarp": map[string]any{"v": 1, "op": "enable", "id": "id-a", "name": "svc-a"},
	})
	waitFor("svc-a active after the enable's re-register", func() bool {
		a, ok := rowFor("svc-a")
		return ok && a.Rejected == "" && !a.Disabled && !a.Reregistering && a.URL == "https://svc-a.example/mcp"
	})
	if b, _ := rowFor("svc-b"); b.Rejected != "QUOTA_EXCEEDED" || b.URL != "" {
		t.Fatalf("svc-b row after svc-a's enable = %+v, want still rejected QUOTA_EXCEEDED with no URL", b)
	}

	// A wrongly chosen FatalExit runs on its own goroutine, spawned while the
	// first reply was handled (well before the enable above was): give it a
	// bounded window to land rather than read exitCode once.
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := exitCode.Load(); got != -1 {
			t.Fatalf("expected no FatalExit for a first reply mixing SERVER_DISABLED with QUOTA_EXCEEDED, got exit code %d", got)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestRegisteredFirstReplyAllRefusedIsFatal is ws-mixer-spec fixture
// tunnel/ms_09_fatal_first_reply (MCPWARP.md §6A.6 G5): a first
// "registered" refusing every service, none with SERVER_DISABLED, exits 1,
// and each name still lands rejected with its own code first.
func TestRegisteredFirstReplyAllRefusedIsFatal(t *testing.T) {
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			if fakeedge.RegisterNames(raw) == nil {
				return
			}
			_ = c.SendApp(context.Background(), map[string]any{
				"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": []any{}, "errors": []map[string]any{
					{"name": "svc-a", "code": "QUOTA_EXCEEDED", "message": "server limit reached"},
					{"name": "svc-b", "code": "CONFLICT", "message": "name in use"},
				}},
			})
		})
	})

	var exitCode atomic.Int32
	exitCode.Store(-1)
	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc-a", Kind: "http"}, {Name: "svc-b", Kind: "http"}},
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
			t.Fatal("FatalExit was never called for a first reply refusing every service with QUOTA_EXCEEDED and CONFLICT")
		}
		time.Sleep(time.Millisecond)
	}
	if got := exitCode.Load(); got != 1 {
		t.Fatalf("expected exit code 1, got %d", got)
	}
	rejected := map[string]string{}
	for _, r := range tun.Registry().Rows() {
		rejected[r.Name] = r.Rejected
	}
	if rejected["svc-a"] != "QUOTA_EXCEEDED" || rejected["svc-b"] != "CONFLICT" {
		t.Fatalf("expected svc-a rejected QUOTA_EXCEEDED and svc-b CONFLICT, got %v", rejected)
	}
}

// TestRegisteredInvalidNameAndUsernameRequiredHints covers the two per-
// service codes for the name-as-URL-slug rollout: INVALID_NAME and
// USERNAME_REQUIRED. Paired with a successful "good" service so
// zeroSuccesses is false, neither is fatal — this asserts the connection
// stays up and each error's AppError event is published with its code,
// mirroring TestRegisteredAllServerDisabledIsNotFatal's approach.
func TestRegisteredInvalidNameAndUsernameRequiredHints(t *testing.T) {
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
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
						{"name": "oops", "code": "INTERNAL", "message": "unexpected error"},
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
		URL:         srv.URL,
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
	hints := map[string]string{}
	deadline := time.After(2 * time.Second)
	for len(seen) < 3 {
		select {
		case evt := <-bus.Control:
			if ae, ok := evt.(eventbus.AppError); ok {
				seen[ae.Code] = true
				hints[ae.Code] = ae.Hint
			}
		case <-deadline:
			t.Fatalf("expected AppError events for INVALID_NAME, USERNAME_REQUIRED and INTERNAL, got %v", seen)
		}
	}
	if !seen["INVALID_NAME"] || !seen["USERNAME_REQUIRED"] || !seen["INTERNAL"] {
		t.Fatalf("expected INVALID_NAME, USERNAME_REQUIRED and INTERNAL AppError events, got %v", seen)
	}
	if hints["INVALID_NAME"] == "" || hints["USERNAME_REQUIRED"] == "" {
		t.Fatalf("expected non-empty hints for INVALID_NAME and USERNAME_REQUIRED, got %v", hints)
	}
	if hints["INTERNAL"] != "" {
		t.Fatalf("expected no hint for INTERNAL, got %q", hints["INTERNAL"])
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

// TestRegisteredQuotaExceededRecordedThenClearedByReRegister: a per-service
// QUOTA_EXCEEDED in a "registered" reply (alongside a success, so not
// fatal) must land in the registry as that name's rejection — the dashboard
// reads it from there — and a later successful re-register of that name
// (here RegisterService, the `e` path; a reconnect takes the same
// handleApp branch) must clear it.
func TestRegisteredQuotaExceededRecordedThenClearedByReRegister(t *testing.T) {
	var registers atomic.Int32
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			var wrapper struct {
				Mcpwarp struct{ Op string } `json:"mcpwarp"`
			}
			_ = json.Unmarshal(raw, &wrapper)
			if wrapper.Mcpwarp.Op != "register" {
				return
			}
			if registers.Add(1) == 1 {
				_ = c.SendApp(context.Background(), map[string]any{
					"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": []any{
						map[string]any{"name": "good", "id": "svc-good", "url": "https://good.example/mcp", "created": true},
					}, "errors": []map[string]any{
						{"name": "deepwiki", "code": "QUOTA_EXCEEDED", "message": "server limit reached"},
					}},
				})
				return
			}
			_ = c.SendApp(context.Background(), map[string]any{
				"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": []any{
					map[string]any{"name": "deepwiki", "id": "svc-dw", "url": "https://dw.example/mcp", "created": true},
				}, "errors": []any{}},
			})
		})
	})

	bus := newTestBus(t)
	var exitCode atomic.Int32
	exitCode.Store(-1)
	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "good", Kind: "http"}, {Name: "deepwiki", Kind: "http"}},
		Bus:         bus,
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(code int) { exitCode.Store(int32(code)) },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	rowFor := func(name string) (registry.Row, bool) {
		for _, r := range tun.Registry().Rows() {
			if r.Name == name {
				return r, true
			}
		}
		return registry.Row{}, false
	}
	waitFor := func(what string, ok func() bool) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for !ok() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s; rows: %+v", what, tun.Registry().Rows())
			}
			time.Sleep(time.Millisecond)
		}
	}

	waitFor("deepwiki's rejection", func() bool {
		r, ok := rowFor("deepwiki")
		return ok && r.Rejected == "QUOTA_EXCEEDED"
	})
	if r, _ := rowFor("deepwiki"); r.URL != "" || r.Kind != "http" {
		t.Fatalf("rejected deepwiki row = %+v, want kind http and no URL", r)
	}
	if r, ok := rowFor("good"); !ok || r.Rejected != "" {
		t.Fatalf("good row = %+v (found=%v), want registered and not rejected", r, ok)
	}

	// d on the rejected, never-registered name: disabled, rejection gone.
	if err := tun.UnregisterService("deepwiki"); err != nil {
		t.Fatalf("UnregisterService: %v", err)
	}
	if r, _ := rowFor("deepwiki"); !r.LocallyDisabled || !r.Disabled || r.Rejected != "" {
		t.Fatalf("deepwiki after d = %+v, want locally disabled with no rejection", r)
	}

	// e: re-registered, and this time the tunnel accepts it.
	tun.RegisterService("deepwiki")
	waitFor("deepwiki's successful re-register", func() bool {
		r, ok := rowFor("deepwiki")
		return ok && r.Rejected == "" && !r.Disabled && !r.Reregistering && r.URL == "https://dw.example/mcp"
	})
	if got := exitCode.Load(); got != -1 {
		t.Fatalf("expected no FatalExit, got exit code %d", got)
	}
}

// TestReconnectAllServicesRejectedIsNotFatal: after a first successful
// registration, a reconnect whose "registered" reply rejects every service
// (handleApp's zeroSuccesses-after-sawFirstRegistered early return) must
// still record each rejection — that return comes after
// ApplyRegisterErrors — and must not exit. The old entries keep their URL
// in the registry; cli.pollTunnelForTUI drops it for a rejected row.
func TestReconnectAllServicesRejectedIsNotFatal(t *testing.T) {
	var conns atomic.Int32
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		n := conns.Add(1)
		c.OnApp(func(raw json.RawMessage) {
			var wrapper struct {
				Mcpwarp struct{ Op string } `json:"mcpwarp"`
			}
			_ = json.Unmarshal(raw, &wrapper)
			if wrapper.Mcpwarp.Op != "register" {
				return
			}
			if n == 1 {
				_ = c.SendApp(context.Background(), map[string]any{
					"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": []any{
						map[string]any{"name": "good", "id": "svc-good", "url": "https://good.example/mcp", "created": true},
						map[string]any{"name": "deepwiki", "id": "svc-dw", "url": "https://dw.example/mcp", "created": true},
					}, "errors": []any{}},
				})
				return
			}
			_ = c.SendApp(context.Background(), map[string]any{
				"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": []any{}, "errors": []map[string]any{
					{"name": "good", "code": "QUOTA_EXCEEDED", "message": "server limit reached"},
					{"name": "deepwiki", "code": "QUOTA_EXCEEDED", "message": "server limit reached"},
				}},
			})
		})
	})

	var exitCode atomic.Int32
	exitCode.Store(-1)
	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "good", Kind: "http"}, {Name: "deepwiki", Kind: "http"}},
		Bus:         newTestBus(t),
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(code int) { exitCode.Store(int32(code)) },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	allRows := func(ok func(registry.Row) bool) bool {
		rows := tun.Registry().Rows()
		if len(rows) != 2 {
			return false
		}
		for _, r := range rows {
			if !ok(r) {
				return false
			}
		}
		return true
	}
	waitFor := func(what string, cond func() bool, d time.Duration) {
		t.Helper()
		deadline := time.Now().Add(d)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s; rows: %+v", what, tun.Registry().Rows())
			}
			time.Sleep(time.Millisecond)
		}
	}

	waitFor("the first registration", func() bool {
		return allRows(func(r registry.Row) bool { return r.URL != "" && r.Rejected == "" })
	}, 2*time.Second)

	_ = srv.Latest().Close(uint32(wsmixer.InternalErrorCode), "forcing reconnect for test")

	waitFor("the reconnect's rejections", func() bool {
		return allRows(func(r registry.Row) bool { return r.Rejected == "QUOTA_EXCEEDED" })
	}, 5*time.Second)
	// FatalExit, if handleApp wrongly chose it, runs on its own goroutine
	// right after the rejections are recorded; give it a moment to land.
	time.Sleep(50 * time.Millisecond)
	if got := exitCode.Load(); got != -1 {
		t.Fatalf("expected no FatalExit for an all-rejected reconnect after a first success, got exit code %d", got)
	}
	if conns.Load() < 2 {
		t.Fatalf("expected a reconnect, saw %d connections", conns.Load())
	}
}

func TestReconnectsAndReregisters(t *testing.T) {
	var registerCount atomic.Int32
	var firstConn atomic.Pointer[fakeedge.Conn]
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
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
		URL:         srv.URL,
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
	var firstConn atomic.Pointer[fakeedge.Conn]
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
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
		URL:         srv.URL,
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
	var firstConn atomic.Pointer[fakeedge.Conn]
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		firstConn.CompareAndSwap(nil, c)
	})

	var exitCode atomic.Int32
	exitCode.Store(-1)
	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
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

	if got := classifyFatalMessage(srv.URL, wsmixer.DisconnectReason{Fatal: true, WSCode: 4011}); got != "session expired, run `mcpwarp login`" {
		t.Fatalf("unexpected classifyFatalMessage for wsCode 4011: %q", got)
	}
}

// TestConnectionLimitCloseIsNonFatalWithHintAndReconnects covers onDisconnect's
// CONNECTION_LIMIT branch: a 4014 (ws-mixer's application-level close, error
// code 0x0e) close whose reason carries the connectionLimitPrefix must not be
// fatal, must publish an AppError with Code CONNECTION_LIMIT and a non-empty
// Hint, and — since this package changes no backoff — the SDK must still be
// retrying underneath it.
func TestConnectionLimitCloseIsNonFatalWithHintAndReconnects(t *testing.T) {
	var conns atomic.Int32
	var firstConn atomic.Pointer[fakeedge.Conn]
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		conns.Add(1)
		firstConn.CompareAndSwap(nil, c)
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
		URL:         srv.URL,
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

	deadline := time.Now().Add(2 * time.Second)
	for firstConn.Load() == nil {
		if time.Now().After(deadline) {
			t.Fatal("fake tunnel never accepted a connection")
		}
		time.Sleep(time.Millisecond)
	}

	_ = firstConn.Load().Close(uint32(wsmixer.ApplicationCloseCode),
		"CONNECTION_LIMIT: too many agent connections for this account (limit 10)")

	var appErr eventbus.AppError
	found := false
	deadline = time.Now().Add(2 * time.Second)
	for !found {
		select {
		case evt := <-bus.Control:
			if ae, ok := evt.(eventbus.AppError); ok && ae.Code == "CONNECTION_LIMIT" {
				appErr = ae
				found = true
			}
		case <-time.After(time.Until(deadline)):
			t.Fatal("expected a CONNECTION_LIMIT AppError, got none")
		}
	}
	if appErr.Hint == "" {
		t.Fatal("expected a non-empty Hint on the CONNECTION_LIMIT AppError")
	}
	if appErr.Message == "" || strings.Contains(appErr.Message, connectionLimitPrefix) {
		t.Fatalf("expected the AppError Message to be the server text with the prefix stripped, got %q", appErr.Message)
	}

	if exitCode.Load() != -1 {
		t.Fatalf("expected no FatalExit for a CONNECTION_LIMIT close, got exit code %d", exitCode.Load())
	}

	// The SDK's own reconnect should still be under way. With ws-mixer-go
	// v0.4.0 the first retry after a post-welcome close is random(0,2s), so
	// by the time we poll the client may already be back to "connected" —
	// poll the wsmixer Client's own state machine (client_reconnect.go) and
	// accept any of "backoff", "dialing", or "connected" as evidence it left
	// "disconnected"; it must never report "closed", and FatalExit must
	// never run.
	deadline = time.Now().Add(2 * time.Second)
	for {
		state := tun.client.State()
		if state == "closed" {
			t.Fatalf("expected the SDK to keep reconnecting after a CONNECTION_LIMIT close, got state %q", state)
		}
		if state == "backoff" || state == "dialing" || state == "connected" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected the SDK to leave disconnected after a CONNECTION_LIMIT close, got stuck at %q", state)
		}
		time.Sleep(time.Millisecond)
	}
	if exitCode.Load() != -1 {
		t.Fatalf("expected no FatalExit while the SDK reconnects after a CONNECTION_LIMIT close, got exit code %d", exitCode.Load())
	}
}

// TestPlainEnhanceYourCalmCloseIsNotConnectionLimit covers the other half of
// onDisconnect's CONNECTION_LIMIT branch: a 4009 (wsmixer.EnhanceYourCalm)
// close, even with text that happens to start with connectionLimitPrefix,
// keeps ws-mixer's own meaning (rate limit / oversize payload) — 4009 is
// never sent by the tunnel for its connection cap — and must not be
// reported as a CONNECTION_LIMIT AppError.
func TestPlainEnhanceYourCalmCloseIsNotConnectionLimit(t *testing.T) {
	var firstConn atomic.Pointer[fakeedge.Conn]
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		firstConn.CompareAndSwap(nil, c)
	})

	bus := eventbus.New(64)
	go func() {
		for range bus.Telemetry {
		}
	}()
	t.Cleanup(bus.Close)

	// Drain bus.Control for the whole test, on a background goroutine
	// stopped only via t.Cleanup: Control is lossless, so letting it go
	// undrained around tun.Close's own ConnStateChanged publish (the defer
	// below runs before this cleanup does) would risk Close parking on a
	// full channel. Watch for an unwanted CONNECTION_LIMIT AppError as it
	// drains.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var sawDisconnected atomic.Bool
	go func() {
		for {
			select {
			case evt := <-bus.Control:
				if ae, ok := evt.(eventbus.AppError); ok && ae.Code == "CONNECTION_LIMIT" {
					t.Errorf("unexpected CONNECTION_LIMIT AppError for a plain 4009 close: %+v", ae)
				}
				if cs, ok := evt.(eventbus.ConnStateChanged); ok && cs.State == "disconnected" {
					sawDisconnected.Store(true)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Bus:         bus,
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(int) {},
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

	_ = firstConn.Load().Close(uint32(wsmixer.EnhanceYourCalm), "CONNECTION_LIMIT: not actually the cap")

	// Give the dispatcher a generous window to (not) publish a
	// CONNECTION_LIMIT AppError; the background drain goroutine above
	// catches it if it shows up, and keeps draining past this point through
	// tun.Close's own deferred call below.
	time.Sleep(500 * time.Millisecond)

	if !sawDisconnected.Load() {
		t.Fatal("expected a ConnStateChanged{State: \"disconnected\"} on bus.Control after the 4009 close")
	}
}

// TestApplicationCloseWithOtherPrefixIsNotConnectionLimit covers the mirror
// case: a bare application-level close (4014, error code 0x0e) whose reason
// does not carry connectionLimitPrefix is some other application-level
// close and must fall through to the generic Info log, not a
// CONNECTION_LIMIT AppError.
func TestApplicationCloseWithOtherPrefixIsNotConnectionLimit(t *testing.T) {
	var firstConn atomic.Pointer[fakeedge.Conn]
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		firstConn.CompareAndSwap(nil, c)
	})

	bus := eventbus.New(64)
	go func() {
		for range bus.Telemetry {
		}
	}()
	t.Cleanup(bus.Close)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var sawDisconnected atomic.Bool
	go func() {
		for {
			select {
			case evt := <-bus.Control:
				if ae, ok := evt.(eventbus.AppError); ok && ae.Code == "CONNECTION_LIMIT" {
					t.Errorf("unexpected CONNECTION_LIMIT AppError for a 4014 close with a different prefix: %+v", ae)
				}
				if cs, ok := evt.(eventbus.ConnStateChanged); ok && cs.State == "disconnected" {
					sawDisconnected.Store(true)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Bus:         bus,
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(int) {},
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

	_ = firstConn.Load().Close(uint32(wsmixer.ApplicationCloseCode), "SOMETHING_ELSE: draining for maintenance")

	// Give the dispatcher a generous window to (not) publish a
	// CONNECTION_LIMIT AppError; the background drain goroutine above
	// catches it if it shows up, and keeps draining past this point through
	// tun.Close's own deferred call below.
	time.Sleep(500 * time.Millisecond)

	if !sawDisconnected.Load() {
		t.Fatal("expected a ConnStateChanged{State: \"disconnected\"} on bus.Control after the 4014 close")
	}
}

// TestConnectionLimitBareCloseWithoutErrorFrame covers onDisconnect's
// CloseReason fallback (ws-mixer-go v0.4.1): when the tunnel closes 4014
// with a CONNECTION_LIMIT reason but never sends a preceding stream-0
// error{} frame — e.g. the error{} write lost a race with the close, or the
// server just closes bare — reason.Message stays the generic "peer closed
// with code 4014" text and only reason.CloseReason carries the server's
// text, so onDisconnect must fall back to CloseReason. The close is
// produced by closing the fake server's underlying coder/websocket conn
// directly, bypassing wsmixer.Conn.Close (which always sends error{}
// first).
func TestConnectionLimitBareCloseWithoutErrorFrame(t *testing.T) {
	var firstConn atomic.Pointer[fakeedge.Conn]
	var registerSeen atomic.Bool
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		firstConn.CompareAndSwap(nil, c)
		// Wait for the client's own register write to have landed before this
		// test's raw close below tears down the socket — otherwise closing
		// the wire out from under a register send still in flight can fail
		// that write outright (a plain "closed network connection" write
		// error), which the client reports as a generic disconnect instead
		// of ever reading this test's close frame/reason at all.
		c.OnApp(func(raw json.RawMessage) {
			if names := fakeedge.RegisterNames(raw); names != nil {
				registerSeen.Store(true)
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
		URL:         srv.URL,
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

	deadline := time.Now().Add(2 * time.Second)
	for !registerSeen.Load() {
		if time.Now().After(deadline) {
			t.Fatal("fake tunnel never saw the register")
		}
		time.Sleep(time.Millisecond)
	}

	c := srv.Latest()
	if c == nil {
		t.Fatal("fake tunnel never accepted a connection")
	}
	const reason = "CONNECTION_LIMIT: too many agent connections for this account (limit 10)"
	go func() { _ = c.CloseBare(websocket.StatusCode(connectionLimitWSCode), reason) }()

	var appErr eventbus.AppError
	found := false
	deadline = time.Now().Add(2 * time.Second)
	for !found {
		select {
		case evt := <-bus.Control:
			if ae, ok := evt.(eventbus.AppError); ok && ae.Code == "CONNECTION_LIMIT" {
				appErr = ae
				found = true
			}
		case <-time.After(time.Until(deadline)):
			t.Fatal("expected a CONNECTION_LIMIT AppError from a bare 4014 close, got none")
		}
	}
	if appErr.Hint == "" {
		t.Fatal("expected a non-empty Hint on the CONNECTION_LIMIT AppError")
	}
	if appErr.Message == "" || strings.Contains(appErr.Message, connectionLimitPrefix) {
		t.Fatalf("expected the AppError Message to be the server text with the prefix stripped, got %q", appErr.Message)
	}
	if exitCode.Load() != -1 {
		t.Fatalf("expected no FatalExit for a bare CONNECTION_LIMIT close, got exit code %d", exitCode.Load())
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

	var serverConn atomic.Pointer[fakeedge.Conn]
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
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
		URL:         srv.URL,
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
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
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
		URL:         srv.URL,
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

// TestConnectionLimitAppErrorHasHint covers handleApp's OpError branch: the
// app-level {"op":"error","code":"CONNECTION_LIMIT",...} envelope (sent
// best-effort just before the matching close) must publish an AppError with
// a non-empty Hint and must never be treated as fatal.
func TestConnectionLimitAppErrorHasHint(t *testing.T) {
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			var wrapper struct {
				Mcpwarp struct{ Op string } `json:"mcpwarp"`
			}
			_ = json.Unmarshal(raw, &wrapper)
			if wrapper.Mcpwarp.Op == "register" {
				_ = c.SendApp(context.Background(), map[string]any{
					"mcpwarp": map[string]any{"v": 1, "op": "error", "code": "CONNECTION_LIMIT",
						"message": "too many agent connections for this account (limit 10)"},
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
		URL:         srv.URL,
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

	var appErr eventbus.AppError
	found := false
	deadline := time.Now().Add(2 * time.Second)
	for !found {
		select {
		case evt := <-bus.Control:
			if ae, ok := evt.(eventbus.AppError); ok && ae.Code == "CONNECTION_LIMIT" {
				appErr = ae
				found = true
			}
		case <-time.After(time.Until(deadline)):
			t.Fatal("expected a CONNECTION_LIMIT AppError, got none")
		}
	}
	if appErr.Hint == "" {
		t.Fatal("expected a non-empty Hint on the CONNECTION_LIMIT AppError")
	}
	if exitCode.Load() != -1 {
		t.Fatalf("expected no FatalExit for a CONNECTION_LIMIT app error, got exit code %d", exitCode.Load())
	}
}

// TestConnectionLimitAppErrorAndCloseDedupe covers the dedupe itself: the
// fake server sends the app-level CONNECTION_LIMIT error envelope and then
// immediately closes with 4014 + the matching prefixed reason, the way the
// real server does best-effort — only one CONNECTION_LIMIT AppError must
// reach the bus, not two.
//
// A second assertion — that a further 4014+prefix close after the SDK's
// next successful reconnect produces a second AppError (proving
// connLimitReported resets per connection, not just deduping forever) —
// isn't included here: driving an actual reconnect deterministically in
// well under a couple of seconds needs a seam this package doesn't expose
// (e.g. forcing the next backoff to ~0), so it's skipped rather than
// reintroducing a multi-second wait.
func TestConnectionLimitAppErrorAndCloseDedupe(t *testing.T) {
	const reason = "CONNECTION_LIMIT: too many agent connections for this account (limit 10)"
	var served atomic.Int32
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		if served.Add(1) != 1 {
			return
		}
		c.OnApp(func(raw json.RawMessage) {
			var wrapper struct {
				Mcpwarp struct{ Op string } `json:"mcpwarp"`
			}
			_ = json.Unmarshal(raw, &wrapper)
			if wrapper.Mcpwarp.Op == "register" {
				_ = c.SendApp(context.Background(), map[string]any{
					"mcpwarp": map[string]any{"v": 1, "op": "error", "code": "CONNECTION_LIMIT", "message": reason},
				})
				_ = c.Close(uint32(wsmixer.ApplicationCloseCode), reason)
			}
		})
	})

	bus := eventbus.New(64)
	go func() {
		for range bus.Telemetry {
		}
	}()
	t.Cleanup(bus.Close)

	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Bus:         bus,
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(int) {},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	var appErrs []eventbus.AppError
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		select {
		case evt := <-bus.Control:
			if ae, ok := evt.(eventbus.AppError); ok && ae.Code == "CONNECTION_LIMIT" {
				appErrs = append(appErrs, ae)
			}
		case <-time.After(10 * time.Millisecond):
		}
	}
	if len(appErrs) != 1 {
		t.Fatalf("expected exactly one CONNECTION_LIMIT AppError from the envelope+close pair, got %d: %+v", len(appErrs), appErrs)
	}
}

// TestEnableClearsPendingDisableEvenWhenNameNotConfigured covers Node
// client.ts's item 4: a live "enable" must cancel any deferred disable for
// the same id regardless of whether this connection's own name-gate
// (hasConfiguredService) accepts the enable itself — otherwise a later
// "registered" batch that reuses that id could resolve a now-stale disable.
func TestEnableClearsPendingDisableEvenWhenNameNotConfigured(t *testing.T) {
	var serverConn atomic.Pointer[fakeedge.Conn]
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		serverConn.Store(c)
	})

	var disabledName atomic.Value
	disabledName.Store("")

	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
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

// TestLocalDisablePersistsAcrossReconnect covers the bug fix: a local `d`
// (UnregisterService) must survive a reconnect instead of the next
// welcome's register batch silently resurrecting the name, and a following
// `e` (RegisterService) must bring it back on the reconnect after that.
func TestLocalDisablePersistsAcrossReconnect(t *testing.T) {
	var mu sync.Mutex
	var batches [][]string

	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			if names := fakeedge.RegisterNames(raw); names != nil {
				mu.Lock()
				batches = append(batches, names)
				mu.Unlock()
			}
		})
	})

	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
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

	waitForConn := func(n int) *fakeedge.Conn {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for srv.ConnCount() < n {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for the fake tunnel to accept connection #%d", n)
			}
			time.Sleep(time.Millisecond)
		}
		return srv.Latest()
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

	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			if names := fakeedge.RegisterNames(raw); names != nil {
				mu.Lock()
				batches = append(batches, names)
				mu.Unlock()
			}
		})
	})

	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
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
	waitForConn := func(n int) *fakeedge.Conn {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for srv.ConnCount() < n {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for the fake tunnel to accept connection #%d", n)
			}
			time.Sleep(time.Millisecond)
		}
		return srv.Latest()
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
	var conn atomic.Pointer[fakeedge.Conn]

	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		conn.Store(c)
		c.OnApp(func(raw json.RawMessage) {
			if names := fakeedge.RegisterNames(raw); names != nil {
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
		URL:         srv.URL,
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
	var conn atomic.Pointer[fakeedge.Conn]

	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		conn.Store(c)
		c.OnApp(func(raw json.RawMessage) {
			if names := fakeedge.RegisterNames(raw); names != nil {
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
		URL:         srv.URL,
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

// retryableTokenErr mirrors *auth.TokenRefreshError's shape: a Retryable()
// true error, without importing internal/auth.
type retryableTokenErr struct{ msg string }

func (e *retryableTokenErr) Error() string   { return e.msg }
func (e *retryableTokenErr) Retryable() bool { return true }

// scriptedToken is a TokenSource whose Nth call (1-based) returns failures[N]
// when present, and "t" otherwise.
type scriptedToken struct {
	calls    atomic.Int32
	failures map[int32]error
}

func (s *scriptedToken) Token(context.Context) (string, error) {
	n := s.calls.Add(1)
	if err, ok := s.failures[n]; ok {
		return "", err
	}
	return "t", nil
}

// recordingHandler is a slog.Handler that keeps every record, so a test can
// assert on the log line (and its attrs) a code path emitted.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler            { return h }
func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

// find returns the first record with message msg, and whether there was one.
func (h *recordingHandler) find(msg string) (slog.Record, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Message == msg {
			return r, true
		}
	}
	return slog.Record{}, false
}

func recordAttr(r slog.Record, key string) (slog.Value, bool) {
	var v slog.Value
	found := false
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			v, found = a.Value, true
			return false
		}
		return true
	})
	return v, found
}

// TestRetryableTokenFailureReconnects covers the token provider seam: a
// Retryable() refresh failure on a reconnect's token call must be marked
// wsmixer.ErrTokenUnavailable so the SDK treats it as a non-fatal dial
// failure and backs off, rather than closing the client for good (which
// with nothing retrying it left `mcpwarp up` hung). The retrying log line
// only fires for a non-fatal PhaseDial disconnect whose Cause wraps
// ErrTokenUnavailable, so seeing it pins that DisconnectReason shape.
func TestRetryableTokenFailureReconnects(t *testing.T) {
	var firstConn atomic.Pointer[fakeedge.Conn]
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		firstConn.CompareAndSwap(nil, c)
	})

	refreshErr := &retryableTokenErr{msg: "could not reach the auth server to refresh the session"}
	tokens := &scriptedToken{failures: map[int32]error{2: refreshErr}}
	logs := &recordingHandler{}

	var exitCode atomic.Int32
	exitCode.Store(-1)
	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
		TokenSource: tokens,
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Bus:         newTestBus(t),
		Log:         slog.New(logs),
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

	// 4012 reconnects after jitter(0,2s); that redial's token call (the 2nd)
	// fails, and the one after the backoff succeeds.
	_ = firstConn.Load().Close(uint32(wsmixer.GoingAwayCode), "forcing a reconnect for test")

	const retryMsg = "could not refresh the session token; retrying"
	var rec slog.Record
	deadline = time.Now().Add(5 * time.Second)
	for {
		var ok bool
		if rec, ok = logs.find(retryMsg); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected a %q log after the provider failed, got none (token calls: %d)", retryMsg, tokens.calls.Load())
		}
		time.Sleep(time.Millisecond)
	}
	v, ok := recordAttr(rec, "err")
	if !ok {
		t.Fatal("retry log line has no err attr")
	}
	cause, _ := v.Any().(error)
	if !errors.Is(cause, wsmixer.ErrTokenUnavailable) {
		t.Fatalf("retry log err = %v, want it to wrap wsmixer.ErrTokenUnavailable", cause)
	}
	var got *retryableTokenErr
	if !errors.As(cause, &got) || got != refreshErr {
		t.Fatalf("retry log err = %v, want it to unwrap to the provider's own retryable error", cause)
	}

	// Full-jitter backoff after one failed dial is at most a few seconds with
	// this package's 1s Base.
	deadline = time.Now().Add(15 * time.Second)
	for srv.ConnCount() < 2 || tun.client.State() != "connected" {
		if state := tun.client.State(); state == "closed" {
			t.Fatalf("client closed after a retryable token failure, want it to reconnect")
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected a second connection after the token failure, got %d conns, state %q", srv.ConnCount(), tun.client.State())
		}
		time.Sleep(time.Millisecond)
	}
	if n := tokens.calls.Load(); n < 3 {
		t.Fatalf("expected at least 3 token calls (connect, failed redial, successful redial), got %d", n)
	}
	if exitCode.Load() != -1 {
		t.Fatalf("expected no FatalExit for a retryable token failure, got exit code %d", exitCode.Load())
	}
}

// TestNonRetryableTokenFailureIsFatal is the other half: a provider error
// that isn't Retryable() stays unmarked, so the SDK closes the client and
// onDisconnect must exit instead of waiting on a retry that never comes.
func TestNonRetryableTokenFailureIsFatal(t *testing.T) {
	var firstConn atomic.Pointer[fakeedge.Conn]
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		firstConn.CompareAndSwap(nil, c)
	})

	tokens := &scriptedToken{failures: map[int32]error{2: errors.New("credentials file is corrupt")}}

	var exitCode atomic.Int32
	exitCode.Store(-1)
	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
		TokenSource: tokens,
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

	_ = firstConn.Load().Close(uint32(wsmixer.GoingAwayCode), "forcing a reconnect for test")

	deadline = time.Now().Add(5 * time.Second)
	for exitCode.Load() == -1 {
		if time.Now().After(deadline) {
			t.Fatalf("FatalExit was never called for a non-retryable token failure (token calls: %d, state %q)", tokens.calls.Load(), tun.client.State())
		}
		time.Sleep(time.Millisecond)
	}
	if exitCode.Load() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode.Load())
	}
	if state := tun.client.State(); state != "closed" {
		t.Fatalf("expected the client closed after a fatal token failure, got state %q", state)
	}
	if n := srv.ConnCount(); n != 1 {
		t.Fatalf("expected no reconnect after a fatal token failure, got %d conns", n)
	}
}

// requestOver opens a stream on c the way the tunnel routes a public
// request, sends a GET for host, and returns the response status.
func requestOver(t *testing.T, c *fakeedge.Conn, host string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := c.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	fmt.Fprintf(stream, "GET /mcp HTTP/1.1\r\nHost: %s\r\n\r\n", host)
	_ = stream.CloseWrite()
	resp, err := http.ReadResponse(bufio.NewReader(stream), nil)
	if err != nil {
		t.Fatalf("reading the response for %s: %v", host, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// waitForRow polls tun's registry until name's row satisfies ok.
func waitForRow(t *testing.T, tun *Tunnel, what, name string, ok func(registry.Row, bool) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var row registry.Row
		found := false
		for _, r := range tun.Registry().Rows() {
			if r.Name == name {
				row, found = r, true
			}
		}
		if ok(row, found) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s; rows: %+v", what, tun.Registry().Rows())
		}
		time.Sleep(time.Millisecond)
	}
}

// TestConflictTakesOverOnlyThatName covers the tunnel's per-name "conflict"
// op (mcpwarp-saas agentconn.go's sendConflict): only that name turns
// rejected (CONFLICT) with the take-over hint while the other name keeps
// serving, and a later successful "registered" for it (here `e`,
// RegisterService) makes it active and routable again.
func TestConflictTakesOverOnlyThatName(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(target.Close)
	targetURL, _ := url.Parse(target.URL + "/mcp")

	var serverConn atomic.Pointer[fakeedge.Conn]
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		serverConn.Store(c)
		c.ReplyRegisteredAll(nil)
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
		URL:         srv.URL,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc-a", Kind: "http"}, {Name: "svc-b", Kind: "http"}},
		Targets:     map[string]*url.URL{"svc-a": targetURL, "svc-b": targetURL},
		Bus:         bus,
		Log:         slog.New(slog.DiscardHandler),
		FatalExit:   func(code int) { exitCode.Store(int32(code)) },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	for _, name := range []string{"svc-a", "svc-b"} {
		waitForRow(t, tun, name+" active", name, func(r registry.Row, ok bool) bool {
			return ok && r.Rejected == "" && r.URL != ""
		})
	}
	c := serverConn.Load()
	if got := requestOver(t, c, "svc-a.example"); got != 200 {
		t.Fatalf("svc-a before the conflict: status %d, want 200", got)
	}

	_ = c.SendApp(context.Background(), map[string]any{
		"mcpwarp": map[string]any{"v": 1, "op": "conflict", "id": "id-svc-a", "name": "svc-a", "reason": "taken_over"},
	})

	var appErr eventbus.AppError
	deadline := time.After(2 * time.Second)
	for appErr.Code == "" {
		select {
		case evt := <-bus.Control:
			if ae, ok := evt.(eventbus.AppError); ok {
				appErr = ae
			}
		case <-deadline:
			t.Fatal("expected a CONFLICT AppError, got none")
		}
	}
	go func() {
		for range bus.Control {
		}
	}()
	if appErr.Code != "CONFLICT" || appErr.Service != "svc-a" || !strings.Contains(appErr.Message, "taken_over") {
		t.Fatalf("AppError = %+v, want CONFLICT for svc-a carrying the reason", appErr)
	}
	if appErr.Hint != takenOverHint {
		t.Fatalf("AppError hint = %q, want the take-over hint %q", appErr.Hint, takenOverHint)
	}

	waitForRow(t, tun, "svc-a rejected", "svc-a", func(r registry.Row, ok bool) bool {
		return ok && r.Rejected == "CONFLICT"
	})
	waitForRow(t, tun, "svc-b untouched", "svc-b", func(r registry.Row, ok bool) bool {
		return ok && r.Rejected == "" && !r.Disabled
	})
	if got := requestOver(t, c, "svc-b.example"); got != 200 {
		t.Fatalf("svc-b after svc-a's conflict: status %d, want 200", got)
	}

	tun.RegisterService("svc-a")
	waitForRow(t, tun, "svc-a active again", "svc-a", func(r registry.Row, ok bool) bool {
		return ok && r.Rejected == "" && !r.Reregistering && r.URL == "https://svc-a.example/mcp"
	})
	if got := requestOver(t, c, "svc-a.example"); got != 200 {
		t.Fatalf("svc-a after re-registering: status %d, want 200", got)
	}
	if got := exitCode.Load(); got != -1 {
		t.Fatalf("expected no FatalExit, got exit code %d", got)
	}
}

// TestConflictIDMismatchWarnsAndApplies: a conflict is keyed on its name;
// an id that differs from the one this name registered with is logged and
// the conflict still applies. A conflict for a name this config doesn't
// hold is ignored and adds no row.
func TestConflictIDMismatchWarnsAndApplies(t *testing.T) {
	var serverConn atomic.Pointer[fakeedge.Conn]
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		serverConn.Store(c)
		c.ReplyRegisteredAll(nil)
	})

	logs := &recordingHandler{}
	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Bus:         newTestBus(t),
		Log:         slog.New(logs),
		FatalExit:   func(int) {},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	waitForRow(t, tun, "svc active", "svc", func(r registry.Row, ok bool) bool { return ok && r.URL != "" })
	c := serverConn.Load()
	_ = c.SendApp(context.Background(), map[string]any{
		"mcpwarp": map[string]any{"v": 1, "op": "conflict", "id": "id-other", "name": "not-mine", "reason": "taken_over"},
	})
	_ = c.SendApp(context.Background(), map[string]any{
		"mcpwarp": map[string]any{"v": 1, "op": "conflict", "id": "id-other", "name": "svc", "reason": "taken_over"},
	})

	waitForRow(t, tun, "svc rejected despite the id mismatch", "svc", func(r registry.Row, ok bool) bool {
		return ok && r.Rejected == "CONFLICT"
	})
	rec, ok := logs.find("conflict id differs from this name's registered id; applying it by name")
	if !ok {
		t.Fatal("expected a warn log for the id mismatch")
	}
	if v, _ := recordAttr(rec, "registeredId"); v.String() != "id-svc" {
		t.Fatalf("registeredId attr = %q, want id-svc", v.String())
	}
	if _, ok := logs.find("conflict for a name this connection doesn't hold; ignoring"); !ok {
		t.Fatal("expected the conflict for an unconfigured name to be logged and ignored")
	}
	for _, r := range tun.Registry().Rows() {
		if r.Name == "not-mine" {
			t.Fatalf("a conflict for an unconfigured name added a row: %+v", r)
		}
	}
}

// TestConflictIsStickyUntilLocalEnable: a taken-over name stays with the
// other session, like ws-mixer-go's mcpwarp layer (TestAgentStickyConflict
// ClosesClient): a dashboard "enable" for it is ignored, a reconnect's
// register batch leaves it out, and only a local `e` (RegisterService)
// re-registers it, whose success makes it active again.
func TestConflictIsStickyUntilLocalEnable(t *testing.T) {
	var mu sync.Mutex
	var batches [][]string
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		c.ReplyRegisteredAll(func(names []string) {
			mu.Lock()
			batches = append(batches, names)
			mu.Unlock()
		})
	})
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

	logs := &recordingHandler{}
	var enabled atomic.Int32
	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc-a", Kind: "http"}, {Name: "svc-b", Kind: "http"}},
		Bus:         newTestBus(t),
		Log:         slog.New(logs),
		FatalExit:   func(int) {},
		OnEnable:    func(string) { enabled.Add(1) },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	waitForBatch(1)
	waitForRow(t, tun, "svc-a active", "svc-a", func(r registry.Row, ok bool) bool { return ok && r.URL != "" })
	first := srv.Latest()
	_ = first.SendApp(context.Background(), map[string]any{
		"mcpwarp": map[string]any{"v": 1, "op": "conflict", "id": "id-svc-a", "name": "svc-a", "reason": "taken_over"},
	})
	waitForRow(t, tun, "svc-a rejected", "svc-a", func(r registry.Row, ok bool) bool { return ok && r.Rejected == "CONFLICT" })

	_ = first.SendApp(context.Background(), map[string]any{
		"mcpwarp": map[string]any{"v": 1, "op": "enable", "id": "id-svc-a", "name": "svc-a"},
	})
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := logs.find("enable for a name another session took over; ignoring"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected the dashboard enable for a taken-over name to be ignored")
		}
		time.Sleep(time.Millisecond)
	}
	if n := enabled.Load(); n != 0 {
		t.Fatalf("OnEnable fired %d times for a taken-over name, want 0", n)
	}
	if n := batchCount(); n != 1 {
		t.Fatalf("the ignored enable sent a register: %d batches, want 1", n)
	}

	_ = first.Close(uint32(wsmixer.InternalErrorCode), "forcing reconnect for test")
	waitForBatch(2)
	if got := batchAt(1); len(got) != 1 || got[0] != "svc-b" {
		t.Fatalf("register batch after the take-over = %v, want [svc-b]", got)
	}
	waitForRow(t, tun, "svc-a still rejected after the reconnect", "svc-a", func(r registry.Row, ok bool) bool {
		return ok && r.Rejected == "CONFLICT"
	})

	tun.RegisterService("svc-a")
	waitForBatch(3)
	if got := batchAt(2); len(got) != 1 || got[0] != "svc-a" {
		t.Fatalf("register batch after e = %v, want [svc-a]", got)
	}
	waitForRow(t, tun, "svc-a active after e", "svc-a", func(r registry.Row, ok bool) bool {
		return ok && r.Rejected == "" && !r.Reregistering && r.URL == "https://svc-a.example/mcp"
	})
}

// TestConflictBeforeItsRegisteredReplyStands: a "conflict" can overtake the
// "registered" reply naming that same success (the tunnel records the hold
// before replying, and a concurrent take-over's frame can go first). The
// late success must not flip the name back to active with a URL — on the
// first batch, and on an `e` whose reply loses the same race — and the next
// batch must still leave it out. A later `e` that isn't overtaken wins.
func TestConflictBeforeItsRegisteredReplyStands(t *testing.T) {
	var raceConflict atomic.Bool
	raceConflict.Store(true)
	var mu sync.Mutex
	var batches [][]string
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			names := fakeedge.RegisterNames(raw)
			if names == nil {
				return
			}
			mu.Lock()
			batches = append(batches, names)
			mu.Unlock()
			services := make([]map[string]any, len(names))
			for i, n := range names {
				if n == "svc-a" && raceConflict.Load() {
					_ = c.SendApp(context.Background(), map[string]any{
						"mcpwarp": map[string]any{"v": 1, "op": "conflict", "id": "id-svc-a", "name": "svc-a", "reason": "taken_over"},
					})
				}
				services[i] = map[string]any{"name": n, "id": "id-" + n, "url": "https://" + n + ".example/mcp", "created": false}
			}
			_ = c.SendApp(context.Background(), map[string]any{
				"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": services, "errors": []any{}},
			})
			// Handled after the reply: once its warn is logged, so is the
			// reply's effect.
			_ = c.SendApp(context.Background(), map[string]any{
				"mcpwarp": map[string]any{"v": 1, "op": "sentinel"},
			})
		})
	})
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

	logs := &recordingHandler{}
	sentinels := func() int {
		logs.mu.Lock()
		defer logs.mu.Unlock()
		n := 0
		for _, r := range logs.records {
			if r.Message == "ignoring unrecognized mcpwarp app message" {
				n++
			}
		}
		return n
	}
	waitUntil := func(what string, ok func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !ok() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(time.Millisecond)
		}
	}

	var exitCode atomic.Int32
	exitCode.Store(-1)
	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc-a", Kind: "http"}, {Name: "svc-b", Kind: "http"}},
		Bus:         newTestBus(t),
		Log:         slog.New(logs),
		FatalExit:   func(code int) { exitCode.Store(int32(code)) },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	stillTakenOver := func(when string) {
		t.Helper()
		for _, r := range tun.Registry().Rows() {
			if r.Name == "svc-a" && (r.Rejected != "CONFLICT" || r.URL != "" || r.Reregistering) {
				t.Fatalf("svc-a %s = %+v, want rejected CONFLICT with no URL", when, r)
			}
		}
		if _, ok := tun.Registry().Get("svc-a"); ok {
			t.Fatalf("svc-a %s got a routable entry from the overtaken success", when)
		}
	}

	waitUntil("the first batch's reply", func() bool { return sentinels() >= 1 })
	stillTakenOver("after the first batch")
	waitForRow(t, tun, "svc-b active", "svc-b", func(r registry.Row, ok bool) bool { return ok && r.URL != "" })

	_ = srv.Latest().Close(uint32(wsmixer.InternalErrorCode), "forcing reconnect for test")
	waitUntil("the reconnect's reply", func() bool { return batchCount() >= 2 && sentinels() >= 2 })
	if got := batchAt(1); len(got) != 1 || got[0] != "svc-b" {
		t.Fatalf("register batch after the take-over = %v, want [svc-b]", got)
	}

	tun.RegisterService("svc-a")
	waitUntil("the overtaken e's reply", func() bool { return batchCount() >= 3 && sentinels() >= 3 })
	if got := batchAt(2); len(got) != 1 || got[0] != "svc-a" {
		t.Fatalf("register batch after e = %v, want [svc-a]", got)
	}
	stillTakenOver("after an e whose reply was overtaken")

	raceConflict.Store(false)
	tun.RegisterService("svc-a")
	waitForRow(t, tun, "svc-a active after an e that wins", "svc-a", func(r registry.Row, ok bool) bool {
		return ok && r.Rejected == "" && !r.Reregistering && r.URL == "https://svc-a.example/mcp"
	})
	if got := exitCode.Load(); got != -1 {
		t.Fatalf("expected no FatalExit, got exit code %d", got)
	}
}

// TestConflictBeforeItsRegisterErrorStands is the error-side sibling of
// TestConflictBeforeItsRegisteredReplyStands: a stale reply's error for a
// taken-over name (here SERVER_DISABLED, which would read as "disabled",
// resumable by a dashboard enable that OpEnable ignores for it) must not
// replace its CONFLICT.
func TestConflictBeforeItsRegisterErrorStands(t *testing.T) {
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			if fakeedge.RegisterNames(raw) == nil {
				return
			}
			_ = c.SendApp(context.Background(), map[string]any{
				"mcpwarp": map[string]any{"v": 1, "op": "conflict", "id": "id-svc-a", "name": "svc-a", "reason": "taken_over"},
			})
			_ = c.SendApp(context.Background(), map[string]any{
				"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": []any{
					map[string]any{"name": "svc-b", "id": "id-svc-b", "url": "https://svc-b.example/mcp", "created": false},
				}, "errors": []map[string]any{
					{"name": "svc-a", "code": "SERVER_DISABLED", "message": "server is disabled"},
				}},
			})
			_ = c.SendApp(context.Background(), map[string]any{
				"mcpwarp": map[string]any{"v": 1, "op": "sentinel"},
			})
		})
	})

	logs := &recordingHandler{}
	var exitCode atomic.Int32
	exitCode.Store(-1)
	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc-a", Kind: "http"}, {Name: "svc-b", Kind: "http"}},
		Bus:         newTestBus(t),
		Log:         slog.New(logs),
		FatalExit:   func(code int) { exitCode.Store(int32(code)) },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := logs.find("ignoring unrecognized mcpwarp app message"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the sentinel after the reply")
		}
		time.Sleep(time.Millisecond)
	}
	waitForRow(t, tun, "svc-a still rejected CONFLICT", "svc-a", func(r registry.Row, ok bool) bool {
		return ok && r.Rejected == "CONFLICT" && !r.Disabled && r.URL == ""
	})
	waitForRow(t, tun, "svc-b active", "svc-b", func(r registry.Row, ok bool) bool {
		return ok && r.Rejected == "" && r.URL == "https://svc-b.example/mcp"
	})
	if got := exitCode.Load(); got != -1 {
		t.Fatalf("expected no FatalExit, got exit code %d", got)
	}
}

// TestTooManyServicesRejectsEveryPendingName: the tunnel refuses an
// oversized register as a whole with an error op and no "registered" reply.
// Every name that batch carried must read rejected with the code (not stay
// pending forever), one AppError with the hint is published, and it isn't
// fatal.
func TestTooManyServicesRejectsEveryPendingName(t *testing.T) {
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			if fakeedge.RegisterNames(raw) == nil {
				return
			}
			_ = c.SendApp(context.Background(), map[string]any{
				"mcpwarp": map[string]any{"v": 1, "op": "error", "code": "TOO_MANY_SERVICES",
					"message": "register carries 2 services, max 1 per message"},
			})
			// A sentinel the dispatcher handles after the error op: once
			// its warn is logged, every event the error op published is
			// already in bus.Control.
			_ = c.SendApp(context.Background(), map[string]any{
				"mcpwarp": map[string]any{"v": 1, "op": "sentinel"},
			})
		})
	})

	bus := eventbus.New(64)
	go func() {
		for range bus.Telemetry {
		}
	}()
	t.Cleanup(bus.Close)

	logs := &recordingHandler{}
	var exitCode atomic.Int32
	exitCode.Store(-1)
	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc-a", Kind: "http"}, {Name: "svc-b", Kind: "stdio"}},
		Bus:         bus,
		Log:         slog.New(logs),
		FatalExit:   func(code int) { exitCode.Store(int32(code)) },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := logs.find("ignoring unrecognized mcpwarp app message"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the sentinel after the TOO_MANY_SERVICES error")
		}
		time.Sleep(time.Millisecond)
	}
	var appErrs []eventbus.AppError
collect:
	for {
		select {
		case evt := <-bus.Control:
			if ae, ok := evt.(eventbus.AppError); ok {
				appErrs = append(appErrs, ae)
			}
		default:
			break collect
		}
	}
	go func() {
		for range bus.Control {
		}
	}()
	if len(appErrs) != 1 {
		t.Fatalf("expected exactly one AppError, got %d: %+v", len(appErrs), appErrs)
	}
	if ae := appErrs[0]; ae.Code != "TOO_MANY_SERVICES" || ae.Service != "" || ae.Hint != tooManyServicesHint {
		t.Fatalf("AppError = %+v, want TOO_MANY_SERVICES with no service and hint %q", ae, tooManyServicesHint)
	}
	for name, kind := range map[string]string{"svc-a": "http", "svc-b": "stdio"} {
		waitForRow(t, tun, name+" rejected", name, func(r registry.Row, ok bool) bool {
			return ok && r.Rejected == "TOO_MANY_SERVICES" && r.Kind == kind && r.URL == ""
		})
	}
	if _, ok := logs.find(tooManyServicesHint); !ok {
		t.Fatal("expected the hint to be logged")
	}
	if got := exitCode.Load(); got != -1 {
		t.Fatalf("expected no FatalExit for TOO_MANY_SERVICES, got exit code %d", got)
	}
}

// TestDrainHandOverKeepsConnectedState pins the ws-mixer-go v0.9.0 drain
// hand-over: the drained connection keeps serving its in-flight stream past
// the replacement's welcome, and its end, reported afterwards as
// Superseded, must not publish "disconnected" over the replacement's
// "connected".
func TestDrainHandOverKeepsConnectedState(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(target.Close)
	targetURL, _ := url.Parse(target.URL + "/mcp")

	var registers atomic.Int32
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			names := fakeedge.RegisterNames(raw)
			if names == nil {
				return
			}
			_ = c.SendApp(context.Background(), map[string]any{
				"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": []map[string]any{
					{"name": "svc", "id": "id-svc", "url": "https://svc.example/mcp", "created": false},
				}, "errors": []any{}},
			})
			registers.Add(1)
		})
	})

	bus := eventbus.New(64)
	var statesMu sync.Mutex
	var states []string
	go func() {
		for evt := range bus.Control {
			if cs, ok := evt.(eventbus.ConnStateChanged); ok {
				statesMu.Lock()
				states = append(states, cs.State)
				statesMu.Unlock()
			}
		}
	}()
	go func() {
		for range bus.Telemetry {
		}
	}()
	t.Cleanup(bus.Close)

	logs := &recordingHandler{}
	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Targets:     map[string]*url.URL{"svc": targetURL},
		Bus:         bus,
		Log:         slog.New(logs),
		FatalExit:   func(int) {},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	waitUntil := func(what string, ok func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !ok() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitUntil("the first register", func() bool { return registers.Load() >= 1 })
	old := srv.Latest()

	// A stream left open on the old connection keeps it alive past the
	// replacement's welcome, so the hand-over (not a plain reconnect) runs.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := old.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	go func() { _ = old.Drain(ctx, "rollout", wsmixer.DrainOptions{Deadline: 5 * time.Second}) }()

	waitUntil("the replacement's register", func() bool { return srv.ConnCount() >= 2 && registers.Load() >= 2 })

	fmt.Fprintf(stream, "GET /mcp HTTP/1.1\r\nHost: svc.example\r\n\r\n")
	_ = stream.CloseWrite()
	resp, err := http.ReadResponse(bufio.NewReader(stream), nil)
	if err != nil {
		t.Fatalf("the drained connection stopped serving its in-flight stream: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("in-flight stream on the drained connection: status %d, want 200", resp.StatusCode)
	}

	waitUntil("the superseded connection's end", func() bool {
		_, ok := logs.find("drained tunnel connection retired; its replacement is live")
		return ok
	})
	time.Sleep(50 * time.Millisecond)
	statesMu.Lock()
	got := append([]string(nil), states...)
	statesMu.Unlock()
	if len(got) == 0 || got[len(got)-1] != "connected" {
		t.Fatalf("connection states = %v, want the last one connected", got)
	}
	for _, s := range got {
		if s == "disconnected" {
			t.Fatalf("connection states = %v, want no disconnected across a drain hand-over", got)
		}
	}
}

// TestDrainHandOverIgnoresRetiringConnApp: app messages are bound per
// connection, so once the replacement has registered, a "disable" still
// arriving on the drained connection (which keeps serving until its drain
// deadline + 1s) is dropped, while the same message on the replacement is
// applied.
func TestDrainHandOverIgnoresRetiringConnApp(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(target.Close)
	targetURL, _ := url.Parse(target.URL + "/mcp")

	var registers atomic.Int32
	srv := fakeedge.Serve(t, func(c *fakeedge.Conn) {
		c.OnApp(func(raw json.RawMessage) {
			if fakeedge.RegisterNames(raw) == nil {
				return
			}
			_ = c.SendApp(context.Background(), map[string]any{
				"mcpwarp": map[string]any{"v": 1, "op": "registered", "services": []map[string]any{
					{"name": "svc", "id": "id-svc", "url": "https://svc.example/mcp", "created": false},
				}, "errors": []any{}},
			})
			registers.Add(1)
		})
	})

	var disables atomic.Int32
	logs := &recordingHandler{}
	tun, err := Start(context.Background(), Config{
		URL:         srv.URL,
		TokenSource: staticToken{token: "t"},
		Services:    []appproto.RegisterService{{Name: "svc", Kind: "http"}},
		Targets:     map[string]*url.URL{"svc": targetURL},
		Bus:         newTestBus(t),
		Log:         slog.New(logs),
		FatalExit:   func(int) {},
		OnDisable:   func(string, string, string) { disables.Add(1) },
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tun.Close(context.Background())

	waitUntil := func(what string, ok func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !ok() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(time.Millisecond)
		}
	}
	active := func(r registry.Row, ok bool) bool { return ok && !r.Disabled && r.URL != "" }

	waitUntil("the first register", func() bool { return registers.Load() >= 1 })
	waitForRow(t, tun, "svc active", "svc", active)
	old := srv.Latest()

	// As in TestDrainHandOverKeepsConnectedState, an in-flight stream keeps
	// the drained connection open past the replacement's welcome.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := old.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	go func() { _ = old.Drain(ctx, "rollout", wsmixer.DrainOptions{Deadline: 5 * time.Second}) }()

	// The replacement's register is sent from onConnect after it bound its
	// handler, so by now the old connection is superseded.
	waitUntil("the replacement's register", func() bool { return srv.ConnCount() >= 2 && registers.Load() >= 2 })
	replacement := srv.Latest()

	disable := map[string]any{"mcpwarp": map[string]any{"v": 1, "op": "disable", "id": "id-svc", "reason": "stale"}}
	if err := old.SendApp(ctx, disable); err != nil {
		t.Fatalf("SendApp on the drained connection: %v", err)
	}
	waitUntil("the drained connection's disable to be dropped", func() bool {
		_, ok := logs.find("ignoring an app message from a superseded tunnel connection")
		return ok
	})
	rec, _ := logs.find("ignoring an app message from a superseded tunnel connection")
	if v, ok := recordAttr(rec, "session"); !ok || v.String() != old.Session() {
		t.Fatalf("dropped message logged session %v, want the drained connection's %q", v, old.Session())
	}
	waitForRow(t, tun, "svc still active after the drained connection's disable", "svc", active)
	if n := disables.Load(); n != 0 {
		t.Fatalf("OnDisable ran %d times for the drained connection's disable, want 0", n)
	}

	// Still serving, and still routing svc: the dropped disable would have
	// made relay answer 503.
	fmt.Fprintf(stream, "GET /mcp HTTP/1.1\r\nHost: svc.example\r\n\r\n")
	_ = stream.CloseWrite()
	resp, err := http.ReadResponse(bufio.NewReader(stream), nil)
	if err != nil {
		t.Fatalf("the drained connection stopped serving its in-flight stream: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("in-flight stream on the drained connection: status %d, want 200", resp.StatusCode)
	}

	if err := replacement.SendApp(ctx, disable); err != nil {
		t.Fatalf("SendApp on the replacement: %v", err)
	}
	waitForRow(t, tun, "svc disabled by the replacement's disable", "svc", func(r registry.Row, ok bool) bool { return ok && r.Disabled })
	waitUntil("OnDisable for the replacement's disable", func() bool { return disables.Load() == 1 })
}
