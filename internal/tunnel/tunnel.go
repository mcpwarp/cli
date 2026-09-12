// Package tunnel is the thin wrapper around ws-mixer-go's reconnecting
// Client (DESIGN.md §3, §4): dials, (re)registers services on every
// welcome, decodes inbound app messages into internal/registry, hands
// accepted streams to internal/relay, classifies disconnects into
// retry-or-fatal-exit, and publishes typed events onto the eventbus.
//
// Every wsmixer callback (OnConnect/OnApp/OnStream/OnDrain/OnDisconnect)
// fires on the SDK's own shared delivery goroutine and must never block
// (CLIENT-SDK.md). Each one here does only a cheap decode before either
// (a) launching its own goroutine for genuinely long-lived work (OnStream's
// relay) or (b) enqueueing the rest of the work onto this package's own
// dispatcher goroutine (registry mutation, eventbus.Publish — the control
// channel is lossless and can block until a consumer reads, so it must
// never be called inline from a wsmixer callback).
package tunnel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/mcpwarp/cli/internal/appproto"
	"github.com/mcpwarp/cli/internal/auth"
	"github.com/mcpwarp/cli/internal/eventbus"
	"github.com/mcpwarp/cli/internal/registry"
	"github.com/mcpwarp/cli/internal/relay"
	"github.com/mcpwarp/cli/internal/shutdown"
	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// TokenSource is the seam this package needs from a token provider —
// narrower than internal/auth.TokenProvider's full surface (this milestone
// doesn't own internal/auth), but satisfied by it directly: its
// Token(ctx) (string, error) method matches this interface as-is. A
// MCPWARP_TOKEN static-token provider (internal/auth.StaticTokenProvider)
// doesn't itself match this shape (its Token() takes no ctx and never
// errors) — the caller wiring cmd/up.go adapts it with a one-line closure.
type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

// accepter is implemented by internal/auth.TokenProvider (MarkAccepted, no
// error) but not by a static-token source, which has nothing to mark.
type accepter interface {
	MarkAccepted()
}

// Meta is the "meta" field sent in the wsmixer hello, plus the AgentInfo
// fields (DESIGN.md §3).
type Meta struct {
	Client  string // AgentInfo.SDK, e.g. "mcpwarp-cli-go"
	Version string // AgentInfo.SDKVersion
	OS      string
}

// Config configures Start.
type Config struct {
	URL         string
	WebURL      string // dashboard base URL, linked from QUOTA_EXCEEDED and USERNAME_REQUIRED messages
	TokenSource TokenSource
	Services    []appproto.RegisterService
	Targets     map[string]*url.URL
	Meta        Meta
	Bus         *eventbus.Bus
	Log         *slog.Logger
	// OnDisable/OnEnable may block (M3B wires them to supervisor stop/kill
	// with their own 2s waits): each is invoked on its own goroutine, never
	// on this package's single dispatcher goroutine, which must stay free
	// to keep draining incoming app messages.
	OnDisable func(name, id, reason string)
	OnEnable  func(name string)
	// FatalExit is a DI seam over shutdown.FatalExit (the default, when
	// nil): called for UNSUPPORTED_VERSION and any non-retryable fatal
	// disconnect. Tests substitute a non-exiting stub — the real one calls
	// os.Exit, which would kill the test binary.
	FatalExit func(code int)
}

// Tunnel is the running wsmixer.Client wrapper. Build with Start.
type Tunnel struct {
	cfg       Config
	log       *slog.Logger
	client    *wsmixer.Client
	registry  *registry.Registry
	forwarder *relay.Forwarder
	overload  *appproto.OverloadQueue
	disp      *dispatcher

	// unreachableWarn is shared across every relayed stream so relay.go's
	// "local server unreachable" warn is rate-limited once per service per
	// minute for the life of this connection, not once per stream.
	unreachableWarn *relay.UnreachableWarnLimiter

	// relayCtx/relayCancel bound every relayed stream's Forward call (Node
	// forward/relay.ts's per-stream AbortController): Close cancels it so a
	// relay stuck on a slow/hung local target doesn't keep Close waiting
	// past relayShutdownBudget. relayWG tracks the in-flight relay
	// goroutines Close waits (boundedly) for.
	relayCtx    context.Context
	relayCancel context.CancelFunc
	relayWG     sync.WaitGroup

	// sendWG tracks every in-flight sendApp/Unregister goroutine (each an
	// OverloadQueue.Send call, which can block for as long as an
	// OVERLOADED pause plus its place in the queue takes) so Close can wait
	// for them, bounded by appSendTimeout.
	sendWG sync.WaitGroup

	registerBody   map[string]any
	unregisterBody map[string]any
	// kindByName is derived from cfg.Services once in Start (each
	// RegisterService already names its own kind, so there's nothing for a
	// caller to supply separately).
	kindByName map[string]string

	// mu guards localUnregistered, touched from whatever goroutine drives a
	// local `d`/`e` keypress (Controller.Disable/Enable) concurrently with
	// onConnect reading it from this package's dispatcher goroutine.
	mu sync.Mutex
	// localUnregistered holds names most recently taken down by a local `d`
	// keypress (UnregisterService) that haven't since been brought back by
	// `e` (RegisterService) — DESIGN.md §9: a local disable must survive a
	// reconnect instead of the next welcome's register batch resurrecting
	// it. Deliberately separate from registry's disabledIds/Status, which
	// also carries a dashboard-driven OpDisable: those names must still be
	// re-registered on reconnect so the server can reply SERVER_DISABLED
	// and the resume-on-enable flow (OpEnable -> RegisterService) keeps
	// working, so this set can't just be "is the registry entry disabled".
	// Empty until the first local `d`, so the very first connect's batch is
	// unaffected.
	localUnregistered map[string]bool

	// sawFirstRegistered is set once this process has ever applied a
	// "registered" batch with at least one success — it lives on the
	// Tunnel, not per-connection, so it stays true across every reconnect
	// this process makes, same as Node client.ts's sawFirstRegistered. Only
	// handleApp's dispatcher goroutine touches it, so it needs no lock.
	sawFirstRegistered bool
}

// Connection state, stream-open/close and app-error events are published
// as the canonical eventbus.* types (eventbus.ConnStateChanged/
// StreamOpened/StreamClosed/AppError) rather than local duplicates.

const unregisterBudget = 2 * time.Second

// Start dials the tunnel and begins the reconnecting client. Registration
// happens automatically on every welcome via OnConnect.
func Start(ctx context.Context, cfg Config) (*Tunnel, error) {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.FatalExit == nil {
		cfg.FatalExit = shutdown.FatalExit
	}
	kindByName := make(map[string]string, len(cfg.Services))
	regServices := make([]appproto.RegisterService, len(cfg.Services))
	unregServices := make([]appproto.UnregisterService, len(cfg.Services))
	for i, s := range cfg.Services {
		kindByName[s.Name] = s.Kind
		regServices[i] = s
		unregServices[i] = appproto.UnregisterService{Name: s.Name}
	}

	relayCtx, relayCancel := context.WithCancel(context.Background())
	reg := registry.New()
	reg.SetLogger(cfg.Log)
	t := &Tunnel{
		cfg:               cfg,
		log:               cfg.Log,
		registry:          reg,
		forwarder:         relay.NewForwarder(),
		overload:          appproto.NewOverloadQueue(),
		disp:              newDispatcher(),
		unreachableWarn:   relay.NewUnreachableWarnLimiter(),
		relayCtx:          relayCtx,
		relayCancel:       relayCancel,
		registerBody:      appproto.EncodeRegister(regServices),
		unregisterBody:    appproto.EncodeUnregister(unregServices),
		kindByName:        kindByName,
		localUnregistered: make(map[string]bool),
	}

	tokenProvider := func(ctx context.Context) (string, error) {
		return cfg.TokenSource.Token(ctx)
	}

	t.client = wsmixer.NewClient(cfg.URL, tokenProvider, wsmixer.ClientConfig{
		Agent: wsmixer.AgentInfo{SDK: cfg.Meta.Client, SDKVersion: cfg.Meta.Version, OS: cfg.Meta.OS},
		Meta:  map[string]string{"client": cfg.Meta.Client, "version": cfg.Meta.Version, "os": cfg.Meta.OS},
		Options: wsmixer.Options{
			Metrics: newBusMetrics(cfg.Bus),
		},
		// Node client.ts's BACKOFF_CAP_MS: cap full-jitter backoff at 30s,
		// not the SDK's own 60s default.
		Reconnect:    wsmixer.ReconnectOptions{Base: time.Second, Cap: 30 * time.Second},
		OnConnect:    t.onConnect,
		OnApp:        t.onApp,
		OnStream:     t.onStream,
		OnDrain:      t.onDrain,
		OnDisconnect: t.onDisconnect,
	})

	if err := t.client.Connect(ctx); err != nil {
		t.relayCancel()
		t.disp.close()
		t.forwarder.Close()
		return nil, fmt.Errorf("tunnel: connect: %w", err)
	}

	return t, nil
}

func (t *Tunnel) publish(evt any) {
	if t.cfg.Bus != nil {
		t.cfg.Bus.Publish(evt)
	}
}

// --- wsmixer callbacks (fire on the SDK's shared delivery goroutine) -----

func (t *Tunnel) onConnect(c *wsmixer.Conn, welcome *wsmixer.WelcomeMsg) {
	session := welcome.Session
	t.disp.enqueue(func() {
		if a, ok := t.cfg.TokenSource.(accepter); ok {
			a.MarkAccepted()
		}
		t.log.Info("connected to tunnel", "session", session)
		t.publish(eventbus.ConnStateChanged{State: "connected", Session: session})
		if body := t.currentRegisterBody(); body != nil {
			t.sendApp(c, body, func(err error) {
				t.log.Warn("failed to send register", "err", err)
			})
		}
	})
}

func (t *Tunnel) onApp(raw json.RawMessage) {
	msg, err := appproto.Decode(raw)
	if err != nil {
		t.disp.enqueue(func() {
			t.log.Warn("rejecting malformed mcpwarp app message", "err", err)
		})
		return
	}
	if msg == nil {
		return // no mcpwarp envelope at all, or an unparseable one — nothing worth logging
	}
	t.disp.enqueue(func() { t.handleApp(msg) })
}

func (t *Tunnel) handleApp(msg *appproto.Inbound) {
	switch msg.Op {
	case appproto.OpUnknown:
		// DESIGN.md §8's "ignored-with-log": an unrecognized op, or a
		// recognized one at an unsupported v — never a protocol violation
		// on our end, just worth noting.
		t.log.Warn("ignoring unrecognized mcpwarp app message", "op", msg.UnknownOp, "v", msg.UnknownV)

	case appproto.OpRegistered:
		// Node client.ts's item B: zero successes in a batch is only fatal
		// the first time this connection has ever registered anything, and
		// never when every error is SERVER_DISABLED (resumable via a later
		// "enable" instead) — a transient all-error reply to a
		// sendRegisterFor re-register, or a reconnect whose only server
		// bounces SERVER_DISABLED, must not kill an otherwise-healthy
		// process.
		zeroSuccesses := len(msg.Services) == 0 && len(msg.Errors) > 0
		allServerDisabled := zeroSuccesses
		for _, svcErr := range msg.Errors {
			if svcErr.Code != "SERVER_DISABLED" {
				allServerDisabled = false
				break
			}
		}
		fatal := zeroSuccesses && !allServerDisabled && !t.sawFirstRegistered

		for _, svcErr := range msg.Errors {
			t.log.Warn("register error", "name", svcErr.Name, "code", svcErr.Code, "message", svcErr.Message)
			hint := registerErrorHint(svcErr.Code, t.cfg.WebURL, fatal)
			t.publish(eventbus.AppError{Code: svcErr.Code, Message: svcErr.Message, Service: svcErr.Name, Hint: hint})
			if hint != "" {
				t.log.Warn(hint)
			}
		}
		if fatal {
			// FatalExit runs shutdown.FatalExit -> Tunnel.Close(ctx), which
			// waits on this same dispatcher goroutine to drain — calling it
			// synchronously here would self-deadlock. Node's counterpart is
			// likewise fire-and-forget ("void fatalExit(1)").
			go t.cfg.FatalExit(1)
			return
		}
		if zeroSuccesses || len(msg.Services) == 0 {
			return
		}
		result := t.registry.ApplyRegistered(msg.Services, t.kindByName)
		t.sawFirstRegistered = true
		for _, name := range result.Created {
			t.log.Info("new public URL assigned", "name", name)
		}
		for _, r := range result.Resurrected {
			t.log.Info("server is active again (was disabled before this reconnect)", "name", r.Name)
			if t.cfg.OnEnable != nil {
				go t.cfg.OnEnable(r.Name)
			}
		}
		for _, resolved := range t.registry.ResolvePendingDisables() {
			t.log.Info("resolving a disable that arrived before this service's name was known", "id", resolved.ID, "name", resolved.Name)
			if t.cfg.OnDisable != nil {
				go t.cfg.OnDisable(resolved.Name, resolved.ID, resolved.Reason)
			}
		}

	case appproto.OpUnregistered:
		t.log.Debug("unregistered", "services", msg.UnregisteredServices)

	case appproto.OpDisable:
		name, known := t.registry.ApplyDisable(msg.DisableID, msg.DisableReason)
		if known {
			t.log.Warn("server disabled by the tunnel", "name", name, "reason", msg.DisableReason)
			if t.cfg.OnDisable != nil {
				go t.cfg.OnDisable(name, msg.DisableID, msg.DisableReason)
			}
		} else {
			t.log.Debug("disable arrived before the matching service was registered; deferring", "id", msg.DisableID)
		}

	case appproto.OpEnable:
		// Node client.ts's item 4: a "disable" for this same id may still be
		// waiting on a name in pendingDisables — this enable must cancel
		// that deferred disable regardless of whether this connection holds
		// the name below, or handleApp's "registered" case would fire a
		// now-stale onDisable once that name finally shows up.
		t.registry.ClearPendingDisable(msg.EnableID)
		if !t.hasConfiguredService(msg.EnableName) {
			t.log.Debug("enable for a name this connection doesn't hold; ignoring", "name", msg.EnableName)
			return
		}
		t.registry.ApplyEnable(msg.EnableName, msg.EnableID)
		t.log.Info("server re-enabled by the tunnel", "name", msg.EnableName)
		if t.cfg.OnEnable != nil {
			go t.cfg.OnEnable(msg.EnableName)
		}
		// RegisterService, not sendRegisterFor directly: a dashboard enable
		// for a name the user had also locally `d`-disabled must clear
		// localUnregistered too, or the child gets respawned/registered now
		// but dropped again silently on the very next reconnect.
		t.RegisterService(msg.EnableName)

	case appproto.OpError:
		if msg.ErrorCode == "UNSUPPORTED_VERSION" {
			t.log.Error("this mcpwarp CLI speaks a protocol version the tunnel no longer supports; upgrade mcpwarp")
			// See the "registered" fatal path above: must not run
			// synchronously on this dispatcher goroutine.
			go t.cfg.FatalExit(1)
			return
		}
		t.publish(eventbus.AppError{Code: msg.ErrorCode, Message: msg.ErrorMessage})
		if msg.ErrorCode == "OVERLOADED" {
			t.overload.Trigger()
		}
		if msg.ErrorCode == "UNKNOWN_OP" || msg.ErrorCode == "BAD_REQUEST" {
			t.log.Warn("protocol mismatch — upgrade mcpwarp", "code", msg.ErrorCode)
		} else {
			t.log.Warn(msg.ErrorMessage, "code", msg.ErrorCode)
		}
	}
}

// registerErrorHint returns the code-specific next step for a "registered"
// reply's per-service error, or "" when the code has none — the single
// source both the log line and the AppError event's Hint are drawn from.
func registerErrorHint(code, webURL string, fatal bool) string {
	switch code {
	case "QUOTA_EXCEEDED":
		return fmt.Sprintf("upgrade your plan at %s/settings to add more servers", webURL)
	case "CONFLICT":
		return "a server with this name is already registered — rename it, or check its kind"
	case "SERVER_DISABLED":
		if fatal {
			return "this server was disabled in the dashboard"
		}
		return "this server was disabled in the dashboard — waiting, toggle it on to resume"
	case "INVALID_NAME":
		return "fix the name in your config — it is the public URL slug and must match [a-z0-9]([a-z0-9-]*[a-z0-9])? (max 30 chars)"
	case "USERNAME_REQUIRED":
		return fmt.Sprintf("sign in once at %s to choose a username, then run `mcpwarp up` again", webURL)
	default:
		return ""
	}
}

// currentRegisterBody builds the "register" payload for this connect or
// reconnect, omitting any name currently in localUnregistered (a local `d`
// keypress not yet followed by `e`) — see that field's doc. The common case
// (nothing locally unregistered, including every fresh `up`) returns the
// same precomputed t.registerBody rather than re-encoding it. Returns nil
// if every configured service is currently locally unregistered: neither
// this CLI nor Node's ever sends a register with an empty services list,
// and the server may bounce an empty batch as BAD_REQUEST, which handleApp
// would then log as a protocol mismatch.
func (t *Tunnel) currentRegisterBody() map[string]any {
	t.mu.Lock()
	n := len(t.localUnregistered)
	skip := make(map[string]bool, n)
	for name := range t.localUnregistered {
		skip[name] = true
	}
	t.mu.Unlock()
	if n == 0 {
		return t.registerBody
	}
	subset := make([]appproto.RegisterService, 0, len(t.cfg.Services))
	for _, s := range t.cfg.Services {
		if !skip[s.Name] {
			subset = append(subset, s)
		}
	}
	if len(subset) == 0 {
		return nil
	}
	return appproto.EncodeRegister(subset)
}

// hasConfiguredService reports whether name is one this connection's own
// config holds, matching Node client.ts's kindByName-derived gate (built
// from the same services list).
func (t *Tunnel) hasConfiguredService(name string) bool {
	for _, s := range t.cfg.Services {
		if s.Name == name {
			return true
		}
	}
	return false
}

// sendRegisterFor re-registers just name after a live "enable" — the
// tunnel strips its own routing state for a disabled service, so
// un-disabling locally isn't enough. Best-effort: a failure just logs, the
// next welcome re-registers everything anyway.
func (t *Tunnel) sendRegisterFor(name string) {
	var subset []appproto.RegisterService
	for _, s := range t.cfg.Services {
		if s.Name == name {
			subset = append(subset, s)
		}
	}
	if len(subset) == 0 {
		return
	}
	conn := t.client.Conn()
	if conn == nil {
		t.log.Debug("cannot re-register after enable — connection is gone", "name", name)
		return
	}
	body := appproto.EncodeRegister(subset)
	t.sendApp(conn, body, func(err error) {
		t.log.Debug("failed to re-register after enable", "name", name, "err", err)
	})
}

// RegisterService re-registers a single configured service — the exported
// counterpart of sendRegisterFor, for a caller-driven "enable" (a local `e`
// keypress/Controller.Enable, DESIGN.md §9) rather than one arriving off
// the wire. Always clears name from localUnregistered first, even with no
// live connection (edge case: `e` pressed while disconnected must still
// make the next welcome's register batch include name again) — the actual
// re-register send below stays a no-op in that case, same as before.
func (t *Tunnel) RegisterService(name string) {
	t.mu.Lock()
	delete(t.localUnregistered, name)
	t.mu.Unlock()
	t.sendRegisterFor(name)
}

// UnregisterService sends "unregister" for a single configured service —
// the exported counterpart of the caller-driven `d` keypress/
// Controller.Disable (DESIGN.md §9). It also flips that entry's registry
// status to disabled locally (if known) so relay/registry.IsDisabled 503s
// the service immediately rather than waiting on a round trip through the
// tunnel's own "disable" reply, and marks name locally unregistered so a
// later reconnect's register batch excludes it (see localUnregistered's
// doc) until a matching RegisterService. Best-effort like every other app
// send: a failure to actually reach the tunnel just logs.
func (t *Tunnel) UnregisterService(name string) error {
	t.mu.Lock()
	t.localUnregistered[name] = true
	t.mu.Unlock()
	if entry, ok := t.registry.Get(name); ok {
		t.registry.ApplyDisable(entry.ID, "unregistered locally")
	}
	conn := t.client.Conn()
	if conn == nil {
		return nil
	}
	body := appproto.EncodeUnregister([]appproto.UnregisterService{{Name: name}})
	t.sendApp(conn, body, func(err error) {
		t.log.Debug("failed to unregister service", "name", name, "err", err)
	})
	return nil
}

// appSendTimeout bounds one overload-queue-guarded SendApp call — long
// enough to ride out an OverloadPause, short enough that a send on a
// connection that's silently died doesn't hang the goroutine sendApp starts
// forever.
const appSendTimeout = 10 * time.Second

// sendApp runs an OverloadQueue-guarded SendApp(body) on conn in its own
// goroutine: OverloadQueue.Send blocks its caller for as long as an
// OVERLOADED pause (and this send's place in its queue) takes, which must
// never happen on this package's single dispatcher goroutine — Node
// client.ts's sendAppOrQueue is fired the same way ("void
// sendAppOrQueue(...).catch(...)"), never awaited inline. onErr, if
// non-nil, is called with the eventual error (nil on success).
func (t *Tunnel) sendApp(conn *wsmixer.Conn, body map[string]any, onErr func(err error)) {
	t.sendWG.Add(1)
	go func() {
		defer t.sendWG.Done()
		err := t.overload.Send(func() error {
			ctx, cancel := context.WithTimeout(context.Background(), appSendTimeout)
			defer cancel()
			return conn.SendApp(ctx, body)
		})
		if err != nil && onErr != nil {
			onErr(err)
		}
	}()
}

func (t *Tunnel) onStream(s *wsmixer.Stream) {
	// Genuinely long-lived (an SSE response can stay open indefinitely) —
	// its own goroutine, not the dispatcher, which is for quick
	// bookkeeping only. t.publish (eventbus.Publish) can itself block until
	// a consumer reads, so it must not run on the SDK's shared delivery
	// goroutine either — both publishes happen inside this goroutine, same
	// as the relay itself. relayWG/relayCtx let Close cancel and (boundedly)
	// wait for every such goroutine still in flight.
	t.relayWG.Add(1)
	go func() {
		defer t.relayWG.Done()
		ctx, cancel := context.WithCancel(t.relayCtx)
		defer cancel()
		t.publish(eventbus.StreamOpened{ID: s.ID()})
		defer t.publish(eventbus.StreamClosed{ID: s.ID()})
		relay.RelayStream(s, relay.Options{
			Ctx:             ctx,
			Registry:        t.registry,
			Forwarder:       t.forwarder,
			Targets:         t.cfg.Targets,
			Log:             t.log,
			UnreachableWarn: t.unreachableWarn,
		})
	}()
}

func (t *Tunnel) onDrain(d *wsmixer.DrainMsg) {
	reason, msg := d.Reason, d.Message
	t.disp.enqueue(func() {
		t.log.Info("tunnel draining; reconnecting", "reason", reason, "message", msg)
	})
}

func (t *Tunnel) onDisconnect(reason wsmixer.DisconnectReason) {
	t.disp.enqueue(func() {
		t.publish(eventbus.ConnStateChanged{State: "disconnected"})
		if !reason.Fatal {
			t.log.Info("tunnel disconnected; SDK reconnecting", "phase", reason.Phase, "message", reason.Message)
			return
		}
		// classifyFatal (client.ts:140-160): only a fatal disconnect caused
		// by the token provider itself failing (a TokenRefreshError from
		// internal/auth, surfaced verbatim as Cause) gets ws-mixer-go's own
		// retry loop — everything else here is truly terminal, exit.
		if isRetryableTokenFailure(reason.Cause) {
			t.log.Warn("could not refresh the session token; ws-mixer-go will retry", "err", reason.Cause)
			return
		}
		message := classifyFatalMessage(t.cfg.URL, reason)
		t.log.Error(message, "wsCode", reason.WSCode, "httpStatus", reason.HTTPStatus)
		// See handleApp's fatal path: FatalExit -> Tunnel.Close(ctx) waits
		// on this dispatcher goroutine, so it must not run synchronously here.
		go t.cfg.FatalExit(1)
	})
}

// unauthorizedWSCode is the wsmixer close code for wsmixer.UnauthorizedCode
// (4000 + 0x0b), used the same way Node client.ts's UNAUTHORIZED_WS_CODE is.
const unauthorizedWSCode = 4011

// classifyFatalMessage is this package's half of client.ts's classifyFatal:
// the message a fatal disconnect is logged (and exited) with, kept in
// parity with the Node CLI's wording so a user sees the same guidance from
// either implementation.
func classifyFatalMessage(url string, reason wsmixer.DisconnectReason) string {
	// client.ts:145-147: a cause of NotLoggedInError/SessionExpiredError
	// carries its own user-facing message — checked before the generic
	// httpStatus/wsCode classification below, same order as Node's
	// classifyFatal.
	if msg, ok := causeMessage(reason.Cause); ok {
		return msg
	}
	if reason.HTTPStatus == 401 || reason.HTTPStatus == 403 ||
		reason.ErrorName == "UNAUTHORIZED" || reason.WSCode == unauthorizedWSCode {
		return "session expired, run `mcpwarp login`"
	}
	if reason.HTTPStatus == 404 {
		return fmt.Sprintf("could not reach %s — check --connect-url / MCPWARP_CONNECT_URL", url)
	}
	msg := reason.Message
	if msg == "" {
		msg = "unknown error"
	}
	return fmt.Sprintf("tunnel connection failed: %s", msg)
}

// causeMessage is client.ts:145-147's other classifyFatal branch: a fatal
// disconnect's Cause of *auth.NotLoggedInError or *auth.SessionExpiredError
// (never retryable, always carrying a non-empty user-facing message) is
// reported verbatim instead of falling through to the generic httpStatus/
// wsCode wording below. internal/auth doesn't import this package, so no
// cycle risk in asserting its concrete types directly here.
func causeMessage(cause error) (string, bool) {
	if cause == nil {
		return "", false
	}
	var notLoggedIn *auth.NotLoggedInError
	var sessionExpired *auth.SessionExpiredError
	if errors.As(cause, &notLoggedIn) {
		return notLoggedIn.Error(), true
	}
	if errors.As(cause, &sessionExpired) {
		return sessionExpired.Error(), true
	}
	return "", false
}

// isRetryableTokenFailure is this package's half of client.ts's
// classifyFatal: internal/auth.TokenRefreshError implements Retryable()
// bool (its Error() alone isn't enough to know) — checked structurally so
// this package doesn't need to import internal/auth just for one error
// type switch.
func isRetryableTokenFailure(cause error) bool {
	if cause == nil {
		return false
	}
	var retryable interface{ Retryable() bool }
	return errors.As(cause, &retryable) && retryable.Retryable()
}

// Unregister best-effort sends "unregister" for every configured service,
// budgeted to unregisterBudget (DESIGN.md §3's shutdown sequence step 1).
// Routed through t.overload (like every other app send, Node client.ts's
// sendAppOrQueue) rather than sent directly, so an unregister made while
// the tunnel is OVERLOADED queues in order instead of overtaking a pending
// register. The send itself keeps running in the background past
// unregisterBudget (bounded by appSendTimeout, tracked by sendWG so Close
// still waits for it) — Unregister only stops waiting on it here.
func (t *Tunnel) Unregister(ctx context.Context) error {
	conn := t.client.Conn()
	if conn == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, unregisterBudget)
	defer cancel()

	done := make(chan error, 1)
	t.sendWG.Add(1)
	go func() {
		defer t.sendWG.Done()
		done <- t.overload.Send(func() error {
			sendCtx, sendCancel := context.WithTimeout(context.Background(), appSendTimeout)
			defer sendCancel()
			return conn.SendApp(sendCtx, t.unregisterBody)
		})
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// relayShutdownBudget bounds how long Close waits for in-flight relay
// goroutines to notice relayCancel and return, after which Close proceeds
// regardless — a stuck local target must not hang shutdown forever.
const relayShutdownBudget = 5 * time.Second

// Close stops the tunnel: drain{client_requested}, wait up to 5s, close —
// DESIGN.md §3's shutdown sequence step 2. Also cancels every in-flight
// relay stream's Forward call and waits (boundedly) for its goroutine to
// exit, then stops this package's own dispatcher and forwarder.
func (t *Tunnel) Close(ctx context.Context) error {
	err := t.client.Close(ctx)

	t.relayCancel()
	relaysDone := make(chan struct{})
	go func() {
		t.relayWG.Wait()
		close(relaysDone)
	}()
	select {
	case <-relaysDone:
	case <-time.After(relayShutdownBudget):
		t.log.Warn("timed out waiting for in-flight relay streams to finish")
	case <-ctx.Done():
		t.log.Warn("shutdown deadline hit waiting for in-flight relay streams to finish")
	}

	sendsDone := make(chan struct{})
	go func() {
		t.sendWG.Wait()
		close(sendsDone)
	}()
	select {
	case <-sendsDone:
	case <-time.After(appSendTimeout):
		t.log.Warn("timed out waiting for in-flight app sends to finish")
	case <-ctx.Done():
		t.log.Warn("shutdown deadline hit waiting for in-flight app sends to finish")
	}

	t.disp.close()
	t.forwarder.Close()
	t.publish(eventbus.ConnStateChanged{State: "closed"})
	return err
}

// Registry exposes the live registry, e.g. for a table renderer.
func (t *Tunnel) Registry() *registry.Registry { return t.registry }

// Stats returns the wsmixer client's cumulative counters (unknown frames,
// stale frames, byte counts, ...) across every connection this Client has
// held, e.g. for a §9 diagnostics panel.
func (t *Tunnel) Stats() wsmixer.Stats { return t.client.Stats() }
