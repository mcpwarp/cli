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
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mcpwarp/cli/internal/appproto"
	"github.com/mcpwarp/cli/internal/auth"
	"github.com/mcpwarp/cli/internal/eventbus"
	"github.com/mcpwarp/cli/internal/registry"
	"github.com/mcpwarp/cli/internal/relay"
	"github.com/mcpwarp/cli/internal/shutdown"
	"github.com/mcpwarp/ws-mixer-go/mcpwarp"
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
	// Reconnect overrides the SDK's reconnect backoff. A zero Base, Cap or
	// StableAfter keeps this CLI's own value (withReconnectDefaults); a test
	// sets a short Cap so a redial scheduled at random(0, Cap) doesn't take
	// up to 30s.
	Reconnect wsmixer.ReconnectOptions

	// afterFunc runs f once d has passed and returns a func that cancels it
	// (time.AfterFunc's shape, the default when nil): the clock behind the
	// OVERLOADED re-send and the transient retry. A test steps it by hand.
	afterFunc func(d time.Duration, f func()) (stop func() bool)
}

// withReconnectDefaults fills r's zero Base/Cap/StableAfter with this CLI's
// policy. Cap: Node client.ts's BACKOFF_CAP_MS caps full-jitter backoff at
// 30s, not the SDK's own 60s default. StableAfter: the backoff attempt
// counter resets only once a connection has stayed up this long past
// welcome, so a server that accepts then drops right away can't pin us at
// Base.
func withReconnectDefaults(r wsmixer.ReconnectOptions) wsmixer.ReconnectOptions {
	if r.Base == 0 {
		r.Base = time.Second
	}
	if r.Cap == 0 {
		r.Cap = 30 * time.Second
	}
	if r.StableAfter == 0 {
		r.StableAfter = 10 * time.Second
	}
	return r
}

// Tunnel is the running wsmixer.Client wrapper. Build with Start.
type Tunnel struct {
	cfg       Config
	log       *slog.Logger
	client    *wsmixer.Client
	registry  *registry.Registry
	forwarder *relay.Forwarder
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

	// sendWG tracks every in-flight writeFrame goroutine (a SendApp, which
	// can block on a full control queue) so Close can wait for them, bounded
	// by appSendTimeout.
	sendWG sync.WaitGroup

	// kindByName is derived from cfg.Services once in Start (each
	// RegisterService already names its own kind, so there's nothing for a
	// caller to supply separately).
	kindByName map[string]string

	// The fields from frames to idleWaiters are ws-mixer-spec MCPWARP.md
	// §2.6's frames in flight and §3.2's transient retry. Only the
	// dispatcher goroutine touches them, so they need no lock.
	//
	// frames holds every register/unregister not yet answered, at most one
	// of them outstanding (plus the closing unregister, FrameQueue.Close) on
	// framesConn, the connection onConnect last started them on (nil once it
	// ended).
	frames     *appproto.FrameQueue
	framesConn *wsmixer.Conn
	// resendStop cancels the outstanding frame's scheduled OVERLOADED
	// re-send, nil when none is.
	resendStop func() bool
	// overloadedReported: an OVERLOADED AppError has been published for the
	// outstanding frame, so its repeats only log at Debug until the frame is
	// settled or dropped (stopResend, frameWriteFailed).
	overloadedReported bool
	// retries are the scheduled per-name transient re-sends (§3.2 Transient
	// per name); a disconnect cancels them, the next welcome registers
	// anyway.
	retries map[*transientRetry]bool
	// transientFailures is §3.2's transient counter: consecutive
	// all-transient "registered" replies, carried across reconnects.
	transientFailures int
	// retrying marks each name in a transient retry series, until a final
	// answer or a local `d`/`e` ends the series, so only the first transient
	// error of a series is reported. It survives reconnects, as the
	// transient counter does.
	retrying map[string]retryMark
	// notes holds, per name and per ServiceChanged code, the edge's text for
	// it (a register error's message, a disable's or conflict's reason) —
	// what serviceState puts in Message.
	notes map[string]map[string]string
	// published is the last eventbus.ServiceChanged published per configured
	// name, seeded pending, so publishServiceChanges reports each change
	// once.
	published map[string]eventbus.ServiceChanged
	// held is ws-mixer-spec MCPWARP.md §3.3's "holds" for the connection
	// frames last started on: the names a "registered" on it carried a
	// services[] entry for (a taken-over one's doesn't count), less those
	// an unregister on it settled or a conflict took. A conflict for any
	// other name is ignored (§2.7). Reset
	// by startFrames, not by a disconnect: a conflict the ended connection
	// still delivers is applied (§2.7 Ended connection).
	held map[string]bool
	// endedConn is the connection a connected-phase disconnect last ended:
	// a registered/unregistered/disable/enable it still delivers after that
	// is ignored (§2.7 Ended connection), as is one delivered once the
	// connection's Done was closed (onApp).
	endedConn *wsmixer.Conn
	// closing is set by Unregister: no register goes out after it (§3.2
	// Unregister).
	closing bool
	// idleWaiters are closed once frames is idle or its connection ended
	// (Unregister's wait for its frame's reply).
	idleWaiters []chan struct{}
	// fatalSent: checkFatal already called FatalExit.
	fatalSent bool
	// stopped is set by Close (stopFrames): no frame is written after it.
	stopped bool

	// connLimitReported is set once a CONNECTION_LIMIT AppError has been
	// published for the current connection attempt, so the app-level error
	// envelope and a matching close reason — both of which can arrive for
	// the same condition — don't surface it twice. Reset on every onConnect,
	// so a new connection attempt reports CONNECTION_LIMIT again. Touched
	// only from the dispatcher goroutine (onConnect, handleApp and
	// onDisconnect all run inside t.disp.enqueue), so it needs no locking.
	connLimitReported bool

	// mu guards localUnregistered and takenOver, touched from whatever
	// goroutine drives a local `d`/`e` keypress (Controller.Disable/Enable)
	// concurrently with onConnect reading them from this package's
	// dispatcher goroutine.
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
	// takenOver holds names a "conflict" op took away (another session of
	// the account registered them, DESIGN.md §8) and not since reclaimed by
	// `e` (RegisterService). Sticky like ws-mixer-go's own mcpwarp layer
	// (its DECISIONS.md D-2026-10-14-03): every register batch leaves them
	// out, so a reconnect can't take the name back and bounce the other
	// session, and a dashboard enable for one is ignored.
	takenOver map[string]bool

	// sawFirstRegistered is set once this process has ever applied a
	// "registered" batch with at least one success — it lives on the
	// Tunnel, not per-connection, so it stays true across every reconnect
	// this process makes, same as Node client.ts's sawFirstRegistered. Only
	// the dispatcher goroutine touches it (handleApp, checkFatal), so it
	// needs no lock.
	sawFirstRegistered bool

	// liveConn is the connection onConnect last bound, stored on the SDK's
	// callback goroutine and read on the dispatcher's (see superseded).
	liveConn atomic.Pointer[wsmixer.Conn]

	// closeStarted is set as Close begins, before the client's own close,
	// so onDisconnect knows the end it reports is ours even when Close runs
	// without Unregister having set closing first.
	closeStarted atomic.Bool
}

// Connection state, stream-open/close, app-error and per-service
// registration events are published as the canonical eventbus.* types
// (eventbus.ConnStateChanged/StreamOpened/StreamClosed/AppError/
// ServiceChanged) rather than local duplicates.

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
	cfg.Reconnect = withReconnectDefaults(cfg.Reconnect)
	if cfg.afterFunc == nil {
		cfg.afterFunc = func(d time.Duration, f func()) func() bool { return time.AfterFunc(d, f).Stop }
	}
	kindByName := make(map[string]string, len(cfg.Services))
	published := make(map[string]eventbus.ServiceChanged, len(cfg.Services))
	for _, s := range cfg.Services {
		kindByName[s.Name] = s.Kind
		published[s.Name] = eventbus.ServiceChanged{Name: s.Name, Kind: s.Kind, State: statePending}
	}

	relayCtx, relayCancel := context.WithCancel(context.Background())
	reg := registry.New()
	reg.SetLogger(cfg.Log)
	t := &Tunnel{
		cfg:               cfg,
		log:               cfg.Log,
		registry:          reg,
		forwarder:         relay.NewForwarder(),
		disp:              newDispatcher(),
		unreachableWarn:   relay.NewUnreachableWarnLimiter(),
		relayCtx:          relayCtx,
		relayCancel:       relayCancel,
		kindByName:        kindByName,
		frames:            appproto.NewFrameQueue(),
		retries:           make(map[*transientRetry]bool),
		retrying:          make(map[string]retryMark),
		notes:             make(map[string]map[string]string),
		published:         published,
		held:              make(map[string]bool),
		localUnregistered: make(map[string]bool),
		takenOver:         make(map[string]bool),
	}

	// A retryable refresh failure (isRetryableTokenFailure) is marked with
	// wsmixer.ErrTokenUnavailable so the SDK backs off and redials instead
	// of closing the client; any other provider error stays fatal.
	tokenProvider := func(ctx context.Context) (string, error) {
		token, err := cfg.TokenSource.Token(ctx)
		if err != nil && isRetryableTokenFailure(err) {
			return "", fmt.Errorf("%w: %w", wsmixer.ErrTokenUnavailable, err)
		}
		return token, err
	}

	t.client = wsmixer.NewClient(cfg.URL, tokenProvider, wsmixer.ClientConfig{
		Agent: wsmixer.AgentInfo{SDK: cfg.Meta.Client, SDKVersion: cfg.Meta.Version, OS: cfg.Meta.OS},
		Meta:  map[string]string{"client": cfg.Meta.Client, "version": cfg.Meta.Version, "os": cfg.Meta.OS},
		Options: wsmixer.Options{
			Metrics: newBusMetrics(cfg.Bus),
		},
		Reconnect:    cfg.Reconnect,
		OnConnect:    t.onConnect,
		OnStream:     t.onStream,
		OnDrain:      t.onDrain,
		OnDisconnect: t.onDisconnect,
		// Every new connection starts with this handler and keeps it until
		// onConnect binds its own: OnConnect runs on the SDK's callback
		// goroutine while the connection's delivery goroutine is already
		// live, so a frame can land first (the reply to an `e` re-register
		// sent on t.client.Conn() in that window, or a disable/enable fanned
		// out to it). Whatever arrives here comes from the newest connection
		// — a retiring one already has its own handler bound — so it is
		// applied.
		OnApp: func(raw json.RawMessage) { t.onApp(nil, raw) },
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

// publishConnLimitError is publish for a CONNECTION_LIMIT eventbus.AppError
// specifically, deduped against connLimitReported so the app-level error
// envelope and a matching close reason don't both surface it for the same
// connection attempt. Only callable from the dispatcher goroutine.
func (t *Tunnel) publishConnLimitError(evt eventbus.AppError) {
	if t.connLimitReported {
		return
	}
	t.connLimitReported = true
	t.publish(evt)
}

// connectionLimitMessage strips connectionLimitPrefix and surrounding
// whitespace from raw (a CONNECTION_LIMIT close reason or app error
// message), falling back to a generic message when that leaves nothing —
// the single source both onDisconnect and handleApp build their
// CONNECTION_LIMIT AppError's Message from, so the two paths produce an
// identical Message (and therefore Hint) for the dedupe to actually match.
func connectionLimitMessage(raw string) string {
	msg := strings.TrimSpace(strings.TrimPrefix(raw, connectionLimitPrefix))
	if msg == "" {
		return "too many agent connections for this account"
	}
	return msg
}

// --- wsmixer callbacks (fire on the SDK's shared delivery goroutine) -----

func (t *Tunnel) onConnect(c *wsmixer.Conn, welcome *wsmixer.WelcomeMsg) {
	session := welcome.Session
	// DESIGN.md §8's drain hand-over: the drained connection keeps serving
	// for up to its drain deadline + 1s after this welcome, so its app
	// messages are bound to it rather than client-wide and dropped once it
	// is superseded. Stored before binding, so a message from c is never
	// checked against the previous connection; bound before the register
	// below is enqueued, so the registered reply can't arrive ahead of the
	// handler.
	t.liveConn.Store(c)
	c.OnApp(func(raw json.RawMessage) { t.onApp(c, raw) })
	t.disp.enqueue(func() {
		t.connLimitReported = false
		if a, ok := t.cfg.TokenSource.(accepter); ok {
			a.MarkAccepted()
		}
		t.log.Info("connected to tunnel", "session", session)
		t.publish(eventbus.ConnStateChanged{State: "connected", Session: session})
		t.startFrames(c)
		t.publishServiceChanges()
	})
}

// onApp decodes one app message that arrived on c, or on a connection
// onConnect hadn't bound yet when c is nil (ClientConfig.OnApp), and hands it
// to handleApp unless c has been superseded by then.
func (t *Tunnel) onApp(c *wsmixer.Conn, raw json.RawMessage) {
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
	// OnApp runs on c's own delivery goroutine, unordered against the
	// disconnect report, so a message flushed after c ended can reach the
	// dispatcher before onDisconnect's task marks endedConn: c's own Done,
	// read at delivery like ws-mixer-go's mcpwarp session does, catches that.
	doneAtDelivery := c != nil && isDone(c.Done())
	t.disp.enqueue(func() {
		if t.superseded(c) {
			t.log.Debug("ignoring an app message from a superseded tunnel connection", "session", c.Session(), "op", msg.Op)
			return
		}
		if ended := c != nil && (c == t.endedConn || doneAtDelivery); ended && !appliesAfterEnd(msg.Op) {
			t.log.Debug("ignoring an app message from an ended tunnel connection", "session", c.Session(), "op", msg.Op)
			return
		}
		t.handleApp(msg)
	})
}

// isDone reports, without blocking, whether done is closed.
func isDone(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

// appliesAfterEnd reports whether op still applies when it arrives from a
// connection that has ended (ws-mixer-spec MCPWARP.md §2.7 Ended
// connection): a conflict is sticky and an error may be fatal; a
// registered, unregistered, disable or enable is stale.
func appliesAfterEnd(op string) bool {
	switch op {
	case appproto.OpRegistered, appproto.OpUnregistered, appproto.OpDisable, appproto.OpEnable:
		return false
	default:
		return true
	}
}

// superseded reports whether c is no longer the live connection, like
// ws-mixer-go's mcpwarp session.supersededLocked: onConnect has bound a newer
// one, or the client already switched to a newer one whose onConnect hasn't
// run yet. nil (an unbound, so newest, connection) never is, and neither is
// the last bound connection once the client has none: an app message that
// arrived before it ended is still owed.
func (t *Tunnel) superseded(c *wsmixer.Conn) bool {
	if c == nil {
		return false
	}
	if c != t.liveConn.Load() {
		return true
	}
	active := t.client.Conn()
	return active != nil && active != c
}

func (t *Tunnel) handleApp(msg *appproto.Inbound) {
	// Whatever msg moved a service to is reported once it is fully applied.
	defer t.publishServiceChanges()
	switch msg.Op {
	case appproto.OpUnknown:
		// DESIGN.md §8's "ignored-with-log": an unrecognized op, or a
		// recognized one at an unsupported v — never a protocol violation
		// on our end, just worth noting.
		t.log.Warn("ignoring unrecognized mcpwarp app message", "op", msg.UnknownOp, "v", msg.UnknownV)

	case appproto.OpRegistered:
		listed := make([]string, 0, len(msg.Services)+len(msg.Errors))
		for _, s := range msg.Services {
			listed = append(listed, s.Name)
		}
		var refused []appproto.ServiceError
		var retry []string
		for _, e := range msg.Errors {
			listed = append(listed, e.Name)
			if mcpwarp.IsTransientRegisterError(mcpwarp.RegisterErrorCode(e.Code)) {
				retry = append(retry, e.Name)
			} else {
				refused = append(refused, e)
			}
		}
		settled := t.frames.Reply(appproto.OpRegister, listed)
		// ws-mixer-spec MCPWARP.md §6A.7 Correlation: a name of the answered
		// register that the reply leaves out is a transient failure, and a
		// reply listing none of its names counts as all-transient (§2.3
		// Empty reply). One that starts a retry series that way shows
		// INTERNAL "unlisted in reply" (§6A.7's retry reason) while pending,
		// so its row says why, until the series' first transient error
		// replaces it; nothing is logged for it.
		listsNone := false
		if settled != nil {
			t.stopResend()
			listsNone = true
			for _, name := range settled.Names {
				if slices.Contains(listed, name) {
					listsNone = false
					continue
				}
				retry = append(retry, name)
				if t.retrying[name].code == "" {
					t.retrying[name] = retryMark{code: unlistedCode}
					t.note(name, unlistedCode, unlistedMessage)
				}
			}
		}

		// A "conflict" can land before the reply naming that same name: the
		// tunnel records this session's hold before replying, so a
		// concurrent take-over's frame can overtake the reply (an `e` reply
		// in flight can lose the same race). The take-over is the newer
		// fact, so neither a success nor an error for a taken-over name may
		// replace its TAKEN_OVER: a success would flip the row back to active
		// with a dead URL, a SERVER_DISABLED would read as resumable by a
		// dashboard enable that OpEnable ignores. The per-error log below
		// still reads the raw reply.
		taken := t.takenOverSnapshot()

		// A refused name never reaches ApplyRegistered, so without this the
		// registry has no record of it at all and publishServiceChanges
		// can't tell "rejected" apart from "not answered yet". A transient
		// code (§2.5) isn't a refusal: its name stays pending and is re-sent
		// below (§3.2 Transient per name).
		t.registry.ApplyRegisterErrors(withoutTaken(refused, taken, func(e appproto.ServiceError) string { return e.Name }), t.kindByName)
		for _, svcErr := range msg.Errors {
			// A transient error is reported once per retry series; its
			// repeats until the name gets a final answer only log at Debug.
			// The first one becomes the series' code (retryMark), replacing
			// the unlisted placeholder a series that began with the name
			// left out of a reply carries until then.
			if mcpwarp.IsTransientRegisterError(mcpwarp.RegisterErrorCode(svcErr.Code)) {
				m := t.retrying[svcErr.Name]
				if m.reported {
					t.log.Debug("register error; still retrying", "name", svcErr.Name, "code", svcErr.Code, "message", svcErr.Message)
					continue
				}
				m.reported = true
				m.code = svcErr.Code
				t.note(svcErr.Name, svcErr.Code, svcErr.Message)
				t.retrying[svcErr.Name] = m
			} else {
				t.note(svcErr.Name, serviceCode(svcErr.Code), svcErr.Message)
			}
			t.log.Warn("register error", "name", svcErr.Name, "code", svcErr.Code, "message", svcErr.Message)
			if hint := registerErrorHint(svcErr.Code, t.cfg.WebURL); hint != "" {
				t.log.Warn(hint)
			}
		}
		for _, e := range refused {
			delete(t.retrying, e.Name)
		}
		for _, s := range msg.Services {
			delete(t.retrying, s.Name)
			if !taken[s.Name] {
				t.held[s.Name] = true
			}
		}
		if len(msg.Services) > 0 {
			t.sawFirstRegistered = true
			t.applyRegisteredServices(withoutTaken(msg.Services, taken, func(s appproto.RegisteredService) string { return s.Name }))
		}

		// §3.2 Transient retry: a reply with any non-transient entry for the
		// frame resets the counter, and its transient names wait the n = 1
		// delay.
		n := 1
		if !listsNone && (len(msg.Services) > 0 || len(refused) > 0) {
			t.transientFailures = 0
		} else if len(retry) > 0 {
			t.transientFailures++
			n = t.transientFailures
		}
		t.retryTransient(retry, appproto.ResendDelay(n))
		t.pump()
		t.checkFatal()

	case appproto.OpUnregistered:
		t.log.Debug("unregistered", "services", msg.UnregisteredServices)
		// §3.2 Transient retry: an "unregistered" is a successful reply.
		t.transientFailures = 0
		names := make([]string, len(msg.UnregisteredServices))
		for i, s := range msg.UnregisteredServices {
			names[i] = s.Name
			delete(t.held, s.Name)
		}
		// §2.7: the settled unregister's names leave the held set whether
		// the reply listed them or not.
		if f := t.frames.Reply(appproto.OpUnregister, names); f != nil {
			for _, name := range f.Names {
				delete(t.held, name)
			}
			t.stopResend()
			t.pump()
		}

	case appproto.OpDisable:
		name, known := t.registry.ApplyDisable(msg.DisableID, msg.DisableReason)
		if known {
			t.log.Warn("server disabled by the tunnel", "name", name, "reason", msg.DisableReason)
			t.note(name, string(mcpwarp.CodeDisabled), msg.DisableReason)
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
		if t.isTakenOver(msg.EnableName) {
			// Another session serves this name now (see takenOver); only a
			// local `e` takes it back.
			t.log.Debug("enable for a name another session took over; ignoring", "name", msg.EnableName)
			return
		}
		t.registry.ApplyEnable(msg.EnableName, msg.EnableID)
		t.log.Info("server re-enabled by the tunnel", "name", msg.EnableName)
		if t.cfg.OnEnable != nil {
			go t.cfg.OnEnable(msg.EnableName)
		}
		// RegisterService clears localUnregistered too: a dashboard enable
		// for a name the user had also locally `d`-disabled would otherwise
		// get the child respawned/registered now but dropped again silently
		// on the very next reconnect.
		t.RegisterService(msg.EnableName)

	case appproto.OpConflict:
		// DESIGN.md §8: a newer session of the same account registered this
		// name, so the tunnel moved it there and reset its streams here;
		// every other name on this connection stays live. Keyed on name,
		// like enable (the frame carries it): the id is only cross-checked,
		// since ignoring a mismatch would leave the stale `active` row this
		// op exists to correct. The name reads taken_over (recorded in the
		// registry as a TAKEN_OVER rejection), which drops its URL from the
		// dashboard, and stays out of every register batch (takenOver) until
		// a local `e` reclaims it. A stdio child is left running, unlike a
		// dashboard disable: `e` brings the name back without a respawn.
		name := msg.ConflictName
		if !t.hasConfiguredService(name) {
			t.log.Debug("conflict for a name this connection doesn't hold; ignoring", "name", name, "id", msg.ConflictID)
			return
		}
		// ws-mixer-spec MCPWARP.md §3.3/§2.7: only a name this connection
		// holds can be taken from it. One only in an outstanding register
		// gets its outcome from that register's reply.
		if !t.held[name] {
			if f := t.frames.Outstanding(); f != nil && f.Op == appproto.OpRegister && slices.Contains(f.Names, name) {
				t.log.Debug("conflict for a name only in an outstanding register; its reply decides", "name", name, "id", msg.ConflictID)
			} else {
				t.log.Debug("conflict for a name this connection doesn't hold; ignoring", "name", name, "id", msg.ConflictID)
			}
			return
		}
		delete(t.held, name)
		if entry, ok := t.registry.Get(name); ok && entry.ID != msg.ConflictID {
			t.log.Warn("conflict id differs from this name's registered id; applying it by name", "name", name, "id", msg.ConflictID, "registeredId", entry.ID)
		}
		t.mu.Lock()
		t.takenOver[name] = true
		t.mu.Unlock()
		delete(t.retrying, name)
		code := string(mcpwarp.CodeTakenOver)
		t.registry.ApplyRegisterErrors([]appproto.ServiceError{{Name: name, Code: code, Message: msg.ConflictReason}}, t.kindByName)
		t.note(name, code, msg.ConflictReason)
		t.log.Warn("server taken over by another mcpwarp session", "name", name, "id", msg.ConflictID, "reason", msg.ConflictReason)
		t.log.Warn(takenOverHint)

	case appproto.OpError:
		if msg.ErrorCode == "UNSUPPORTED_VERSION" {
			t.log.Error("this mcpwarp CLI speaks a protocol version the tunnel no longer supports; upgrade mcpwarp")
			// See checkFatal: must not run synchronously on this dispatcher
			// goroutine.
			go t.cfg.FatalExit(1)
			return
		}
		if msg.ErrorCode == "CONNECTION_LIMIT" {
			message := connectionLimitMessage(msg.ErrorMessage)
			t.log.Warn("too many mcpwarp agents connected for this account; "+connectionLimitHint, "message", message)
			t.publishConnLimitError(eventbus.AppError{Code: msg.ErrorCode, Message: message, Hint: connectionLimitHint})
			return
		}
		if msg.ErrorCode == "OVERLOADED" {
			// ws-mixer-spec MCPWARP.md §2.6 OVERLOADED re-send: the
			// outstanding frame was dropped unprocessed and goes out again
			// after its own backoff — unless it is a register while closing,
			// which is abandoned (§3.2 Unregister) and frees the slot. Only
			// the first OVERLOADED of a frame is reported; the re-sends are
			// automatic, so its repeats only log at Debug.
			if t.overloadedReported {
				t.log.Debug(msg.ErrorMessage, "code", msg.ErrorCode)
			} else {
				t.overloadedReported = true
				t.publish(eventbus.AppError{Code: msg.ErrorCode, Message: msg.ErrorMessage, Hint: overloadedHint})
				t.log.Warn(msg.ErrorMessage, "code", msg.ErrorCode)
			}
			if delay, ok := t.frames.Overloaded(); ok {
				t.armResend(delay)
			} else {
				t.pump()
			}
			return
		}
		// Any other code answers the outstanding frame (§2.6 One in flight;
		// §6A.6 G7): a register's names are not registered on this
		// connection and nothing is re-sent, so each is recorded rejected
		// with the code rather than left pending on a reply that never
		// comes, and reported per name (publishServiceChanges) rather than
		// as an AppError. A taken-over name keeps its TAKEN_OVER;
		// ApplyRegisterErrors skips a locally disabled one likewise.
		// TOO_MANY_SERVICES is the tunnel refusing a register naming more
		// services than its cap as a whole, with no "registered" reply
		// (agentconn.go's maxServicesPerRegister).
		hint := registerErrorHint(msg.ErrorCode, t.cfg.WebURL)
		switch msg.ErrorCode {
		case "TOO_MANY_SERVICES":
			hint = tooManyServicesHint
			t.log.Warn(msg.ErrorMessage, "code", msg.ErrorCode)
		case "UNKNOWN_OP", "BAD_REQUEST":
			t.log.Warn("protocol mismatch — upgrade mcpwarp", "code", msg.ErrorCode)
		default:
			t.log.Warn(msg.ErrorMessage, "code", msg.ErrorCode)
		}
		if hint != "" {
			t.log.Warn(hint)
		}
		f := t.frames.FrameError()
		if f != nil {
			t.stopResend()
		}
		switch {
		case f != nil && f.Op == appproto.OpRegister:
			errs := make([]appproto.ServiceError, 0, len(f.Names))
			for _, name := range f.Names {
				delete(t.retrying, name)
				if !t.isTakenOver(name) {
					errs = append(errs, appproto.ServiceError{Name: name, Code: msg.ErrorCode, Message: msg.ErrorMessage})
					t.note(name, msg.ErrorCode, msg.ErrorMessage)
				}
			}
			t.registry.ApplyRegisterErrors(errs, t.kindByName)
		case f != nil:
			// §6A.7 Correlation: settled like an "unregistered" would be.
			t.log.Warn("an error op answered an unregister; treating it as unregistered", "names", f.Names, "code", msg.ErrorCode)
			for _, name := range f.Names {
				delete(t.held, name)
			}
			t.publish(eventbus.AppError{Code: msg.ErrorCode, Message: msg.ErrorMessage, Hint: hint})
		default:
			t.publish(eventbus.AppError{Code: msg.ErrorCode, Message: msg.ErrorMessage, Hint: hint})
		}
		t.pump()
		t.checkFatal()
	}
}

// registerErrorHint returns the code-specific next step for a "registered"
// reply's per-service error, or "" when the code has none — the single
// source both the log line and the ServiceChanged event's Hint are drawn
// from.
func registerErrorHint(code, webURL string) string {
	switch code {
	case "QUOTA_EXCEEDED":
		return fmt.Sprintf("upgrade your plan at %s/settings to add more servers", webURL)
	case "CONFLICT":
		return "a server with this name is already registered — rename it, or check its kind"
	case "SERVER_DISABLED":
		return "this server was disabled in the dashboard — waiting, toggle it on to resume"
	case "INVALID_NAME":
		return "fix the name in your config — it is the public URL slug and must match [a-z0-9]([a-z0-9-]*[a-z0-9])? (max 30 chars)"
	case "USERNAME_REQUIRED":
		return fmt.Sprintf("sign in once at %s to choose a username, then run `mcpwarp up` again", webURL)
	default:
		return ""
	}
}

// eventbus.ServiceChanged's State values.
const (
	statePending   = "pending"
	stateActive    = "active"
	stateRejected  = "rejected"
	stateDisabled  = "disabled"
	stateTakenOver = "taken_over"
)

// serviceCode maps a register error's wire code to the ServiceChanged Code
// it is reported under: SERVER_DISABLED is the dashboard's disable, DISABLED
// (ws-mixer-spec MCPWARP.md §6A.4), like a "disable" op; every other code
// as received.
func serviceCode(wireCode string) string {
	if wireCode == "SERVER_DISABLED" {
		return string(mcpwarp.CodeDisabled)
	}
	return wireCode
}

// serviceHint is the ServiceChanged Hint for code: a register error's own
// hint, the whole-frame TOO_MANY_SERVICES one, the take-over one, or for a
// dashboard disable SERVER_DISABLED's.
func (t *Tunnel) serviceHint(code string) string {
	switch code {
	case "TOO_MANY_SERVICES":
		return tooManyServicesHint
	case string(mcpwarp.CodeTakenOver):
		return takenOverHint
	case string(mcpwarp.CodeDisabled):
		return registerErrorHint("SERVER_DISABLED", t.cfg.WebURL)
	default:
		return registerErrorHint(code, t.cfg.WebURL)
	}
}

// note records message as the edge's text for name's code (see notes).
func (t *Tunnel) note(name, code, message string) {
	if t.notes[name] == nil {
		t.notes[name] = make(map[string]string)
	}
	t.notes[name][code] = message
}

// serviceState is name's registration state as a ServiceChanged, from its
// registry row and whether it is taken over, first match wins
// (ws-mixer-spec MCPWARP.md §6A.5): a local `d`; a take-over; a register
// outstanding, queued or scheduled for a transient retry (pending, carrying
// the series' first code, see retryMark); SERVER_DISABLED (disabled,
// DISABLED); any other refusal (rejected with its code); a dashboard
// disable (disabled, DISABLED); a success (active, with its URL); else
// pending.
func (t *Tunnel) serviceState(name, kind string, row registry.Row, takenOver bool) eventbus.ServiceChanged {
	evt := eventbus.ServiceChanged{Name: name, Kind: kind}
	switch {
	case row.LocallyDisabled:
		evt.State = stateDisabled
	case takenOver:
		evt.State, evt.Code = stateTakenOver, string(mcpwarp.CodeTakenOver)
	case row.Reregistering:
		evt.State, evt.Code = statePending, t.retrying[name].code
	case row.Rejected == "SERVER_DISABLED":
		evt.State, evt.Code = stateDisabled, serviceCode(row.Rejected)
	case row.Rejected != "":
		evt.State, evt.Code = stateRejected, row.Rejected
	case row.Disabled:
		evt.State, evt.Code = stateDisabled, string(mcpwarp.CodeDisabled)
	case row.URL != "":
		evt.State, evt.URL = stateActive, row.URL
	default:
		evt.State = statePending
	}
	if evt.Code != "" {
		evt.Message = t.notes[name][evt.Code]
		evt.Hint = t.serviceHint(evt.Code)
	}
	return evt
}

// publishServiceChanges publishes an eventbus.ServiceChanged for every
// configured service, in config order, whose serviceState differs from the
// one last published for it. Called on the dispatcher goroutine after
// whatever can move a state: a welcome's register, an app message, a local
// `d`/`e`.
func (t *Tunnel) publishServiceChanges() {
	rows := make(map[string]registry.Row, len(t.cfg.Services))
	for _, r := range t.registry.Rows() {
		rows[r.Name] = r
	}
	taken := t.takenOverSnapshot()
	for _, s := range t.cfg.Services {
		evt := t.serviceState(s.Name, s.Kind, rows[s.Name], taken[s.Name])
		if t.published[s.Name] == evt {
			continue
		}
		t.published[s.Name] = evt
		t.publish(evt)
	}
}

// applyRegisteredServices applies a "registered" reply's successes (taken-
// over names already left out) and runs what they unblock.
func (t *Tunnel) applyRegisteredServices(services []appproto.RegisteredService) {
	if len(services) == 0 {
		return
	}
	result := t.registry.ApplyRegistered(services, t.kindByName)
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
		t.note(resolved.Name, string(mcpwarp.CodeDisabled), resolved.Reason)
		if t.cfg.OnDisable != nil {
			go t.cfg.OnDisable(resolved.Name, resolved.ID, resolved.Reason)
		}
	}
}

// currentRegisterNames lists, in config order, the names this connect or
// reconnect registers: every configured one except those in
// localUnregistered (a local `d` keypress not yet followed by `e`) or
// takenOver (a "conflict" not yet followed by `e`) — see those fields'
// docs. Empty when every one is left out: no frame is built for no names,
// since neither this CLI nor Node's ever sends a register with an empty
// services list, and the server may bounce an empty batch as BAD_REQUEST,
// which handleApp would then log as a protocol mismatch. Each name listed is
// marked re-registering, so it reads pending until its reply (ws-mixer-spec
// MCPWARP.md §6A.5: a welcome's re-register moves every name it carries to
// pending) — under t.mu, like retryTransient, so a concurrent local `d`
// can't be undone by the mark.
func (t *Tunnel) currentRegisterNames() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	names := make([]string, 0, len(t.cfg.Services))
	for _, s := range t.cfg.Services {
		if !t.localUnregistered[s.Name] && !t.takenOver[s.Name] {
			t.registry.MarkReregistering(s.Name, s.Kind)
			names = append(names, s.Name)
		}
	}
	return names
}

// isTakenOver reports whether name is in takenOver.
func (t *Tunnel) isTakenOver(name string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.takenOver[name]
}

// takenOverSnapshot copies takenOver under t.mu, so one "registered" reply
// is filtered against a single view of it.
func (t *Tunnel) takenOverSnapshot() map[string]bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	snap := make(map[string]bool, len(t.takenOver))
	for name := range t.takenOver {
		snap[name] = true
	}
	return snap
}

// withoutTaken returns items minus every one whose name (per nameOf) is in
// taken; items itself when taken is empty.
func withoutTaken[T any](items []T, taken map[string]bool, nameOf func(T) string) []T {
	if len(taken) == 0 {
		return items
	}
	kept := make([]T, 0, len(items))
	for _, it := range items {
		if !taken[nameOf(it)] {
			kept = append(kept, it)
		}
	}
	return kept
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

// RegisterService re-registers a single configured service, for a
// caller-driven "enable" (a local `e` keypress/Controller.Enable, DESIGN.md
// §9) or a dashboard one (OpEnable) — the tunnel strips its own routing
// state for a disabled service, so un-disabling locally isn't enough.
// Always clears name from localUnregistered and takenOver first, even with
// no live connection (edge case: `e` pressed while disconnected must still
// make the next welcome's register batch include name again) — the
// register itself is then left to that welcome. Clearing takenOver is the
// one way back from a "conflict": OpEnable skips a taken-over name before
// reaching here, so only a local `e` reclaims it (and takes it from the
// other session).
//
// The registry is told too (MarkReregistering), so the dashboard shows the
// name pending until the reply rather than still disabled — only for a
// name this connection actually holds, so a stray enable can't add a row.
func (t *Tunnel) RegisterService(name string) {
	t.mu.Lock()
	delete(t.localUnregistered, name)
	delete(t.takenOver, name)
	t.mu.Unlock()
	kind, ok := t.kindByName[name]
	if !ok {
		return
	}
	t.registry.MarkReregistering(name, kind)
	t.queueFrame(appproto.OpRegister, name)
}

// UnregisterService sends "unregister" for a single configured service —
// the exported counterpart of the caller-driven `d` keypress/
// Controller.Disable (DESIGN.md §9). It also flips that entry's registry
// status to disabled locally (if known) so relay/registry.IsDisabled 503s
// the service immediately rather than waiting on a round trip through the
// tunnel's own "disable" reply, and marks name locally unregistered so a
// later reconnect's register batch excludes it (see localUnregistered's
// doc) until a matching RegisterService. Best-effort like every other app
// send: a failure to actually reach the tunnel just logs. The registry's
// MarkLocallyDisabled covers what ApplyDisable can't: a name with no entry
// yet (pending, or rejected on its first register) still reads disabled,
// and a reply already in flight for it doesn't land as active.
func (t *Tunnel) UnregisterService(name string) error {
	t.mu.Lock()
	t.localUnregistered[name] = true
	t.mu.Unlock()
	if kind, ok := t.kindByName[name]; ok {
		t.registry.MarkLocallyDisabled(name, kind)
	}
	if entry, ok := t.registry.Get(name); ok {
		t.registry.ApplyDisable(entry.ID, "unregistered locally")
	}
	t.queueFrame(appproto.OpUnregister, name)
	return nil
}

// queueFrame hands op for names, a local `d`/`e` (or a dashboard enable),
// to frames on the dispatcher goroutine, behind whatever is already
// outstanding (ws-mixer-spec MCPWARP.md §2.6 One in flight). Either ends
// the names' transient retry series (retrying), and the state the caller
// already marked in the registry (pending, disabled) is published. With no
// live connection nothing is queued: the next welcome registers the current
// list anyway, and a new connection holds nothing to unregister.
func (t *Tunnel) queueFrame(op string, names ...string) {
	live := t.client.Conn() != nil
	if !live {
		t.log.Debug("no tunnel connection; leaving it to the next welcome", "op", op, "names", names)
	}
	t.disp.enqueue(func() {
		for _, name := range names {
			delete(t.retrying, name)
		}
		t.publishServiceChanges()
		if !live || !t.framesLive() || (op == appproto.OpRegister && t.closing) {
			return
		}
		t.frames.Push(op, names...)
		t.pump()
	})
}

// framesLive reports whether frames has a connection to go out on: the one
// startFrames last began, or a current one whose onConnect hasn't run yet
// (its startFrames then sends what is queued).
func (t *Tunnel) framesLive() bool {
	return t.framesConn != nil || t.client.Conn() != nil
}

// startFrames begins c's frames on its welcome: whatever was outstanding on
// the previous connection is dropped, and the current register list goes
// out first, in batches, ahead of any intent still queued (§3.2 Register on
// every welcome). A scheduled transient retry is dropped too: this
// register covers it.
func (t *Tunnel) startFrames(c *wsmixer.Conn) {
	if t.stopped {
		return
	}
	t.stopResend()
	t.cancelRetries()
	t.framesConn = c
	t.held = make(map[string]bool)
	var names []string
	if !t.closing {
		names = t.currentRegisterNames()
	}
	t.frames.Restart(names)
	t.pump()
}

// stopFrames is Close's last word on frames: nothing is outstanding or
// scheduled afterwards and no frame is written again, not even for an
// onConnect the SDK had already queued, so writeFrame can no longer add to
// sendWG.
func (t *Tunnel) stopFrames() {
	t.stopped = true
	t.closing = true
	t.stopResend()
	t.cancelRetries()
	t.frames.Abandon()
	t.framesConn = nil
	t.releaseIdleWaiters()
}

// endFrames drops the outstanding frame and every scheduled re-send once
// the connection has ended (§3.2 Transient retry: a disconnect cancels a
// pending retry), and ends Unregister's wait.
func (t *Tunnel) endFrames() {
	t.stopResend()
	t.cancelRetries()
	t.frames.Abandon()
	t.framesConn = nil
	t.releaseIdleWaiters()
}

// pump writes the next queued frame once the slot is free, and ends
// Unregister's wait once nothing is left.
func (t *Tunnel) pump() {
	if t.stopped || t.framesConn == nil {
		return
	}
	if f := t.frames.Next(); f != nil {
		t.writeFrame(f)
		return
	}
	if t.frames.Idle() {
		t.releaseIdleWaiters()
	}
}

// appSendTimeout bounds one frame's SendApp — short enough that a send on
// a connection that's silently died doesn't hang the goroutine writeFrame
// starts forever.
const appSendTimeout = 10 * time.Second

// writeFrame sends f (the outstanding frame, or the closing unregister
// beside it) on framesConn from its own goroutine: SendApp can block on a
// full control queue, which must never happen on this package's single
// dispatcher goroutine. f is encoded here first, since Push may change a
// paused frame's names later. A failed write frees f's slot, as no reply
// can follow.
func (t *Tunnel) writeFrame(f *appproto.Frame) {
	if t.stopped || t.framesConn == nil {
		return
	}
	conn := t.framesConn
	body := t.encodeFrame(f)
	t.sendWG.Add(1)
	go func() {
		defer t.sendWG.Done()
		ctx, cancel := context.WithTimeout(context.Background(), appSendTimeout)
		defer cancel()
		if err := conn.SendApp(ctx, body); err != nil {
			t.disp.enqueue(func() { t.frameWriteFailed(conn, f, err) })
		}
	}()
}

func (t *Tunnel) frameWriteFailed(conn *wsmixer.Conn, f *appproto.Frame, err error) {
	if f.Op == appproto.OpRegister {
		t.log.Warn("failed to send register", "names", f.Names, "err", err)
	} else {
		t.log.Debug("failed to send unregister", "names", f.Names, "err", err)
	}
	if conn != t.framesConn || !t.frames.Fail(f) {
		return
	}
	t.overloadedReported = false
	t.pump()
}

func (t *Tunnel) encodeFrame(f *appproto.Frame) map[string]any {
	if f.Op == appproto.OpUnregister {
		services := make([]appproto.UnregisterService, len(f.Names))
		for i, name := range f.Names {
			services[i] = appproto.UnregisterService{Name: name}
		}
		return appproto.EncodeUnregister(services)
	}
	services := make([]appproto.RegisterService, len(f.Names))
	for i, name := range f.Names {
		services[i] = appproto.RegisterService{Name: name, Kind: t.kindByName[name]}
	}
	return appproto.EncodeRegister(services)
}

// armResend schedules the outstanding frame's OVERLOADED re-send (§2.6).
func (t *Tunnel) armResend(delay time.Duration) {
	f := t.frames.Outstanding()
	t.log.Info("tunnel overloaded; re-sending after a pause", "op", f.Op, "names", f.Names, "delay", delay)
	t.resendStop = t.cfg.afterFunc(delay, func() {
		t.disp.enqueue(func() { t.resendDue(f) })
	})
}

// resendDue re-sends f once its pause is over, unless a reply settled it
// meanwhile (a stale timer sends nothing).
func (t *Tunnel) resendDue(f *appproto.Frame) {
	if t.frames.Outstanding() != f || !t.frames.Paused() {
		return
	}
	t.resendStop = nil
	if next := t.frames.Resend(); next != nil {
		t.writeFrame(next)
		return
	}
	t.pump()
}

// stopResend cancels a scheduled OVERLOADED re-send. It runs whenever the
// outstanding frame is settled or dropped, which also ends the OVERLOADED
// series overloadedReported tracks.
func (t *Tunnel) stopResend() {
	t.overloadedReported = false
	if t.resendStop != nil {
		t.resendStop()
		t.resendStop = nil
	}
}

// retryMark is one name's place in a transient retry series (retrying).
type retryMark struct {
	// code is what the series' pending ServiceChanged carries: its first
	// transient error's code, or unlistedCode while a name a reply left out
	// has had no transient error yet.
	code string
	// reported: the series' first transient error has been logged.
	reported bool
}

// unlistedCode and unlistedMessage are what a name left out of the reply
// to its register shows while pending (ws-mixer-spec MCPWARP.md §6A.7's
// "register failed: INTERNAL: unlisted in reply").
const (
	unlistedCode    = string(mcpwarp.CodeInternal)
	unlistedMessage = "unlisted in reply"
)

// transientRetry is one scheduled re-send of a reply's transient names.
type transientRetry struct {
	names []string
	stop  func() bool
}

// retryTransient schedules a register for the names a reply failed
// transiently (§3.2 Transient per name) after delay. Each stays pending
// meanwhile (§6A.4: transient codes never reach a service), unless it is
// unconfigured, taken over or locally disabled, which drops it.
func (t *Tunnel) retryTransient(names []string, delay time.Duration) {
	if t.closing || len(names) == 0 {
		return
	}
	kept := make([]string, 0, len(names))
	t.mu.Lock()
	for _, name := range names {
		kind, ok := t.kindByName[name]
		if !ok || t.takenOver[name] || t.localUnregistered[name] || slices.Contains(kept, name) {
			continue
		}
		t.registry.MarkReregistering(name, kind)
		kept = append(kept, name)
	}
	t.mu.Unlock()
	if len(kept) == 0 {
		return
	}
	t.log.Debug("re-sending register after a transient error", "names", kept, "delay", delay)
	r := &transientRetry{names: kept}
	r.stop = t.cfg.afterFunc(delay, func() {
		t.disp.enqueue(func() { t.retryDue(r) })
	})
	t.retries[r] = true
}

func (t *Tunnel) retryDue(r *transientRetry) {
	if !t.retries[r] {
		return
	}
	delete(t.retries, r)
	if t.closing || t.framesConn == nil {
		return
	}
	t.mu.Lock()
	names := slices.DeleteFunc(slices.Clone(r.names), func(name string) bool {
		return t.takenOver[name] || t.localUnregistered[name]
	})
	t.mu.Unlock()
	if len(names) > 0 {
		t.frames.Push(appproto.OpRegister, names...)
	}
	t.pump()
	t.checkFatal()
}

func (t *Tunnel) cancelRetries() {
	for r := range t.retries {
		r.stop()
		delete(t.retries, r)
	}
}

func (t *Tunnel) releaseIdleWaiters() {
	for _, w := range t.idleWaiters {
		close(w)
	}
	t.idleWaiters = nil
}

// checkFatal is ws-mixer-spec MCPWARP.md §6A.6 G5, judged on the overall
// service state rather than one reply: exit 1 only when this process has
// never registered anything, nothing is outstanding, queued or scheduled
// for a re-send, and every configured service not taken over or locally
// disabled ended refused. A SERVER_DISABLED service keeps the process up,
// since its dashboard "enable" arrives on this connection, and a transient
// code never counts (§3.2 Transient per name). Never while closing: a
// refusal arriving during shutdown must not turn it into an exit 1.
func (t *Tunnel) checkFatal() {
	if t.closing || t.sawFirstRegistered || t.fatalSent || !t.frames.Idle() || len(t.retries) > 0 {
		return
	}
	rows := make(map[string]registry.Row)
	for _, r := range t.registry.Rows() {
		rows[r.Name] = r
	}
	t.mu.Lock()
	refused := 0
	settledAll := true
	for _, s := range t.cfg.Services {
		if t.takenOver[s.Name] || t.localUnregistered[s.Name] {
			continue
		}
		r := rows[s.Name]
		if r.Rejected == "" || r.Rejected == "SERVER_DISABLED" || r.Reregistering {
			settledAll = false
			break
		}
		refused++
	}
	t.mu.Unlock()
	if !settledAll || refused == 0 {
		return
	}
	t.fatalSent = true
	t.log.Error("No service could be registered.", "code", "INTERNAL")
	// FatalExit runs shutdown.FatalExit -> Tunnel.Close(ctx), which waits on
	// this same dispatcher goroutine to drain — calling it synchronously
	// here would self-deadlock. Node's counterpart is likewise
	// fire-and-forget ("void fatalExit(1)").
	go t.cfg.FatalExit(1)
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
			OnPathRefused: func(service, path, publicURL string) {
				t.publish(eventbus.AppError{
					Code:    "PATH_REFUSED",
					Service: service,
					Message: fmt.Sprintf("request for %s refused: only /mcp is forwarded", path),
					Hint:    "point the MCP client at " + publicURL,
				})
			},
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
	// A connected-phase end is the connection onConnect last bound: both
	// run in order on the SDK's callback goroutine, so liveConn is read here
	// rather than on the dispatcher, where a later onConnect may already
	// have moved it. An end in the dial or handshake phase never reached
	// onConnect and says nothing about liveConn.
	var ended *wsmixer.Conn
	if reason.Phase == wsmixer.PhaseConnected && !reason.Superseded {
		ended = t.liveConn.Load()
	}
	t.disp.enqueue(func() {
		// ws-mixer-go v0.9.0's drain hand-over (its DECISIONS.md
		// D-2026-10-14-03): a drained connection keeps serving its in-flight
		// streams past its replacement's welcome and its end is reported as
		// Superseded, possibly after the replacement's onConnect has
		// published "connected" and re-registered. The replacement is live
		// either way, so this end is no loss and must not publish
		// "disconnected" over it.
		if reason.Superseded {
			t.log.Debug("drained tunnel connection retired; its replacement is live", "message", reason.Message)
			return
		}
		if ended != nil {
			t.endedConn = ended
		}
		t.endFrames()
		// Our own shutdown (Unregister, then Close) ends the connection with
		// a NO_ERROR close: nothing went wrong, so no Reason.
		why := ""
		if !t.closing && !t.closeStarted.Load() {
			why = disconnectReasonText(reason)
		}
		t.publish(eventbus.ConnStateChanged{State: "disconnected", Reason: why})
		if !reason.Fatal {
			text := reason.CloseReason
			if text == "" {
				text = reason.Message
			}
			if reason.WSCode == connectionLimitWSCode && strings.HasPrefix(text, connectionLimitPrefix) {
				message := connectionLimitMessage(text)
				t.log.Warn("too many mcpwarp agents connected for this account; "+connectionLimitHint, "message", message)
				t.publishConnLimitError(eventbus.AppError{Code: "CONNECTION_LIMIT", Message: message, Hint: connectionLimitHint})
				return
			}
			// A retryable token refresh failure reaches here as a non-fatal
			// dial failure (the provider wrapped it in
			// wsmixer.ErrTokenUnavailable); the SDK redials with normal backoff.
			if reason.Phase == wsmixer.PhaseDial && errors.Is(reason.Cause, wsmixer.ErrTokenUnavailable) {
				t.log.Warn("could not refresh the session token; retrying", "err", reason.Cause)
				return
			}
			t.log.Info("tunnel disconnected; SDK reconnecting", "phase", reason.Phase, "message", reason.Message)
			return
		}
		// classifyFatal (client.ts:140-160): everything that reaches here is
		// terminal, including any token provider error not marked
		// wsmixer.ErrTokenUnavailable — the SDK has already closed the
		// client and never retries it, so exit.
		message := classifyFatalMessage(t.cfg.URL, reason)
		t.log.Error(message, "wsCode", reason.WSCode, "httpStatus", reason.HTTPStatus)
		// See checkFatal: FatalExit -> Tunnel.Close(ctx) waits on this
		// dispatcher goroutine, so it must not run synchronously here.
		go t.cfg.FatalExit(1)
	})
}

// disconnectReasonText is the short Reason a "disconnected"
// ConnStateChanged carries for the TUI header ("DISCONNECTED (…)",
// DESIGN.md §9), drawn from the same DisconnectReason fields onDisconnect
// and classifyFatalMessage classify it by: a fatal end by its auth/HTTP
// facts (the full classifyFatalMessage text goes to the log); a
// CONNECTION_LIMIT close; a retryable token refresh failure; an HTTP
// upgrade status; a clean close (NO_ERROR, or a bare 1000) as closed by the
// tunnel; any other ws-mixer error code by its name; any other close frame
// as closed by the tunnel; no close frame at all (or an abnormal 1006) as
// a network error. onDisconnect skips it for this process's own shutdown.
func disconnectReasonText(r wsmixer.DisconnectReason) string {
	text := r.CloseReason
	if text == "" {
		text = r.Message
	}
	var notLoggedIn *auth.NotLoggedInError
	var sessionExpired *auth.SessionExpiredError
	switch {
	case r.Fatal && errors.As(r.Cause, &notLoggedIn):
		return "not logged in"
	case r.Fatal && (errors.As(r.Cause, &sessionExpired) || r.HTTPStatus == 401 || r.HTTPStatus == 403 ||
		r.ErrorName == "UNAUTHORIZED" || r.WSCode == unauthorizedWSCode):
		return "session expired"
	case r.Fatal && r.HTTPStatus == 404:
		return "tunnel URL not found"
	case r.Fatal:
		return "connection failed"
	case r.WSCode == connectionLimitWSCode && strings.HasPrefix(text, connectionLimitPrefix):
		return "connection limit"
	case r.Phase == wsmixer.PhaseDial && errors.Is(r.Cause, wsmixer.ErrTokenUnavailable):
		return "token refresh failed"
	case r.HTTPStatus != 0:
		return fmt.Sprintf("HTTP %d", r.HTTPStatus)
	case r.HasErrorCode && r.ErrorCode == wsmixer.NoError, !r.HasErrorCode && r.WSCode == normalClosureWSCode:
		return "closed by the tunnel"
	case r.HasErrorCode:
		return strings.ToLower(strings.ReplaceAll(r.ErrorName, "_", " "))
	case r.WSCode != 0 && r.WSCode != abnormalClosureWSCode:
		return "closed by the tunnel"
	default:
		return "network error"
	}
}

// normalClosureWSCode and abnormalClosureWSCode are RFC 6455's 1000 (a
// clean close) and 1006 (the connection dropped without a close frame).
const (
	normalClosureWSCode   = 1000
	abnormalClosureWSCode = 1006
)

// unauthorizedWSCode is the wsmixer close code for wsmixer.UnauthorizedCode
// (4000 + 0x0b), used the same way Node client.ts's UNAUTHORIZED_WS_CODE is.
const unauthorizedWSCode = 4011

// connectionLimitWSCode is the WS close code the tunnel server uses for its
// account connection cap: ws-mixer's application-level close
// (wsmixer.ApplicationCloseCode, 0x0e), distinguished only by
// connectionLimitPrefix on the close reason — a bare 4014 without that
// prefix is ws-mixer's own application-level close for something else and
// must not be treated as a connection-limit close. 4009
// (wsmixer.EnhanceYourCalm) is ws-mixer's own rate-limit/oversize meaning
// and the tunnel never sends it for the connection cap.
var connectionLimitWSCode = wsmixer.ApplicationCloseCode.CloseCode()

// connectionLimitPrefix is the fixed lead-in of a CONNECTION_LIMIT close
// reason or app error message; only the prefix is a stable contract, the
// account's connection limit number after it is not.
const connectionLimitPrefix = "CONNECTION_LIMIT:"

// connectionLimitHint is the guidance surfaced for a CONNECTION_LIMIT close
// or app error, both in the log line and as the AppError's Hint. With
// ws-mixer-go v0.4.1 a connected-phase 4014 reconnects at random(0, cap)
// (this CLI's Cap is 30s), same as 4009 — see client_reconnect.go's
// scheduleReconnectAtCap. Recovery just needs one fewer live mcpwarp agent
// for this account.
const connectionLimitHint = "close another `mcpwarp up` and this one will reconnect"

// overloadedHint is the AppError Hint for the first OVERLOADED of a frame:
// nothing for the user to do, the frame goes out again on its own.
const overloadedHint = "tunnel busy; re-sending automatically"

// tooManyServicesHint is the guidance for a TOO_MANY_SERVICES app error.
// config.Validate already caps a config at config.MaxServers, so this only
// fires against a tunnel whose cap is lower than the CLI's — hence no
// number here; the tunnel's own message carries its real cap.
const tooManyServicesHint = "the tunnel refused this many servers in one register; remove some from the config"

// takenOverHint is the guidance for a "conflict" op: unlike a register
// reply's CONFLICT (registerErrorHint), the name is fine and its kind is
// irrelevant — another session simply holds it now.
const takenOverHint = "another `mcpwarp up` on this account now serves it; stop that one or rename this server, then press e (or restart this `mcpwarp up`)"

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

// isRetryableTokenFailure reports whether a TokenSource error is a
// temporary refresh failure, via the error's Retryable() bool (implemented by
// internal/auth.TokenRefreshError), mirroring Node client.ts's classifyFatal.
// Start's token provider wraps a match in wsmixer.ErrTokenUnavailable, the
// only signal the SDK reads; adding that marker here keeps internal/auth
// SDK-agnostic.
func isRetryableTokenFailure(cause error) bool {
	if cause == nil {
		return false
	}
	var retryable interface{ Retryable() bool }
	return errors.As(cause, &retryable) && retryable.Retryable()
}

// Unregister best-effort sends "unregister" for every configured service
// not taken over (§3.3 Per name: never for a taken-over name) and waits for
// its reply, budgeted to unregisterBudget (DESIGN.md §3's shutdown sequence
// step 1; ws-mixer-spec MCPWARP.md §3.2 Unregister). It goes through frames
// (FrameQueue.Close): a register not yet on the wire, queued or paused by
// OVERLOADED, is abandoned, no register goes out after this, and the
// unregister is written at once even beside a register still on the wire
// (§2.6 One in flight: only close() may), or behind an outstanding
// unregister. The wait also ends when the connection does. The write itself
// may outlive the budget (bounded by appSendTimeout, tracked by sendWG so
// Close still waits for it) — Unregister only stops waiting on it here.
func (t *Tunnel) Unregister(ctx context.Context) error {
	if t.client.Conn() == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, unregisterBudget)
	defer cancel()

	done := make(chan struct{})
	t.disp.enqueue(func() {
		t.closing = true
		t.cancelRetries()
		if t.frames.DropRegisters() {
			t.stopResend()
		}
		t.idleWaiters = append(t.idleWaiters, done)
		if !t.framesLive() {
			t.releaseIdleWaiters()
			return
		}
		t.mu.Lock()
		names := make([]string, 0, len(t.cfg.Services))
		for _, s := range t.cfg.Services {
			if !t.takenOver[s.Name] {
				names = append(names, s.Name)
			}
		}
		t.mu.Unlock()
		if f := t.frames.Close(names...); f != nil {
			t.writeFrame(f)
		}
		t.pump()
	})

	select {
	case <-done:
		return nil
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
	t.closeStarted.Store(true)
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

	// Stop frames first, on the dispatcher: once this has run nothing can
	// call writeFrame, so no sendWG.Add races the Wait below.
	framesStopped := make(chan struct{})
	t.disp.enqueue(func() {
		t.stopFrames()
		close(framesStopped)
	})
	select {
	case <-framesStopped:
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
