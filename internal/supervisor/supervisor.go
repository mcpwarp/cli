// Package supervisor restarts one bridge's stdio child with backoff,
// tracking a small state machine (healthy/restarting/failed/disabled/
// stopped). Ported from mcpwarp-cli's src/bridge/supervisor.ts.
//
// Deviation carried over from the Node source: base 1s, doubling, capped
// at 30s (not DESIGN.md §7's plain "base 500ms" prose) and "10 consecutive
// failures without a healthy-uptime reset" (not "max 10 per rolling
// 5 min") — the Node implementation's task spec, which this ports
// faithfully, took precedence there; see supervisor.ts's file header.
package supervisor

import (
	"context"
	"log/slog"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/mcpwarp/cli/internal/bridge"
	"github.com/mcpwarp/cli/internal/eventbus"
)

const (
	backoffBaseMS          = 1000
	backoffCapMS           = 30_000
	maxConsecutiveFailures = 10
	healthyResetMS         = 60_000
)

// State mirrors supervisor.ts's SupervisorState.
type State string

const (
	Healthy    State = "healthy"
	Restarting State = "restarting"
	Failed     State = "failed"
	Disabled   State = "disabled"
	Stopped    State = "stopped"
)

// SupervisedBridge is the subset of bridge.Bridge the supervisor drives —
// kept as an interface so unit tests can pass a stub instead of standing
// up a real HTTP server.
type SupervisedBridge interface {
	ReplaceChild(child *bridge.StdioChild)
	MarkFailed(reason string)
}

// Deps are the DI seams mirroring the Node port's fetchImpl/sleep/now/
// random/spawn: fakeable without a real clock, RNG, or process.
type Deps struct {
	// Spawn replaces bridge.NewStdioChild for restart spawns.
	Spawn func(spec bridge.SpawnSpec, log *slog.Logger, name string) (*bridge.StdioChild, error)
	Sleep func(d time.Duration)
	// Random returns a float in [0,1) — replaces math/rand for full-jitter
	// backoff.
	Random func() float64
	Now    func() time.Time
}

func defaultDeps() Deps {
	return Deps{
		Spawn: func(spec bridge.SpawnSpec, log *slog.Logger, name string) (*bridge.StdioChild, error) {
			return bridge.NewStdioChild(spec, log, name)
		},
		Sleep:  time.Sleep,
		Random: rand.Float64,
		Now:    time.Now,
	}
}

// Options configures a new Supervisor.
type Options struct {
	Name      string
	SpawnSpec bridge.SpawnSpec
	Log       *slog.Logger
	// Child is the already-spawned (and started) initial child — the
	// caller owns spawning the first one; the supervisor only spawns
	// replacements.
	Child  *bridge.StdioChild
	Bridge SupervisedBridge
	// Bus, if non-nil, receives eventbus.ServerStateChanged on every
	// transition (DESIGN.md §3).
	Bus *eventbus.Bus
}

// Supervisor restarts one bridge's stdio child with full-jitter backoff.
// Not safe to construct with a nil Options.Child/Options.Bridge.
type Supervisor struct {
	name      string
	spawnSpec bridge.SpawnSpec
	log       *slog.Logger
	br        SupervisedBridge
	bus       *eventbus.Bus

	spawn  func(spec bridge.SpawnSpec, log *slog.Logger, name string) (*bridge.StdioChild, error)
	sleep  func(d time.Duration)
	random func() float64
	now    func() time.Time

	mu                  sync.Mutex
	state               State
	current             *bridge.StdioChild
	consecutiveFailures int
	restarts            int
	lastSpawnAt         time.Time
	failedListeners     []func(reason string)
	restarting          chan struct{} // non-nil while a restart/enable is in flight
	terminating         chan struct{} // non-nil while disable()/stop() is in flight
	sleepCancel         chan struct{} // non-nil while a backoff sleep is in flight; closed to cut it short
}

// New constructs a Supervisor already wired to opts.Child. Pass a zero
// Deps{} to use real time/spawn/random.
func New(opts Options, deps Deps) *Supervisor {
	d := defaultDeps()
	merge(&d, deps)
	s := &Supervisor{
		name:      opts.Name,
		spawnSpec: opts.SpawnSpec,
		log:       opts.Log,
		br:        opts.Bridge,
		bus:       opts.Bus,
		spawn:     d.Spawn,
		sleep:     d.Sleep,
		random:    d.Random,
		now:       d.Now,
		state:     Healthy,
		current:   opts.Child,
	}
	s.lastSpawnAt = s.now()
	s.wire(opts.Child)
	return s
}

func merge(base *Deps, override Deps) {
	if override.Spawn != nil {
		base.Spawn = override.Spawn
	}
	if override.Sleep != nil {
		base.Sleep = override.Sleep
	}
	if override.Random != nil {
		base.Random = override.Random
	}
	if override.Now != nil {
		base.Now = override.Now
	}
}

// Name returns the service name this supervisor was constructed with.
func (s *Supervisor) Name() string {
	return s.name
}

// GetState returns the current state — test/inspection seam.
func (s *Supervisor) GetState() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// Restarts returns the current consecutive-restart count (for
// eventbus.ServerStateChanged / TUI display).
func (s *Supervisor) Restarts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.restarts
}

// OnFailed registers a callback fired exactly once, the moment the
// supervisor gives up restarting (10 consecutive failures).
func (s *Supervisor) OnFailed(fn func(reason string)) {
	s.mu.Lock()
	s.failedListeners = append(s.failedListeners, fn)
	s.mu.Unlock()
}

func (s *Supervisor) publish(state State) {
	if s.bus == nil {
		return
	}
	s.mu.Lock()
	restarts := s.restarts
	s.mu.Unlock()
	s.bus.Publish(eventbus.ServerStateChanged{Name: s.name, State: string(state), Restarts: restarts})
}

// Disable stops the child and marks the service disabled — the tunnel's
// remote disable{id,reason}. Not terminal: a later Enable respawns it.
func (s *Supervisor) Disable() {
	s.terminate(Disabled, context.Background())
}

// Stop stops the child for orderly shutdown and marks the supervisor
// stopped — genuinely terminal, unlike Disable. Equivalent to
// StopContext(context.Background()).
func (s *Supervisor) Stop() {
	s.terminate(Stopped, context.Background())
}

// StopContext is Stop, but returns as soon as ctx is done rather than
// waiting out the full in-flight-restart/child-close sequence — so a
// caller enforcing DESIGN.md §3's 5s shutdown deadline across several
// supervisors isn't held hostage by one that's mid backoff-restart. The
// child close this triggers still runs in the background to completion
// (bounded by its own internal ~2s grace timeout) even if StopContext
// itself returns early.
func (s *Supervisor) StopContext(ctx context.Context) {
	s.terminate(Stopped, ctx)
}

func (s *Supervisor) terminate(target State, ctx context.Context) {
	s.mu.Lock()
	s.state = target
	done := make(chan struct{})
	s.terminating = done
	restarting := s.restarting
	if s.sleepCancel != nil {
		close(s.sleepCancel)
		s.sleepCancel = nil
	}
	s.mu.Unlock()

	if restarting != nil {
		select {
		case <-restarting:
		case <-ctx.Done():
		}
	}
	s.mu.Lock()
	current := s.current
	s.mu.Unlock()

	closeDone := make(chan struct{})
	go func() {
		_ = current.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-ctx.Done():
	}

	s.mu.Lock()
	if s.terminating == done {
		s.terminating = nil
	}
	s.mu.Unlock()
	close(done)
	s.publish(target)
}

// Enable respawns the child immediately (no backoff) with fresh
// backoff/failure counters — the tunnel's remote enable{id}. A no-op
// unless the current state is Disabled or Failed.
func (s *Supervisor) Enable() {
	s.mu.Lock()
	if s.state != Disabled && s.state != Failed {
		state := s.state
		s.mu.Unlock()
		s.log.Debug("enable requested but service is not disabled/failed; ignoring", "name", s.name, "state", state)
		return
	}
	terminating := s.terminating
	s.mu.Unlock()

	if terminating != nil {
		<-terminating
		s.mu.Lock()
		if s.state != Disabled && s.state != Failed {
			state := s.state
			s.mu.Unlock()
			s.log.Debug("enable requested but a race left the service in a non-resurrectable state; ignoring", "name", s.name, "state", state)
			return
		}
		s.mu.Unlock()
	}

	s.mu.Lock()
	s.consecutiveFailures = 0
	s.state = Restarting
	done := make(chan struct{})
	s.restarting = done
	s.mu.Unlock()
	s.publish(Restarting)

	s.activate()

	s.mu.Lock()
	if s.restarting == done {
		s.restarting = nil
	}
	s.mu.Unlock()
	close(done)
}

// Restart stops the current child and immediately spawns a replacement —
// a manual "restart now" for M3B/M4 (e.g. a tunnel-driven restart
// command), distinct from Enable in two ways: it works from any
// non-terminal state (not just Disabled/Failed) and it does not reset
// consecutiveFailures, so it doesn't forgive an ongoing crash loop the
// way Enable's fresh-counters semantics do. A no-op once Disable()/Stop()
// has already made the supervisor terminal. Publishes
// ServerStateChanged restarting→healthy, same shape as Enable.
func (s *Supervisor) Restart() {
	s.mu.Lock()
	terminating := s.terminating
	restarting := s.restarting
	s.mu.Unlock()
	if terminating != nil {
		<-terminating
	}
	if restarting != nil {
		<-restarting
	}

	s.mu.Lock()
	if s.state == Disabled || s.state == Stopped {
		s.mu.Unlock()
		return
	}
	current := s.current
	// Clear s.current before closing it, mirroring activate()'s own
	// current-then-wire ordering: handleExit's staleness guard compares
	// an exiting child against s.current, so with current nil'd out here,
	// the exit this Close() below triggers is correctly treated as
	// already-superseded/inert rather than kicking off a second, backoff
	// driven restart racing this one.
	s.current = nil
	s.state = Restarting
	done := make(chan struct{})
	s.restarting = done
	s.mu.Unlock()
	s.publish(Restarting)

	_ = current.Close()
	s.activate()

	s.mu.Lock()
	if s.restarting == done {
		s.restarting = nil
	}
	s.mu.Unlock()
	close(done)
}

func (s *Supervisor) wire(child *bridge.StdioChild) {
	child.OnExit(func(info bridge.ExitInfo) {
		s.handleExit(child, info)
	})
}

func (s *Supervisor) handleExit(child *bridge.StdioChild, info bridge.ExitInfo) {
	s.mu.Lock()
	if child != nil && child != s.current {
		// Stale child's exit (already superseded) — inert.
		s.mu.Unlock()
		return
	}
	if s.state == Disabled || s.state == Failed || s.state == Stopped {
		s.mu.Unlock()
		return
	}

	healthyUptime := s.now().Sub(s.lastSpawnAt)
	if healthyUptime >= healthyResetMS*time.Millisecond {
		s.consecutiveFailures = 0
	}
	s.consecutiveFailures++

	if s.consecutiveFailures >= maxConsecutiveFailures {
		s.state = Failed
		failures := s.consecutiveFailures
		s.mu.Unlock()
		reason := s.name + ": giving up after too many consecutive crashes"
		s.log.Error(reason, "name", s.name, "code", info.Code, "signal", info.Signal, "failures", failures)
		s.br.MarkFailed(reason)
		s.mu.Lock()
		listeners := append([]func(string){}, s.failedListeners...)
		s.mu.Unlock()
		for _, fn := range listeners {
			fn(reason)
		}
		s.publish(Failed)
		return
	}

	delay := s.backoffDelayLocked()
	s.state = Restarting
	done := make(chan struct{})
	s.restarting = done
	s.mu.Unlock()
	s.publish(Restarting)

	s.log.Info(s.name+": exited, restarting", "name", s.name, "code", info.Code, "signal", info.Signal, "delayMs", delay.Milliseconds())

	go func() {
		s.restartAfter(delay)
		s.mu.Lock()
		if s.restarting == done {
			s.restarting = nil
		}
		s.mu.Unlock()
		close(done)
	}()
}

// backoffDelay: full-jitter exponential backoff, base 1s, doubling,
// capped at 30s, jitter uniform in [0, cap). consecutiveFailures is
// already incremented by the time this runs, so the first restart
// (failures=1) uses 2^0 = 1x base.
func (s *Supervisor) backoffDelay() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.backoffDelayLocked()
}

// backoffDelayLocked is backoffDelay for a caller that already holds s.mu
// (handleExit computes this mid-transition, without re-entering the lock).
func (s *Supervisor) backoffDelayLocked() time.Duration {
	expMS := math.Min(backoffCapMS, backoffBaseMS*math.Pow(2, float64(s.consecutiveFailures-1)))
	return time.Duration(s.random()*expMS) * time.Millisecond
}

// restartAfter sleeps delay, then activates a replacement child — unless
// a concurrent Disable()/Stop() cuts the sleep short via sleepCancel, in
// which case terminate() is already waiting and must not block for the
// rest of a backoff that can be up to 30s.
func (s *Supervisor) restartAfter(delay time.Duration) {
	cancel := make(chan struct{})
	s.mu.Lock()
	s.sleepCancel = cancel
	s.mu.Unlock()

	sleepDone := make(chan struct{})
	go func() {
		s.sleep(delay)
		close(sleepDone)
	}()

	select {
	case <-sleepDone:
	case <-cancel:
	}

	s.mu.Lock()
	if s.sleepCancel == cancel {
		s.sleepCancel = nil
	}
	state := s.state
	s.mu.Unlock()
	if state == Disabled || state == Stopped {
		return
	}
	s.activate()
}

// activate spawns a fresh child and, if nothing raced it to a terminal
// state meanwhile, hands it to the bridge as the new live child.
func (s *Supervisor) activate() {
	child, err := s.spawn(s.spawnSpec, s.log, s.name)
	if err != nil {
		s.log.Warn(s.name+": spawn failed", "name", s.name, "err", err)
		s.handleExit(nil, bridge.ExitInfo{Code: -1})
		return
	}

	s.mu.Lock()
	state := s.state
	s.mu.Unlock()
	if state == Disabled || state == Stopped {
		_ = child.Close()
		return
	}

	// Set s.current before wiring the exit listener, and hand the child to
	// the bridge before wiring it: handleExit's staleness guard compares
	// an exiting child against s.current, so setting current first means
	// an exit racing this handoff (including one that already happened)
	// is never misattributed to the stale previous child and dropped;
	// handing off to the bridge first means its own (fast, non-blocking)
	// exit-triggered rejection always beats the supervisor's (which may
	// block on eventbus.Publish) restart-state transition.
	s.mu.Lock()
	s.current = child
	s.mu.Unlock()
	s.br.ReplaceChild(child)
	s.wire(child)
	if child.HasExited() {
		// wire's OnExit already ran handleExit synchronously for an
		// already-exited child (with s.current correctly pointing at it).
		return
	}

	s.mu.Lock()
	if s.state == Disabled || s.state == Stopped {
		// A Disable()/Stop() call landed while we were blocked in
		// ReplaceChild above (terminate() waits on s.restarting before
		// reading s.current, so it hasn't closed this child yet) — don't
		// stomp its terminal state with an unconditional Healthy.
		s.mu.Unlock()
		_ = child.Close()
		return
	}
	s.lastSpawnAt = s.now()
	s.restarts++
	s.state = Healthy
	s.mu.Unlock()
	s.publish(Healthy)
}
