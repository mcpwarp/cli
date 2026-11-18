package bridge

import "fmt"

// The MCP version of one child process is decided by eraMachine. The bridge
// writes one server/discover at spawn; the answer fixes the process as v2 or
// v1 for its life. Only a process whose probe went unanswered within the
// probe timeout ("v1 for now") can still change: once, to v2, or to v1 for
// its life on a late answer that is not modern or on the first initialize
// the child accepts. Every event goes through apply, under Bridge.mu, and
// apply is pure: no I/O, goroutines, timers or clock reads. It returns the
// next state and the effects for the caller to carry out (applyEraLocked in
// era.go). Each process gets a fresh machine, never shared or reused.
//
// A server whose config fixes its version (ServeV1, ServeV2) gets a forced
// machine instead: v1 or v2 from spawn for its life, with no probe and no
// timer. All it can still learn is that the child does not speak that
// version, once: a forced v1 child rejecting an initialize as v2 alone, or a
// forced v2 child answering a client's server/discover with an answer that is
// not modern.

// mcpVersion is what a process is served as.
type mcpVersion int

const (
	versionUnknown mcpVersion = iota // the probe is out
	versionV1
	versionV2
)

type eraPhase int

const (
	phasePending eraPhase = iota // the probe is out, its timer runs
	phaseV2                      // a modern answer, the one flip from phaseTimeout, or forced from spawn
	phaseV1                      // any other answer, or forced from spawn
	phaseTimeout                 // no answer within the probe timeout: v1 for now
)

type eraMachine struct {
	phase eraPhase
	// versions: in phaseV2, the modern versions the refusals name (never
	// empty).
	versions []string
	ended    bool // the process exited or was replaced, or the bridge closed
	// forced: the config fixed the version (phaseV1 or phaseV2 from spawn).
	// mismatch: a forced process's child turned out not to speak it.
	forced   bool
	mismatch bool

	// The rest matters only in phaseTimeout.

	// inits counts the 2025 initializes forwarded before any succeeded and
	// not yet answered by the child. One whose client left or timed out after
	// it was written still counts: the child's answer to it is the evidence.
	inits int
	// held is a modern probe answer that came while inits > 0; it is applied
	// once they have all failed, and dropped if one succeeds.
	held []string
	// initOK: an initialize succeeded, so the process is v1 until it ends.
	initOK bool
}

func (m eraMachine) version() mcpVersion {
	switch m.phase {
	case phasePending:
		return versionUnknown
	case phaseV2:
		return versionV2
	}
	return versionV1
}

// countsInits reports whether the child's answer to a 2025 initialize can
// still change the process's version: v1 for now, and none has succeeded.
func (m eraMachine) countsInits() bool {
	return m.phase == phaseTimeout && !m.initOK && !m.ended
}

// newEraMachine is a process's machine as it starts: pending on the probe for
// ServeAuto, otherwise forced to the version as names.
func newEraMachine(as ServeAs) eraMachine {
	switch as {
	case ServeV1:
		return eraMachine{phase: phaseV1, forced: true}
	case ServeV2:
		return eraMachine{phase: phaseV2, versions: []string{modernProtocolVersion}, forced: true}
	}
	return eraMachine{}
}

// checksForced reports whether the child's answer to a forwarded initialize
// (forced v1) or client server/discover (forced v2) is still fed to the
// machine: the child has not yet been found not to speak its forced version.
func (m eraMachine) checksForced() bool {
	return m.forced && !m.mismatch && !m.ended
}

type eraEventKind int

const (
	evAnswer         eraEventKind = iota // the child answered the probe
	evTimeout                            // the probe's timer fired
	evInitSent                           // a 2025 initialize is being forwarded
	evInitOK                             // the child accepted a forwarded initialize
	evInitFailed                         // the child rejected a forwarded initialize
	evClientDiscover                     // the child answered a client's forwarded server/discover
	evGone                               // the process exited or was replaced, a write to it failed, or the bridge is closing
)

var eraEventNames = [...]string{"answer", "timeout", "init-sent", "init-ok", "init-failed", "client-discover", "gone"}

func (k eraEventKind) String() string {
	if int(k) < len(eraEventNames) {
		return eraEventNames[k]
	}
	return fmt.Sprintf("event(%d)", int(k))
}

type eraEvent struct {
	kind eraEventKind
	// versions: for evAnswer and evClientDiscover, the answer's modern
	// versions (nil: not a modern answer); for evInitFailed, the rejection's
	// list if it names only modern versions (nil otherwise).
	versions []string
}

// eraEffects is what the caller does after apply.
type eraEffects struct {
	decided bool     // the process left phasePending: release the requests waiting for it
	toV2    bool     // the process became v2: it has no push stream
	noV1    bool     // a process forced to v1 speaks only v2: tell the user
	log     []string // lines for the log, in order
	ignored string   // why the event changed nothing
}

func withLog(lines ...string) eraEffects { return eraEffects{log: lines} }

func ignored(reason string) eraEffects { return eraEffects{ignored: reason} }

// apply is the machine's one transition function. An ended machine ignores
// every event; otherwise every pair of state and event the bridge cannot
// feed, and every pair that changes nothing, falls through to its last line.
func (m eraMachine) apply(ev eraEvent) (eraMachine, eraEffects) {
	if m.ended {
		return m, ignored("the process is gone")
	}
	if ev.kind == evGone {
		decided := m.phase == phasePending
		m.ended, m.held = true, nil
		return m, eraEffects{decided: decided}
	}
	switch m.phase {
	case phasePending:
		switch ev.kind {
		case evAnswer:
			if ev.versions != nil {
				m.phase, m.versions = phaseV2, ev.versions
				return m, eraEffects{decided: true, toV2: true, log: []string{logSpeaksV2(ev.versions)}}
			}
			m.phase = phaseV1
			return m, eraEffects{decided: true, log: []string{logSpeaksV1}}
		case evTimeout:
			m.phase = phaseTimeout
			return m, eraEffects{decided: true, log: []string{logNoAnswer}}
		}
	case phaseTimeout:
		switch ev.kind {
		case evAnswer:
			switch {
			case ev.versions == nil && m.initOK:
				// The first successful initialize already logged the v1 line.
				m.phase = phaseV1
				return m, eraEffects{}
			case ev.versions == nil:
				m.phase, m.held = phaseV1, nil
				return m, withLog(logSpeaksV1)
			case m.initOK:
				return m, withLog(logLateAfterV1(ev.versions))
			case m.inits > 0:
				m.held = ev.versions
				return m, eraEffects{}
			}
			return m.flip(ev.versions, logAnsweredLate(ev.versions))
		case evInitSent:
			m.inits++
			return m, eraEffects{}
		case evInitOK:
			m.inits--
			if m.initOK {
				return m, eraEffects{}
			}
			// The first success settles the process as v1: its verdict line,
			// then the one for a modern answer held until now.
			m.initOK = true
			if m.held != nil {
				held := m.held
				m.held = nil
				return m, withLog(logSpeaksV1, logLateAfterV1(held))
			}
			return m, withLog(logSpeaksV1)
		case evInitFailed:
			m.inits--
			switch {
			case m.initOK:
			case ev.versions != nil && m.held != nil:
				return m.flip(ev.versions, logAnsweredLate(ev.versions))
			case ev.versions != nil:
				return m.flip(ev.versions, logSpeaksV2(ev.versions))
			case m.held != nil && m.inits == 0:
				return m.flip(m.held, logAnsweredLate(m.held))
			}
			return m, eraEffects{}
		}
	case phaseV1:
		// Forced v1: only a rejection naming v2 alone says the child does not
		// speak v1. The client still gets the child's own rejection.
		if !m.checksForced() {
			break
		}
		switch ev.kind {
		case evInitOK:
			return m, eraEffects{}
		case evInitFailed:
			if ev.versions == nil {
				return m, eraEffects{}
			}
			m.mismatch = true
			return m, eraEffects{noV1: true, log: []string{logForcedV1Mismatch(ev.versions)}}
		}
	case phaseV2:
		// Forced v2: an answer to a client's server/discover that is not
		// modern says the child does not speak v2.
		if !m.checksForced() || ev.kind != evClientDiscover {
			break
		}
		if ev.versions != nil {
			return m, eraEffects{}
		}
		m.mismatch = true
		return m, withLog(logForcedV2Mismatch)
	}
	return m, ignored("the verdict is final")
}

// flip is the one change a verdict can still make: v1 for now becomes v2.
func (m eraMachine) flip(versions []string, line string) (eraMachine, eraEffects) {
	m.phase, m.versions = phaseV2, versions
	m.inits, m.held = 0, nil
	return m, eraEffects{toV2: true, log: []string{line}}
}
