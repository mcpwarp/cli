package bridge

import (
	"fmt"
	"time"
)

// The protocol era of one child process is decided by eraMachine. Every event
// goes through apply, under Bridge.mu, and apply is pure: no I/O, goroutines,
// timers or clock reads (the time comes in on the event). It returns the next
// state and the effects for the caller to carry out (applyEraLocked in
// era.go). Each process gets a fresh machine, never shared or reused.

// verdictKind is what the bridge believes about a process's era. The child's
// modern or -32601 answer to a probe outranks an initialize rejection listing
// only modern versions, which outranks the probe timing out or getting some
// other error: those two are only "legacy for now", and re-probed.
type verdictKind int

const (
	verdictNone      verdictKind = iota
	verdictTimeout               // legacy for now: a written probe went unanswered
	verdictSoft                  // legacy for now: a probe got an error that is neither modern nor -32601
	verdictLegacy                // the child answered -32601, or a result without supportedVersions: never re-probed
	verdictModern                // the child answered a probe as a modern server
	verdictRejection             // modern-only: an initialize rejection listed only modern versions
)

// eraVerdict is a verdict and the versions behind it: for verdictModern the
// probe answer's list (nil when it gave none, or an invalid one), for
// verdictRejection the rejection's list (never empty, all modern).
type eraVerdict struct {
	kind     verdictKind
	versions []string
	// borrowed: a verdictModern whose probe answer named no versions took
	// them from an initialize rejection listing only modern ones.
	borrowed bool
}

// recheckable reports a modern-only verdict that rests on an initialize
// rejection's list. While it stands the bridge refuses initialize itself,
// so nothing would ever reset it: traffic keeps it re-checked by background
// probes instead, on the usual interval and backoff, until a probe answers
// with versions of its own or as explicit legacy.
func (v eraVerdict) recheckable() bool {
	return v.kind == verdictRejection || (v.kind == verdictModern && v.borrowed)
}

// weak reports a verdict that a rejection's version evidence or a soft-legacy
// answer may replace.
func (v eraVerdict) weak() bool {
	return v.kind == verdictNone || v.kind == verdictTimeout || v.kind == verdictSoft
}

// final reports a verdict from the child's own modern or explicit legacy
// answer, which only a live probe's answer may replace. Borrowed versions are
// not the child's answer, so a verdict holding them is not final.
func (v eraVerdict) final() bool {
	return (v.kind == verdictModern && !v.borrowed) || v.kind == verdictLegacy
}

// modernOnly reports version evidence that the child speaks only 2026-era
// versions: a rejection's modern-only list, or modern versions alone from a
// probe answer. On its own it is not enough to refuse 2025 traffic (see
// refusesLegacy): dual-era servers answer the probe with only the modern
// version too.
func (v eraVerdict) modernOnly() bool {
	switch v.kind {
	case verdictRejection:
		return true
	case verdictModern:
		return onlyModernVersions(v.versions)
	}
	return false
}

// answer is what a modern request gets from v; no verdict reads as legacy for
// now.
func (v eraVerdict) answer() eraAnswer {
	if v.kind == verdictModern || v.kind == verdictRejection {
		return answerModern
	}
	return answerLegacy
}

type probePhase int

const (
	probeIdle    probePhase = iota
	probeWriting            // its write to stdin has not returned; its timer runs
	probeWritten            // in flight in the child; its timer runs
	// probeUnsent: its timer fired before its write returned. Its waiters are
	// released; a write that still completes leaves it outstanding in the
	// child, and no other probe is written meanwhile.
	probeUnsent
)

// probeState is the one probe a process has out at a time.
type probeState struct {
	phase    probePhase
	id       string
	deadline time.Time // when its timer is due
	// pre: the process's first successful initialize landed while it was out.
	// It still settles its waiters, but caches nothing and is never adopted.
	pre bool
}

func (p probeState) live() bool { return p.phase == probeWriting || p.phase == probeWritten }

type probeOutcome int

const (
	probeModern     probeOutcome = iota // a DiscoverResult with supportedVersions, or -32020/-32021/-32022
	probeLegacy                         // -32601, or a result without supportedVersions
	probeSoftLegacy                     // any other error: legacy for now
)

type eraMachine struct {
	timeout     time.Duration // a probe's lifetime, and the base re-probe interval
	maxInterval time.Duration // the cap on the re-probe interval's backoff
	seq         int           // probe ids minted so far
	ended       bool          // the process exited or was replaced, or the bridge closed

	verdict       eraVerdict
	probe         probeState
	initOK        bool   // the process has accepted an initialize
	legacyVersion string // protocolVersion from its initialize result
	// rejectedInit: the process rejected a forwarded initialize in a
	// modern-looking way before accepting any. It is half of the evidence
	// for refusing 2025 traffic (refusesLegacy); the version evidence may
	// come before or after it. Never set once an initialize was accepted.
	rejectedInit bool
	// evidence is modern-only evidence from an initialize rejection that came
	// while a probe was live and no verdict was cached. It stands in for that
	// probe if it times out, fails to be written or gets a soft-legacy answer.
	evidence []string
	// outstanding holds the ids of timed-out probes still unanswered in the
	// child (no cancellation is sent), oldest first, at most
	// maxOutstandingProbes. The first answer to any of them settles the
	// verdict and drops the rest. They hold no request slot.
	outstanding []string
	// quietUntil: no background probe starts before this. Every probe that
	// ends (answered, timed out after being written, adopted late, or failed
	// to be written) puts it at least one interval past its end: the probe
	// timeout, or the backoff. A written probe's timeout and a late-adopted
	// write set it by direct assignment to a probe timeout from now, which
	// can move it backward, since that timeout also resets the backoff; every
	// other path only moves it forward (arm). Left alone: a probe from before
	// the first initialize (the next request probes afresh), and a probe
	// whose write was still blocked when its timer fired (no second probe
	// starts until that write returns anyway). The first initialize clears
	// it. Zero until a probe ends.
	quietUntil time.Time
	// softStreak counts consecutive soft-legacy answers (and modern answers
	// without versions to a re-check of a rejection's list); the interval
	// after one doubles with each (a legacy server that answers an unknown
	// method with -32602 and logs a warning each time is asked ever more
	// rarely). The timeout of a written probe, a modern answer that sets the
	// verdict, an explicit legacy answer, adopting a rejection's modern-only
	// list and the first successful initialize reset it; a probe that was
	// never written does neither. Zero also means "no soft answer since the
	// last timeout" (rejection gate).
	softStreak int
	// writeFailed: the last probe's write failed. Like a soft answer, it is
	// not silence: a rejection waits out the interval rather than probing at
	// once, and a modern request with no verdict is told "not running" until
	// the interval has passed instead of starting another doomed probe. A
	// written probe's timeout, an answer and the first initialize clear it.
	writeFailed bool
}

// arm moves quietUntil to at least until; it never shortens an interval
// already running.
func (m eraMachine) arm(until time.Time) eraMachine {
	if until.After(m.quietUntil) {
		m.quietUntil = until
	}
	return m
}

// reprobeInterval is the wait before re-probing after the softStreak-th
// consecutive soft-legacy answer: the probe timeout, doubled for each one
// after the first, capped at maxInterval.
func (m eraMachine) reprobeInterval() time.Duration {
	d := m.timeout
	for i := 1; i < m.softStreak && d < m.maxInterval; i++ {
		d *= 2
	}
	if d > m.maxInterval {
		d = m.maxInterval
	}
	return d
}

type eraEventKind int

const (
	evModernRequest eraEventKind = iota // a modern-shaped request needs a verdict
	evInitAccepted                      // the child accepted an initialize
	evInitRejected                      // the child rejected an initialize
	evProbeAnswer                       // the child answered a probe id
	evProbeTimer                        // a flight's timer fired
	evWriteDone                         // a probe's write to stdin returned
	evGone                              // the process exited or was replaced, or the bridge is closing
	evLegacyRefused                     // the bridge refused 2025-style traffic (POST, GET, DELETE) itself
)

var eraEventNames = [...]string{"modern-request", "init-accepted", "init-rejected", "probe-answer", "probe-timer", "write-done", "gone", "legacy-refused"}

func (k eraEventKind) String() string {
	if int(k) < len(eraEventNames) {
		return eraEventNames[k]
	}
	return fmt.Sprintf("event(%d)", int(k))
}

type rejectionClass int

const (
	rejNotModern    rejectionClass = iota // not -32601/-32022 and no data.supported
	rejBare                               // -32601 or -32022 without data.supported
	rejMixed                              // data.supported, but empty or not a valid version list
	rejModernOnly                         // data.supported lists modern versions only
	rejLegacyListed                       // data.supported is a valid list naming a 2025-era version
)

type eraEvent struct {
	kind   eraEventKind
	now    time.Time // all but evInitAccepted and evGone
	exited bool      // evModernRequest: the process has exited and not been replaced
	id     string    // evProbeAnswer, evProbeTimer, evWriteDone

	outcome  probeOutcome   // evProbeAnswer
	versions []string       // evProbeAnswer (modern); evInitRejected (rejModernOnly)
	rejClass rejectionClass // evInitRejected

	version    string // evInitAccepted: protocolVersion of the result, if hasVersion
	hasVersion bool

	failed bool // evWriteDone: the write returned an error
}

type eraAnswer int

const (
	answerNone eraAnswer = iota
	answerModern
	answerLegacy    // possibly only for now
	answerGone      // the process exited or was replaced, or the bridge closed
	answerCancelled // a waiter's own context ended first (never an effect)
)

// eraEffects is what the caller does after apply.
type eraEffects struct {
	start   string    // start a probe with this id: register its waiters and timer, write it
	wait    bool      // evModernRequest: the request waits on the live probe
	again   bool      // with wait: the probe predates the first initialize; once it settles, ask again
	answer  eraAnswer // evModernRequest that does not wait: its answer now
	release eraAnswer // settle the live probe's waiters with this (answerNone: no)
	ignored string    // why the event changed nothing
}

func ignored(reason string) eraEffects { return eraEffects{ignored: reason} }

// newEraMachine returns a fresh process's machine; the backoff cap is never
// below the probe timeout.
func newEraMachine(timeout, maxInterval time.Duration) eraMachine {
	return eraMachine{timeout: timeout, maxInterval: max(maxInterval, timeout)}
}

// apply is the era state machine's one transition function.
func (m eraMachine) apply(ev eraEvent) (eraMachine, eraEffects) {
	if m.ended {
		if ev.kind == evModernRequest {
			return m, eraEffects{answer: answerGone}
		}
		return m, ignored("the process is gone")
	}
	switch ev.kind {
	case evModernRequest:
		return m.onModernRequest(ev)
	case evInitAccepted:
		return m.onInitAccepted(ev), eraEffects{}
	case evInitRejected:
		return m.onInitRejected(ev)
	case evProbeAnswer:
		return m.onProbeAnswer(ev)
	case evProbeTimer:
		return m.onProbeTimer(ev)
	case evWriteDone:
		return m.onWriteDone(ev)
	case evGone:
		return m.onGone()
	case evLegacyRefused:
		return m.onLegacyRefused(ev)
	}
	return m, ignored("unknown event " + ev.kind.String())
}

func (m eraMachine) onModernRequest(ev eraEvent) (eraMachine, eraEffects) {
	if ev.exited {
		// Nothing to forward to and nobody to probe: "not running".
		return m, eraEffects{answer: answerGone}
	}
	switch m.verdict.kind {
	case verdictNone:
		switch m.probe.phase {
		case probeIdle:
			if m.writeFailed && ev.now.Before(m.quietUntil) {
				// The last probe could not even be written: the child is not
				// taking input. Not running, until the interval has passed.
				return m, eraEffects{answer: answerGone}
			}
			var id string
			m, id = m.startProbe(ev.now)
			return m, eraEffects{start: id, wait: true}
		case probeWriting, probeWritten:
			// A probe sent before the first initialize cannot answer for the
			// initialized child: wait for it to settle (one probe at a time),
			// then ask again, which probes afresh within what is left of the
			// same wait.
			return m, eraEffects{wait: true, again: m.probe.pre}
		case probeUnsent:
			// An earlier probe's write is still blocked: legacy for now, and no
			// second write is queued behind it.
			return m, eraEffects{answer: answerLegacy}
		}
	case verdictTimeout, verdictSoft:
		// Answered from the verdict at once, never waiting on a probe.
		eff := eraEffects{answer: answerLegacy}
		if m.backgroundProbeDue(ev.now) {
			m, eff.start = m.startProbe(ev.now)
		}
		return m, eff
	}
	eff := eraEffects{answer: m.verdict.answer()}
	if m.verdict.recheckable() && m.backgroundProbeDue(ev.now) {
		m, eff.start = m.startProbe(ev.now)
	}
	return m, eff
}

// onLegacyRefused: 2025-style traffic refused on a rejection's say-so re-checks
// it, never delaying the refusal. A probe's own answer is final until the
// process is initialized or replaced.
func (m eraMachine) onLegacyRefused(ev eraEvent) (eraMachine, eraEffects) {
	if !m.verdict.recheckable() {
		return m, ignored("not a verdict taken from a rejection")
	}
	var eff eraEffects
	if m.backgroundProbeDue(ev.now) {
		m, eff.start = m.startProbe(ev.now)
	}
	return m, eff
}

// backgroundProbeDue reports whether a probe nobody waits on may start: none
// is out, and the re-probe interval has passed.
func (m eraMachine) backgroundProbeDue(now time.Time) bool {
	return m.probe.phase == probeIdle && !now.Before(m.quietUntil)
}

// rejectionProbeDue is the gate for a probe started by a modern-looking
// initialize rejection: at once on no verdict, or a weak one with no soft
// answer or failed write since the last timeout (the child may simply have
// been silent); after the interval otherwise, and on the usual interval for
// a verdict taken from a rejection. One probe at a time either way, so a
// rejection flood against a silent child yields about one probe per probe
// timeout.
func (m eraMachine) rejectionProbeDue(now time.Time) bool {
	switch {
	case m.verdict.weak() && m.softStreak == 0 && !m.writeFailed:
		return m.probe.phase == probeIdle
	case m.verdict.weak(), m.verdict.recheckable():
		return m.backgroundProbeDue(now)
	}
	return false
}

// adoptEvidence makes a rejection's modern-only list the verdict; it resets
// the soft backoff.
func (m eraMachine) adoptEvidence(versions []string) eraMachine {
	m.verdict, m.softStreak = eraVerdict{kind: verdictRejection, versions: versions}, 0
	return m
}

func (m eraMachine) startProbe(now time.Time) (eraMachine, string) {
	m.seq++
	id := fmt.Sprintf("%s%d", probeIDPrefix, m.seq)
	m.probe = probeState{phase: probeWriting, id: id, deadline: now.Add(m.timeout)}
	return m, id
}

// onInitAccepted: the first successful initialize drops the verdict, the
// outstanding ids, remembered evidence, the re-probe interval and its
// backoff, so the next modern request probes afresh: a child that said
// nothing (or something else) before initialize may answer differently
// after it. A probe still out was sent before it, so it becomes pre.
func (m eraMachine) onInitAccepted(ev eraEvent) eraMachine {
	if !m.initOK {
		m.verdict, m.outstanding, m.evidence, m.quietUntil, m.softStreak = eraVerdict{}, nil, nil, time.Time{}, 0
		m.writeFailed, m.rejectedInit = false, false
		if m.probe.phase != probeIdle {
			m.probe.pre = true
		}
		m.initOK = true
	}
	if ev.hasVersion {
		m.legacyVersion = ev.version
	}
	return m
}

// onInitRejected: only a modern-looking rejection says anything (a legacy
// child may reject one initialize and accept the next). It may start a
// background probe (rejectionProbeDue), decided on the verdict as it stood
// before the rejection's own evidence. Its evidence counts only before the
// process has accepted an initialize: it replaces a weak verdict, fills in
// the versions of a modern answer that named none, and is remembered while
// the first probe is out. A rejection whose list names a 2025-era version is
// the child saying it takes initialize: it is no evidence of any kind, and it
// withdraws an earlier rejection's half of the evidence for refusing 2025
// traffic; it may still start the probe.
func (m eraMachine) onInitRejected(ev eraEvent) (eraMachine, eraEffects) {
	if ev.rejClass == rejNotModern {
		return m, ignored("not a modern-looking rejection")
	}
	probe := m.rejectionProbeDue(ev.now)
	if !m.initOK {
		m.rejectedInit = ev.rejClass != rejLegacyListed
	}
	if ev.rejClass == rejModernOnly && !m.initOK {
		switch {
		case m.verdict.kind == verdictNone && m.probe.live():
			m.evidence = ev.versions
		case m.verdict.weak():
			m = m.adoptEvidence(ev.versions)
		case m.verdict.kind == verdictModern && m.verdict.versions == nil:
			m.verdict = eraVerdict{kind: verdictModern, versions: ev.versions, borrowed: true}
		}
	}
	var eff eraEffects
	if probe {
		m, eff.start = m.startProbe(ev.now)
	}
	return m, eff
}

func (m eraMachine) onProbeAnswer(ev eraEvent) (eraMachine, eraEffects) {
	p := m.probe
	if ev.id != "" && ev.id == p.id {
		switch p.phase {
		case probeWriting, probeWritten:
			// The answer also proves the write landed: the probe is done, and its
			// write goroutine's report, when it comes, is stale.
			evidence := m.evidence
			m.probe, m.evidence = probeState{}, nil
			if p.pre {
				if ev.outcome == probeModern {
					return m, eraEffects{release: answerModern}
				}
				return m, eraEffects{release: answerLegacy}
			}
			if ev.outcome == probeSoftLegacy && m.verdict.kind == verdictNone && evidence != nil {
				// Soft legacy ranks with a timeout, which the evidence beats.
				// The probe has ended, so the rejection verdict's re-checks wait
				// out an interval.
				m = m.adoptEvidence(evidence).arm(ev.now.Add(m.timeout))
				m.writeFailed = false
			} else {
				// A live probe's answer overwrites any verdict, even an explicit
				// one a late answer to an older id set meanwhile: first answer
				// wins only among the outstanding ids.
				m, _ = m.settle(ev)
			}
			return m, eraEffects{release: m.verdict.answer()}
		case probeUnsent:
			// Answered before its write goroutine could report: it was written,
			// so the answer counts as a late one.
			m.probe = probeState{}
			if p.pre {
				return m, eraEffects{}
			}
			return m.late(ev)
		}
	}
	for i, x := range m.outstanding {
		if x == ev.id {
			m.outstanding = without(m.outstanding, i)
			return m.late(ev)
		}
	}
	return m, ignored("not a live or outstanding probe id")
}

// late applies an answer to a probe that had already timed out (its id is
// already consumed). It never replaces a final verdict, and a soft-legacy one
// does not replace a rejection's evidence.
func (m eraMachine) late(ev eraEvent) (eraMachine, eraEffects) {
	if !m.verdict.final() {
		m, _ = m.settle(ev)
	}
	return m, eraEffects{}
}

// settle records a probe's answer. Every answer moves quietUntil at least one
// interval past it, never shortening one already running, so no answer can
// make probing more frequent than the interval. A modern or explicit legacy
// answer replaces any verdict and resets the backoff; the interval is the
// probe timeout. The exception is a modern answer without versions to a
// re-check of a rejection's list: it confirms the child is modern but says
// nothing against the list, so the list stands (dropping it would forward
// 2025 traffic until the next rejection borrowed it back, round and round),
// and like a soft answer it advances the backoff, so the re-checks decay to
// the cap. A soft-legacy answer advances the backoff wherever further probes
// could follow (a weak verdict, or one taken from a rejection) and waits that
// long, but replaces only a weak verdict: it ranks with a timeout. When the
// verdict changes, the other outstanding ids are dropped: the first answer
// settles it.
func (m eraMachine) settle(ev eraEvent) (eraMachine, bool) {
	m.writeFailed = false
	switch ev.outcome {
	case probeModern:
		if ev.versions == nil && m.verdict.recheckable() {
			m.softStreak++
			m = m.arm(ev.now.Add(m.reprobeInterval()))
			return m.arm(ev.now.Add(m.timeout)), false
		}
		m.verdict = eraVerdict{kind: verdictModern, versions: ev.versions}
		m.softStreak = 0
	case probeLegacy:
		m.verdict = eraVerdict{kind: verdictLegacy}
		m.softStreak = 0
	default:
		if m.verdict.weak() || m.verdict.recheckable() {
			m.softStreak++
			m = m.arm(ev.now.Add(m.reprobeInterval()))
		}
		if !m.verdict.weak() {
			return m.arm(ev.now.Add(m.timeout)), false
		}
		m.verdict = eraVerdict{kind: verdictSoft}
	}
	m = m.arm(ev.now.Add(m.timeout))
	m.outstanding, m.evidence = nil, nil
	return m, true
}

func (m eraMachine) onProbeTimer(ev eraEvent) (eraMachine, eraEffects) {
	p := m.probe
	if ev.id == "" || ev.id != p.id || !p.live() {
		return m, ignored("stale timer: its probe was already settled")
	}
	evidence := m.evidence
	m.evidence = nil
	written := p.phase == probeWritten
	if written {
		m.probe = probeState{}
	} else {
		m.probe.phase = probeUnsent
	}
	if p.pre {
		return m, eraEffects{release: answerLegacy}
	}
	if written {
		// Still outstanding in the child, so its late answer can say more,
		// unless the child's own answer is already final. The timeout of a
		// written probe restarts the interval from the base and resets the
		// backoff.
		if !m.verdict.final() {
			m.outstanding = withOutstanding(m.outstanding, p.id)
		}
		m.quietUntil, m.softStreak, m.writeFailed = ev.now.Add(m.timeout), 0, false
	}
	if m.verdict.kind == verdictNone {
		// Not written: no verdict, since nothing can answer it unless the write
		// still completes (onWriteDone).
		switch {
		case evidence != nil:
			m = m.adoptEvidence(evidence)
		case written:
			m.verdict = eraVerdict{kind: verdictTimeout}
		}
	}
	return m, eraEffects{release: m.verdict.answer()}
}

func (m eraMachine) onWriteDone(ev eraEvent) (eraMachine, eraEffects) {
	p := m.probe
	if ev.id == "" || ev.id != p.id {
		return m, ignored("stale write report: its probe was answered or dropped")
	}
	switch p.phase {
	case probeWriting:
		if ev.failed {
			// Its waiters get "gone"; remembered evidence stands in for it as
			// it would on a timeout. A write that fails at once (a child that
			// closed its stdin) must not let every request start another
			// probe: the gate stays closed for at least a probe timeout from
			// now (a longer interval already running stands), and the backoff
			// is left as it was.
			evidence := m.evidence
			m.probe, m.evidence = probeState{}, nil
			if p.pre {
				return m, eraEffects{release: answerGone}
			}
			if evidence != nil && m.verdict.kind == verdictNone {
				m = m.adoptEvidence(evidence)
			}
			m = m.arm(ev.now.Add(m.timeout))
			m.writeFailed = true
			return m, eraEffects{release: answerGone}
		}
		m.probe.phase = probeWritten
		return m, eraEffects{}
	case probeUnsent:
		// Its write was blocked through its timer, so no second probe could
		// start meanwhile; a failure now leaves the interval alone.
		m.probe = probeState{}
		if ev.failed || p.pre || m.verdict.final() {
			return m, eraEffects{}
		}
		// Written after its timer fired: outstanding from now, and this is the
		// timeout of a written probe, so the interval counts from now and the
		// backoff resets.
		m.outstanding = withOutstanding(m.outstanding, p.id)
		m.quietUntil, m.softStreak, m.writeFailed = ev.now.Add(m.timeout), 0, false
		if m.verdict.kind == verdictNone {
			m.verdict = eraVerdict{kind: verdictTimeout}
		}
		return m, eraEffects{}
	}
	return m, ignored("duplicate write report")
}

// onGone ends the machine: waiters get "gone", and nothing from this process
// is applied any more. The verdict is kept: a dead process's refusal of 2025
// traffic still decides GET and DELETE until it is replaced (POSTs check
// HasExited first).
func (m eraMachine) onGone() (eraMachine, eraEffects) {
	var eff eraEffects
	if m.probe.live() {
		eff.release = answerGone
	}
	m.ended = true
	m.probe, m.outstanding, m.evidence = probeState{}, nil, nil
	return m, eff
}

// timeoutAnswer is what the live probe's waiters would get if its timer
// fired now.
func (m eraMachine) timeoutAnswer(now time.Time) eraAnswer {
	if _, eff := m.apply(eraEvent{kind: evProbeTimer, id: m.probe.id, now: now}); eff.release != answerNone {
		return eff.release
	}
	return m.verdict.answer()
}

// refusesLegacy reports whether the bridge refuses 2025-style traffic for
// this process itself (initialize and other 2025-shaped requests get its own
// 400, GET and DELETE a 405), and with which versions, always the child's
// own. It takes both halves of the evidence: the process rejected a forwarded
// initialize in a modern-looking way, and there is version evidence that it
// speaks only 2026-era versions (that rejection's list, or a probe answer
// with no legacy version, in either order). A probe answer alone never
// suffices, since dual-era servers answer it with only the modern version
// too; until a rejection, 2025 traffic is forwarded and the child decides.
// An accepted initialize rules it out for the life of the process.
func (m eraMachine) refusesLegacy() ([]string, bool) {
	if !m.rejectedInit || m.initOK || !m.verdict.modernOnly() {
		return nil, false
	}
	return append([]string(nil), m.verdict.versions...), true
}

// withOutstanding returns ids plus id, the oldest dropped when full, in a new
// slice: apply never writes to a slice the previous state still holds.
func withOutstanding(ids []string, id string) []string {
	for _, x := range ids {
		if x == id {
			return ids
		}
	}
	if len(ids) >= maxOutstandingProbes {
		ids = ids[len(ids)-maxOutstandingProbes+1:]
	}
	out := make([]string, 0, len(ids)+1)
	return append(append(out, ids...), id)
}

func without(ids []string, i int) []string {
	out := make([]string, 0, len(ids)-1)
	return append(append(out, ids[:i]...), ids[i+1:]...)
}
