package bridge

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"
)

// The era machine's executable spec: for each state and event, the next state
// and the effects. No processes, HTTP or timers. A rule change should show up
// here as a row diff.
//
// States print as
//
//	<verdict>[versions][+borrowed][ <phase>(<id>@<deadline>[,pre])][ init]
//	         [ v=<legacy version>][ out=[...]][ ev=[...]][ quiet=<s>]
//	         [ streak=<n>][ write-failed][ ENDED]
//
// with times in seconds after t0, probe ids as pN, and effects as
//
//	[start pN][ wait][ again][ answer <a>][ release <a>][ ignored]   ("-" for none).

const (
	tmo    = 15 * time.Second // the probe timeout in these rows
	tmoCap = 40 * tmo         // its default backoff cap: 10 minutes
	vNew   = "2026-07-28"
	vOld   = "2025-11-25"
)

var t0 = time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC)

func at(s float64) time.Time { return t0.Add(time.Duration(s * float64(time.Second))) }

func pid(n int) string { return fmt.Sprintf("%s%d", probeIDPrefix, n) }

type mopt func(*eraMachine)

func mach(opts ...mopt) eraMachine {
	m := newEraMachine(tmo, tmoCap)
	for _, o := range opts {
		o(&m)
	}
	return m
}

func vd(kind verdictKind, versions ...string) mopt {
	return func(m *eraMachine) { m.verdict = eraVerdict{kind: kind, versions: versions} }
}

// pr puts probe n in phase, started at start seconds; seq follows it.
func pr(phase probePhase, n int, start float64) mopt {
	return func(m *eraMachine) {
		m.probe = probeState{phase: phase, id: pid(n), deadline: at(start).Add(tmo)}
		if m.seq < n {
			m.seq = n
		}
	}
}

func seq(n int) mopt { return func(m *eraMachine) { m.seq = n } }
func pre() mopt      { return func(m *eraMachine) { m.probe.pre = true } }
func inited() mopt   { return func(m *eraMachine) { m.initOK = true } }
func ended() mopt    { return func(m *eraMachine) { m.ended = true } }
func wfail() mopt    { return func(m *eraMachine) { m.writeFailed = true } }
func rejected() mopt { return func(m *eraMachine) { m.rejectedInit = true } }
func streak(n int) mopt {
	return func(m *eraMachine) { m.softStreak = n }
}
func quiet(s float64) mopt { return func(m *eraMachine) { m.quietUntil = at(s) } }
func evid(v ...string) mopt {
	return func(m *eraMachine) { m.evidence = v }
}
func out(ns ...int) mopt {
	return func(m *eraMachine) {
		for _, n := range ns {
			m.outstanding = append(m.outstanding, pid(n))
			if m.seq < n {
				m.seq = n
			}
		}
	}
}

func req(s float64) eraEvent       { return eraEvent{kind: evModernRequest, now: at(s)} }
func reqExited(s float64) eraEvent { return eraEvent{kind: evModernRequest, now: at(s), exited: true} }
func initAcc(v string) eraEvent {
	return eraEvent{kind: evInitAccepted, version: v, hasVersion: v != ""}
}
func rej(c rejectionClass, s float64, v ...string) eraEvent {
	return eraEvent{kind: evInitRejected, rejClass: c, now: at(s), versions: v}
}
func ans(n int, o probeOutcome, s float64, v ...string) eraEvent {
	return eraEvent{kind: evProbeAnswer, id: pid(n), outcome: o, now: at(s), versions: v}
}
func timer(n int, s float64) eraEvent { return eraEvent{kind: evProbeTimer, id: pid(n), now: at(s)} }
func wrote(n int, s float64) eraEvent { return eraEvent{kind: evWriteDone, id: pid(n), now: at(s)} }
func writeFailed(n int, s float64) eraEvent {
	return eraEvent{kind: evWriteDone, id: pid(n), now: at(s), failed: true}
}
func gone() eraEvent { return eraEvent{kind: evGone} }
func refused(s float64) eraEvent {
	return eraEvent{kind: evLegacyRefused, now: at(s)}
}

func secs(t time.Time) string { return fmt.Sprintf("%g", t.Sub(t0).Seconds()) }

func short(id string) string { return "p" + strings.TrimPrefix(id, probeIDPrefix) }

func shortAll(ids []string) string {
	s := make([]string, len(ids))
	for i, id := range ids {
		s[i] = short(id)
	}
	return strings.Join(s, ",")
}

func describe(m eraMachine) string {
	var b strings.Builder
	b.WriteString([]string{"none", "timeout", "soft", "legacy", "modern", "rejection"}[m.verdict.kind])
	if m.verdict.versions != nil {
		fmt.Fprintf(&b, "[%s]", strings.Join(m.verdict.versions, ","))
	}
	if m.verdict.borrowed {
		b.WriteString("+borrowed")
	}
	if p := m.probe; p.phase != probeIdle {
		fmt.Fprintf(&b, " %s(%s@%s", []string{"", "writing", "written", "unsent"}[p.phase], short(p.id), secs(p.deadline))
		if p.pre {
			b.WriteString(",pre")
		}
		b.WriteString(")")
	}
	if m.initOK {
		b.WriteString(" init")
	}
	if m.legacyVersion != "" {
		b.WriteString(" v=" + m.legacyVersion)
	}
	if len(m.outstanding) > 0 {
		fmt.Fprintf(&b, " out=[%s]", shortAll(m.outstanding))
	}
	if m.evidence != nil {
		fmt.Fprintf(&b, " ev=[%s]", strings.Join(m.evidence, ","))
	}
	if !m.quietUntil.IsZero() {
		b.WriteString(" quiet=" + secs(m.quietUntil))
	}
	if m.softStreak != 0 {
		fmt.Fprintf(&b, " streak=%d", m.softStreak)
	}
	if m.writeFailed {
		b.WriteString(" write-failed")
	}
	if m.ended {
		b.WriteString(" ENDED")
	}
	return b.String()
}

func describeEff(e eraEffects) string {
	answers := []string{"", "modern", "legacy", "gone", "cancelled"}
	var parts []string
	if e.start != "" {
		parts = append(parts, "start "+short(e.start))
	}
	if e.wait {
		parts = append(parts, "wait")
	}
	if e.again {
		parts = append(parts, "again")
	}
	if e.answer != answerNone {
		parts = append(parts, "answer "+answers[e.answer])
	}
	if e.release != answerNone {
		parts = append(parts, "release "+answers[e.release])
	}
	if e.ignored != "" {
		parts = append(parts, "ignored")
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, " ")
}

type machineRow struct {
	name string
	from eraMachine
	ev   eraEvent
	want string // next state
	eff  string
}

// Named states the rows start from. The first probe was started at 0, so its
// timer was due at 15; "timeout" is what its written timeout at 15 leaves.
var (
	sNone        = mach()
	sWriting     = mach(pr(probeWriting, 1, 0))
	sWritten     = mach(pr(probeWritten, 1, 0))
	sUnsent      = mach(pr(probeUnsent, 1, 0))
	sTimeout     = mach(vd(verdictTimeout), out(1), quiet(30))
	sSoft        = mach(vd(verdictSoft), seq(1), quiet(20), streak(1))
	sLegacy      = mach(vd(verdictLegacy), seq(1))
	sModernDual  = mach(vd(verdictModern, vNew, vOld), seq(1))
	sModernOnly  = mach(vd(verdictModern, vNew), seq(1))
	sModernNoVer = mach(vd(verdictModern), seq(1))
	sRejection   = mach(vd(verdictRejection, vNew), seq(1))
	sBorrowed    = mach(vd(verdictModern, vNew), seq(1), func(m *eraMachine) { m.verdict.borrowed = true })
)

// with derives a state from a named one without sharing its slices.
func with(m eraMachine, opts ...mopt) eraMachine {
	m.outstanding = append([]string(nil), m.outstanding...)
	for _, o := range opts {
		o(&m)
	}
	return m
}

func machineRows() []machineRow {
	return []machineRow{
		// --- a modern-shaped request ---
		{"no verdict: probe and wait", sNone, req(0), "none writing(p1@15)", "start p1 wait"},
		{"no verdict, probe being written: share it", sWriting, req(1), "none writing(p1@15)", "wait"},
		{"no verdict, probe written: share it", sWritten, req(1), "none written(p1@15)", "wait"},
		{"no verdict, earlier write still blocked: legacy for now, no second write", sUnsent, req(16), "none unsent(p1@15)", "answer legacy"},
		{"dead process, no verdict: not running, no probe", sNone, reqExited(0), "none", "answer gone"},
		{"no verdict, last write failed, inside the interval: not running, no probe", mach(seq(1), quiet(15), wfail()), req(10), "none quiet=15 write-failed", "answer gone"},
		{"no verdict, last write failed, interval over: probe again", mach(seq(1), quiet(15), wfail()), req(15), "none writing(p2@30) quiet=15 write-failed", "start p2 wait"},
		{"dead process, legacy verdict: not running", sLegacy, reqExited(0), "legacy", "answer gone"},
		{"timeout verdict right after the timeout: answered, not re-probed", sTimeout, req(16), "timeout out=[p1] quiet=30", "answer legacy"},
		{"timeout verdict inside the interval, though the probe was sent long before", sTimeout, req(29.9), "timeout out=[p1] quiet=30", "answer legacy"},
		{"timeout verdict, interval over: answered, background re-probe", sTimeout, req(30), "timeout writing(p2@45) out=[p1] quiet=30", "start p2 answer legacy"},
		{"timeout verdict with a re-probe out: answered without waiting, no second probe", with(sTimeout, pr(probeWritten, 2, 30)), req(50), "timeout written(p2@45) out=[p1] quiet=30", "answer legacy"},
		{"timeout verdict, re-probe write blocked: no second probe", with(sTimeout, pr(probeUnsent, 2, 30)), req(50), "timeout unsent(p2@45) out=[p1] quiet=30", "answer legacy"},
		{"soft verdict inside the interval", sSoft, req(10), "soft quiet=20 streak=1", "answer legacy"},
		{"soft verdict, interval over: re-probe", sSoft, req(20), "soft writing(p2@35) quiet=20 streak=1", "start p2 answer legacy"},
		{"explicit legacy: never re-probed", sLegacy, req(1000), "legacy", "answer legacy"},
		{"dual-era answer: modern", sModernDual, req(1), "modern[2026-07-28,2025-11-25]", "answer modern"},
		{"modern answer without versions: modern", sModernNoVer, req(1), "modern", "answer modern"},
		{"modern verdict with a probe out: answered, no wait", with(sModernDual, pr(probeWritten, 2, 30)), req(31), "modern[2026-07-28,2025-11-25] written(p2@45)", "answer modern"},
		{"rejection verdict, gate open: answered at once, background re-check", sRejection, req(1), "rejection[2026-07-28] writing(p2@16)", "start p2 answer modern"},
		{"rejection verdict inside the interval: answered, no re-check yet", with(sRejection, quiet(30)), req(20), "rejection[2026-07-28] quiet=30", "answer modern"},
		{"rejection verdict, interval over: re-check", with(sRejection, quiet(30)), req(30), "rejection[2026-07-28] writing(p2@45) quiet=30", "start p2 answer modern"},
		{"rejection verdict, its probe out: answered, no wait", with(sRejection, pr(probeWritten, 1, 0)), req(1), "rejection[2026-07-28] written(p1@15)", "answer modern"},
		{"borrowed versions: answered modern, re-checked like a rejection", with(sBorrowed), req(1), "modern[2026-07-28]+borrowed writing(p2@16)", "start p2 answer modern"},
		{"borrowed versions inside the interval: no re-check", with(sBorrowed, quiet(40)), req(30), "modern[2026-07-28]+borrowed quiet=40", "answer modern"},
		{"probe-derived modern-only verdict: final, no re-check", sModernOnly, req(1000), "modern[2026-07-28]", "answer modern"},
		{"no verdict, probe from before the first initialize: wait, then ask again", with(sWritten, pre(), inited()), req(5), "none written(p1@15,pre) init", "wait again"},
		{"same while that probe is still being written", with(sWriting, pre(), inited()), req(5), "none writing(p1@15,pre) init", "wait again"},
		{"asking again once it settled: a fresh probe", with(sNone, inited(), seq(1)), req(15), "none writing(p2@30) init", "start p2 wait"},
		{"process gone", with(sLegacy, ended()), req(1), "legacy ENDED", "answer gone"},

		// --- a successful initialize ---
		{"first initialize, no verdict", sNone, initAcc(vOld), "none init v=2025-11-25", "-"},
		{"first initialize clears a timeout verdict, its ids and the interval", sTimeout, initAcc(vOld), "none init v=2025-11-25", "-"},
		{"first initialize clears a soft verdict and its backoff", with(sSoft, streak(4), quiet(500)), initAcc(""), "none init", "-"},
		{"first initialize clears a failed write", mach(seq(1), quiet(15), wfail()), initAcc(""), "none init", "-"},
		{"first initialize clears an explicit legacy verdict", sLegacy, initAcc(vOld), "none init v=2025-11-25", "-"},
		{"first initialize clears a modern-only verdict", sModernOnly, initAcc(vOld), "none init v=2025-11-25", "-"},
		{"first initialize clears a rejection verdict", sRejection, initAcc(vOld), "none init v=2025-11-25", "-"},
		{"a probe out across the first initialize becomes pre", sWritten, initAcc(""), "none written(p1@15,pre) init", "-"},
		{"a blocked write across the first initialize is never adopted", sUnsent, initAcc(""), "none unsent(p1@15,pre) init", "-"},
		{"first initialize drops remembered evidence", with(sWritten, evid(vNew)), initAcc(""), "none written(p1@15,pre) init", "-"},
		{"a later initialize only updates the version", with(sLegacy, inited()), initAcc("2025-06-18"), "legacy init v=2025-06-18", "-"},
		{"initialize after the process is gone", with(sNone, ended()), initAcc(vOld), "none ENDED", "ignored"},

		// --- a rejected initialize ---
		{"not modern-looking: nothing", sNone, rej(rejNotModern, 1), "none", "ignored"},
		{"bare, nothing sent yet: background probe at once", sNone, rej(rejBare, 1), "none writing(p1@16)", "start p1"},
		{"mixed list: background probe, nothing cached", sNone, rej(rejMixed, 1), "none writing(p1@16)", "start p1"},
		{"modern-only list, no verdict: cached, background probe", sNone, rej(rejModernOnly, 1, vNew), "rejection[2026-07-28] writing(p1@16)", "start p1"},
		{"bare, probe out: no second probe", sWritten, rej(rejBare, 1), "none written(p1@15)", "-"},
		{"bare, write blocked: no second probe", sUnsent, rej(rejBare, 16), "none unsent(p1@15)", "-"},
		{"bare after a failed write, inside the interval: no probe", mach(seq(1), quiet(15), wfail()), rej(rejBare, 10), "none quiet=15 write-failed", "-"},
		{"bare after a failed write, interval over: probe", mach(seq(1), quiet(15), wfail()), rej(rejBare, 15), "none writing(p2@30) quiet=15 write-failed", "start p2"},
		{"modern-only list while the first probe is out: remembered", sWritten, rej(rejModernOnly, 1, vNew), "none written(p1@15) ev=[2026-07-28]", "-"},
		{"modern-only list while a blocked write has no verdict: cached", sUnsent, rej(rejModernOnly, 16, vNew), "rejection[2026-07-28] unsent(p1@15)", "-"},
		{"bare on a timeout verdict after silence: probe at once, ignoring the interval", sTimeout, rej(rejBare, 20), "timeout writing(p2@35) out=[p1] quiet=30", "start p2"},
		{"bare on a timeout verdict, interval over: probe", sTimeout, rej(rejBare, 30), "timeout writing(p2@45) out=[p1] quiet=30", "start p2"},
		{"bare on a timeout verdict with a probe out: none more (a flood gets one per probe timeout)", with(sTimeout, pr(probeWritten, 2, 20)), rej(rejBare, 21), "timeout written(p2@35) out=[p1] quiet=30", "-"},
		{"modern-only list on a timeout verdict after silence: cached, probe at once", sTimeout, rej(rejModernOnly, 20, vNew), "rejection[2026-07-28] writing(p2@35) out=[p1] quiet=30", "start p2"},
		{"modern-only list during a re-probe on a timeout verdict: cached at once", with(sTimeout, pr(probeWritten, 2, 30)), rej(rejModernOnly, 31, vNew), "rejection[2026-07-28] written(p2@45) out=[p1] quiet=30", "-"},
		{"modern-only list on a timeout verdict while a write is blocked: cached, no probe", with(sTimeout, pr(probeUnsent, 2, 30)), rej(rejModernOnly, 46, vNew), "rejection[2026-07-28] unsent(p2@45) out=[p1] quiet=30", "-"},
		{"bare on a timeout verdict while a write is blocked: nothing", with(sTimeout, pr(probeUnsent, 2, 30)), rej(rejBare, 46), "timeout unsent(p2@45) out=[p1] quiet=30", "-"},
		{"modern-only list on a soft verdict: cached, backoff reset", sSoft, rej(rejModernOnly, 5, vNew), "rejection[2026-07-28] quiet=20", "-"},
		{"bare on a soft verdict inside the interval: no probe", sSoft, rej(rejBare, 10), "soft quiet=20 streak=1", "-"},
		{"bare on a soft verdict, interval over: probe", sSoft, rej(rejBare, 20), "soft writing(p2@35) quiet=20 streak=1", "start p2"},
		{"bare on a soft verdict with no soft answer since a timeout: probe at once", mach(vd(verdictSoft), out(2), quiet(50)), rej(rejBare, 40), "soft writing(p3@55) out=[p2] quiet=50", "start p3"},
		{"modern-only list on explicit legacy: nothing, no probe", sLegacy, rej(rejModernOnly, 1, vNew), "legacy", "-"},
		{"mixed list on explicit legacy: nothing", sLegacy, rej(rejMixed, 1), "legacy", "-"},
		{"modern-only list on a dual-era probe answer: nothing", sModernDual, rej(rejModernOnly, 1, vNew), "modern[2026-07-28,2025-11-25]", "-"},
		{"bare on a modern probe answer: nothing", sModernDual, rej(rejBare, 1), "modern[2026-07-28,2025-11-25]", "-"},
		{"modern-only list on a modern answer that named no versions: borrowed", sModernNoVer, rej(rejModernOnly, 1, vNew), "modern[2026-07-28]+borrowed", "-"},
		{"modern-only list on a versionless modern answer inside its interval: borrowed, the gate stays closed", with(sModernNoVer, quiet(40)), rej(rejModernOnly, 30, vNew), "modern[2026-07-28]+borrowed quiet=40", "-"},
		{"no borrowing after an accepted initialize", with(sModernNoVer, inited()), rej(rejModernOnly, 1, vNew), "modern init", "-"},
		{"a second rejection's list does not replace the first, but re-checks it", sRejection, rej(rejModernOnly, 1, "2027-01-01"), "rejection[2026-07-28] writing(p2@16)", "start p2"},
		{"bare on a rejection verdict inside the interval: nothing", with(sRejection, quiet(30)), rej(rejBare, 20), "rejection[2026-07-28] quiet=30", "-"},
		{"mixed on a rejection verdict, interval over: re-check", with(sRejection, quiet(30)), rej(rejMixed, 30), "rejection[2026-07-28] writing(p2@45) quiet=30", "start p2"},
		{"evidence after an accepted initialize is ignored, the probe still starts", with(sNone, inited()), rej(rejModernOnly, 1, vNew), "none writing(p1@16) init", "start p1"},
		{"rejection after the process is gone", with(sNone, ended()), rej(rejModernOnly, 1, vNew), "none ENDED", "ignored"},

		// --- the live probe's answer: every non-pre answer closes the gate for at least an interval ---
		{"modern-only answer", sWritten, ans(1, probeModern, 5, vNew), "modern[2026-07-28] quiet=20", "release modern"},
		{"answer before its write goroutine reported", sWriting, ans(1, probeModern, 5, vNew), "modern[2026-07-28] quiet=20", "release modern"},
		{"dual-era answer", sWritten, ans(1, probeModern, 5, vNew, vOld), "modern[2026-07-28,2025-11-25] quiet=20", "release modern"},
		{"modern answer without a usable list: modern, versions unknown", sWritten, ans(1, probeModern, 5), "modern quiet=20", "release modern"},
		{"explicit legacy answer", sWritten, ans(1, probeLegacy, 5), "legacy quiet=20", "release legacy"},
		{"soft legacy: legacy at once, re-probeable after the base interval", sWritten, ans(1, probeSoftLegacy, 5), "soft quiet=20 streak=1", "release legacy"},
		{"an answer clears a failed write", with(sWritten, wfail()), ans(1, probeSoftLegacy, 5), "soft quiet=20 streak=1", "release legacy"},
		{"soft legacy with remembered evidence: the evidence wins, the gate closes", with(sWritten, evid(vNew)), ans(1, probeSoftLegacy, 5), "rejection[2026-07-28] quiet=20", "release modern"},
		{"probe answer beats remembered evidence", with(sWritten, evid(vNew)), ans(1, probeModern, 5, vNew, vOld), "modern[2026-07-28,2025-11-25] quiet=20", "release modern"},
		{"pre-initialize probe: waiters get its answer, nothing cached or armed", with(sWritten, pre(), inited()), ans(1, probeModern, 5, vNew), "none init", "release modern"},
		{"pre-initialize probe, soft answer", with(sWritten, pre(), inited()), ans(1, probeSoftLegacy, 5), "none init", "release legacy"},
		{"re-probe answer settles, outstanding dropped", with(sTimeout, pr(probeWritten, 2, 30)), ans(2, probeModern, 31, vNew), "modern[2026-07-28] quiet=46", "release modern"},
		{"re-probe soft answer on a timeout verdict: soft from the answer", with(sTimeout, pr(probeWritten, 2, 30)), ans(2, probeSoftLegacy, 40), "soft quiet=55 streak=1", "release legacy"},
		{"soft re-check answer does not replace a rejection verdict but arms the backoff", with(sRejection, pr(probeWritten, 1, 0)), ans(1, probeSoftLegacy, 5), "rejection[2026-07-28] quiet=20 streak=1", "release modern"},
		{"consecutive soft re-check answers on a rejection verdict double the interval", with(sRejection, streak(1), quiet(20), pr(probeWritten, 2, 20)), ans(2, probeSoftLegacy, 25), "rejection[2026-07-28] quiet=55 streak=2", "release modern"},
		{"soft re-check answer on borrowed versions arms the backoff", with(sBorrowed, pr(probeWritten, 2, 30)), ans(2, probeSoftLegacy, 31), "modern[2026-07-28]+borrowed quiet=46 streak=1", "release modern"},
		{"explicit legacy answer replaces a rejection verdict", with(sRejection, pr(probeWritten, 1, 0)), ans(1, probeLegacy, 5), "legacy quiet=20", "release legacy"},
		{"soft answer never downgrades a modern answer", with(sModernDual, pr(probeWritten, 2, 30)), ans(2, probeSoftLegacy, 31), "modern[2026-07-28,2025-11-25] quiet=46", "release modern"},
		{"a live modern answer replaces an explicit legacy one a late answer set", with(sLegacy, pr(probeWritten, 2, 30)), ans(2, probeModern, 31, vNew), "modern[2026-07-28] quiet=46", "release modern"},
		{"a live legacy answer replaces a modern one a late answer set", with(sModernDual, pr(probeWritten, 2, 30)), ans(2, probeLegacy, 31), "legacy quiet=46", "release legacy"},
		{"explicit legacy answer resets the backoff", with(sSoft, streak(3), pr(probeWritten, 2, 20)), ans(2, probeLegacy, 25), "legacy quiet=40", "release legacy"},
		{"re-check answer on a rejection verdict overwrites it", with(sRejection, pr(probeWritten, 2, 30), quiet(30)), ans(2, probeModern, 31, vNew, vOld), "modern[2026-07-28,2025-11-25] quiet=46", "release modern"},
		{"a probe answer replaces borrowed versions", with(sBorrowed, pr(probeWritten, 2, 30)), ans(2, probeModern, 31, vNew, vOld), "modern[2026-07-28,2025-11-25] quiet=46", "release modern"},
		{"a re-check answering modern without versions keeps borrowed versions and advances the backoff", with(sBorrowed, pr(probeWritten, 2, 25)), ans(2, probeModern, 25), "modern[2026-07-28]+borrowed quiet=40 streak=1", "release modern"},
		{"a re-check answering modern without versions keeps a rejection's list", with(sRejection, pr(probeWritten, 2, 30)), ans(2, probeModern, 31), "rejection[2026-07-28] quiet=46 streak=1", "release modern"},
		{"consecutive versionless modern re-check answers double the interval", with(sRejection, streak(1), quiet(40), pr(probeWritten, 2, 40)), ans(2, probeModern, 45), "rejection[2026-07-28] quiet=75 streak=2", "release modern"},
		{"versionless modern re-check answers decay to the cap", with(sRejection, streak(6), quiet(40), pr(probeWritten, 2, 40)), ans(2, probeModern, 45), "rejection[2026-07-28] quiet=645 streak=7", "release modern"},
		{"a re-check answer with versions replaces the kept list", with(sRejection, streak(3), quiet(20), pr(probeWritten, 2, 20)), ans(2, probeModern, 25, vNew, vOld), "modern[2026-07-28,2025-11-25] quiet=40", "release modern"},
		{"an explicit legacy re-check answer overrides the kept list", with(sRejection, streak(3), quiet(20), pr(probeWritten, 2, 20)), ans(2, probeLegacy, 25), "legacy quiet=40", "release legacy"},
		{"second consecutive soft answer: interval doubles", with(sSoft, pr(probeWritten, 2, 20)), ans(2, probeSoftLegacy, 25), "soft quiet=55 streak=2", "release legacy"},
		{"sixth consecutive soft answer: 15*2^5 = 480s", with(sSoft, streak(5), pr(probeWritten, 2, 20)), ans(2, probeSoftLegacy, 25), "soft quiet=505 streak=6", "release legacy"},
		{"seventh: 960s capped at 600s", with(sSoft, streak(6), pr(probeWritten, 2, 20)), ans(2, probeSoftLegacy, 25), "soft quiet=625 streak=7", "release legacy"},
		{"past the cap: stays at the cap", with(sSoft, streak(20), pr(probeWritten, 2, 20)), ans(2, probeSoftLegacy, 25), "soft quiet=625 streak=21", "release legacy"},
		{"soft then modern: upgraded, backoff reset", with(sSoft, streak(3), pr(probeWritten, 2, 20)), ans(2, probeModern, 25, vNew), "modern[2026-07-28] quiet=40", "release modern"},

		// --- a late answer: first answer among the outstanding ids wins ---
		{"answer to a probe whose write had not reported: applied", sUnsent, ans(1, probeModern, 16, vNew), "modern[2026-07-28] quiet=31", "-"},
		{"late answer to a pre-initialize probe: nothing", with(sUnsent, pre(), inited()), ans(1, probeModern, 16, vNew), "none init", "-"},
		{"late modern answer upgrades a timeout verdict", sTimeout, ans(1, probeModern, 20, vNew), "modern[2026-07-28] quiet=35", "-"},
		{"late explicit legacy answer makes it explicit", sTimeout, ans(1, probeLegacy, 20), "legacy quiet=35", "-"},
		{"first answer among outstanding ids wins, the rest dropped", with(sTimeout, out(2, 3)), ans(2, probeModern, 20, vNew), "modern[2026-07-28] quiet=35", "-"},
		{"a later answer to a dropped id is ignored", with(sModernOnly, seq(3)), ans(3, probeLegacy, 21), "modern[2026-07-28]", "ignored"},
		{"late explicit legacy answer overrides a rejection verdict", with(sRejection, out(1)), ans(1, probeLegacy, 20), "legacy quiet=35", "-"},
		{"late modern answer corrects a rejection verdict", with(sRejection, out(1)), ans(1, probeModern, 20, vNew, vOld), "modern[2026-07-28,2025-11-25] quiet=35", "-"},
		{"late versionless modern answer keeps a rejection's list; the other ids stay", with(sRejection, out(1, 2)), ans(1, probeModern, 20), "rejection[2026-07-28] out=[p2] quiet=35 streak=1", "-"},
		{"late soft answer does not replace a rejection verdict; the other ids stay", with(sRejection, out(1, 2)), ans(1, probeSoftLegacy, 20), "rejection[2026-07-28] out=[p2] quiet=35 streak=1", "-"},
		{"late soft answer on a timeout verdict: soft, backoff starts", sTimeout, ans(1, probeSoftLegacy, 20), "soft quiet=35 streak=1", "-"},
		{"late soft answer never shortens a running interval", with(sTimeout, quiet(100)), ans(1, probeSoftLegacy, 20), "soft quiet=100 streak=1", "-"},
		{"late soft answer under a soft verdict: counts once, the other ids dropped", mach(vd(verdictSoft), out(2, 3), quiet(20), streak(1)), ans(2, probeSoftLegacy, 30), "soft quiet=60 streak=2", "-"},
		{"late explicit legacy answer under a soft verdict: backoff reset", mach(vd(verdictSoft), out(2), quiet(20), streak(2)), ans(2, probeLegacy, 30), "legacy quiet=45", "-"},
		{"late answer to a timed-out re-check of borrowed versions applies", with(sBorrowed, out(2), quiet(60)), ans(2, probeModern, 50, vNew, vOld), "modern[2026-07-28,2025-11-25] quiet=65", "-"},
		{"late answer on a blocked write under a rejection verdict: a modern answer applies", with(sRejection, pr(probeUnsent, 2, 30)), ans(2, probeModern, 46, vNew, vOld), "modern[2026-07-28,2025-11-25] quiet=61", "-"},
		{"late soft answer on a blocked write under a rejection verdict: the verdict stands", with(sRejection, pr(probeUnsent, 2, 30)), ans(2, probeSoftLegacy, 46), "rejection[2026-07-28] quiet=61 streak=1", "-"},
		{"late answer on a blocked write under a modern answer: nothing", with(sModernDual, pr(probeUnsent, 2, 30)), ans(2, probeLegacy, 46), "modern[2026-07-28,2025-11-25]", "-"},
		{"unknown id", sTimeout, ans(9, probeModern, 20, vNew), "timeout out=[p1] quiet=30", "ignored"},
		{"empty id", sNone, eraEvent{kind: evProbeAnswer, outcome: probeModern, now: at(1)}, "none", "ignored"},
		{"answer after the process is gone", with(sNone, ended()), ans(1, probeModern, 1, vNew), "none ENDED", "ignored"},

		// --- the probe's timer ---
		{"written probe times out: legacy for now, outstanding, interval from now", sWritten, timer(1, 15), "timeout out=[p1] quiet=30", "release legacy"},
		{"a written probe's timeout clears a failed write", with(sWritten, wfail()), timer(1, 15), "timeout out=[p1] quiet=30", "release legacy"},
		{"probe never written: no verdict, no interval", sWriting, timer(1, 15), "none unsent(p1@15)", "release legacy"},
		{"timeout with remembered evidence: the rejection's verdict", with(sWritten, evid(vNew)), timer(1, 15), "rejection[2026-07-28] out=[p1] quiet=30", "release modern"},
		{"unwritten timeout with remembered evidence", with(sWriting, evid(vNew)), timer(1, 15), "rejection[2026-07-28] unsent(p1@15)", "release modern"},
		{"pre-initialize probe times out: nothing cached or outstanding", with(sWritten, pre(), inited()), timer(1, 15), "none init", "release legacy"},
		{"pre-initialize probe times out unwritten", with(sWriting, pre(), inited()), timer(1, 15), "none unsent(p1@15,pre) init", "release legacy"},
		{"re-probe timeout leaves the verdict, both ids outstanding", with(sTimeout, pr(probeWritten, 2, 30)), timer(2, 45), "timeout out=[p1,p2] quiet=60", "release legacy"},
		{"a timeout resets the soft backoff", with(sSoft, streak(3), pr(probeWritten, 2, 20)), timer(2, 35), "soft out=[p2] quiet=50", "release legacy"},
		{"a re-check timeout leaves a rejection verdict and re-arms the gate", with(sRejection, pr(probeWritten, 1, 0)), timer(1, 15), "rejection[2026-07-28] out=[p1] quiet=30", "release modern"},
		{"a re-check timeout on borrowed versions keeps its id like a rejection", with(sBorrowed, pr(probeWritten, 2, 30)), timer(2, 45), "modern[2026-07-28]+borrowed out=[p2] quiet=60", "release modern"},
		{"timeout under a final answer adds no outstanding id", with(sModernDual, pr(probeWritten, 2, 30)), timer(2, 45), "modern[2026-07-28,2025-11-25] quiet=60", "release modern"},
		{"outstanding ids capped at 4, oldest dropped", with(sTimeout, out(2, 3, 4), pr(probeWritten, 5, 60)), timer(5, 75), "timeout out=[p2,p3,p4,p5] quiet=90", "release legacy"},
		{"stale timer: probe already settled", sTimeout, timer(1, 15), "timeout out=[p1] quiet=30", "ignored"},
		{"stale timer: probe already timed out unwritten", sUnsent, timer(1, 15), "none unsent(p1@15)", "ignored"},
		{"timer after the process is gone", with(sNone, ended()), timer(1, 15), "none ENDED", "ignored"},

		// --- the probe's write returning ---
		{"write done", sWriting, wrote(1, 0), "none written(p1@15)", "-"},
		{"write failed: waiters get gone, nothing cached, the gate closes", sWriting, writeFailed(1, 0), "none quiet=15 write-failed", "release gone"},
		{"write failed with remembered evidence: the evidence stands in", with(sWriting, evid(vNew)), writeFailed(1, 0), "rejection[2026-07-28] quiet=15 write-failed", "release gone"},
		{"write of a pre-initialize probe failed: nothing armed", with(sWriting, pre(), inited()), writeFailed(1, 0), "none init", "release gone"},
		{"background probe's write fails under a timeout verdict: the gate closes", with(sTimeout, pr(probeWriting, 2, 30)), writeFailed(2, 31), "timeout out=[p1] quiet=46 write-failed", "release gone"},
		{"background probe's write fails under a soft verdict: backoff kept, gate closed", with(sSoft, streak(2), pr(probeWriting, 2, 20)), writeFailed(2, 21), "soft quiet=36 streak=2 write-failed", "release gone"},
		{"re-check write fails under a rejection verdict: the gate closes", with(sRejection, pr(probeWriting, 2, 30)), writeFailed(2, 31), "rejection[2026-07-28] quiet=46 write-failed", "release gone"},
		{"late write adopted: outstanding, interval from the write", sUnsent, wrote(1, 20), "timeout out=[p1] quiet=35", "-"},
		{"not due inside the window after an adopted write", mach(vd(verdictTimeout), out(1), quiet(35)), req(30), "timeout out=[p1] quiet=35", "answer legacy"},
		{"due once the window after an adopted write is over", mach(vd(verdictTimeout), out(1), quiet(35)), req(35), "timeout writing(p2@50) out=[p1] quiet=35", "start p2 answer legacy"},
		{"late re-probe write adopted under a timeout verdict", with(sTimeout, pr(probeUnsent, 2, 30)), wrote(2, 50), "timeout out=[p1,p2] quiet=65", "-"},
		{"an adopted write counts as a timeout: backoff reset", with(sSoft, streak(3), pr(probeUnsent, 2, 20)), wrote(2, 40), "soft out=[p2] quiet=55", "-"},
		{"write blocked through its timer, then failed: forgotten, no interval, backoff kept", with(sSoft, streak(3), pr(probeUnsent, 2, 20)), writeFailed(2, 40), "soft quiet=20 streak=3", "-"},
		{"first probe blocked through its timer, then failed: no interval", sUnsent, writeFailed(1, 20), "none", "-"},
		{"late write of a pre-initialize probe: not adopted", with(sUnsent, pre(), inited()), wrote(1, 20), "none init", "-"},
		{"late write under a final answer: not adopted", with(sModernDual, pr(probeUnsent, 2, 30)), wrote(2, 50), "modern[2026-07-28,2025-11-25]", "-"},
		{"stale write report: its probe was answered", sModernOnly, wrote(1, 1), "modern[2026-07-28]", "ignored"},
		{"duplicate write report", sWritten, wrote(1, 1), "none written(p1@15)", "ignored"},
		{"write report after the process is gone", with(sNone, ended()), wrote(1, 1), "none ENDED", "ignored"},

		// --- 2025-style traffic refused as modern-only ---
		{"refusal on a rejection verdict: background re-check", sRejection, refused(1), "rejection[2026-07-28] writing(p2@16)", "start p2"},
		{"refusal inside the interval: nothing", with(sRejection, quiet(30)), refused(20), "rejection[2026-07-28] quiet=30", "-"},
		{"refusal with a re-check out: nothing more", with(sRejection, pr(probeWritten, 2, 30)), refused(31), "rejection[2026-07-28] written(p2@45)", "-"},
		{"refusal on borrowed versions: re-check", with(sBorrowed), refused(1), "modern[2026-07-28]+borrowed writing(p2@16)", "start p2"},
		{"refusal on borrowed versions inside the interval: nothing", with(sBorrowed, quiet(40)), refused(30), "modern[2026-07-28]+borrowed quiet=40", "-"},
		{"refusal on a probe's modern-only answer: final", sModernOnly, refused(1), "modern[2026-07-28]", "ignored"},
		{"refusal after the process is gone", with(sRejection, ended()), refused(1), "rejection[2026-07-28] ENDED", "ignored"},

		// --- process replaced / exited / bridge closing ---
		{"live probe released with gone", with(sWritten, evid(vNew)), gone(), "none ENDED", "release gone"},
		{"ids and interval dropped, verdict kept", with(sModernOnly, out(1), pr(probeUnsent, 2, 0)), gone(), "modern[2026-07-28] ENDED", "-"},
		{"already gone", with(sNone, ended()), gone(), "none ENDED", "ignored"},
	}
}

// When the bridge refuses 2025 traffic itself: only once this process has
// rejected a forwarded initialize in a modern-looking way and there is
// version evidence that it speaks only 2026-era versions, in either order.
// A probe answer alone never does it.
func TestEraMachineRefusesLegacy(t *testing.T) {
	probe := func(o probeOutcome, s float64, v ...string) []eraEvent {
		return []eraEvent{wrote(1, s), ans(1, o, s, v...)}
	}
	cat := func(parts ...[]eraEvent) []eraEvent {
		var out []eraEvent
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}
	one := func(ev eraEvent) []eraEvent { return []eraEvent{ev} }
	rows := []struct {
		name     string
		events   []eraEvent
		versions string // "" when 2025 traffic is forwarded
	}{
		{"probe answer with only the modern version: forwarded", cat(one(req(0)), probe(probeModern, 1, vNew)), ""},
		{"probe answer, then a bare rejection: refused", cat(one(req(0)), probe(probeModern, 1, vNew), one(rej(rejBare, 2))), vNew},
		{"bare rejection, then the probe answer: refused", cat(one(rej(rejBare, 0)), probe(probeModern, 1, vNew)), vNew},
		{"bare rejection, no probe answer yet: forwarded", one(rej(rejBare, 0)), ""},
		{"invalid list, then a probe answer with only the modern version: refused", cat(one(rej(rejMixed, 0)), probe(probeModern, 1, vNew)), vNew},
		{"list naming a legacy version, then a probe answer with only the modern version: forwarded", cat(one(rej(rejLegacyListed, 0)), probe(probeModern, 1, vNew)), ""},
		{"probe answer with only the modern version, then a list naming a legacy version: forwarded", cat(one(req(0)), probe(probeModern, 1, vNew), one(rej(rejLegacyListed, 2))), ""},
		{"bare rejection, then a list naming a legacy version, then the probe answer: forwarded", cat(one(rej(rejBare, 0)), one(rej(rejLegacyListed, 0.5)), probe(probeModern, 1, vNew)), ""},
		{"modern-only list, then a list naming a legacy version (an initialize already in flight): forwarded", cat(one(rej(rejModernOnly, 0, vNew)), one(rej(rejLegacyListed, 0.5))), ""},
		{"list naming a legacy version, then a bare rejection, then the probe answer: refused", cat(one(rej(rejLegacyListed, 0)), one(rej(rejBare, 0.5)), probe(probeModern, 1, vNew)), vNew},
		{"rejection with a modern-only list, no probe answer: refused", one(rej(rejModernOnly, 0, "2027-01-01")), "2027-01-01"},
		{"probe answer listing a legacy version, then a bare rejection: forwarded", cat(one(req(0)), probe(probeModern, 1, vNew, vOld), one(rej(rejBare, 2))), ""},
		{"probe answer listing a legacy version clears a refusal", cat(one(rej(rejModernOnly, 0, vNew)), probe(probeModern, 1, vNew, vOld)), ""},
		{"explicit legacy probe answer clears a refusal", cat(one(rej(rejModernOnly, 0, vNew)), probe(probeLegacy, 1)), ""},
		{"re-check answering only the modern version keeps the refusal", cat(one(rej(rejModernOnly, 0, vNew)), probe(probeModern, 1, vNew)), vNew},
		{"re-check answering modern without versions keeps the refusal", cat(one(rej(rejModernOnly, 0, vNew)), probe(probeModern, 1)), vNew},
		{"an accepted initialize clears a refusal", cat(one(req(0)), probe(probeModern, 1, vNew), one(rej(rejBare, 2)), one(initAcc(vOld))), ""},
		{"no refusal after an accepted initialize", cat(one(initAcc(vOld)), one(rej(rejModernOnly, 1, vNew))), ""},
		{"a not-modern-looking rejection counts for nothing", cat(one(req(0)), probe(probeModern, 1, vNew), one(rej(rejNotModern, 2))), ""},
	}
	for _, r := range rows {
		m := mach()
		for _, ev := range r.events {
			m, _ = m.apply(ev)
		}
		versions, refuses := m.refusesLegacy()
		if got := strings.Join(versions, ","); refuses != (r.versions != "") || got != r.versions {
			t.Errorf("%s: refuses=%v versions=%q, want %q (%s)", r.name, refuses, got, r.versions, describe(m))
		}
	}

	// A refusal on the child's own final answer is not re-checked by refused
	// traffic; one resting on a rejection's list is.
	m := mach()
	for _, ev := range cat(one(rej(rejModernOnly, 0, vNew)), probe(probeModern, 1, vNew)) {
		m, _ = m.apply(ev)
	}
	if _, eff := m.apply(refused(1000)); eff.start != "" {
		t.Errorf("a refusal on a final probe answer was re-checked: %s", describe(m))
	}
	m = with(sRejection, rejected())
	if _, eff := m.apply(refused(1000)); eff.start == "" {
		t.Errorf("a refusal on a rejection's list was not re-checked: %s", describe(m))
	}
}

func TestEraMachineTable(t *testing.T) {
	for _, r := range machineRows() {
		before := describe(r.from)
		next, eff := r.from.apply(r.ev)
		got, gotEff := describe(next), describeEff(eff)
		if got != r.want || gotEff != r.eff {
			t.Errorf("%s\n  from %s\n  on   %v\n  got  %s | %s\n  want %s | %s", r.name, before, r.ev.kind, got, gotEff, r.want, r.eff)
		}
		if after := describe(r.from); after != before {
			t.Errorf("%s: apply changed its input: %s -> %s", r.name, before, after)
		}
	}
}

// The re-probe interval under steady soft-legacy answers: 15, 30, 60, ... up
// to the cap, then the cap; a timeout puts it back to the base.
func TestEraMachineBackoffSequence(t *testing.T) {
	m := mach()
	now := t0
	var gaps []string
	step := func(o probeOutcome) {
		t.Helper()
		var eff eraEffects
		m, eff = m.apply(eraEvent{kind: evModernRequest, now: now})
		if eff.start == "" {
			t.Fatalf("no probe at %s: %s", secs(now), describe(m))
		}
		id := eff.start
		m, _ = m.apply(eraEvent{kind: evWriteDone, id: id, now: now})
		if o == -1 {
			now = now.Add(tmo)
			m, _ = m.apply(eraEvent{kind: evProbeTimer, id: id, now: now})
		} else {
			m, _ = m.apply(eraEvent{kind: evProbeAnswer, id: id, outcome: o, now: now})
		}
		gaps = append(gaps, fmt.Sprint(m.quietUntil.Sub(now).Seconds()))
		// Not a moment before the interval is over.
		if _, eff := m.apply(eraEvent{kind: evModernRequest, now: m.quietUntil.Add(-time.Nanosecond)}); eff.start != "" {
			t.Fatalf("re-probe before the interval was over: %s", describe(m))
		}
		now = m.quietUntil
	}
	for i := 0; i < 9; i++ {
		step(probeSoftLegacy)
	}
	step(-1) // a timeout
	step(probeSoftLegacy)
	if got, want := strings.Join(gaps, " "), "15 30 60 120 240 480 600 600 600 15 15"; got != want {
		t.Fatalf("intervals %s, want %s", got, want)
	}
	// A cap below the probe timeout is raised to it.
	if m := newEraMachine(tmo, time.Second); m.maxInterval != tmo {
		t.Fatalf("cap %s", m.maxInterval)
	}
}

// padded copies s with two spare slots of capacity holding a sentinel.
func padded(s []string) []string {
	if s == nil {
		return nil
	}
	p := make([]string, len(s), len(s)+2)
	copy(p, s)
	full := p[:cap(p)]
	full[len(s)], full[len(s)+1] = "spare", "spare"
	return p
}

// toCap copies s up to its capacity.
func toCap(s []string) []string {
	if s == nil {
		return nil
	}
	return append([]string(nil), s[:cap(s)]...)
}

// Every named state against every event kind, checking the invariants the
// impure layer relies on. The rows above say what happens; this says nothing
// that should never happen does, for combinations the rows do not list.
func TestEraMachineInvariants(t *testing.T) {
	states := map[string]eraMachine{
		"none": sNone, "writing": sWriting, "written": sWritten, "unsent": sUnsent,
		"timeout": sTimeout, "soft": sSoft, "legacy": sLegacy, "modern-dual": sModernDual,
		"modern-only": sModernOnly, "modern-nover": sModernNoVer, "rejection": sRejection,
	}
	for name, m := range map[string]eraMachine{
		"written+ev":          with(sWritten, evid(vNew)),
		"written pre":         with(sWritten, pre(), inited()),
		"writing pre":         with(sWriting, pre(), inited()),
		"unsent pre":          with(sUnsent, pre(), inited()),
		"timeout+written":     with(sTimeout, pr(probeWritten, 2, 30)),
		"timeout+writing":     with(sTimeout, pr(probeWriting, 2, 30)),
		"timeout+unsent":      with(sTimeout, pr(probeUnsent, 2, 30)),
		"timeout full":        with(sTimeout, out(2, 3, 4), pr(probeWritten, 5, 60)),
		"soft+written":        with(sSoft, pr(probeWritten, 2, 20)),
		"rejection+written":   with(sRejection, pr(probeWritten, 1, 0)),
		"rejection+out":       with(sRejection, out(1, 2)),
		"modern+written":      with(sModernDual, pr(probeWritten, 2, 30)),
		"legacy init":         with(sLegacy, inited()),
		"ended":               with(sModernOnly, ended()),
		"ended with old data": with(sTimeout, ended()),
		"rejection quiet":     with(sRejection, quiet(30)),
		"borrowed":            mach(vd(verdictModern, vNew), seq(1), func(m *eraMachine) { m.verdict.borrowed = true }),
		"soft after timeout":  mach(vd(verdictSoft), out(2), quiet(50)),
		"soft long backoff":   with(sSoft, streak(6), quiet(500), pr(probeWritten, 2, 20)),
		"soft+writing":        with(sSoft, streak(2), pr(probeWriting, 2, 20)),
		"soft+unsent":         with(sSoft, streak(2), pr(probeUnsent, 2, 20)),
		"write failed":        mach(seq(1), quiet(15), wfail()),
		"rejection refusing":  with(sRejection, rejected()),
		"probe-only modern":   sModernOnly,
		"modern refusing":     with(sModernOnly, rejected()),
		"rejected, probe out": with(sWritten, rejected()),
		"written+wfail":       with(sWritten, wfail()),
		"borrowed+written":    with(sBorrowed, pr(probeWritten, 2, 30)),
		"borrowed+unsent":     with(sBorrowed, pr(probeUnsent, 2, 30)),
		"rejection+writing":   with(sRejection, pr(probeWriting, 2, 30)),
		"rejection+unsent":    with(sRejection, streak(2), pr(probeUnsent, 2, 30)),
	} {
		states[name] = m
	}
	var events []eraEvent
	for _, s := range []float64{0, 10, 16, 29, 30, 1000} {
		events = append(events, req(s), reqExited(s), refused(s))
		for n := 1; n <= 6; n++ {
			for _, o := range []probeOutcome{probeModern, probeLegacy, probeSoftLegacy} {
				events = append(events, ans(n, o, s, vNew), ans(n, o, s))
			}
			events = append(events, timer(n, s), wrote(n, s), writeFailed(n, s))
		}
		for _, c := range []rejectionClass{rejNotModern, rejBare, rejMixed, rejLegacyListed} {
			events = append(events, rej(c, s))
		}
		events = append(events, rej(rejModernOnly, s, vNew))
	}
	events = append(events, initAcc(vOld), initAcc(""), gone())

	rank := func(v eraVerdict) int {
		switch v.kind {
		case verdictTimeout, verdictSoft:
			return 1
		case verdictRejection:
			return 2
		case verdictModern, verdictLegacy:
			return 3
		}
		return 0
	}
	for name, base := range states {
		for _, ev := range events {
			// Each slice gets spare capacity holding a sentinel, and a deep copy
			// of everything up to capacity is kept, so a write into the input's
			// backing arrays shows up even beyond its length.
			m := base
			m.outstanding, m.evidence, m.verdict.versions = padded(m.outstanding), padded(m.evidence), padded(m.verdict.versions)
			snap, snapOut, snapEv, snapVer := m, toCap(m.outstanding), toCap(m.evidence), toCap(m.verdict.versions)
			before := describe(m)
			next, eff := m.apply(ev)
			fail := func(why string) {
				t.Errorf("%s on %v(%s): %s\n  -> %s | %s", name, ev.kind, short(ev.id), why, describe(next), describeEff(eff))
			}
			if !reflect.DeepEqual(m, snap) || !reflect.DeepEqual(toCap(m.outstanding), snapOut) ||
				!reflect.DeepEqual(toCap(m.evidence), snapEv) || !reflect.DeepEqual(toCap(m.verdict.versions), snapVer) {
				fail("input mutated")
			}
			if rank(next.verdict) < rank(m.verdict) && ev.kind != evInitAccepted {
				fail("verdict precedence went down")
			}
			if next.quietUntil.Before(m.quietUntil) && ev.kind != evInitAccepted &&
				!(ev.kind == evProbeTimer && m.probe.phase == probeWritten) &&
				!(ev.kind == evWriteDone && m.probe.phase == probeUnsent) {
				fail("the re-probe interval went backwards without a written probe's timeout")
			}
			// A probe that ends (answered, timed out after being written,
			// adopted late, failed to be written) while further probes could
			// follow on traffic must close the gate for at least an interval.
			// Exempt: a pre-initialize probe (the next request probes afresh)
			// and a write that was blocked through its timer.
			ended := (m.probe.live() && next.probe.phase == probeIdle) ||
				(m.probe.phase == probeUnsent && next.probe.phase == probeIdle && !(ev.kind == evWriteDone && ev.failed))
			if ended && !m.probe.pre && !next.ended && (next.verdict.weak() || next.verdict.recheckable()) && !next.quietUntil.After(ev.now) {
				fail("a probe ended without closing the gate for an interval")
			}
			if eff.again && !(eff.wait && m.probe.pre) {
				fail("asked to ask again without waiting on a pre-initialize probe")
			}
			if eff.release != answerNone && !m.probe.live() {
				fail("released waiters of a probe that was not live")
			}
			if m.probe.live() && !next.probe.live() && eff.release == answerNone {
				fail("a live probe went away without releasing its waiters")
			}
			if m.probe.live() && next.probe.live() && m.probe.id != next.probe.id {
				fail("a live probe was replaced")
			}
			if eff.start != "" && (m.probe.phase != probeIdle || next.probe.phase != probeWriting || next.probe.id != eff.start) {
				fail("a probe started while another was out")
			}
			if eff.wait && (!next.probe.live() || ev.kind != evModernRequest) {
				fail("a wait with no live probe")
			}
			if ev.kind == evModernRequest && eff.wait == (eff.answer != answerNone) {
				fail("a modern request must either wait or be answered")
			}
			if eff.wait && m.verdict.kind != verdictNone {
				fail("a request waited although a verdict was cached")
			}
			if m.ended && (describe(next) != before || (ev.kind != evModernRequest && eff.ignored == "")) {
				fail("an ended machine changed or did not ignore the event")
			}
			if eff.ignored != "" && describe(next) != before {
				fail("an ignored event changed the state")
			}
			if len(next.outstanding) > maxOutstandingProbes {
				fail("too many outstanding ids")
			}
			if next.evidence != nil && (next.verdict.kind != verdictNone || !next.probe.live() || next.initOK) {
				fail("evidence held without a live probe and no verdict")
			}
			if next.verdict.kind == verdictRejection && !onlyModernVersions(next.verdict.versions) {
				fail("a rejection verdict without modern-only versions")
			}
			if _, refuses := next.refusesLegacy(); refuses && (!next.rejectedInit || next.initOK) {
				fail("refuses 2025 traffic without a rejected forwarded initialize, or after an accepted one")
			}
			if _, refuses := next.refusesLegacy(); refuses && !(m.rejectedInit || ev.kind == evInitRejected) {
				fail("refuses 2025 traffic although no initialize was ever rejected")
			}
			if next.verdict.final() && len(next.outstanding) > len(m.outstanding) {
				fail("outstanding ids added under a probe answer")
			}
			if next.quietUntil.Sub(t0) > 1000*time.Second+tmoCap {
				fail("interval beyond the cap")
			}
		}
	}
}

// A child that answers every probe "modern" without versions and rejects
// every initialize with a modern-only list, under a 2025 client sending one
// initialize a second for ten simulated minutes: the list, once cached, is
// re-checked on the backoff schedule and never dropped, so only the first
// initialize is forwarded.
func TestEraMachineVersionlessRecheckKeepsTheList(t *testing.T) {
	m := mach()
	now := t0
	forwarded, probes := 0, 0
	feed := func(ev eraEvent) {
		var eff eraEffects
		m, eff = m.apply(ev)
		if eff.start == "" {
			return
		}
		probes++
		m, _ = m.apply(eraEvent{kind: evWriteDone, id: eff.start, now: now})
		m, _ = m.apply(eraEvent{kind: evProbeAnswer, id: eff.start, outcome: probeModern, now: now.Add(10 * time.Millisecond)})
	}
	for s := 0; s < 600; s++ {
		now = t0.Add(time.Duration(s) * time.Second)
		if m.verdict.modernOnly() {
			feed(eraEvent{kind: evLegacyRefused, now: now})
		} else {
			forwarded++
			feed(eraEvent{kind: evInitRejected, rejClass: rejModernOnly, versions: []string{vNew}, now: now})
		}
	}
	// Probes at 0, 15, 45, 105, 225 and 465s.
	if forwarded != 1 || probes != 6 {
		t.Fatalf("%d initializes forwarded, %d probes; want 1 and 6 (%s)", forwarded, probes, describe(m))
	}
}

// Start-to-start spacing over pseudo-random walks: whatever the traffic and
// however the child behaves, two probe starts on one process are at least a
// probe timeout apart. Exempt, by design:
//   - the first start after the first successful initialize, which drops the
//     interval so the initialized child is asked afresh;
//   - the first start after a write that was blocked through its timer and
//     then failed: no probe could start while it was blocked, and it started
//     no interval.
//
// The walks also deliver answers to probes whose write has not reported yet,
// let traffic arrive between a probe's deadline and its timer, and include a
// child that always answers soft-legacy on a machine with a small backoff cap
// and no accepted initialize, so walks sit at the cap. The counts at the end
// make sure those states are actually reached.
func TestEraMachineProbeStartsAreSpaced(t *testing.T) {
	behaviours := []string{"silent", "soft", "modern-without-versions", "mixed", "failing-writes", "blocked-writes", "late", "soft-at-cap"}
	outcome := func(rng *rand.Rand, beh string) (probeOutcome, []string, bool) {
		switch beh {
		case "silent", "failing-writes", "blocked-writes":
			return 0, nil, false
		case "soft", "soft-at-cap":
			return probeSoftLegacy, nil, true
		case "modern-without-versions":
			return probeModern, nil, true
		}
		switch rng.Intn(6) {
		case 0, 1:
			return probeSoftLegacy, nil, true
		case 2, 3:
			return probeModern, nil, true
		case 4:
			return probeLegacy, nil, true
		}
		return probeModern, []string{vNew, vOld}, true
	}
	type tally struct{ starts, recheckStarts, capStarts, unsentAnswers, dueRequests int }
	counts := map[string]*tally{}
	for _, beh := range behaviours {
		counts[beh] = &tally{}
	}
	defer func() {
		// The walks must reach the states that matter, or the check is empty.
		// Floors sit well below the observed counts.
		unsent, due := 0, 0
		for _, beh := range behaviours {
			c := counts[beh]
			t.Logf("%s: %+v", beh, *c)
			if c.starts < 200 {
				t.Errorf("%s: only %d probe starts", beh, c.starts)
			}
			unsent += c.unsentAnswers
			due += c.dueRequests
		}
		for _, beh := range []string{"modern-without-versions", "mixed", "late"} {
			if counts[beh].recheckStarts < 30 {
				t.Errorf("%s: only %d starts on a verdict taken from a rejection", beh, counts[beh].recheckStarts)
			}
		}
		if c := counts["soft-at-cap"].capStarts; c < 200 {
			t.Errorf("soft-at-cap: only %d starts with the backoff at its cap", c)
		}
		if unsent < 150 {
			t.Errorf("only %d answers to probes whose write had not reported", unsent)
		}
		if due < 150 {
			t.Errorf("only %d modern requests between a probe's deadline and its timer", due)
		}
	}()
	for seed := int64(0); seed < 800; seed++ {
		rng := rand.New(rand.NewSource(seed))
		beh := behaviours[seed%int64(len(behaviours))]
		c := counts[beh]
		m := mach()
		if beh == "soft-at-cap" {
			m = newEraMachine(tmo, 4*tmo)
		}
		now := t0
		var last time.Time
		started, exempt := false, false
		feed := func(ev eraEvent) {
			ev.now = now
			if ev.kind == evModernRequest && m.probe.live() && !now.Before(m.probe.deadline) {
				c.dueRequests++
			}
			next, eff := m.apply(ev)
			if eff.start != "" {
				if started && !exempt && now.Sub(last) < tmo {
					t.Fatalf("seed %d (%s): probes started %s apart\n  from %s\n  on   %v\n  to   %s", seed, beh, now.Sub(last), describe(m), ev.kind, describe(next))
				}
				last, started, exempt = now, true, false
				c.starts++
				if m.verdict.recheckable() {
					c.recheckStarts++
				}
				if m.softStreak >= 3 && m.reprobeInterval() == m.maxInterval {
					c.capStarts++
				}
			}
			if ev.kind == evInitAccepted && !m.initOK {
				exempt = true
			}
			if ev.kind == evWriteDone && ev.failed && m.probe.phase == probeUnsent && ev.id == m.probe.id {
				exempt = true
			}
			m = next
		}
		for step := 0; step < 500; step++ {
			// A due timer usually fires next, but traffic can come first.
			if m.probe.live() && !now.Before(m.probe.deadline) && rng.Intn(2) == 0 {
				feed(eraEvent{kind: evProbeTimer, id: m.probe.id})
				continue
			}
			switch rng.Intn(12) {
			case 0, 1, 2:
				if rng.Intn(8) == 0 {
					now = now.Add(time.Duration(rng.Intn(60)) * time.Second)
				} else {
					now = now.Add(time.Duration(rng.Intn(3000)) * time.Millisecond)
				}
			case 3:
				feed(eraEvent{kind: evModernRequest})
			case 4:
				feed(eraEvent{kind: evLegacyRefused})
			case 5:
				cls := []rejectionClass{rejNotModern, rejBare, rejMixed, rejModernOnly, rejLegacyListed}[rng.Intn(5)]
				var v []string
				if cls == rejModernOnly {
					v = []string{vNew}
				}
				feed(eraEvent{kind: evInitRejected, rejClass: cls, versions: v})
			case 6, 7:
				switch p := m.probe; p.phase {
				case probeWriting:
					switch beh {
					case "failing-writes":
						feed(eraEvent{kind: evWriteDone, id: p.id, failed: true})
					case "blocked-writes":
					case "soft-at-cap":
						// Answered at once, so no timeout resets the backoff.
						feed(eraEvent{kind: evWriteDone, id: p.id})
						feed(eraEvent{kind: evProbeAnswer, id: p.id, outcome: probeSoftLegacy})
					default:
						feed(eraEvent{kind: evWriteDone, id: p.id})
					}
				case probeWritten:
					if beh != "late" {
						if o, v, ok := outcome(rng, beh); ok {
							feed(eraEvent{kind: evProbeAnswer, id: p.id, outcome: o, versions: v})
						}
					}
				case probeUnsent:
					if rng.Intn(3) == 0 {
						// The write landed and the child answered before the
						// write goroutine could report.
						o, v, ok := outcome(rng, beh)
						if !ok {
							o, v, _ = outcome(rng, "mixed")
						}
						c.unsentAnswers++
						feed(eraEvent{kind: evProbeAnswer, id: p.id, outcome: o, versions: v})
					} else {
						feed(eraEvent{kind: evWriteDone, id: p.id, failed: rng.Intn(2) == 0})
					}
				}
			case 8, 9:
				if len(m.outstanding) > 0 && (beh == "late" || beh == "mixed") {
					o, v, _ := outcome(rng, "mixed")
					feed(eraEvent{kind: evProbeAnswer, id: m.outstanding[rng.Intn(len(m.outstanding))], outcome: o, versions: v})
				}
			case 10:
				if beh != "soft-at-cap" && rng.Intn(10) == 0 {
					feed(eraEvent{kind: evInitAccepted})
				}
			case 11:
				feed(eraEvent{kind: evModernRequest})
			}
		}
	}
}
