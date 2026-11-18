package bridge

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// The era machine's executable spec: for each state and event, the next state
// and the effects. No processes, HTTP or timers. A rule change should show up
// here as a row diff.
//
// States print as
//
//	<phase>[versions][ inits=<n>][ held=[...]][ init-ok][ forced][ mismatch][ ENDED]
//
// and effects as
//
//	[decided][ to-v2][ no-v1][ log "<line>"...][ ignored]   ("-" for none).

const (
	vNew  = "2026-07-28"
	vNext = "2027-01-15"
)

type mopt func(*eraMachine)

func mach(phase eraPhase, opts ...mopt) eraMachine {
	m := eraMachine{phase: phase}
	for _, o := range opts {
		o(&m)
	}
	return m
}

func vers(v ...string) mopt { return func(m *eraMachine) { m.versions = v } }
func inits(n int) mopt      { return func(m *eraMachine) { m.inits = n } }
func held(v ...string) mopt { return func(m *eraMachine) { m.held = v } }
func initOK() mopt          { return func(m *eraMachine) { m.initOK = true } }
func ended() mopt           { return func(m *eraMachine) { m.ended = true } }
func forced() mopt          { return func(m *eraMachine) { m.forced = true } }
func mismatch() mopt        { return func(m *eraMachine) { m.mismatch = true } }
func answer(v ...string) eraEvent {
	return eraEvent{kind: evAnswer, versions: v}
}
func initFailed(v ...string) eraEvent {
	return eraEvent{kind: evInitFailed, versions: v}
}
func clientDiscover(v ...string) eraEvent {
	return eraEvent{kind: evClientDiscover, versions: v}
}

var (
	timeout  = eraEvent{kind: evTimeout}
	initSent = eraEvent{kind: evInitSent}
	initDone = eraEvent{kind: evInitOK}
	gone     = eraEvent{kind: evGone}
)

func describe(m eraMachine) string {
	var b strings.Builder
	b.WriteString([]string{"pending", "v2", "v1", "timeout"}[m.phase])
	if m.versions != nil {
		fmt.Fprintf(&b, "[%s]", strings.Join(m.versions, ","))
	}
	if m.inits != 0 {
		fmt.Fprintf(&b, " inits=%d", m.inits)
	}
	if m.held != nil {
		fmt.Fprintf(&b, " held=[%s]", strings.Join(m.held, ","))
	}
	if m.initOK {
		b.WriteString(" init-ok")
	}
	if m.forced {
		b.WriteString(" forced")
	}
	if m.mismatch {
		b.WriteString(" mismatch")
	}
	if m.ended {
		b.WriteString(" ENDED")
	}
	return b.String()
}

func effects(e eraEffects) string {
	var parts []string
	if e.decided {
		parts = append(parts, "decided")
	}
	if e.toV2 {
		parts = append(parts, "to-v2")
	}
	if e.noV1 {
		parts = append(parts, "no-v1")
	}
	for _, line := range e.log {
		parts = append(parts, fmt.Sprintf("log %q", line))
	}
	if e.ignored != "" {
		parts = append(parts, "ignored")
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, " ")
}

type eraRowCase struct {
	name string
	from eraMachine
	ev   eraEvent
	to   eraMachine
	eff  string
}

func eraTable() []eraRowCase {
	v2New := mach(phaseV2, vers(vNew))
	v1 := mach(phaseV1)
	tmo := mach(phaseTimeout)
	logV2 := fmt.Sprintf("log %q", logSpeaksV2([]string{vNew}))
	logLate := fmt.Sprintf("log %q", logAnsweredLate([]string{vNew}))
	logAfterV1 := fmt.Sprintf("log %q", logLateAfterV1([]string{vNew}))
	logV1 := fmt.Sprintf("log %q", logSpeaksV1)
	fv1, fv1Found := mach(phaseV1, forced()), mach(phaseV1, forced(), mismatch())
	fv2, fv2Found := mach(phaseV2, vers(vNew), forced()), mach(phaseV2, vers(vNew), forced(), mismatch())
	logForcedV2 := fmt.Sprintf("log %q", logForcedV2Mismatch)
	return []eraRowCase{
		// Pending: the first answer or the timer decides.
		{"modern answer", mach(phasePending), answer(vNew), v2New, "decided to-v2 " + logV2},
		{"modern answer, several versions", mach(phasePending), answer(vNew, vNext), mach(phaseV2, vers(vNew, vNext)),
			fmt.Sprintf("decided to-v2 log %q", logSpeaksV2([]string{vNew, vNext}))},
		{"any other answer", mach(phasePending), answer(), v1, fmt.Sprintf("decided log %q", logSpeaksV1)},
		{"timeout", mach(phasePending), timeout, tmo, fmt.Sprintf("decided log %q", logNoAnswer)},
		{"pending gone", mach(phasePending), gone, mach(phasePending, ended()), "decided"},

		// v2 and answered v1 are final: no flip from an answered v1. The
		// timer can race the answer that decided, and initializes counted
		// while v1 for now can still be answered.
		{"v2 answer", v2New, answer(), v2New, "ignored"},
		{"v2 stale timer", v2New, timeout, v2New, "ignored"},
		{"v2 init-ok", v2New, initDone, v2New, "ignored"},
		{"v2 init-failed", v2New, initFailed(vNext), v2New, "ignored"},
		{"v2 gone", v2New, gone, mach(phaseV2, vers(vNew), ended()), "-"},
		{"v1 answer", v1, answer(vNew), v1, "ignored"},
		{"v1 stale timer", v1, timeout, v1, "ignored"},
		{"v1 init-ok", mach(phaseV1, inits(1)), initDone, mach(phaseV1, inits(1)), "ignored"},
		{"v1 qualifying rejection", mach(phaseV1, inits(1)), initFailed(vNew), mach(phaseV1, inits(1)), "ignored"},
		{"v1 gone", v1, gone, mach(phaseV1, ended()), "-"},

		// Timeout: v1 for now.
		{"late modern answer", tmo, answer(vNew), v2New, "to-v2 " + logLate},
		{"late other answer", tmo, answer(), v1, logV1},
		{"late other answer drops held", mach(phaseTimeout, inits(1), held(vNew)), answer(), mach(phaseV1, inits(1)), logV1},
		// The first successful initialize already logged the v1 line.
		{"late other answer after init-ok", mach(phaseTimeout, initOK()), answer(), mach(phaseV1, initOK()), "-"},
		{"init-sent", tmo, initSent, mach(phaseTimeout, inits(1)), "-"},
		{"second init-sent", mach(phaseTimeout, inits(1)), initSent, mach(phaseTimeout, inits(2)), "-"},
		// A late modern answer after the first initialize succeeded.
		{"late modern answer after init-ok", mach(phaseTimeout, initOK()), answer(vNew), mach(phaseTimeout, initOK()), logAfterV1},
		// Held while an initialize is in flight.
		{"late modern answer held", mach(phaseTimeout, inits(1)), answer(vNew), mach(phaseTimeout, inits(1), held(vNew)), "-"},
		{"held, init succeeds", mach(phaseTimeout, inits(1), held(vNew)), initDone, mach(phaseTimeout, initOK()), logV1 + " " + logAfterV1},
		{"held, init fails", mach(phaseTimeout, inits(1), held(vNew)), initFailed(), v2New, "to-v2 " + logLate},
		{"held, one of two inits fails", mach(phaseTimeout, inits(2), held(vNew)), initFailed(), mach(phaseTimeout, inits(1), held(vNew)), "-"},
		// The rejection's list wins over the held one; the answer was late.
		{"held, qualifying rejection", mach(phaseTimeout, inits(1), held(vNew)), initFailed(vNext), mach(phaseV2, vers(vNext)),
			fmt.Sprintf("to-v2 log %q", logAnsweredLate([]string{vNext}))},
		// The one flip.
		{"qualifying rejection", mach(phaseTimeout, inits(1)), initFailed(vNew), v2New, "to-v2 " + logV2},
		{"qualifying rejection with another init in flight", mach(phaseTimeout, inits(2)), initFailed(vNew), v2New, "to-v2 " + logV2},
		{"non-qualifying rejection", mach(phaseTimeout, inits(1)), initFailed(), tmo, "-"},
		{"qualifying rejection after init-ok", mach(phaseTimeout, inits(1), initOK()), initFailed(vNew), mach(phaseTimeout, initOK()), "-"},
		// The first success is the verdict; a later one, of an initialize
		// counted before it, says nothing more.
		{"init-ok", mach(phaseTimeout, inits(1)), initDone, mach(phaseTimeout, initOK()), logV1},
		{"second init-ok", mach(phaseTimeout, inits(1), initOK()), initDone, mach(phaseTimeout, initOK()), "-"},
		{"timeout gone", mach(phaseTimeout, inits(1), held(vNew)), gone, mach(phaseTimeout, inits(1), ended()), "-"},

		// Forced v1: final from spawn. Only a rejection naming v2 alone says
		// the child does not speak v1, once.
		{"forced v1 init-ok", fv1, initDone, fv1, "-"},
		{"forced v1 non-qualifying rejection", fv1, initFailed(), fv1, "-"},
		{"forced v1 qualifying rejection", fv1, initFailed(vNew), fv1Found,
			fmt.Sprintf("no-v1 log %q", logForcedV1Mismatch([]string{vNew}))},
		{"forced v1 qualifying rejection, several versions", fv1, initFailed(vNew, vNext), fv1Found,
			fmt.Sprintf("no-v1 log %q", logForcedV1Mismatch([]string{vNew, vNext}))},
		// An initialize already in flight when the mismatch was found.
		{"forced v1 second qualifying rejection", fv1Found, initFailed(vNew), fv1Found, "ignored"},
		{"forced v1 init-ok after the mismatch", fv1Found, initDone, fv1Found, "ignored"},
		{"forced v1 gone", fv1, gone, mach(phaseV1, forced(), ended()), "-"},
		{"forced v1 gone after the mismatch", fv1Found, gone, mach(phaseV1, forced(), mismatch(), ended()), "-"},

		// Forced v2: final from spawn. Only an answer to a client's
		// server/discover that is not modern says the child does not speak v2,
		// once.
		{"forced v2 modern client discover", fv2, clientDiscover(vNew), fv2, "-"},
		{"forced v2 other client discover", fv2, clientDiscover(), fv2Found, logForcedV2},
		// A server/discover already in flight when the mismatch was found.
		{"forced v2 second other client discover", fv2Found, clientDiscover(), fv2Found, "ignored"},
		{"forced v2 modern client discover after the mismatch", fv2Found, clientDiscover(vNew), fv2Found, "ignored"},
		{"forced v2 gone", fv2, gone, mach(phaseV2, vers(vNew), forced(), ended()), "-"},
		{"forced v2 gone after the mismatch", fv2Found, gone, mach(phaseV2, vers(vNew), forced(), mismatch(), ended()), "-"},

		// Ended: nothing applies.
		{"ended answer", mach(phasePending, ended()), answer(vNew), mach(phasePending, ended()), "ignored"},
		{"ended timer", mach(phasePending, ended()), timeout, mach(phasePending, ended()), "ignored"},
		{"ended init-ok", mach(phaseTimeout, inits(1), ended()), initDone, mach(phaseTimeout, inits(1), ended()), "ignored"},
		{"ended init-failed", mach(phaseTimeout, inits(1), ended()), initFailed(vNew), mach(phaseTimeout, inits(1), ended()), "ignored"},
		{"ended client discover", mach(phaseV2, vers(vNew), forced(), ended()), clientDiscover(), mach(phaseV2, vers(vNew), forced(), ended()), "ignored"},
		{"ended gone", mach(phaseV1, ended()), gone, mach(phaseV1, ended()), "ignored"},
	}
}

func TestEraMachine_Table(t *testing.T) {
	for _, c := range eraTable() {
		if !feedable(c.from, c.ev) {
			t.Errorf("%s: the bridge cannot feed %s to %s", c.name, c.ev.kind, describe(c.from))
		}
		got, eff := c.from.apply(c.ev)
		if describe(got) != describe(c.to) || effects(eff) != c.eff {
			t.Errorf("%s: %s + %s\n  want %s / %s\n  got  %s / %s",
				c.name, describe(c.from), c.ev.kind, describe(c.to), c.eff, describe(got), effects(eff))
		}
	}
}

// Every phase, automatic or forced, with and without a mismatch found, live
// and ended (the ended ones counted as one), meets every event the bridge can
// feed it in some row.
func TestEraMachine_TableCoversEveryFeedablePair(t *testing.T) {
	pair := func(m eraMachine, k eraEventKind) string {
		if m.ended {
			return fmt.Sprintf("an ended machine + %s", k)
		}
		return fmt.Sprintf("%s + %s", describe(eraMachine{phase: m.phase, forced: m.forced, mismatch: m.mismatch}), k)
	}
	rows := map[string]bool{}
	for _, c := range eraTable() {
		rows[pair(c.from, c.ev.kind)] = true
	}
	for _, m := range reachable() {
		for _, ev := range sweepEvents {
			if p := pair(m, ev.kind); feedable(m, ev) && !rows[p] {
				t.Errorf("no row for %s", p)
				rows[p] = true
			}
		}
	}
}

// sweepEvents is every event the bridge can feed, with the version lists that
// matter.
var sweepEvents = []eraEvent{
	answer(vNew), answer(vNew, vNext), answer(), timeout, initSent, initDone, initFailed(), initFailed(vNext),
	clientDiscover(vNew), clientDiscover(), gone,
}

// feedable reports whether the bridge can feed ev to m. The child can answer
// the probe at any time, and the probe's one timer can fire until it has,
// racing whatever decided first. An initialize is counted only while v1 for
// now and before one succeeded, and the child's answer to a counted one comes
// in any later state; a flip to v2 forgets how many are still in flight, so
// there any number may come. A forced process has no probe and no timer; it
// forwards initializes only as v1 and client server/discovers only as v2, and
// the answers to those already in flight can still come once its mismatch is
// found or it has ended.
func feedable(m eraMachine, ev eraEvent) bool {
	switch ev.kind {
	case evAnswer:
		return !m.forced
	case evTimeout:
		return !m.forced && m.phase != phaseTimeout
	case evInitSent:
		return m.countsInits() && m.inits < 3
	case evInitOK, evInitFailed:
		if m.forced {
			return m.phase == phaseV1
		}
		return m.phase == phaseV2 || m.phase != phasePending && m.inits > 0
	case evClientDiscover:
		return m.forced && m.phase == phaseV2
	}
	return true
}

// reachable is every state reachable from a fresh machine of each ServeAs,
// with at most three initializes in flight.
func reachable() []eraMachine {
	seen := map[string]bool{}
	var out []eraMachine
	queue := []eraMachine{newEraMachine(ServeAuto), newEraMachine(ServeV1), newEraMachine(ServeV2)}
	for len(queue) > 0 {
		m := queue[0]
		queue = queue[1:]
		if seen[describe(m)] {
			continue
		}
		seen[describe(m)] = true
		out = append(out, m)
		for _, ev := range sweepEvents {
			if !feedable(m, ev) {
				continue
			}
			next, _ := m.apply(ev)
			queue = append(queue, next)
		}
	}
	return out
}

func TestEraMachine_Invariants(t *testing.T) {
	states := reachable()
	phases := map[eraPhase]bool{}
	applied := map[eraEventKind]bool{}
	for _, m := range states {
		phases[m.phase] = true
		switch {
		case m.inits < 0:
			t.Errorf("%s: negative initializes in flight", describe(m))
		case m.held != nil && (m.phase != phaseTimeout || m.inits == 0 || m.initOK):
			t.Errorf("%s: a held answer outside an initialize in flight", describe(m))
		case m.phase == phaseV2 && len(m.versions) == 0:
			t.Errorf("%s: v2 with nothing to name", describe(m))
		case m.forced && m.phase != phaseV1 && m.phase != phaseV2:
			t.Errorf("%s: forced without a version", describe(m))
		case m.mismatch && !m.forced:
			t.Errorf("%s: a mismatch on automatic", describe(m))
		}
		for _, ev := range sweepEvents {
			if !feedable(m, ev) {
				continue
			}
			before := describe(m)
			snapshot := append([]string(nil), m.versions...)
			next, eff := m.apply(ev)
			if describe(m) != before || !reflect.DeepEqual(append([]string(nil), m.versions...), snapshot) {
				t.Errorf("%s + %s: apply changed its input", before, ev.kind)
			}
			if eff.ignored == "" {
				applied[ev.kind] = true
			}
			if eff.decided != (m.phase == phasePending && !m.ended && (ev.kind == evAnswer || ev.kind == evTimeout || ev.kind == evGone)) {
				t.Errorf("%s + %s: decided=%v", before, ev.kind, eff.decided)
			}
			if eff.toV2 != (next.phase == phaseV2 && m.phase != phaseV2) {
				t.Errorf("%s + %s: to-v2=%v but %s", before, ev.kind, eff.toV2, describe(next))
			}
			if (m.phase == phaseV2 || m.phase == phaseV1 || m.ended) && next.phase != m.phase {
				t.Errorf("%s + %s: a final verdict changed to %s", before, ev.kind, describe(next))
			}
			if m.ended && (!next.ended || eff.ignored == "") {
				t.Errorf("%s + %s: an ended machine took an event", before, ev.kind)
			}
			if next.version() == versionUnknown && next.phase != phasePending {
				t.Errorf("%s: unknown version outside pending", describe(next))
			}
			found := next.mismatch && !m.mismatch
			if next.forced != m.forced || m.mismatch && !next.mismatch {
				t.Errorf("%s + %s: forced or mismatch undone: %s", before, ev.kind, describe(next))
			}
			if eff.noV1 != (found && m.phase == phaseV1) {
				t.Errorf("%s + %s: no-v1=%v", before, ev.kind, eff.noV1)
			}
			// A forced process logs one line, on finding the mismatch, and
			// nothing else.
			if m.forced && (len(eff.log) == 1) != found || m.forced && len(eff.log) > 1 {
				t.Errorf("%s + %s: a forced process logged %q", before, ev.kind, eff.log)
			}
		}
	}
	for p := phasePending; p <= phaseTimeout; p++ {
		if !phases[p] {
			t.Errorf("phase %s is unreachable", describe(mach(p)))
		}
	}
	for k := evAnswer; k <= evGone; k++ {
		if !applied[k] {
			t.Errorf("event %s never changes anything", k)
		}
	}
}

// What counts as a modern discover answer, and the versions it names.
func TestDiscoverVersions(t *testing.T) {
	errMsg := func(code any, data map[string]any) map[string]any {
		e := map[string]any{"code": code, "message": "x"}
		if data != nil {
			e["data"] = data
		}
		return map[string]any{"error": e}
	}
	result := func(versions any) map[string]any {
		return map[string]any{"result": map[string]any{"supportedVersions": versions}}
	}
	for _, c := range []struct {
		name string
		msg  map[string]any
		want string
	}{
		{"modern only", result([]any{vNew}), vNew},
		{"modern and 2025", result([]any{vNew, "2025-11-25"}), vNew},
		{"several modern", result([]any{vNew, vNext}), vNew + "," + vNext},
		{"draft beside a modern one", result([]any{"draft", vNew}), vNew},
		{"only 2025", result([]any{"2025-11-25", "2025-06-18"}), ""},
		{"only draft", result([]any{"draft"}), ""},
		{"empty list", result([]any{}), ""},
		{"not a date", result([]any{"2026-7-28"}), ""},
		{"not a list", result(vNew), ""},
		{"no supportedVersions", map[string]any{"result": map[string]any{}}, ""},
		{"-32022 with a modern list", errMsg(-32022.0, map[string]any{"supported": []any{vNew}}), vNew},
		{"bare -32022", errMsg(-32022.0, nil), ""},
		{"-32022 with only 2025", errMsg(-32022.0, map[string]any{"supported": []any{"2025-11-25"}}), ""},
		{"-32022 as a fraction", errMsg(-32022.5, map[string]any{"supported": []any{vNew}}), ""},
		{"-32022 as a string", errMsg("-32022", map[string]any{"supported": []any{vNew}}), ""},
		// Every other error is v1.
		{"-32601", errMsg(-32601.0, nil), ""},
		{"-32602", errMsg(-32602.0, nil), ""},
		{"-32603", errMsg(-32603.0, nil), ""},
		{"-32020", errMsg(-32020.0, map[string]any{"supported": []any{vNew}}), ""},
		{"-32021", errMsg(-32021.0, nil), ""},
		{"neither result nor error", map[string]any{"jsonrpc": "2.0"}, ""},
	} {
		if got := strings.Join(discoverVersions(c.msg), ","); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
}

// Only a rejection naming modern versions alone is evidence.
func TestRejectionVersions(t *testing.T) {
	rej := func(code float64, supported any) map[string]any {
		e := map[string]any{"code": code, "message": "x"}
		if supported != nil {
			e["data"] = map[string]any{"supported": supported}
		}
		return map[string]any{"error": e}
	}
	for _, c := range []struct {
		name string
		msg  map[string]any
		want string
	}{
		{"-32022 modern only", rej(-32022, []any{vNew}), vNew},
		{"-32602 modern only", rej(-32602, []any{vNew, vNext}), vNew + "," + vNext},
		{"bare -32601", rej(-32601, nil), ""},
		{"bare -32022", rej(-32022, nil), ""},
		{"empty list", rej(-32022, []any{}), ""},
		{"a 2025 version too", rej(-32022, []any{vNew, "2025-11-25"}), ""},
		{"draft too", rej(-32022, []any{vNew, "draft"}), ""},
		{"a non-string entry", rej(-32022, []any{vNew, 7.0}), ""},
		{"not a list", rej(-32022, vNew), ""},
	} {
		if got := strings.Join(rejectionVersions(c.msg), ","); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
}

// The bridge's own words for a child that names two modern versions.
func TestMessages_SeveralVersions(t *testing.T) {
	two := []string{vNew, vNext}
	for _, c := range []struct{ got, want string }{
		{v2InitializeRefusal(two), "This server speaks MCP v2 (protocols 2026-07-28, 2027-01-15) and does not accept the older MCP v1 initialize handshake."},
		{v2RequestRefusal(two), "This server speaks MCP v2 (protocols 2026-07-28, 2027-01-15) and does not accept older MCP v1 requests."},
		{v2NoSessions(two), "This server speaks MCP v2 (protocols 2026-07-28, 2027-01-15), which has no sessions and no GET stream."},
		{logSpeaksV2(two), "The server speaks MCP v2 (protocols 2026-07-28, 2027-01-15)."},
		{logAnsweredLate(two), "The server answered late; it speaks MCP v2 (protocols 2026-07-28, 2027-01-15)."},
		{logLateAfterV1(two), "The server said it speaks MCP v2 (protocols 2026-07-28, 2027-01-15) after an MCP v1 client had connected; it stays MCP v1 until it restarts."},
		{logRefusedClient("Claude", two), `The client "Claude" uses MCP v1 and was refused: this server speaks MCP v2 (protocols 2026-07-28, 2027-01-15).`},
		{logRefusedNamelessClient(two), "A client uses MCP v1 and was refused: this server speaks MCP v2 (protocols 2026-07-28, 2027-01-15)."},
		{logForcedV1Mismatch(two), `This server is set to MCP v1, but it does not speak it: it accepts only MCP v2 (protocols 2026-07-28, 2027-01-15). Set its MCP version to Automatic in its config entry ("mcp_version": "auto").`},
	} {
		if c.got != c.want {
			t.Errorf("got  %q\nwant %q", c.got, c.want)
		}
	}
}

// The bridge's own words about a server's forced version, and the sentence
// the refused-client line gains on automatic.
func TestMessages_ForcedVersion(t *testing.T) {
	for _, c := range []struct{ got, want string }{
		{logForcedV1Mismatch([]string{vNew}), `This server is set to MCP v1, but it does not speak it: it accepts only MCP v2 (protocol 2026-07-28). Set its MCP version to Automatic in its config entry ("mcp_version": "auto").`},
		{logForcedV2Mismatch, `This server is set to MCP v2 (protocol 2026-07-28), but it does not speak it. Set its MCP version to Automatic in its config entry ("mcp_version": "auto").`},
		{forcedV1ErrorCode, "MCP_VERSION"},
		{forcedV1ErrorMessage + " " + suggestAuto, `This server doesn't speak MCP v1. Set its MCP version to Automatic in its config entry ("mcp_version": "auto").`},
		{suggestV1, `If the server also supports v1, set its MCP version to v1 in its config entry ("mcp_version": "v1").`},
	} {
		if c.got != c.want {
			t.Errorf("got  %q\nwant %q", c.got, c.want)
		}
	}
}

// A forced process starts decided, with its version, and no probe out.
func TestEraMachine_ForcedStart(t *testing.T) {
	for _, c := range []struct {
		as      ServeAs
		version mcpVersion
		want    string
	}{
		{ServeAuto, versionUnknown, "pending"},
		{ServeV1, versionV1, "v1 forced"},
		{ServeV2, versionV2, "v2[2026-07-28] forced"},
	} {
		st := newEraState(newBareChild(), c.as)
		if got := describe(st.m); got != c.want || st.m.version() != c.version {
			t.Errorf("ServeAs %d: %s (version %d), want %s (version %d)", c.as, got, st.m.version(), c.want, c.version)
		}
		select {
		case <-st.decided:
			if c.as == ServeAuto {
				t.Errorf("ServeAuto: decided before the probe's answer")
			}
		default:
			if c.as != ServeAuto {
				t.Errorf("ServeAs %d: not decided at creation", c.as)
			}
		}
	}
}
