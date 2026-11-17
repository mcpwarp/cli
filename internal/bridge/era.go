package bridge

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// modernProtocolVersion is the first stateless MCP revision. Any
	// supportedVersions entry that sorts before it (the revisions are
	// YYYY-MM-DD) is a 2025-era, initialize-based version.
	modernProtocolVersion = "2026-07-28"

	protocolVersionMetaKey    = "io.modelcontextprotocol/protocolVersion"
	clientCapabilitiesMetaKey = "io.modelcontextprotocol/clientCapabilities"
	clientInfoMetaKey         = "io.modelcontextprotocol/clientInfo"

	// probeIDPrefix marks the bridge-owned id of a server/discover probe.
	// Client ids are always rewritten to integers before reaching the child,
	// so a string id with this prefix can never be a client's.
	probeIDPrefix = "mcpwarp-probe-"

	// defaultProbeTimeout bounds the lazy server/discover probe. A cold
	// stdio child can take a good while to start; giving up earlier would
	// pin its first 2026 request on a legacy verdict. The wait belongs to
	// the request itself (discover runs after ensureLiveChild has returned,
	// with its own timer), not to ensureChildTimeout. A modern child slower
	// than this is picked up by the late answer (no cancel is sent). It is
	// also the base re-probe interval.
	defaultProbeTimeout = 15 * time.Second
	// reprobeBackoffFactor caps the re-probe interval's backoff at this many
	// probe timeouts: 10 minutes at the default, and scaled along with a
	// test's shorter probe timeout unless the test sets its own cap.
	reprobeBackoffFactor = 40
	// defaultHeadHold bounds how long a modern request's SSE head is held
	// for the child's first message so an error can still pick the HTTP
	// status. The relay forwarder in front of the bridge has a 30s
	// response-header timeout; the probe plus this (17s), and ensureChild's
	// 5s before them when a respawn is needed, stay under it.
	defaultHeadHold = 2 * time.Second
	// defaultInitializeHold bounds how long a 2025-style initialize's SSE
	// head is held so Mcp-Session-Id can still ride on it. Longer than
	// defaultHeadHold because a cold stdio child can take a while to answer
	// initialize, yet still under the forwarder's 30s.
	defaultInitializeHold = 20 * time.Second

	// maxOutstandingProbes caps how many timed-out, unanswered probe ids a
	// process keeps as valid late answers; the oldest is dropped when full, so
	// a child that never answers cannot grow it.
	maxOutstandingProbes = 4
)

// isModernShaped is the single place that decides which era a message
// belongs to: it carries params._meta["io.modelcontextprotocol/protocolVersion"]
// (key presence, any value). Headers are deliberately not consulted.
func isModernShaped(msg map[string]any) bool {
	params, ok := isRequestParams(msg)
	if !ok {
		return false
	}
	meta, ok := getMeta(params)
	if !ok {
		return false
	}
	_, has := meta[protocolVersionMetaKey]
	return has
}

// isLegacyVersion compares revision dates; callers pass only versions that
// versionList accepted.
func isLegacyVersion(v string) bool { return v < modernProtocolVersion }

// versionList returns v as a list of protocol versions, or false if it is not
// a JSON array of YYYY-MM-DD strings. Such a list is no evidence either way:
// an entry that is not a date can neither be called modern nor legacy.
func versionList(v any) ([]string, bool) {
	arr, ok := v.([]any)
	if !ok {
		return nil, false
	}
	var out []string
	for _, e := range arr {
		s, ok := e.(string)
		if !ok {
			return nil, false
		}
		if _, err := time.Parse("2006-01-02", s); err != nil {
			return nil, false
		}
		out = append(out, s)
	}
	return out, true
}

func onlyModernVersions(supported []string) bool {
	if len(supported) == 0 {
		return false
	}
	for _, v := range supported {
		if isLegacyVersion(v) {
			return false
		}
	}
	return true
}

// eraState is one child process's era: its machine, and what carries out the
// machine's effects. One is created per process (StartBridge, ReplaceChild)
// and never reused. Every field is guarded by Bridge.mu.
type eraState struct {
	child *StdioChild
	m     eraMachine
	// wait holds the live probe's waiters and timer: non-nil exactly while
	// m.probe is live, for the same id.
	wait *probeWait
}

type probeWait struct {
	id     string
	done   chan struct{} // closed once, under Bridge.mu, when the probe settles
	timer  *time.Timer
	result eraAnswer // set before done is closed
}

func (b *Bridge) newEraState(child *StdioChild) *eraState {
	return &eraState{child: child, m: newEraMachine(b.probeTimeout, b.reprobeCap)}
}

// applyEraLocked feeds ev to st's machine and carries out its effects; it is
// the only thing that changes era state. b.mu must be held.
func (b *Bridge) applyEraLocked(st *eraState, ev eraEvent) eraEffects {
	var eff eraEffects
	st.m, eff = st.m.apply(ev)
	if eff.release != answerNone {
		w := st.wait
		st.wait = nil
		w.timer.Stop()
		w.result = eff.release
		close(w.done)
	}
	if eff.start != "" {
		b.startProbeLocked(st, eff.start)
		go b.writeProbe(st, eff.start)
	}
	if eff.ignored != "" {
		msg := "era event ignored"
		if ev.kind == evProbeAnswer {
			msg = "late or unknown server/discover answer, dropping"
		}
		b.log.Debug(msg, "server", b.name, "event", ev.kind, "id", ev.id, "reason", eff.ignored)
	}
	return eff
}

// startProbeLocked registers the waiters and timer of st's new probe id; the
// caller writes it.
func (b *Bridge) startProbeLocked(st *eraState, id string) {
	w := &probeWait{id: id, done: make(chan struct{})}
	w.timer = time.AfterFunc(time.Until(st.m.probe.deadline), func() { b.probeTimedOut(st, id) })
	st.wait = w
}

// writeProbe writes probe id to st's child and reports how that went. It runs
// on its own goroutine: a write blocked on a full stdin must not outlast the
// probe's timer for the requests waiting on it.
func (b *Bridge) writeProbe(st *eraState, id string) {
	err := st.child.Send(probeRequest(id))
	b.mu.Lock()
	b.applyEraLocked(st, eraEvent{kind: evWriteDone, id: id, failed: err != nil, now: time.Now()})
	b.mu.Unlock()
	if err != nil {
		b.log.Debug("failed to send server/discover probe to stdio child", "server", b.name, "err", err)
	}
}

// probeTimedOut is a probe's timer. It treats the child as legacy for now.
// The discover request is deliberately not cancelled: the cancellation would
// sit in the child's stdin right behind it, and a slow-starting modern server
// that honours it would never answer, leaving the process legacy for good.
func (b *Bridge) probeTimedOut(st *eraState, id string) {
	b.mu.Lock()
	eff := b.applyEraLocked(st, eraEvent{kind: evProbeTimer, id: id, now: time.Now()})
	b.mu.Unlock()
	if eff.release != answerNone {
		b.log.Debug("server/discover probe timed out; treating the child as legacy for now", "server", b.name)
	}
}

// endEraLocked ends st's machine: the process exited or was replaced, or the
// bridge is closing. Waiters on its probe get "gone".
func (b *Bridge) endEraLocked(st *eraState) {
	if st != nil {
		b.applyEraLocked(st, eraEvent{kind: evGone})
	}
}

func (b *Bridge) currentChildEra() (*StdioChild, *eraState) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.child, b.era
}

func (b *Bridge) eraModernOnly() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.era.m.verdict.modernOnly()
}

// modernOnlyVersions returns st's supported versions iff st is modern-only.
func (b *Bridge) modernOnlyVersions(st *eraState) ([]string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return st.m.modernOnlyVersions()
}

func (b *Bridge) legacyVersionOf(st *eraState) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return st.m.legacyVersion
}

// discover returns st's era answer for a modern request. With a verdict it
// answers at once (possibly starting a background probe); with none it joins
// the live probe, or starts one, and waits. The probe runs independently of
// any caller: a caller whose ctx ends just leaves. Its wait is bounded by the
// probe's timer and by until, the point where this request stops waiting
// whatever the probe does; a zero until sets no bound of its own. A request
// that joined a probe from before the first initialize asks once more when it
// settles, within a probe timeout of its start in all.
func (b *Bridge) discover(ctx context.Context, st *eraState, until time.Time) eraAnswer {
	start := time.Now()
	ans, again := b.discoverOnce(ctx, st, until)
	if again && ans != answerGone && ans != answerCancelled {
		if until.IsZero() {
			until = start.Add(b.probeTimeout)
		}
		ans, _ = b.discoverOnce(ctx, st, until)
	}
	return ans
}

func (b *Bridge) discoverOnce(ctx context.Context, st *eraState, until time.Time) (eraAnswer, bool) {
	exited := st.child.HasExited()
	b.mu.Lock()
	if b.era != st {
		b.mu.Unlock()
		return answerGone, false
	}
	eff := b.applyEraLocked(st, eraEvent{kind: evModernRequest, now: time.Now(), exited: exited})
	if !eff.wait {
		b.mu.Unlock()
		return eff.answer, false
	}
	w := st.wait
	b.mu.Unlock()
	return b.waitProbe(ctx, st, w, until), eff.again
}

// waitProbe waits for w's probe to settle, until, or ctx.
func (b *Bridge) waitProbe(ctx context.Context, st *eraState, w *probeWait, until time.Time) eraAnswer {
	var expireC <-chan time.Time
	if !until.IsZero() {
		expire := time.NewTimer(time.Until(until))
		defer expire.Stop()
		expireC = expire.C
	}
	select {
	case <-w.done:
	case <-expireC:
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.era != st {
			return answerGone
		}
		now := time.Now()
		if st.wait == w && !now.Before(st.m.probe.deadline) {
			// The timer is due but has not run: settle the probe here, so the
			// answer returned and the verdict cached are the same.
			b.applyEraLocked(st, eraEvent{kind: evProbeTimer, id: w.id, now: now})
		}
		if st.wait != w {
			return w.result
		}
		return st.m.timeoutAnswer(now)
	case <-ctx.Done():
		return answerCancelled
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.era != st {
		return answerGone
	}
	return w.result
}

func probeRequest(id string) map[string]any {
	return map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  "server/discover",
		"params": map[string]any{"_meta": map[string]any{
			protocolVersionMetaKey:    modernProtocolVersion,
			clientCapabilitiesMetaKey: map[string]any{},
			clientInfoMetaKey:         map[string]any{"name": "mcpwarp-bridge", "version": "0"},
		}},
	}
}

// handleProbeResponse feeds the child's answer to a probe id to st's machine,
// which tells a live probe's answer from a late one.
func (b *Bridge) handleProbeResponse(st *eraState, id string, msg map[string]any) {
	outcome, versions := classifyProbe(msg)
	b.mu.Lock()
	eff := b.applyEraLocked(st, eraEvent{kind: evProbeAnswer, id: id, outcome: outcome, versions: versions, now: time.Now()})
	b.mu.Unlock()
	if eff.release == answerNone && eff.ignored == "" {
		b.log.Debug("late server/discover answer applied", "server", b.name, "id", id, "outcome", outcome)
	}
}

// classifyProbe sorts a probe's answer three ways. Modern: a DiscoverResult
// with supportedVersions, or a recognised modern error (-32020, -32021,
// -32022). Explicit legacy: -32601, or a result without supportedVersions
// (null counts as absent).
// Anything else is soft legacy: legacy for now, as a booting or uninitialised
// child may answer. The versions are the child's list, nil when it gave none
// or one versionList rejects (modern, but not provably modern-only).
func classifyProbe(msg map[string]any) (probeOutcome, []string) {
	if result, ok := isRecord(msg["result"]); ok {
		raw := result["supportedVersions"]
		if raw == nil {
			return probeLegacy, nil
		}
		list, _ := versionList(raw)
		return probeModern, list
	}
	errObj, _ := isRecord(msg["error"])
	code, _ := eraErrorCode(errObj)
	switch code {
	case -32020, -32021:
		return probeModern, nil
	case -32022:
		data, _ := isRecord(errObj["data"])
		list, _ := versionList(data["supported"])
		return probeModern, list
	case -32601:
		return probeLegacy, nil
	}
	return probeSoftLegacy, nil
}

func errorCode(errObj map[string]any) int {
	code, _ := errObj["code"].(float64)
	return int(code)
}

// eraErrorCode is an error's code compared as a number, for era
// classification: -32601.0 is -32601, while a fractional or non-numeric code
// is no code at all (ok false, and 0, which no rule matches).
func eraErrorCode(errObj map[string]any) (int, bool) {
	switch c := errObj["code"].(type) {
	case float64:
		if c == math.Trunc(c) && math.Abs(c) <= 1<<31 {
			return int(c), true
		}
	case int:
		return c, true
	}
	return 0, false
}

// modernErrorStatus maps a modern child's final error to the HTTP status
// the 2026-07-28 HTTP binding expects; everything else stays 200.
func modernErrorStatus(msg map[string]any) int {
	errObj, ok := isRecord(msg["error"])
	if !ok {
		return http.StatusOK
	}
	switch errorCode(errObj) {
	case -32020, -32021, -32022:
		return http.StatusBadRequest
	case -32601:
		return http.StatusNotFound
	}
	return http.StatusOK
}

// legacyChildError is the 400 a modern-shaped request gets from a legacy
// child. -32600 on purpose: a modern server's own answers (-32601, -32602,
// -32020..-32022) would make a dual-era client stay modern instead of
// falling back to initialize.
func legacyChildError(id any, version string) map[string]any {
	msg := "initialize is required: this server speaks the legacy MCP protocol"
	if version != "" {
		msg += " (version " + version + ")"
	}
	msg += "; send initialize first"
	return jsonRPCErrorBody(id, -32600, msg, nil)
}

// legacyRequestRefusal is the 400 the bridge itself gives, without forwarding,
// to any 2025-style request once the child is known modern-only; lead is what
// the client asked for that is refused.
func legacyRequestRefusal(id any, lead string, supported []string) map[string]any {
	msg := lead + ": this server only speaks MCP protocol version(s) " +
		strings.Join(supported, ", ") + "; send requests carrying _meta[\"" + protocolVersionMetaKey + "\"]"
	list := make([]any, len(supported))
	for i, v := range supported {
		list[i] = v
	}
	return jsonRPCErrorBody(id, -32600, msg, map[string]any{"supported": list})
}

func jsonRPCErrorBody(id any, code int, message string, data any) map[string]any {
	e := map[string]any{"code": code, "message": message}
	if data != nil {
		e["data"] = data
	}
	return map[string]any{"jsonrpc": "2.0", "id": id, "error": e}
}

// refuseLegacyRequest answers a 2025-style request with the 400 refusal if
// the child is known to be modern-only, and reports whether it did.
func (b *Bridge) refuseLegacyRequest(w http.ResponseWriter, parsed map[string]any, st *eraState) bool {
	supported, only := b.modernOnlyVersions(st)
	if !only {
		return false
	}
	method, _ := parsed["method"].(string)
	lead := "2025-style " + method + " is not supported"
	id := parsed["id"]
	switch method {
	case "initialize":
		lead = "initialize is not supported"
	case "":
		// The id of a client response is in the server's id space, not a
		// request of the client's to answer.
		lead = "2025-style client responses are not supported"
		id = nil
	}
	writeJSON(w, http.StatusBadRequest, legacyRequestRefusal(id, lead, supported))
	b.noteLegacyRefused(st)
	return true
}

// refuseStatelessMethod answers GET or DELETE with 405 if the current child is
// known to be modern-only (the 2026-07-28 HTTP binding has no standalone
// stream and no sessions to terminate), and reports whether it did.
func (b *Bridge) refuseStatelessMethod(w http.ResponseWriter) bool {
	b.mu.Lock()
	st := b.era
	only := st.m.verdict.modernOnly()
	b.mu.Unlock()
	if !only {
		return false
	}
	writeMethodNotAllowed(w)
	b.noteLegacyRefused(st)
	return true
}

// noteLegacyRefused tells st's machine that 2025-style traffic was refused
// on its verdict, after the refusal is written.
func (b *Bridge) noteLegacyRefused(st *eraState) {
	b.mu.Lock()
	b.applyEraLocked(st, eraEvent{kind: evLegacyRefused, now: time.Now()})
	b.mu.Unlock()
}

// admitModern gates a modern-shaped request on the child's era. It reports
// whether the request may be forwarded, and to which process: a respawn can
// land between the caller's snapshot and the probe, so a stale snapshot is
// retried once against the current child. Both attempts share one deadline of
// a probe timeout from the first, so the retry gets only what is left; the
// first attempt is bounded by the probe's own timer, which ends within that
// timeout. Otherwise it has already answered (or, for a client that went away,
// deliberately not).
func (b *Bridge) admitModern(w http.ResponseWriter, r *http.Request, parsed map[string]any, child *StdioChild, st *eraState) (*StdioChild, *eraState, bool) {
	start := time.Now()
	v := b.discover(r.Context(), st, time.Time{})
	if v == answerGone {
		if child2, st2 := b.currentChildEra(); st2 != st {
			child, st = child2, st2
			v = b.discover(r.Context(), st, start.Add(b.probeTimeout))
		}
	}
	switch v {
	case answerModern:
		return child, st, true
	case answerLegacy:
		writeJSON(w, http.StatusBadRequest, legacyChildError(parsed["id"], b.legacyVersionOf(st)))
	case answerGone:
		writeJSONFlush(w, 502, map[string]any{"error": "local server is not running"})
	}
	return nil, nil, false
}

// noteInitializeAnswer feeds the child's answer to an initialize to st's
// machine. It runs on the child's pump for every initialize answer, whether
// or not a handler is still waiting for it, so an abandoned initialize still
// counts.
func (b *Bridge) noteInitializeAnswer(st *eraState, msg map[string]any) {
	var ev eraEvent
	if isResultMsg(msg) {
		result, _ := isRecord(msg["result"])
		v, has := result["protocolVersion"].(string)
		ev = eraEvent{kind: evInitAccepted, version: v, hasVersion: has}
	} else {
		cls, versions := classifyInitializeRejection(msg)
		ev = eraEvent{kind: evInitRejected, rejClass: cls, versions: versions, now: time.Now()}
	}
	b.mu.Lock()
	b.applyEraLocked(st, ev)
	b.mu.Unlock()
}

// completeInitialize answers a 2025-style initialize from the handler
// goroutine, off the child's pump (which has already fed the answer to the
// era state). A rejection is the child's own error in a 400, unchanged, never
// waiting on a probe: pure 2025 traffic must not pay for era detection.
func (b *Bridge) completeInitialize(entry *pendingEntry, msg map[string]any) {
	if isResultMsg(msg) {
		entry.resolveInitialized(msg, func() map[string]string {
			return map[string]string{"Mcp-Session-Id": b.session.check("initialize", "", false).sessionID}
		})
		return
	}
	entry.respond(http.StatusBadRequest, msg)
}

// classifyInitializeRejection sorts a rejected initialize. It looks modern
// ("no initialize here") with code -32601 or -32022, or a data.supported array;
// it is modern-only evidence only when data.supported is a non-empty list of
// modern versions, returned as the second value. The bridge never makes up a
// list.
func classifyInitializeRejection(msg map[string]any) (rejectionClass, []string) {
	errObj, _ := isRecord(msg["error"])
	data, _ := isRecord(errObj["data"])
	// Only an array counts as carrying a list; null or anything else leaves
	// the rejection to its code.
	raw, hasList := data["supported"].([]any)
	code, _ := eraErrorCode(errObj)
	switch {
	case !hasList && code != -32601 && code != -32022:
		return rejNotModern, nil
	case !hasList:
		return rejBare, nil
	}
	if list, ok := versionList(raw); ok && onlyModernVersions(list) {
		return rejModernOnly, list
	}
	return rejMixed, nil
}

func isResultMsg(msg map[string]any) bool {
	_, isErr := msg["error"]
	_, hasResult := msg["result"]
	return hasResult && !isErr
}

// modernRequestProblem validates a modern-shaped request that is about to be
// forwarded to a modern child: the standard request headers must match the
// body (-32020), and the per-request clientCapabilities must be present
// (-32602). The caller has already established that the era is modern.
func modernRequestProblem(r *http.Request, parsed map[string]any) (int, string) {
	params, _ := isRequestParams(parsed)
	meta, _ := getMeta(params)
	method, _ := parsed["method"].(string)

	header := func(name string) (string, bool) {
		vals := r.Header.Values(name)
		if len(vals) == 0 {
			return "", false
		}
		return vals[0], true
	}
	if v, ok := header("MCP-Protocol-Version"); !ok {
		return -32020, "Header mismatch: MCP-Protocol-Version header is missing"
	} else if body, _ := meta[protocolVersionMetaKey].(string); v != body {
		return -32020, fmt.Sprintf("Header mismatch: MCP-Protocol-Version header value %q does not match body value %q", v, body)
	}
	if v, ok := header("Mcp-Method"); !ok {
		return -32020, "Header mismatch: Mcp-Method header is missing"
	} else if v != method {
		return -32020, fmt.Sprintf("Header mismatch: Mcp-Method header value %q does not match body value %q", v, method)
	}
	var nameField string
	switch method {
	case "tools/call", "prompts/get":
		nameField = "name"
	case "resources/read":
		nameField = "uri"
	}
	if nameField != "" {
		raw, ok := header("Mcp-Name")
		if !ok {
			return -32020, "Header mismatch: Mcp-Name header is missing"
		}
		v, err := decodeMcpName(raw)
		if err != nil {
			return -32020, "Header mismatch: Mcp-Name header " + err.Error()
		}
		if body, _ := params[nameField].(string); v != body {
			return -32020, fmt.Sprintf("Header mismatch: Mcp-Name header value %q does not match body value %q", v, body)
		}
	}
	if _, ok := meta[clientCapabilitiesMetaKey]; !ok {
		return -32602, "Invalid params: _meta[\"" + clientCapabilitiesMetaKey + "\"] is required"
	}
	return 0, ""
}

// decodeMcpName undoes the =?base64?...?= sentinel encoding of a header
// value (spec "Value Encoding"); a value without the sentinel is returned as
// is. The markers are case-sensitive. The raw value must consist of visible
// ASCII, space and tab only, and the payload must be Base64 of UTF-8. The
// spec names no alphabet or padding rule beyond its examples, which use the
// standard padded alphabet, so that is what is accepted.
func decodeMcpName(v string) (string, error) {
	const prefix, suffix = "=?base64?", "?="
	for i := 0; i < len(v); i++ {
		if c := v[i]; c != '\t' && (c < 0x20 || c > 0x7e) {
			return "", errors.New("contains invalid characters")
		}
	}
	if len(v) >= len(prefix)+len(suffix) && strings.HasPrefix(v, prefix) && strings.HasSuffix(v, suffix) {
		dec, err := base64.StdEncoding.DecodeString(v[len(prefix) : len(v)-len(suffix)])
		if err != nil {
			return "", errors.New("is not valid base64")
		}
		if !utf8.Valid(dec) {
			return "", errors.New("is not valid UTF-8 once decoded")
		}
		return string(dec), nil
	}
	return v, nil
}
