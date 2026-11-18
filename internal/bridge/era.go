package bridge

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/mcpwarp/cli/internal/eventbus"
)

const (
	// modernProtocolVersion is the first MCP v2 revision. A version is modern
	// if it is a YYYY-MM-DD revision that does not sort before it.
	modernProtocolVersion = "2026-07-28"

	protocolVersionMetaKey    = "io.modelcontextprotocol/protocolVersion"
	clientCapabilitiesMetaKey = "io.modelcontextprotocol/clientCapabilities"
	clientInfoMetaKey         = "io.modelcontextprotocol/clientInfo"

	// probeID is the id of the one server/discover the bridge writes to each
	// process. Client ids are always rewritten to integers before reaching the
	// child, so a string id can never be a client's.
	probeID = "mcpwarp-probe"

	// defaultProbeTimeout bounds the wait for the answer to the probe written
	// at spawn. A cold stdio child can take a good while to start; past this
	// the process is served as v1 until it answers.
	defaultProbeTimeout = 15 * time.Second
	// defaultHeadHold bounds how long a modern request's SSE head is held
	// for the child's first message so an error can still pick the HTTP
	// status. The relay forwarder in front of the bridge has a 30s
	// response-header timeout; the verdict plus this (17s), and ensureChild's
	// 5s before them when a respawn is needed, stay under it.
	defaultHeadHold = 2 * time.Second
	// defaultInitializeHold bounds how long a 2025-style initialize's SSE
	// head is held so Mcp-Session-Id can still ride on it. It counts from the
	// client's arrival, so the wait for ensureChild and the verdict (20s at
	// most) is inside it, not added to it: the head goes out within 20s,
	// under the forwarder's 30s.
	defaultInitializeHold = 20 * time.Second

	// maxRefusedNames caps the client names a process logs a refusal for.
	maxRefusedNames = 16
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

// isModernVersion reports a YYYY-MM-DD revision on or after
// modernProtocolVersion; "draft" or any other string is not one.
func isModernVersion(v any) bool {
	s, ok := v.(string)
	if !ok {
		return false
	}
	if _, err := time.Parse("2006-01-02", s); err != nil {
		return false
	}
	return s >= modernProtocolVersion
}

// modernVersions returns the modern entries of a version list, nil if raw is
// not a list or has none.
func modernVersions(raw any) []string {
	list, _ := raw.([]any)
	var out []string
	for _, v := range list {
		if isModernVersion(v) {
			out = append(out, v.(string))
		}
	}
	return out
}

// discoverVersions is the modern versions a probe answer names: from a
// result's supportedVersions or a -32022 error's data.supported. nil means
// the child is v1: any other error, a result without modern versions, or
// anything else.
func discoverVersions(msg map[string]any) []string {
	if result, ok := isRecord(msg["result"]); ok {
		return modernVersions(result["supportedVersions"])
	}
	errObj, _ := isRecord(msg["error"])
	if code, _ := errObj["code"].(float64); code == -32022 {
		data, _ := isRecord(errObj["data"])
		return modernVersions(data["supported"])
	}
	return nil
}

// rejectionVersions is the list a rejected initialize names in data.supported
// if that is a non-empty list of modern versions only, whatever the code; nil
// otherwise. Only such a rejection says the child speaks v2 alone.
func rejectionVersions(msg map[string]any) []string {
	errObj, _ := isRecord(msg["error"])
	data, _ := isRecord(errObj["data"])
	list, _ := data["supported"].([]any)
	if len(list) == 0 {
		return nil
	}
	out := make([]string, len(list))
	for i, v := range list {
		if !isModernVersion(v) {
			return nil
		}
		out[i] = v.(string)
	}
	return out
}

// eraState is one child process's version: its machine, and what carries out
// the machine's effects. One is created per process (StartBridge,
// ReplaceChild) and never reused. Every field but flushMu is guarded by
// Bridge.mu.
type eraState struct {
	child   *StdioChild
	m       eraMachine
	decided chan struct{} // closed once, when m leaves phasePending
	timer   *time.Timer   // the probe's timeout
	// refused holds the clients whose refused initialize was logged: one
	// entry per name, and one shared by every client without a string name.
	refused map[refusedClient]bool
	// queued is what the applied events and refusals leave to do outside
	// Bridge.mu (log lines, closing the push stream, version reports), in the
	// order they happened; flushEra runs it once Bridge.mu is released, under
	// flushMu so that two flushes cannot reorder it.
	queued  []func()
	flushMu sync.Mutex
}

type refusedClient struct {
	name     string
	nameless bool
}

func newEraState(child *StdioChild) *eraState {
	return &eraState{child: child, decided: make(chan struct{}), refused: map[refusedClient]bool{}}
}

// startProbe arms st's probe timer and writes the probe. The write runs on its
// own goroutine: a child that does not read its stdin is the timer's to
// decide. A failed write ends st through the child's error listener
// (wireChild), or its exit.
func (b *Bridge) startProbe(st *eraState) {
	b.mu.Lock()
	if st.m.ended {
		b.mu.Unlock()
		return
	}
	st.timer = time.AfterFunc(b.probeTimeout, func() {
		b.mu.Lock()
		b.applyEraLocked(st, eraEvent{kind: evTimeout})
		b.mu.Unlock()
		b.flushEra(st)
	})
	b.mu.Unlock()
	go func() { _ = st.child.Send(probeRequest()) }()
}

// applyEraLocked feeds ev to st's machine and carries out its effects; it is
// the only thing that changes era state. b.mu must be held. What cannot be
// done under b.mu (the log, closing the push stream, publishing the version)
// is queued on st: the caller runs flushEra(st) once b.mu is released.
func (b *Bridge) applyEraLocked(st *eraState, ev eraEvent) {
	counted := st.m.countsInits()
	var eff eraEffects
	st.m, eff = st.m.apply(ev)
	if eff.decided {
		if st.timer != nil {
			st.timer.Stop()
		}
		close(st.decided)
	}
	if counted && !st.m.countsInits() {
		b.dropAbandonedInitsLocked(st)
	}
	for _, line := range eff.log {
		st.queued = append(st.queued, func() { b.log.Info(line, "server", b.name) })
	}
	if eff.ignored != "" {
		st.queued = append(st.queued, func() {
			b.log.Debug("era event ignored", "server", b.name, "event", ev.kind, "reason", eff.ignored)
		})
	}
	if eff.toV2 {
		st.queued = append(st.queued, func() { b.closePushStream(st) })
	}
	b.noteVersionLocked(st)
}

// noteVersionLocked queues a ServerVersionChanged on st when the version the
// bridge serves has changed: st is the current process, and its version (none
// once it has ended) is not the one last reported. Queued on st, the reports
// of one process go out in order, and ReplaceChild flushes the old process's
// last one before the new process can queue its first. A queued report reads
// b.closing when it runs, releases b.mu, then publishes: one that runs after
// CloseContext has set closing publishes nothing, whenever it was queued, but
// one that read closing just before still publishes, possibly after
// CloseContext has returned. b.mu must be held.
func (b *Bridge) noteVersionLocked(st *eraState) {
	if b.bus == nil || st != b.era {
		return
	}
	v := st.m.version()
	if st.m.ended {
		v = versionUnknown
	}
	if v == b.reported {
		return
	}
	b.reported = v
	evt := eventbus.ServerVersionChanged{Name: b.name}
	switch v {
	case versionV1:
		evt.Version = eventbus.VersionV1
	case versionV2:
		evt.Version = eventbus.VersionV2
	}
	st.queued = append(st.queued, func() {
		b.mu.Lock()
		closing := b.closing
		b.mu.Unlock()
		if !closing {
			b.bus.Publish(evt)
		}
	})
}

// flushEra runs what st has queued, in the order it was queued. b.mu must not
// be held.
func (b *Bridge) flushEra(st *eraState) {
	st.flushMu.Lock()
	defer st.flushMu.Unlock()
	b.mu.Lock()
	queued := st.queued
	st.queued = nil
	b.mu.Unlock()
	for _, f := range queued {
		f()
	}
}

// closePushStream ends the push stream of st, a process that turned v2. A
// respawn that has already put another process in st's place owns the hub
// now, so it is left alone.
func (b *Bridge) closePushStream(st *eraState) {
	b.mu.Lock()
	current := b.era == st
	b.mu.Unlock()
	if current {
		b.pushHub.Close()
	}
}

// endEraLocked ends st's machine: the process exited or was replaced, a write
// to it failed, or the bridge is closing. Requests waiting for its verdict are
// told it is not running.
func (b *Bridge) endEraLocked(st *eraState) {
	b.applyEraLocked(st, eraEvent{kind: evGone})
}

// endDeadChild ends st and closes c, a child whose stdin can no longer be
// written: requests get "not running", and its exit takes the host's usual
// restart path. b.mu must not be held.
func (b *Bridge) endDeadChild(c *StdioChild, st *eraState) {
	b.mu.Lock()
	b.endEraLocked(st)
	b.mu.Unlock()
	b.flushEra(st)
	go func() { _ = c.Close() }()
}

// snapshotEra returns the current process and its machine as they are now.
func (b *Bridge) snapshotEra() (*StdioChild, *eraState, eraMachine) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.child, b.era, b.era.m
}

type waitOutcome int

const (
	waitDecided   waitOutcome = iota
	waitGone                  // st is no longer live, or until passed first
	waitCancelled             // ctx ended first
)

// waitVerdict waits until st has a verdict, and returns its machine. The wait
// is bounded by the probe's timer, by ctx, and by until unless it is zero.
func (b *Bridge) waitVerdict(ctx context.Context, st *eraState, until time.Time) (eraMachine, waitOutcome) {
	var expire <-chan time.Time
	if !until.IsZero() {
		t := time.NewTimer(time.Until(until))
		defer t.Stop()
		expire = t.C
	}
	select {
	case <-st.decided:
	case <-expire:
	case <-ctx.Done():
		return eraMachine{}, waitCancelled
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.era != st || st.m.ended || st.m.version() == versionUnknown {
		return st.m, waitGone
	}
	return st.m, waitDecided
}

// awaitVerdict waits for the verdict of st, the process a request found. A
// respawn can land while it waits, so a process that is gone by then is
// retried once against the current one; both waits share one deadline of a
// probe timeout from the first.
func (b *Bridge) awaitVerdict(ctx context.Context, child *StdioChild, st *eraState) (*StdioChild, *eraState, eraMachine, waitOutcome) {
	start := time.Now()
	m, out := b.waitVerdict(ctx, st, time.Time{})
	if out == waitGone {
		if child2, st2, _ := b.snapshotEra(); st2 != st {
			child, st = child2, st2
			m, out = b.waitVerdict(ctx, st, start.Add(b.probeTimeout))
		}
	}
	return child, st, m, out
}

// admit waits for the verdict of the process a request found (awaitVerdict).
// Unless it gets one, the request has already been answered (or, for a client
// that went away, deliberately not).
func (b *Bridge) admit(w http.ResponseWriter, r *http.Request, child *StdioChild, st *eraState) (*StdioChild, *eraState, eraMachine, bool) {
	child, st, m, out := b.awaitVerdict(r.Context(), child, st)
	switch out {
	case waitDecided:
		return child, st, m, true
	case waitGone:
		writeJSONFlush(w, 502, map[string]any{"error": "local server is not running"})
	}
	return nil, nil, eraMachine{}, false
}

func probeRequest() map[string]any {
	return map[string]any{
		"jsonrpc": "2.0",
		"id":      probeID,
		"method":  "server/discover",
		"params": map[string]any{"_meta": map[string]any{
			protocolVersionMetaKey:    modernProtocolVersion,
			clientCapabilitiesMetaKey: map[string]any{},
			clientInfoMetaKey:         map[string]any{"name": "mcpwarp-bridge", "version": "0"},
		}},
	}
}

// handleProbeResponse feeds the child's answer to the probe to st's machine.
func (b *Bridge) handleProbeResponse(st *eraState, msg map[string]any) {
	b.mu.Lock()
	b.applyEraLocked(st, eraEvent{kind: evAnswer, versions: discoverVersions(msg)})
	b.mu.Unlock()
	b.flushEra(st)
}

func errorCode(errObj map[string]any) int {
	code, _ := errObj["code"].(float64)
	return int(code)
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

func jsonRPCErrorBody(id any, code int, message string, data any) map[string]any {
	e := map[string]any{"code": code, "message": message}
	if data != nil {
		e["data"] = data
	}
	return map[string]any{"jsonrpc": "2.0", "id": id, "error": e}
}

// refusalBody is the bridge's own error answering msg in its stead: it echoes
// msg's id, and has no id member if msg has none (a notification).
func refusalBody(msg map[string]any, code int, message string, data any) map[string]any {
	body := jsonRPCErrorBody(msg["id"], code, message, data)
	if _, hasID := msg["id"]; !hasID {
		delete(body, "id")
	}
	return body
}

// v1FallbackError is the 400 a modern-shaped message gets on a v1 process.
// -32600 on purpose: a modern server's own answers (-32601, -32602,
// -32020..-32022) would make a dual-era client stay modern instead of
// falling back to initialize.
func v1FallbackError(msg map[string]any) map[string]any {
	return refusalBody(msg, -32600, v1Fallback, nil)
}

// refuseV1Message answers a 2025-style message other than initialize on a v2
// process with the bridge's own 400; the child never sees it. versions are
// the process's.
func refuseV1Message(w http.ResponseWriter, parsed map[string]any, versions []string) {
	writeJSON(w, http.StatusBadRequest, refusalBody(parsed, -32600, v2RequestRefusal(versions), map[string]any{"supported": versions}))
}

// refuseInitialize is the 400 body answering a 2025 initialize on a v2
// process. It logs the refused client once per name for the process, after the
// lines st's events queued before it.
func (b *Bridge) refuseInitialize(parsed map[string]any, st *eraState, versions []string) map[string]any {
	params, _ := isRequestParams(parsed)
	requested, _ := params["protocolVersion"].(string)
	info, _ := isRecord(params["clientInfo"])
	name, isName := info["name"].(string)
	key := refusedClient{name: name, nameless: !isName}
	b.mu.Lock()
	if !st.refused[key] && len(st.refused) < maxRefusedNames {
		st.refused[key] = true
		line := logRefusedNamelessClient(versions)
		if isName {
			line = logRefusedClient(name, versions)
		}
		st.queued = append(st.queued, func() { b.log.Warn(line, "server", b.name) })
	}
	b.mu.Unlock()
	b.flushEra(st)
	return jsonRPCErrorBody(parsed["id"], -32022, v2InitializeRefusal(versions),
		map[string]any{"supported": versions, "requested": requested})
}

// refuseStatelessMethod answers GET or DELETE with 405 once the current
// process is v2 (the 2026-07-28 HTTP binding has no standalone stream and no
// sessions to terminate), and reports whether the request is answered. A
// request to a process that has already ended is not its to answer. Otherwise
// it waits for the verdict, and gets "not running" if there is none by the
// end of the wait.
func (b *Bridge) refuseStatelessMethod(w http.ResponseWriter, r *http.Request) bool {
	child, st, m := b.snapshotEra()
	if m.ended {
		return false
	}
	_, _, m, out := b.awaitVerdict(r.Context(), child, st)
	switch {
	case out == waitCancelled:
		return true
	case m.version() == versionUnknown:
		writeJSONFlush(w, 502, map[string]any{"error": "local server is not running"})
		return true
	case out != waitDecided || m.version() != versionV2:
		return false
	}
	w.Header().Set("Allow", "POST")
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusMethodNotAllowed)
	_, _ = io.WriteString(w, v2NoSessions(m.versions))
	return true
}

// noteInitializeAnswer feeds the child's answer to a forwarded initialize to
// st's machine. It runs on the child's pump, before the handler sees the
// answer.
func (b *Bridge) noteInitializeAnswer(st *eraState, msg map[string]any) {
	ev := eraEvent{kind: evInitOK}
	if !isResultMsg(msg) {
		ev = eraEvent{kind: evInitFailed, versions: rejectionVersions(msg)}
	}
	b.mu.Lock()
	b.applyEraLocked(st, ev)
	b.mu.Unlock()
	b.flushEra(st)
}

// completeInitialize answers a 2025-style initialize from the handler
// goroutine, off the child's pump (which has already fed the answer to the
// era state). A rejection is the child's own error in a 400, unchanged. st is
// the process the initialize was sent to. If another initialize's rejection
// turned it v2 while this one was in flight, the child's acceptance is
// dropped and the client refused as on any v2 process, with no session: a
// process cannot turn v2 once its child has accepted an initialize, so its
// version now is its version when the acceptance arrived.
func (b *Bridge) completeInitialize(entry *pendingEntry, parsed map[string]any, st *eraState, msg map[string]any) {
	if !isResultMsg(msg) {
		entry.respond(http.StatusBadRequest, msg)
		return
	}
	b.mu.Lock()
	v2, versions := st.m.version() == versionV2, st.m.versions
	b.mu.Unlock()
	if v2 {
		entry.respond(http.StatusBadRequest, b.refuseInitialize(parsed, st, versions))
		return
	}
	entry.resolveInitialized(msg, func() map[string]string {
		return map[string]string{"Mcp-Session-Id": b.session.initialize()}
	})
}

func isResultMsg(msg map[string]any) bool {
	_, isErr := msg["error"]
	_, hasResult := msg["result"]
	return hasResult && !isErr
}

// modernRequestProblem validates a modern-shaped request that is about to be
// forwarded to a v2 child: the standard request headers must match the
// body (-32020), and the per-request clientCapabilities must be present
// (-32602).
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
