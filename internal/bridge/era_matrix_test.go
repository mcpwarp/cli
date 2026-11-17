package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// This file pins how the bridge serves each child's MCP version: a 2025-style
// client (initialize, then Mcp-Session-Id), a 2026-07-28-style client
// (stateless _meta plus MCP-Protocol-Version/Mcp-Method/Mcp-Name headers, no
// session) and a dual-era client against fakemcp's --era modes, in both
// response forms.
// Expectations live in eraWant (era_matrix_want_test.go); when the bridge
// changes on purpose, that table is the diff.
//
// Bridge timeouts are generous (they run under t.Parallel on ~50 bridges)
// except in the scenarios where the timeout itself is under test.

const (
	// eraHangBound is a client-side backstop for a request the bridge never
	// answers, far above anything a healthy bridge takes under load.
	eraHangBound = 20 * time.Second

	metaVersion = "io.modelcontextprotocol/protocolVersion"
	metaCaps    = "io.modelcontextprotocol/clientCapabilities"
	metaInfo    = "io.modelcontextprotocol/clientInfo"
)

// eraModes are the child modes of the matrix: discover-error is a modern
// child that answers server/discover with -32603, and so is served as v1.
var eraModes = []string{"legacy", "legacy-silent", "discover-error", "modern"}

var eraForms = []struct{ name, accept string }{
	{"sse", "application/json, text/event-stream"},
	{"json", "application/json"},
}

type eraExpect struct {
	status   int
	session  bool
	body     string
	outcome  string
	decision string
}

type eraRow struct {
	key, scenario, client, mode, form, step string
	got                                     eraExpect
}

var (
	eraRowsMu sync.Mutex
	eraRows   []eraRow
	eraSeen   = map[string]bool{}
)

func eraHangOutcome() string {
	return fmt.Sprintf("hang (no response within %s)", eraHangBound)
}

func eraMeta(version string) map[string]any {
	return map[string]any{
		metaVersion: version,
		metaCaps:    map[string]any{},
		metaInfo:    map[string]any{"name": "era-test", "version": "0"},
	}
}

type eraReq struct {
	headers map[string]string
	body    map[string]any
}

func eraModernReq(id int, method string, params map[string]any, version string) eraReq {
	p := map[string]any{"_meta": eraMeta(version)}
	for k, v := range params {
		p[k] = v
	}
	h := map[string]string{"MCP-Protocol-Version": version, "Mcp-Method": method}
	if method == "tools/call" {
		h["Mcp-Name"] = fmt.Sprint(p["name"])
	}
	return eraReq{h, map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": p}}
}

func eraToolsCall(id int) eraReq {
	return eraModernReq(id, "tools/call", map[string]any{"name": "echo", "arguments": map[string]any{}}, "2026-07-28")
}

func eraInitialize(id int) eraReq {
	return eraReq{nil, map[string]any{
		"jsonrpc": "2.0", "id": id, "method": "initialize",
		"params": map[string]any{
			"protocolVersion": "2025-11-25",
			"capabilities":    map[string]any{},
			"clientInfo":      map[string]any{"name": "era-test", "version": "0"},
		},
	}}
}

func eraLegacyToolsCall(id int) eraReq {
	return eraReq{
		map[string]string{"MCP-Protocol-Version": "2025-11-25"},
		map[string]any{"jsonrpc": "2.0", "id": id, "method": "tools/call", "params": map[string]any{"name": "echo", "arguments": map[string]any{}}},
	}
}

// eraCase is one (scenario, client, mode, form) run against its own bridge.
type eraCase struct {
	t        *testing.T
	scenario string
	client   string
	mode     string
	form     string
	accept   string
	url      string
	http     *http.Client
	session  string
	last     eraExpect
}

func (c *eraCase) step(name string, req eraReq, extra map[string]string) eraExpect {
	c.t.Helper()
	got := c.do(req, extra)
	key := strings.Join([]string{c.scenario, c.client, c.mode, c.form, name}, "|")
	row := eraRow{key: key, scenario: c.scenario, client: c.client, mode: c.mode, form: c.form, step: name, got: got}
	eraRowsMu.Lock()
	eraRows = append(eraRows, row)
	eraSeen[key] = true
	eraRowsMu.Unlock()

	want, ok := eraWant[key]
	switch {
	case !ok:
		c.t.Errorf("no expectation for %q; observed: %s", key, eraLiteral(key, got))
	case eraNoDecision(want) != got:
		c.t.Errorf("%s\n  want %+v\n  got  %+v", key, want, got)
	}
	c.last = got
	return got
}

// stepMethod is step for a GET (asking for an SSE stream) or a DELETE,
// carrying the session if there is one. A stream's body is "(stream)" and
// is not read; a 405 names its Allow header.
func (c *eraCase) stepMethod(name, method string) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), eraHangBound)
	defer cancel()
	hr, _ := http.NewRequestWithContext(ctx, method, c.url, nil)
	hr.Header.Set("Accept", "text/event-stream")
	if c.session != "" {
		hr.Header.Set("Mcp-Session-Id", c.session)
	}
	resp, err := c.http.Do(hr)
	if err != nil {
		c.t.Fatalf("%s: %v", method, err)
	}
	defer resp.Body.Close()
	got := eraExpect{status: resp.StatusCode, session: resp.Header.Get("Mcp-Session-Id") != "", body: "(empty)", outcome: eraOutcome(resp.StatusCode, nil)}
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		got.body, got.outcome = "(stream)", "stream"
	} else if buf, _ := io.ReadAll(resp.Body); len(buf) > 0 {
		got.body = string(buf)
	}
	if allow := resp.Header.Get("Allow"); allow != "" {
		got.body += " (Allow: " + allow + ")"
	}
	key := strings.Join([]string{c.scenario, c.client, c.mode, c.form, name}, "|")
	eraRowsMu.Lock()
	eraRows = append(eraRows, eraRow{key: key, scenario: c.scenario, client: c.client, mode: c.mode, form: c.form, step: name, got: got})
	eraSeen[key] = true
	eraRowsMu.Unlock()
	if want, ok := eraWant[key]; !ok {
		c.t.Errorf("no expectation for %q; observed: %s", key, eraLiteral(key, got))
	} else if want != got {
		c.t.Errorf("%s\n  want %+v\n  got  %+v", key, want, got)
	}
}

func eraNoDecision(e eraExpect) eraExpect {
	e.decision = ""
	return e
}

func eraLiteral(key string, e eraExpect) string {
	s := fmt.Sprintf("%q: {%d, %t, %q, %q", key, e.status, e.session, e.body, e.outcome)
	if e.decision != "" {
		s += fmt.Sprintf(", %q", e.decision)
	}
	return s + "},"
}

func (c *eraCase) do(req eraReq, extra map[string]string) eraExpect {
	c.t.Helper()
	raw, _ := json.Marshal(req.body)
	ctx, cancel := context.WithTimeout(context.Background(), eraHangBound)
	defer cancel()
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, strings.NewReader(string(raw)))
	if err != nil {
		c.t.Fatal(err)
	}
	hr.Header.Set("Content-Type", "application/json")
	hr.Header.Set("Accept", c.accept)
	for k, v := range req.headers {
		hr.Header.Set(k, v)
	}
	for k, v := range extra {
		if v == "" {
			hr.Header.Del(k)
		} else {
			hr.Header.Set(k, v)
		}
	}
	resp, err := c.http.Do(hr)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return eraExpect{body: "(none)", outcome: eraHangOutcome()}
		}
		c.t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	got := eraExpect{status: resp.StatusCode}
	if sid := resp.Header.Get("Mcp-Session-Id"); sid != "" {
		got.session = true
		c.session = sid
	}
	buf, err := io.ReadAll(resp.Body)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		c.t.Fatalf("read body: %v", err)
	}
	msgs := eraDecodeMessages(string(buf), strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream"))
	got.body = "(empty)"
	if len(msgs) > 0 {
		got.body = strings.Join(msgs, " | ")
	}
	if err != nil {
		got.outcome = eraHangOutcome()
		return got
	}
	got.outcome = eraOutcome(got.status, msgs)
	return got
}

func eraDecodeMessages(body string, sse bool) []string {
	var raws []string
	if sse {
		for _, line := range strings.Split(body, "\n") {
			if data, ok := strings.CutPrefix(strings.TrimRight(line, "\r"), "data:"); ok {
				raws = append(raws, strings.TrimSpace(data))
			}
		}
	} else if s := strings.TrimSpace(body); s != "" {
		raws = append(raws, s)
	}
	var out []string
	for _, r := range raws {
		var v any
		if err := json.Unmarshal([]byte(r), &v); err != nil {
			out = append(out, "(non-JSON) "+r)
			continue
		}
		if m, ok := v.(map[string]any); ok && m["id"] != nil {
			m["id"] = "ID"
		}
		b, _ := json.Marshal(v)
		out = append(out, string(b))
	}
	return out
}

func eraOutcome(status int, msgs []string) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	case status == 202:
		return "accepted"
	case len(msgs) == 0:
		return "empty"
	}
	last := msgs[len(msgs)-1]
	if strings.Contains(last, `"error":`) {
		return "error-in-200"
	}
	return "ok"
}

// eraDiscoverDecision applies the 2026-07-28 HTTP backward-compat rule to
// what a dual-era client got back for its first modern request: a result or
// a recognised modern error (-32020, -32021, -32022) means stay modern;
// anything else, including no answer, means fall back to initialize.
// -32601 is deliberately not recognised: the stdio section calls it the
// typical legacy answer, while the HTTP section lists unknown-method among
// modern errors, so the spec is ambiguous there.
func eraDiscoverDecision(g eraExpect) string {
	if strings.HasPrefix(g.outcome, "hang") {
		return "fallback (timeout)"
	}
	if g.outcome == "ok" {
		return "modern"
	}
	for _, code := range []string{"-32020", "-32021", "-32022"} {
		if strings.Contains(g.body, `"code":`+code) {
			return "modern (error " + code + ")"
		}
	}
	return "fallback"
}

// runEra runs fn against a fresh bridge in front of the fakemcp of mode, as
// a parallel subtest, one per (mode, form). A silent child is the one case
// that needs the probe to give up, so it gets a short probe timeout.
func runEra(t *testing.T, scenario, client, mode string, formIdx int, fn func(c *eraCase)) {
	var opts []func(*StartBridgeOptions)
	args := []string{"--era=" + mode}
	switch mode {
	case "legacy-silent":
		opts = append(opts, func(o *StartBridgeOptions) { o.ProbeTimeout = 500 * time.Millisecond })
	case "discover-error":
		args = []string{"--era=modern", "--discover-error=-32603"}
	}
	runEraWith(t, scenario, client, mode, args, formIdx, fn, opts...)
}

// runEraWith is runEra with a free-form child: label is the "child mode"
// column, args the fakemcp command line, opts the timeouts the scenario is
// about (the rest are generous).
func runEraWith(t *testing.T, scenario, client, mode string, args []string, formIdx int, fn func(c *eraCase), opts ...func(*StartBridgeOptions)) {
	form := eraForms[formIdx]
	t.Run(fmt.Sprintf("%s/%s/%s", client, mode, form.name), func(t *testing.T) {
		t.Parallel()
		so := StartBridgeOptions{
			Name:                  "fake-" + mode,
			SpawnSpec:             SpawnSpec{Command: buildFakeMCP(t), Args: args},
			Log:                   testLogger(),
			ProbeTimeout:          30 * time.Second,
			HeadHoldTimeout:       30 * time.Second,
			InitializeHoldTimeout: 30 * time.Second,
		}
		for _, o := range opts {
			o(&so)
		}
		b, err := StartBridge(so)
		if err != nil {
			t.Fatalf("StartBridge: %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })
		tr := &http.Transport{DisableKeepAlives: true}
		t.Cleanup(tr.CloseIdleConnections)
		fn(&eraCase{
			t: t, scenario: scenario, client: client, mode: mode,
			form: form.name, accept: form.accept, url: b.URL(), http: &http.Client{Transport: tr},
		})
	})
}

// releaseHeld makes a --discover-hold child send its held answers; they have
// been handled by the bridge by the time this returns. It is a 2025-style
// request, so only for a process served as v1.
func (c *eraCase) releaseHeld() {
	c.t.Helper()
	got := c.do(eraReq{nil, map[string]any{"jsonrpc": "2.0", "id": 0, "method": "debug/release"}}, nil)
	if got.status != 200 {
		c.t.Fatalf("debug/release: %+v", got)
	}
}

func sessionHdr(c *eraCase) map[string]string {
	if c.session == "" {
		return nil
	}
	return map[string]string{"Mcp-Session-Id": c.session}
}

func TestEraMatrix(t *testing.T) {
	eraRowsMu.Lock()
	eraRows = nil
	eraSeen = map[string]bool{}
	eraRowsMu.Unlock()

	wantKeys := map[string]bool{}
	for k := range eraWant {
		wantKeys[k] = true
	}

	t.Run("cases", func(t *testing.T) {
		for _, mode := range eraModes {
			for fi := range eraForms {
				runEra(t, "matrix", "2025", mode, fi, func(c *eraCase) {
					c.step("initialize", eraInitialize(1), nil)
					c.step("tools/call with session", eraLegacyToolsCall(2), sessionHdr(c))
					c.stepMethod("GET stream", http.MethodGet)
					c.stepMethod("DELETE session", http.MethodDelete)
				})
				runEra(t, "matrix", "2026", mode, fi, func(c *eraCase) {
					c.step("tools/call stateless", eraToolsCall(1), nil)
					c.step("notification", eraReq{nil, map[string]any{"jsonrpc": "2.0", "method": "notifications/message",
						"params": map[string]any{"_meta": eraMeta("2026-07-28")}}}, nil)
				})
				runEra(t, "matrix", "dual-era", mode, fi, func(c *eraCase) {
					probe := c.step("server/discover probe", eraModernReq(1, "server/discover", nil, "2026-07-28"), nil)
					decision := eraDiscoverDecision(probe)
					c.recordDecision(decision)
					if strings.HasPrefix(decision, "modern") {
						c.step("tools/call stateless", eraToolsCall(2), nil)
						return
					}
					c.step("fallback initialize", eraInitialize(2), nil)
					c.step("tools/call with session", eraLegacyToolsCall(3), sessionHdr(c))
				})
			}
		}

		for fi := range eraForms {
			bogus := map[string]string{"Mcp-Session-Id": "no-such-session"}
			runEra(t, "unknown-session", "2026", "modern", fi, func(c *eraCase) {
				c.step("stateless with unknown Mcp-Session-Id, none minted", eraToolsCall(1), bogus)
			})
			// A 2025 client holding a session from before a restart.
			runEra(t, "stale-session", "2025", "modern", fi, func(c *eraCase) {
				c.step("tools/call with an unknown session", eraLegacyToolsCall(1), bogus)
			})
			runEra(t, "stale-session", "2025", "legacy", fi, func(c *eraCase) {
				c.step("1 initialize", eraInitialize(1), nil)
				c.step("2 tools/call with an unknown session", eraLegacyToolsCall(2), bogus)
			})

			runEra(t, "headers", "2026", "modern", fi, func(c *eraCase) {
				noCaps := eraToolsCall(6)
				delete(noCaps.body["params"].(map[string]any)["_meta"].(map[string]any), metaCaps)
				c.step("1 all headers match", eraToolsCall(1), nil)
				c.step("2 MCP-Protocol-Version differs from _meta", eraToolsCall(2), map[string]string{"MCP-Protocol-Version": "2025-11-25"})
				c.step("3 Mcp-Method missing", eraToolsCall(3), map[string]string{"Mcp-Method": ""})
				c.step("4 Mcp-Name differs from params.name", eraToolsCall(4), map[string]string{"Mcp-Name": "other"})
				c.step("5 Mcp-Name base64 sentinel, matching", eraToolsCall(5), map[string]string{"Mcp-Name": "=?base64?ZWNobw==?="})
				c.step("6 clientCapabilities missing from _meta", noCaps, nil)
			})
			runEra(t, "headers", "2026", "legacy", fi, func(c *eraCase) {
				c.step("1 bad MCP-Protocol-Version on a legacy child", eraToolsCall(1), map[string]string{"MCP-Protocol-Version": "1900-01-01"})
			})

			// A v1 child rejecting a single initialize is not locked out.
			runEraWith(t, "bad-initialize", "2025", "legacy (rejects first initialize)", []string{"--era=legacy", "--reject-initialize=1"}, fi, func(c *eraCase) {
				c.step("1 initialize rejected", eraInitialize(1), nil)
				c.step("2 initialize again", eraInitialize(2), nil)
				c.step("3 tools/call with session", eraLegacyToolsCall(3), sessionHdr(c))
			})
			// SSE: the head is held for the answer and goes out at the
			// deadline with a provisional session, which a late rejection
			// removes. JSON: the head waits for the answer.
			runEraWith(t, "slow-initialize", "2025", "legacy (slow, rejects first initialize)",
				[]string{"--era=legacy", "--initialize-delay-ms=1500", "--reject-initialize=1"}, fi, func(c *eraCase) {
					c.step("1 initialize, late rejection", eraInitialize(1), nil)
					c.step("2 initialize again", eraInitialize(2), nil)
				}, func(o *StartBridgeOptions) { o.InitializeHoldTimeout = 300 * time.Millisecond })
			// A modern child slower to answer discover than the probe
			// timeout is v1 for now until its late answer arrives (held
			// here until released, so the order is not left to timing).
			runEraWith(t, "late-probe", "2026", "modern (slow discover)", []string{"--era=modern", "--discover-hold"}, fi, func(c *eraCase) {
				c.step("1 probe times out", eraToolsCall(1), nil)
				c.releaseHeld()
				c.step("2 after the late answer", eraToolsCall(2), nil)
			}, func(o *StartBridgeOptions) { o.ProbeTimeout = 500 * time.Millisecond })

			runEra(t, "modern-errors", "2026", "modern", fi, func(c *eraCase) {
				c.step("unknown method", eraModernReq(1, "foo/bar", nil, "2026-07-28"), nil)
				c.step("unsupported protocol version", eraModernReq(2, "tools/list", nil, "1900-01-01"), nil)
			})
		}
	})

	for k := range wantKeys {
		if !eraSeen[k] {
			t.Errorf("expectation %q matched no case", k)
		}
	}
	eraLogTable(t)
}

func (c *eraCase) recordDecision(decision string) {
	eraRowsMu.Lock()
	defer eraRowsMu.Unlock()
	for i := len(eraRows) - 1; i >= 0; i-- {
		r := &eraRows[i]
		if r.scenario == c.scenario && r.client == c.client && r.mode == c.mode && r.form == c.form && r.step == "server/discover probe" {
			r.got.decision = decision
			break
		}
	}
	key := strings.Join([]string{c.scenario, c.client, c.mode, c.form, "server/discover probe"}, "|")
	if want, ok := eraWant[key]; ok && want.decision != decision {
		c.t.Errorf("%s: client decision want %q, got %q", key, want.decision, decision)
	}
}

func eraLogTable(t *testing.T) {
	eraRowsMu.Lock()
	rows := append([]eraRow(nil), eraRows...)
	eraRowsMu.Unlock()
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].key < rows[j].key })

	esc := func(s string) string { return strings.ReplaceAll(s, "|", "\\|") }
	var sb strings.Builder
	sb.WriteString("\n| scenario | client | child mode | form | step | HTTP | session hdr | body | outcome | dual-era decision |\n")
	sb.WriteString("|---|---|---|---|---|---|---|---|---|---|\n")
	for _, r := range rows {
		status := "-"
		if r.got.status != 0 {
			status = fmt.Sprint(r.got.status)
		}
		fmt.Fprintf(&sb, "| %s | %s | %s | %s | %s | %s | %t | `%s` | %s | %s |\n",
			r.scenario, r.client, r.mode, r.form, esc(r.step), status, r.got.session, esc(r.got.body), r.got.outcome, r.got.decision)
	}
	t.Log(sb.String())
}
