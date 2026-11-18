package bridge

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mcpwarp/cli/internal/eventbus"
)

// A server whose config forces its MCP version: no server/discover is sent,
// the version is known from spawn and never changes, and a child that does
// not speak it is reported once per process.

func startForced(t *testing.T, as ServeAs, logs *logCapture, args ...string) (*Bridge, *eventbus.Bus) {
	t.Helper()
	return startReporting(t, func(o *StartBridgeOptions) {
		o.ServeAs = as
		o.Log = logs.logger()
	}, args...)
}

// nextEvent waits for the next event on bus.
func nextEvent(t *testing.T, bus *eventbus.Bus) any {
	t.Helper()
	select {
	case evt := <-bus.Control:
		return evt
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for an event")
	}
	return nil
}

// noVerdictLines fails if the process logged a line of the automatic verdict.
func noVerdictLines(t *testing.T, logs *logCapture) {
	t.Helper()
	for _, line := range []string{logSpeaksV1, logNoAnswer, logSpeaksV2([]string{eraTestVersion})} {
		if n := logs.logged(line); n != 0 {
			t.Errorf("%q logged %d times on a forced process", line, n)
		}
	}
}

// Forced v1 on a child that speaks only v2: 2026 requests get the fallback at
// once, a 2025 initialize gets the child's own rejection, and the mismatch is
// logged and reported once.
func TestEraForced_V1OnAModernChild(t *testing.T) {
	logs := &logCapture{}
	b, bus := startForced(t, ServeV1, logs, "--era=modern")
	if got := nextVersion(t, bus); got != eventbus.VersionV1 {
		t.Fatalf("at spawn: reported %q", got)
	}

	if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 400 || !strings.Contains(r.body, v1Fallback) || r.elapsed >= 5*time.Second {
		t.Fatalf("2026 request: %d after %s: %s", r.status, r.elapsed, r.body)
	}

	mismatchLine := `This server is set to MCP v1, but it does not speak it: it accepts only MCP v2 (protocol 2026-07-28). Set its MCP version to Automatic in its config entry ("mcp_version": "auto").`
	for i := 2; i <= 3; i++ {
		r := eraPost(t, b, acceptJSON, eraInitialize(i).body, nil)
		if r.status != 400 || !strings.Contains(r.body, `"code":-32022`) || !strings.Contains(r.body, "Unsupported protocol version") {
			t.Fatalf("initialize %d: %d %s", i, r.status, r.body)
		}
		logs.waitLogged(t, mismatchLine, 1)
	}
	want := eventbus.AppError{
		Code:    "MCP_VERSION",
		Message: "This server doesn't speak MCP v1.",
		Service: "fake-era",
		Hint:    `Set its MCP version to Automatic in its config entry ("mcp_version": "auto").`,
	}
	if got := nextEvent(t, bus); got != want {
		t.Fatalf("after the rejection: %#v, want %#v", got, want)
	}

	if n := discoverCount(t, b); n != 0 {
		t.Fatalf("the child got %d server/discover", n)
	}
	if got := phaseOf(b); got != phaseV1 {
		t.Fatalf("phase %s", describe(mach(got)))
	}
	settle(b)
	noMoreVersions(t, bus)
	if n := logs.closeAndCount(b, mismatchLine); n != 1 {
		t.Fatalf("the mismatch line was logged %d times", n)
	}
	noVerdictLines(t, logs)
}

// Forced v2 on a child that speaks only v1: no probe, 2025 initializes are
// refused by the bridge, and a client's server/discover gets the child's own
// error and logs the mismatch once, after which refused clients are no longer
// logged.
func TestEraForced_V2OnALegacyChild(t *testing.T) {
	logs := &logCapture{}
	b, bus := startForced(t, ServeV2, logs, "--era=legacy")
	if got := nextVersion(t, bus); got != eventbus.VersionV2 {
		t.Fatalf("at spawn: reported %q", got)
	}

	named := func(id int, name string) map[string]any {
		body := eraInitialize(id).body
		body["params"].(map[string]any)["clientInfo"] = map[string]any{"name": name, "version": "1"}
		return body
	}
	r := eraPost(t, b, acceptJSON, named(1, "Claude"), nil)
	if r.status != 400 || !strings.Contains(r.body, v2InitializeRefusal([]string{eraTestVersion})) || !strings.Contains(r.body, `"supported":["2026-07-28"]`) {
		t.Fatalf("initialize before any discover: %d %s", r.status, r.body)
	}
	logs.waitLogged(t, `The client "Claude" uses MCP v1 and was refused: this server speaks MCP v2 (protocol 2026-07-28).`, 1)

	methods, _ := debugCall(t, b, "debug/received", nil)["methods"].([]any)
	if len(methods) != 0 {
		t.Fatalf("the child received %v before any client message was forwarded", methods)
	}

	mismatchLine := `This server is set to MCP v2 (protocol 2026-07-28), but it does not speak it. Set its MCP version to Automatic in its config entry ("mcp_version": "auto").`
	for i := 2; i <= 3; i++ {
		r := eraPost(t, b, acceptJSON, modernBody(i, "server/discover", nil, nil), nil)
		if r.status != 404 || !strings.Contains(r.body, "Method not found: server/discover") {
			t.Fatalf("server/discover %d: %d %s", i, r.status, r.body)
		}
		logs.waitLogged(t, mismatchLine, 1)
	}

	otherLine := `The client "Other" uses MCP v1 and was refused: this server speaks MCP v2 (protocol 2026-07-28).`
	if r := eraPost(t, b, acceptJSON, named(4, "Other"), nil); r.status != 400 {
		t.Fatalf("initialize after the mismatch: %d %s", r.status, r.body)
	}
	if got := phaseOf(b); got != phaseV2 {
		t.Fatalf("phase %s", describe(mach(got)))
	}
	settle(b)
	noMoreVersions(t, bus)
	if n := logs.closeAndCount(b, otherLine); n != 0 {
		t.Fatalf("a refused client was logged after the mismatch")
	}
	if n := logs.logged(mismatchLine); n != 1 {
		t.Fatalf("the mismatch line was logged %d times", n)
	}
	noVerdictLines(t, logs)
}

// A forced version on a child that speaks it, or that never answers
// server/discover: the child is served as that version from spawn with no
// probe and no probe timer, and nothing about its version is logged or
// reported after the spawn report.
func TestEraForced_MatchingOrSilentChild(t *testing.T) {
	const probe = 50 * time.Millisecond
	mismatch := "but it does not speak it"
	// pastTheProbeDeadline round-trips a debug call until one has started
	// later than the probe timer would have fired, had there been one.
	pastTheProbeDeadline := func(t *testing.T, b *Bridge, start time.Time) {
		t.Helper()
		deadline := start.Add(4 * probe)
		for done := false; !done; {
			done = time.Now().After(deadline)
			debugCall(t, b, "debug/requestCount", nil)
		}
	}
	for _, c := range []struct {
		name  string
		as    ServeAs
		args  []string
		phase eraPhase
		want  string
		check func(t *testing.T, b *Bridge, start time.Time)
	}{
		{"v1 on a legacy child", ServeV1, []string{"--era=legacy"}, phaseV1, eventbus.VersionV1, func(t *testing.T, b *Bridge, _ time.Time) {
			r := eraPost(t, b, acceptJSON, eraInitialize(1).body, nil)
			if r.status != 200 || r.header.Get("Mcp-Session-Id") == "" || !sessionMinted(b) {
				t.Fatalf("initialize: %d, session %q: %s", r.status, r.header.Get("Mcp-Session-Id"), r.body)
			}
			methods, _ := debugCall(t, b, "debug/received", nil)["methods"].([]any)
			for _, m := range methods {
				if m == "server/discover" {
					t.Fatalf("the child received %v", methods)
				}
			}
		}},
		{"v2 on a modern child", ServeV2, []string{"--era=modern"}, phaseV2, eventbus.VersionV2, func(t *testing.T, b *Bridge, _ time.Time) {
			if r := eraPost(t, b, acceptJSON, modernCall(1), nil); r.status != 200 || !strings.Contains(r.body, "modern:echo") {
				t.Fatalf("2026 request: %d %s", r.status, r.body)
			}
			for _, method := range []string{http.MethodGet, http.MethodDelete} {
				if status, allow, body := getOrDelete(t, b, method, nil); status != 405 || allow != "POST" {
					t.Fatalf("%s: %d, Allow %q, %q", method, status, allow, body)
				}
			}
			if r := eraPost(t, b, acceptJSON, modernBody(2, "server/discover", nil, nil), nil); r.status != 200 || !strings.Contains(r.body, `"supportedVersions":["2026-07-28"]`) {
				t.Fatalf("client server/discover: %d %s", r.status, r.body)
			}
			// The client's server/discover is the only one, and the first
			// message the child read came from a client.
			if n := discoverCount(t, b); n != 1 {
				t.Fatalf("the child got %d server/discover, want the client's alone", n)
			}
			methods, _ := debugCall(t, b, "debug/received", nil)["methods"].([]any)
			if len(methods) == 0 || methods[0] != "tools/call" {
				t.Fatalf("the child received %v", methods)
			}
		}},
		{"v1 on a silent child", ServeV1, []string{"--era=legacy-silent"}, phaseV1, eventbus.VersionV1, pastTheProbeDeadline},
		{"v2 on a silent child", ServeV2, []string{"--era=modern", "--discover-hold"}, phaseV2, eventbus.VersionV2, pastTheProbeDeadline},
	} {
		t.Run(c.name, func(t *testing.T) {
			logs := &logCapture{}
			start := time.Now()
			b, bus := startReporting(t, func(o *StartBridgeOptions) {
				o.ServeAs = c.as
				o.Log = logs.logger()
				o.ProbeTimeout = probe
			}, c.args...)
			if got := nextVersion(t, bus); got != c.want {
				t.Fatalf("at spawn: reported %q", got)
			}
			c.check(t, b, start)
			if got := phaseOf(b); got != c.phase {
				t.Fatalf("phase %s", describe(mach(got)))
			}
			settle(b)
			noMoreVersions(t, bus)
			if n := logs.closeAndCount(b, logNoAnswer); n != 0 {
				t.Fatalf("%q was logged %d times", logNoAnswer, n)
			}
			for _, sub := range []string{"No answer from the server", mismatch} {
				if n := logs.count(sub); n != 0 {
					t.Fatalf("%q was logged:\n%s", sub, logs.text())
				}
			}
			noVerdictLines(t, logs)
		})
	}
}

// The forced version applies to every process of the bridge: a replacement
// child is reported as the same version, after the old one's clear, and is
// sent no server/discover either.
func TestEraForced_SurvivesReplaceChild(t *testing.T) {
	for _, c := range []struct {
		as    ServeAs
		phase eraPhase
		want  string
	}{
		{ServeV1, phaseV1, eventbus.VersionV1},
		{ServeV2, phaseV2, eventbus.VersionV2},
	} {
		logs := &logCapture{}
		b, bus := startForced(t, c.as, logs, "--era=modern")
		old := b.GetCurrentChild()
		t.Cleanup(func() { _ = old.Close() })
		handled := exitHandled(old)
		if got := nextVersion(t, bus); got != c.want {
			t.Fatalf("ServeAs %d at spawn: reported %q", c.as, got)
		}
		b.ReplaceChild(newEraChild(t, "--era=modern"))
		if got := nextVersion(t, bus); got != "" {
			t.Fatalf("ServeAs %d after ReplaceChild: reported %q, want the clear first", c.as, got)
		}
		if got := nextVersion(t, bus); got != c.want {
			t.Fatalf("ServeAs %d new process: reported %q", c.as, got)
		}
		b.mu.Lock()
		m := b.era.m
		b.mu.Unlock()
		if m.phase != c.phase || !m.forced {
			t.Fatalf("ServeAs %d new process: %s", c.as, describe(m))
		}
		if n := discoverCount(t, b); n != 0 {
			t.Fatalf("ServeAs %d: the new child got %d server/discover", c.as, n)
		}
		_ = old.Close()
		waitExitHandled(t, handled)
		settle(b)
		noMoreVersions(t, bus)
	}
}
