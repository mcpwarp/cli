package bridge

import (
	"fmt"
	"strings"
)

// eraWant is the recorded behaviour of the bridge with era detection,
// keyed "scenario|client|child mode|form|step"; a %s in the key stands for
// both response forms ("sse" and "json"). Fields: HTTP status, whether
// Mcp-Session-Id came back, normalised body (ids replaced by "ID"),
// outcome, and for the dual-era probe the client's decision.
//
// The comments give the reason for each expectation: what a client of that
// era needs from the bridge, and why forwarding the request as-is, or minting
// a session regardless of the child's answer, would be wrong.
var eraWant = buildEraWant()

const (
	bodyLegacyCall     = `{"id":"ID","jsonrpc":"2.0","result":{"content":[{"text":"legacy:echo","type":"text"}]}}`
	bodyModernCall     = `{"id":"ID","jsonrpc":"2.0","result":{"content":[{"text":"modern:echo","type":"text"}],"resultType":"complete"}}`
	bodyDiscoverModern = `{"id":"ID","jsonrpc":"2.0","result":{"cacheScope":"private","capabilities":{"tools":{}},"resultType":"complete","supportedVersions":["2026-07-28"],"ttlMs":0}}`
	bodyDiscoverDual   = `{"id":"ID","jsonrpc":"2.0","result":{"cacheScope":"private","capabilities":{"tools":{}},"resultType":"complete","supportedVersions":["2026-07-28","2025-11-25"],"ttlMs":0}}`
	bodyUnknownMethod  = `{"error":{"code":-32601,"message":"Method not found: foo/bar"},"id":"ID","jsonrpc":"2.0"}`
	bodyBadVersion     = `{"error":{"code":-32022,"data":{"requested":"1900-01-01","supported":["2026-07-28"]},"message":"Unsupported protocol version"},"id":"ID","jsonrpc":"2.0"}`
	bodyBadVersionDual = `{"error":{"code":-32022,"data":{"requested":"1900-01-01","supported":["2026-07-28","2025-11-25"]},"message":"Unsupported protocol version"},"id":"ID","jsonrpc":"2.0"}`

	// What the bridge itself answers. bodyLegacyChild must not look like a
	// modern server's answer (no -32601/-32602/-32020..-32022), or a
	// dual-era client would not fall back to initialize.
	bodyLegacyChild    = `{"error":{"code":-32600,"message":"initialize is required: this server speaks the legacy MCP protocol; send initialize first"},"id":"ID","jsonrpc":"2.0"}`
	bodyLegacyChildVer = `{"error":{"code":-32600,"message":"initialize is required: this server speaks the legacy MCP protocol (version 2025-11-25); send initialize first"},"id":"ID","jsonrpc":"2.0"}`
	bodyCallRefused    = `{"error":{"code":-32600,"data":{"supported":["2026-07-28"]},"message":"2025-style tools/call is not supported: this server only speaks MCP protocol version(s) 2026-07-28; send requests carrying _meta[\"io.modelcontextprotocol/protocolVersion\"]"},"id":"ID","jsonrpc":"2.0"}`
	bodyInitRefused    = `{"error":{"code":-32600,"data":{"supported":["2026-07-28"]},"message":"initialize is not supported: this server only speaks MCP protocol version(s) 2026-07-28; send requests carrying _meta[\"io.modelcontextprotocol/protocolVersion\"]"},"id":"ID","jsonrpc":"2.0"}`

	// What the modern child itself answers to initialize: forwarded, so
	// returned unchanged.
	bodyInitChildError = `{"error":{"code":-32601,"data":{"supported":["2026-07-28"]},"message":"Method not found: initialize. This server supports protocol versions: 2026-07-28"},"id":"ID","jsonrpc":"2.0"}`
)

func bodyInitOK(mode string) string {
	return `{"id":"ID","jsonrpc":"2.0","result":{"capabilities":{"tools":{}},"protocolVersion":"2025-11-25","serverInfo":{"name":"fakemcp-` + mode + `","version":"0"}}}`
}

func buildEraWant() map[string]eraExpect {
	m := map[string]eraExpect{}
	both := func(key string, e eraExpect) {
		m[fmt.Sprintf(key, "sse")] = e
		m[fmt.Sprintf(key, "json")] = e
	}
	ok := func(body string, session bool) eraExpect { return eraExpect{200, session, body, "ok", ""} }
	errIn200 := func(body string, session bool) eraExpect { return eraExpect{200, session, body, "error-in-200", ""} }
	c4xx := func(status int, body string) eraExpect { return eraExpect{status, false, body, "4xx", ""} }

	// 2025-style client: initialize, then a request carrying the session.
	for _, mode := range []string{"legacy", "legacy-silent", "dual"} {
		both("matrix|2025|"+mode+"|%s|initialize", ok(bodyInitOK(mode), true))
		both("matrix|2025|"+mode+"|%s|tools/call with session", ok(bodyLegacyCall, true))
	}
	// A rejected initialize is a clean 400 carrying the child's own error,
	// with no session: a failed handshake must not leave a session behind, so
	// the 2025-style call after it carries no session header.
	// Once the child is known modern-only (here from that rejection's
	// data.supported, which lists only 2026-era versions), any 2025-style
	// request gets the bridge's own 400 -32600 refusal naming the supported
	// versions and is not forwarded: the child could only answer it with an
	// error a 2025 client would not recognise as "wrong protocol era".
	both("matrix|2025|modern|%s|initialize", c4xx(400, bodyInitChildError))
	both("matrix|2025|modern|%s|tools/call with session", c4xx(400, bodyCallRefused))

	// 2026-style client: stateless request, headers, no session.
	// Legacy and legacy-silent: a modern-shaped request is not forwarded; the
	// bridge answers 400 -32600 (after the discover probe has timed out, for
	// the silent child), which a dual-era client reads as "fall back to
	// initialize", rather than a 200-with-error or a request the silent child
	// would never answer.
	both("matrix|2026|legacy|%s|tools/call stateless", c4xx(400, bodyLegacyChild))
	both("matrix|2026|legacy-silent|%s|tools/call stateless", c4xx(400, bodyLegacyChild))
	both("matrix|2026|modern|%s|tools/call stateless", ok(bodyModernCall, false))
	both("matrix|2026|dual|%s|tools/call stateless", ok(bodyModernCall, false))

	// Dual-era client: modern probe first, decision, optional fallback.
	// Both legacy children answer the client's probe with a 400 that is not a
	// recognised modern error, so the client falls back to initialize; for
	// the silent child it gets that answer instead of having to time out.
	for _, mode := range []string{"legacy", "legacy-silent"} {
		both("matrix|dual-era|"+mode+"|%s|server/discover probe", c4xx(400, bodyLegacyChild))
		both("matrix|dual-era|"+mode+"|%s|fallback initialize", ok(bodyInitOK(mode), true))
		both("matrix|dual-era|"+mode+"|%s|tools/call with session", ok(bodyLegacyCall, true))
	}
	both("matrix|dual-era|modern|%s|server/discover probe", ok(bodyDiscoverModern, false))
	both("matrix|dual-era|dual|%s|server/discover probe", ok(bodyDiscoverDual, false))
	for _, mode := range []string{"modern", "dual"} {
		both("matrix|dual-era|"+mode+"|%s|tools/call stateless", ok(bodyModernCall, false))
	}

	// Shared child: a 2026-style request before and after a 2025 client
	// initializes it through the same bridge.
	for _, mode := range []string{"legacy", "legacy-silent", "modern", "dual"} {
		k := "shared-child|2026+2025|" + mode + "|%s|"
		switch mode {
		case "legacy", "legacy-silent":
			// The bridge's own 400, as for any modern-shaped request to a
			// legacy child.
			both(k+"1 stateless before initialize", c4xx(400, bodyLegacyChild))
			both(k+"2 initialize by 2025 client", ok(bodyInitOK(mode), true))
			// A modern-shaped request is stateless, so it is not gated on a
			// session header; it is still not forwarded to a legacy child, and
			// the 400 names the version learned from the initialize.
			both(k+"3 stateless after initialize, no session", c4xx(400, bodyLegacyChildVer))
			both(k+"4 stateless after initialize, live session", c4xx(400, bodyLegacyChildVer))
		case "modern":
			both(k+"1 stateless before initialize", ok(bodyModernCall, false))
			// The probe answer alone does not tell a 2026-only child from a
			// dual-era one, so initialize is forwarded: the child's own
			// rejection comes back in a clean 400, and no session is minted.
			// That rejection, with the probe answer, is what makes the bridge
			// refuse later 2025 traffic itself.
			both(k+"2 initialize by 2025 client", c4xx(400, bodyInitChildError))
			// No session was minted, so the stateless client is not locked
			// out by a session gate.
			both(k+"3 stateless after initialize, no session", ok(bodyModernCall, false))
			both(k+"4 stateless after initialize, live session", ok(bodyModernCall, false))
		default:
			both(k+"1 stateless before initialize", ok(bodyModernCall, false))
			both(k+"2 initialize by 2025 client", ok(bodyInitOK(mode), true))
			// A modern request is stateless: it works without the session
			// header, and with one the header is ignored, never echoed.
			both(k+"3 stateless after initialize, no session", ok(bodyModernCall, false))
			both(k+"4 stateless after initialize, live session", ok(bodyModernCall, false))
		}
	}

	// Failed initialize: a failed initialize must not mint a session, so a
	// 2026-style client on a modern child is not locked out afterwards.
	// The first is the child's own error; the second the bridge's cached
	// refusal, which the rejection's data.supported (only 2026-era versions)
	// made known; a bare -32601 would cache nothing.
	both("failed-initialize|2025|modern|%s|1 initialize rejected by child", c4xx(400, bodyInitChildError))
	both("failed-initialize|2025|modern|%s|2 initialize again", c4xx(400, bodyInitRefused))
	both("failed-initialize|2026|modern|%s|1 initialize rejected by child", c4xx(400, bodyInitChildError))
	both("failed-initialize|2026|modern|%s|2 stateless tools/call afterwards", ok(bodyModernCall, false))

	// Unknown Mcp-Session-Id on a stateless request: ignored, since the
	// 2026-07-28 binding has no sessions.
	both("unknown-session|2026|modern|%s|stateless with unknown Mcp-Session-Id, none minted", ok(bodyModernCall, false))
	both("unknown-session|2026|dual|%s|1 initialize by 2025 client", ok(bodyInitOK("dual"), true))
	both("unknown-session|2026|dual|%s|2 stateless with unknown Mcp-Session-Id", ok(bodyModernCall, false))

	// The 2026-07-28 HTTP binding maps a modern child's final errors to HTTP
	// statuses: unknown method 404, unsupported version 400, the JSON-RPC
	// body unchanged.
	both("modern-errors|2026|modern|%s|unknown method", c4xx(404, bodyUnknownMethod))
	both("modern-errors|2026|modern|%s|unsupported protocol version", c4xx(400, bodyBadVersion))
	both("modern-errors|2026|dual|%s|unknown method", c4xx(404, bodyUnknownMethod))
	both("modern-errors|2026|dual|%s|unsupported protocol version", c4xx(400, bodyBadVersionDual))

	// Request-header validation on a modern child. A mismatch or a
	// missing required header is a 400 -32020 and is not forwarded; a base64
	// sentinel Mcp-Name is decoded first; missing clientCapabilities is a
	// 400 -32602 from the bridge. The legacy child's -32600 wins over bad
	// headers.
	both("headers|2026|modern|%s|1 all headers match", ok(bodyModernCall, false))
	both("headers|2026|modern|%s|2 MCP-Protocol-Version differs from _meta", c4xx(400, `{"error":{"code":-32020,"message":"Header mismatch: MCP-Protocol-Version header value \"2025-11-25\" does not match body value \"2026-07-28\""},"id":"ID","jsonrpc":"2.0"}`))
	both("headers|2026|modern|%s|3 Mcp-Method missing", c4xx(400, `{"error":{"code":-32020,"message":"Header mismatch: Mcp-Method header is missing"},"id":"ID","jsonrpc":"2.0"}`))
	both("headers|2026|modern|%s|4 Mcp-Name differs from params.name", c4xx(400, `{"error":{"code":-32020,"message":"Header mismatch: Mcp-Name header value \"other\" does not match body value \"echo\""},"id":"ID","jsonrpc":"2.0"}`))
	both("headers|2026|modern|%s|5 Mcp-Name base64 sentinel, matching", ok(bodyModernCall, false))
	both("headers|2026|modern|%s|6 clientCapabilities missing from _meta", c4xx(400, `{"error":{"code":-32602,"message":"Invalid params: _meta[\"io.modelcontextprotocol/clientCapabilities\"] is required"},"id":"ID","jsonrpc":"2.0"}`))
	both("headers|2026|legacy|%s|1 bad MCP-Protocol-Version on a legacy child", c4xx(400, bodyLegacyChild))

	// A legacy child rejecting one bad initialize must not be locked out. The child's own
	// error comes back as a 400 with no session, nothing is cached as
	// modern-only, and the next good initialize gets a session.
	const bodyBadInit = `{"error":{"code":-32602,"message":"Invalid params: bad initialize"},"id":"ID","jsonrpc":"2.0"}`
	const kBad = "bad-initialize|2025|legacy (rejects first initialize)|%s|"
	both(kBad+"1 initialize rejected", c4xx(400, bodyBadInit))
	both(kBad+"2 initialize again", ok(bodyInitOK("legacy"), true))
	both(kBad+"3 tools/call with session", ok(bodyLegacyCall, true))

	// The same with a child slower than the initialize hold. JSON waits
	// for the answer; SSE commits 200 with a provisional session at the
	// hold deadline and a late rejection arrives inside the stream (the
	// session is revoked, which the next initialize shows by minting anew).
	const kSlow = "slow-initialize|2025|legacy (slow, rejects first initialize)|%s|"
	m[fmt.Sprintf(kSlow, "json")+"1 initialize, late rejection"] = c4xx(400, bodyBadInit)
	m[fmt.Sprintf(kSlow, "sse")+"1 initialize, late rejection"] = errIn200(bodyBadInit, true)
	both(kSlow+"2 initialize again", ok(bodyInitOK("legacy"), true))

	// A modern child that answers discover after the probe timeout
	// reads as legacy until its late answer upgrades the verdict.
	both("late-probe|2026|modern (slow discover)|%s|1 probe times out", c4xx(400, bodyLegacyChild))
	both("late-probe|2026|modern (slow discover)|%s|2 after the late answer", ok(bodyModernCall, false))

	// Dual-era decisions, set on the probe rows.
	for k, e := range m {
		if strings.Contains(k, "|dual-era|") && strings.Contains(k, "|server/discover probe") {
			if strings.Contains(k, "|legacy|") || strings.Contains(k, "|legacy-silent|") {
				e.decision = "fallback"
			} else {
				e.decision = "modern"
			}
			m[k] = e
		}
	}
	return m
}
