package bridge

import (
	"fmt"
	"strings"
)

// eraWant is the recorded behaviour of the bridge, keyed
// "scenario|client|child mode|form|step"; a %s in the key stands for both
// response forms ("sse" and "json"). Fields: HTTP status, whether
// Mcp-Session-Id came back, normalised body (ids replaced by "ID"), outcome,
// and for the dual-era probe the client's decision.
//
// The bridge asks each child server/discover at spawn and serves it as one
// MCP version for its life: v2 on a modern answer, v1 on any other answer or
// none in time. The comments give the reason for each expectation.
var eraWant = buildEraWant()

const (
	bodyLegacyCall     = `{"id":"ID","jsonrpc":"2.0","result":{"content":[{"text":"legacy:echo","type":"text"}]}}`
	bodyModernCall     = `{"id":"ID","jsonrpc":"2.0","result":{"content":[{"text":"modern:echo","type":"text"}],"resultType":"complete"}}`
	bodyDiscoverModern = `{"id":"ID","jsonrpc":"2.0","result":{"cacheScope":"private","capabilities":{"tools":{}},"resultType":"complete","supportedVersions":["2026-07-28"],"ttlMs":0}}`
	bodyUnknownMethod  = `{"error":{"code":-32601,"message":"Method not found: foo/bar"},"id":"ID","jsonrpc":"2.0"}`
	bodyBadVersion     = `{"error":{"code":-32022,"data":{"requested":"1900-01-01","supported":["2026-07-28"]},"message":"Unsupported protocol version"},"id":"ID","jsonrpc":"2.0"}`
	bodyUnknownSession = `{"error":"unknown session"}`

	// What the bridge itself answers. bodyV1Fallback must not look like a
	// modern server's answer (no -32601/-32602/-32020..-32022), or a
	// dual-era client would not fall back to initialize.
	bodyV1Fallback     = `{"error":{"code":-32600,"message":"This server speaks MCP v1 (protocol 2025-11-25 and earlier) and does not accept MCP v2 requests."},"id":"ID","jsonrpc":"2.0"}`
	bodyV1FallbackNote = `{"error":{"code":-32600,"message":"This server speaks MCP v1 (protocol 2025-11-25 and earlier) and does not accept MCP v2 requests."},"jsonrpc":"2.0"}`
	bodyInitRefusedV2  = `{"error":{"code":-32022,"data":{"requested":"2025-11-25","supported":["2026-07-28"]},"message":"This server speaks MCP v2 (protocol 2026-07-28) and does not accept the older MCP v1 initialize handshake."},"id":"ID","jsonrpc":"2.0"}`
	bodyCallRefusedV2  = `{"error":{"code":-32600,"data":{"supported":["2026-07-28"]},"message":"This server speaks MCP v2 (protocol 2026-07-28) and does not accept older MCP v1 requests."},"id":"ID","jsonrpc":"2.0"}`
	bodyNoSessionsV2   = `This server speaks MCP v2 (protocol 2026-07-28), which has no sessions and no GET stream. (Allow: POST)`

	// What the modern child itself answers to 2025 traffic it is given on a
	// process served as v1: forwarded, so returned unchanged.
	bodyInitChildError    = `{"error":{"code":-32022,"data":{"requested":"2025-11-25","supported":["2026-07-28"]},"message":"Unsupported protocol version"},"id":"ID","jsonrpc":"2.0"}`
	bodyCallChildNoEnvlop = `{"error":{"code":-32602,"message":"Invalid params: _meta.io.modelcontextprotocol/protocolVersion is required"},"id":"ID","jsonrpc":"2.0"}`
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
	stream := func(session bool) eraExpect { return eraExpect{200, session, "(stream)", "stream", ""} }
	accepted := eraExpect{202, false, "(empty)", "accepted", ""}

	// 2025-style client: initialize, a request carrying the session, the GET
	// stream and DELETE. A v1 child takes all of it.
	for _, mode := range []string{"legacy", "legacy-silent"} {
		both("matrix|2025|"+mode+"|%s|initialize", ok(bodyInitOK(mode), true))
		both("matrix|2025|"+mode+"|%s|tools/call with session", ok(bodyLegacyCall, true))
		both("matrix|2025|"+mode+"|%s|GET stream", stream(true))
		both("matrix|2025|"+mode+"|%s|DELETE session", eraExpect{204, true, "(empty)", "empty", ""})
	}
	// A child whose discover failed is served as v1 even if it is modern: its
	// 2025 traffic is forwarded and it answers with its own errors; no
	// session is minted for its rejected initialize.
	both("matrix|2025|discover-error|%s|initialize", c4xx(400, bodyInitChildError))
	both("matrix|2025|discover-error|%s|tools/call with session", errIn200(bodyCallChildNoEnvlop, false))
	both("matrix|2025|discover-error|%s|GET stream", stream(false))
	both("matrix|2025|discover-error|%s|DELETE session", c4xx(404, bodyUnknownSession))
	// A v2 child is given nothing 2025-style: the bridge refuses initialize
	// with -32022 naming its versions, other requests with -32600, and GET and
	// DELETE with 405, since the 2026-07-28 binding has no sessions.
	both("matrix|2025|modern|%s|initialize", c4xx(400, bodyInitRefusedV2))
	both("matrix|2025|modern|%s|tools/call with session", c4xx(400, bodyCallRefusedV2))
	both("matrix|2025|modern|%s|GET stream", c4xx(405, bodyNoSessionsV2))
	both("matrix|2025|modern|%s|DELETE session", c4xx(405, bodyNoSessionsV2))

	// 2026-style client: stateless request and notification. A v1 child gets
	// neither; the bridge answers 400 -32600, which a dual-era client reads as
	// "fall back to initialize".
	for _, mode := range []string{"legacy", "legacy-silent", "discover-error"} {
		both("matrix|2026|"+mode+"|%s|tools/call stateless", c4xx(400, bodyV1Fallback))
		both("matrix|2026|"+mode+"|%s|notification", c4xx(400, bodyV1FallbackNote))
	}
	both("matrix|2026|modern|%s|tools/call stateless", ok(bodyModernCall, false))
	both("matrix|2026|modern|%s|notification", accepted)

	// Dual-era client: modern probe first, decision, optional fallback.
	both("matrix|dual-era|modern|%s|server/discover probe", ok(bodyDiscoverModern, false))
	both("matrix|dual-era|modern|%s|tools/call stateless", ok(bodyModernCall, false))
	for _, mode := range []string{"legacy", "legacy-silent", "discover-error"} {
		both("matrix|dual-era|"+mode+"|%s|server/discover probe", c4xx(400, bodyV1Fallback))
	}
	for _, mode := range []string{"legacy", "legacy-silent"} {
		both("matrix|dual-era|"+mode+"|%s|fallback initialize", ok(bodyInitOK(mode), true))
		both("matrix|dual-era|"+mode+"|%s|tools/call with session", ok(bodyLegacyCall, true))
	}
	both("matrix|dual-era|discover-error|%s|fallback initialize", c4xx(400, bodyInitChildError))
	both("matrix|dual-era|discover-error|%s|tools/call with session", errIn200(bodyCallChildNoEnvlop, false))

	// Unknown Mcp-Session-Id on a stateless request: ignored, since the
	// 2026-07-28 binding has no sessions.
	both("unknown-session|2026|modern|%s|stateless with unknown Mcp-Session-Id, none minted", ok(bodyModernCall, false))
	// A session from before a restart: on a v2 child the version refusal, not
	// a 404; on a v1 child the session gate's 404.
	both("stale-session|2025|modern|%s|tools/call with an unknown session", c4xx(400, bodyCallRefusedV2))
	both("stale-session|2025|legacy|%s|1 initialize", ok(bodyInitOK("legacy"), true))
	both("stale-session|2025|legacy|%s|2 tools/call with an unknown session", c4xx(404, bodyUnknownSession))

	// The 2026-07-28 HTTP binding maps a v2 child's final errors to HTTP
	// statuses: unknown method 404, unsupported version 400, the JSON-RPC
	// body unchanged.
	both("modern-errors|2026|modern|%s|unknown method", c4xx(404, bodyUnknownMethod))
	both("modern-errors|2026|modern|%s|unsupported protocol version", c4xx(400, bodyBadVersion))

	// Request-header validation on a v2 child. A mismatch or a missing
	// required header is a 400 -32020 and is not forwarded; a base64 sentinel
	// Mcp-Name is decoded first; missing clientCapabilities is a 400 -32602
	// from the bridge. The v1 fallback wins over bad headers.
	both("headers|2026|modern|%s|1 all headers match", ok(bodyModernCall, false))
	both("headers|2026|modern|%s|2 MCP-Protocol-Version differs from _meta", c4xx(400, `{"error":{"code":-32020,"message":"Header mismatch: MCP-Protocol-Version header value \"2025-11-25\" does not match body value \"2026-07-28\""},"id":"ID","jsonrpc":"2.0"}`))
	both("headers|2026|modern|%s|3 Mcp-Method missing", c4xx(400, `{"error":{"code":-32020,"message":"Header mismatch: Mcp-Method header is missing"},"id":"ID","jsonrpc":"2.0"}`))
	both("headers|2026|modern|%s|4 Mcp-Name differs from params.name", c4xx(400, `{"error":{"code":-32020,"message":"Header mismatch: Mcp-Name header value \"other\" does not match body value \"echo\""},"id":"ID","jsonrpc":"2.0"}`))
	both("headers|2026|modern|%s|5 Mcp-Name base64 sentinel, matching", ok(bodyModernCall, false))
	both("headers|2026|modern|%s|6 clientCapabilities missing from _meta", c4xx(400, `{"error":{"code":-32602,"message":"Invalid params: _meta[\"io.modelcontextprotocol/clientCapabilities\"] is required"},"id":"ID","jsonrpc":"2.0"}`))
	both("headers|2026|legacy|%s|1 bad MCP-Protocol-Version on a legacy child", c4xx(400, bodyV1Fallback))

	// A v1 child rejecting one bad initialize must not be locked out. The
	// child's own error comes back as a 400 with no session, and the next good
	// initialize gets a session.
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

	// A modern child that answers discover after the probe timeout is v1 for
	// now, then v2 once its late answer arrives.
	both("late-probe|2026|modern (slow discover)|%s|1 probe times out", c4xx(400, bodyV1Fallback))
	both("late-probe|2026|modern (slow discover)|%s|2 after the late answer", ok(bodyModernCall, false))

	// Dual-era decisions, set on the probe rows.
	for k, e := range m {
		if strings.Contains(k, "|dual-era|") && strings.Contains(k, "|server/discover probe") {
			if strings.Contains(k, "|modern|") {
				e.decision = "modern"
			} else {
				e.decision = "fallback"
			}
			m[k] = e
		}
	}
	return m
}
