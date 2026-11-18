package bridge

import "strings"

// What the bridge itself says about a server's MCP version, to clients and in
// the log. v2 lines name the versions the child listed, modern ones only.

const (
	v1Fallback = "This server speaks MCP v1 (protocol 2025-11-25 and earlier) and does not accept MCP v2 requests."

	logSpeaksV1 = "The server speaks MCP v1 (protocol 2025-11-25 and earlier)."
	logNoAnswer = "No answer from the server within 15 s; serving it as MCP v1 until it answers."

	// suggestAuto is the way out for a server whose forced version is wrong;
	// suggestV1 is appended to the refused-client line on automatic only.
	suggestAuto = `Set its MCP version to Automatic in its config entry ("mcp_version": "auto").`
	suggestV1   = `If the server also supports v1, set its MCP version to v1 in its config entry ("mcp_version": "v1").`

	logForcedV2Mismatch = "This server is set to MCP v2 (protocol 2026-07-28), but it does not speak it. " + suggestAuto

	// The eventbus.AppError for a server forced to v1 that speaks only v2.
	forcedV1ErrorCode    = "MCP_VERSION"
	forcedV1ErrorMessage = "This server doesn't speak MCP v1."
)

// v2Protocols is "protocol 2026-07-28", or "protocols 2026-07-28, 2027-01-15"
// for several.
func v2Protocols(versions []string) string {
	if len(versions) == 1 {
		return "protocol " + versions[0]
	}
	return "protocols " + strings.Join(versions, ", ")
}

func v2InitializeRefusal(versions []string) string {
	return "This server speaks MCP v2 (" + v2Protocols(versions) + ") and does not accept the older MCP v1 initialize handshake."
}

func v2RequestRefusal(versions []string) string {
	return "This server speaks MCP v2 (" + v2Protocols(versions) + ") and does not accept older MCP v1 requests."
}

func v2NoSessions(versions []string) string {
	return "This server speaks MCP v2 (" + v2Protocols(versions) + "), which has no sessions and no GET stream."
}

func logSpeaksV2(versions []string) string {
	return "The server speaks MCP v2 (" + v2Protocols(versions) + ")."
}

func logAnsweredLate(versions []string) string {
	return "The server answered late; it speaks MCP v2 (" + v2Protocols(versions) + ")."
}

func logLateAfterV1(versions []string) string {
	return "The server said it speaks MCP v2 (" + v2Protocols(versions) + ") after an MCP v1 client had connected; it stays MCP v1 until it restarts."
}

func logRefusedClient(name string, versions []string) string {
	return `The client "` + name + `" uses MCP v1 and was refused: this server speaks MCP v2 (` + v2Protocols(versions) + ")."
}

// logRefusedNamelessClient is logRefusedClient for a client whose clientInfo
// has no string name.
func logRefusedNamelessClient(versions []string) string {
	return "A client uses MCP v1 and was refused: this server speaks MCP v2 (" + v2Protocols(versions) + ")."
}

// logForcedV1Mismatch names the versions the child's rejection of an
// initialize listed.
func logForcedV1Mismatch(versions []string) string {
	return "This server is set to MCP v1, but it does not speak it: it accepts only MCP v2 (" + v2Protocols(versions) + "). " + suggestAuto
}
