// Package bridge implements the stdio<->HTTP bridge (DESIGN.md §3, §7):
// one NDJSON-framed stdio child per configured server, fronted by a
// loopback net/http listener that speaks both the session-based and
// stateless MCP streamable-HTTP transports, classifying JSON-RPC traffic
// by shape rather than by protocol version. Ported from mcpwarp-cli's
// src/bridge/{stdio-child,http-bridge,push-stream,session}.ts.
package bridge

// SpawnSpec describes how to launch one stdio MCP server child process.
type SpawnSpec struct {
	Command string
	Args    []string
	// Env is merged onto os.Environ(); on key collision Env wins
	// (DESIGN.md §3's "env" field, ported from stdio-child.ts).
	Env map[string]string
	Cwd string
}
