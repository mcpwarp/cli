// Package bridge implements the stdio<->HTTP bridge (DESIGN.md §3, §7):
// one NDJSON-framed stdio child per configured server, fronted by a
// loopback net/http listener. Each child process is asked server/discover
// at spawn, unless the config forces its version (ServeAs), and served as
// one MCP version for its life: v1 over the session-based streamable-HTTP
// transport, or v2 over the stateless one; traffic of the other version gets
// the bridge's own answer. Ported from mcpwarp-cli's
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
