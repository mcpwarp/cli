// Package config loads and validates ~/.mcpwarp/config.json, matching the
// Node CLI's zod schema, error messages, and exit codes.
package config

// Server kind tags, matching Node's discriminated union values.
const (
	KindStdio = "stdio"
	KindHTTP  = "http"
)

// MaxServers caps a config at the tunnel's per-message cap on services
// (mcpwarp-saas agentconn.go's maxServicesPerRegister). It is a config
// limit, not a wire necessity: internal/tunnel registers in frames of at
// most 100 names, one in flight (appproto.MaxServicesPerFrame), so a larger
// list would still go out; the cap is kept at 100 by choice (DESIGN.md §6).
const MaxServers = 100

// Server is one entry of config.json's "servers" array.
type Server struct {
	Name string
	Kind string

	// stdio fields
	Command string
	Args    []string
	Env     map[string]string
	Cwd     string
	HasCwd  bool

	// http fields
	URL string
}

// Config is the parsed, validated contents of config.json.
type Config struct {
	Servers []Server
}
