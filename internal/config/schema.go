// Package config loads and validates ~/.mcpwarp/config.json, matching the
// Node CLI's zod schema, error messages, and exit codes.
package config

// Server kind tags, matching Node's discriminated union values.
const (
	KindStdio = "stdio"
	KindHTTP  = "http"
)

// MaxServers is the tunnel's cap on services per register message
// (mcpwarp-saas agentconn.go's maxServicesPerRegister). The CLI registers
// every configured server in one message on each welcome, and the tunnel
// refuses a bigger one as a whole (TOO_MANY_SERVICES, nothing registered),
// so a config over the cap is rejected here instead (DESIGN.md §6).
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
