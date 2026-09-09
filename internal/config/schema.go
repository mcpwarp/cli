// Package config loads and validates ~/.mcpwarp/config.json, matching the
// Node CLI's zod schema, error messages, and exit codes.
package config

// Server kind tags, matching Node's discriminated union values.
const (
	KindStdio = "stdio"
	KindHTTP  = "http"
)

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
