package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

var namePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

// issue is one validation problem, rendered as "  path: message" — matching
// Node's zod-issue formatting (config/load.ts formatIssuePath).
type issue struct {
	path    string
	message string
}

func (i issue) String() string {
	return fmt.Sprintf("  %s: %s", i.path, i.message)
}

type issueList []issue

func (l issueList) Error() string {
	lines := make([]string, len(l))
	for i, is := range l {
		lines[i] = is.String()
	}
	return strings.Join(lines, "\n")
}

// validateConfig mirrors ConfigSchema.safeParse (schema.ts): strictObject
// semantics plus hand-written rules, in the same field order zod would visit
// them so per-object issues surface in a stable, matching sequence.
func validateConfig(raw []byte) (*Config, issueList) {
	if isJSONNull(raw) {
		return nil, issueList{{path: "", message: fmt.Sprintf("Invalid input: expected object, received %s", jsonTypeName(raw))}}
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		// Caller already validated raw is syntactically valid JSON; a
		// non-object top level (e.g. `[]` or `"x"`) lands here.
		return nil, issueList{{path: "", message: fmt.Sprintf("Invalid input: expected object, received %s", jsonTypeName(raw))}}
	}

	var issues issueList

	serversRaw, hasServers := top["servers"]
	extraKeys := unrecognizedKeys(top, []string{"servers"})
	// appendExtraKeys is called after every servers-field issue below: zod
	// reports unrecognized-key issues after field issues, never before.
	appendExtraKeys := func() {
		if len(extraKeys) > 0 {
			issues = append(issues, issue{path: "", message: unrecognizedKeysMessage(extraKeys)})
		}
	}

	if !hasServers {
		issues = append(issues, issue{path: "servers", message: "Invalid input: expected array, received undefined"})
		appendExtraKeys()
		return nil, issues
	}

	if isJSONNull(serversRaw) {
		issues = append(issues, issue{path: "servers", message: fmt.Sprintf("Invalid input: expected array, received %s", jsonTypeName(serversRaw))})
		appendExtraKeys()
		return nil, issues
	}
	var rawServers []json.RawMessage
	if err := json.Unmarshal(serversRaw, &rawServers); err != nil {
		issues = append(issues, issue{path: "servers", message: fmt.Sprintf("Invalid input: expected array, received %s", jsonTypeName(serversRaw))})
		appendExtraKeys()
		return nil, issues
	}

	if len(rawServers) == 0 {
		issues = append(issues, issue{path: "servers", message: "servers must not be empty"})
	}

	servers := make([]Server, 0, len(rawServers))
	names := map[string]int{}
	for i, rawServer := range rawServers {
		path := fmt.Sprintf("servers[%d]", i)
		server, serverIssues := validateServer(rawServer, path)
		issues = append(issues, serverIssues...)
		if serverIssues == nil {
			if first, seen := names[server.Name]; seen {
				issues = append(issues, issue{
					path:    path + ".name",
					message: fmt.Sprintf("duplicate server name %q (already used at servers[%d])", server.Name, first),
				})
			} else {
				names[server.Name] = i
			}
			servers = append(servers, server)
		}
	}

	appendExtraKeys()

	if len(issues) > 0 {
		return nil, issues
	}
	return &Config{Servers: servers}, nil
}

func validateServer(raw json.RawMessage, path string) (Server, issueList) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return Server{}, issueList{{path: path, message: fmt.Sprintf("Invalid input: expected object, received %s", jsonTypeName(raw))}}
	}

	// kind is zod's discriminant (schema.ts's z.discriminatedUnion("kind",
	// ...)): it's checked before any other field, and unlike every other
	// field here it never goes through requireString — a discriminated
	// union that fails to route never descends into either branch schema,
	// so on a bad kind zod reports only this one issue and skips name,
	// command/url, etc. entirely, no matter how invalid they are.
	kind, kindValid := discriminantKind(obj)
	if !kindValid {
		return Server{}, issueList{{path: path + ".kind", message: "Invalid discriminator value. Expected 'stdio' | 'http'"}}
	}

	var issues issueList
	var server Server

	// name
	name, nameIssues := requireString(obj, "name", path+".name")
	issues = append(issues, nameIssues...)
	if nameIssues == nil {
		// zod runs min, max, and the regex refine unconditionally (every
		// chained check on a field runs regardless of earlier failures), so
		// a bad name can carry more than one issue at once — these are not
		// else-if.
		nameLen := utf8.RuneCountInString(name)
		if nameLen < 1 {
			issues = append(issues, issue{path: path + ".name", message: "Too small: expected string to have >=1 characters"})
		}
		if nameLen > 30 {
			issues = append(issues, issue{path: path + ".name", message: "Too big: expected string to have <=30 characters"})
		}
		if !namePattern.MatchString(name) {
			issues = append(issues, issue{
				path:    path + ".name",
				message: "must be lowercase alphanumeric with hyphens, starting and ending with a lowercase letter or digit",
			})
		}
		server.Name = name
	}

	server.Kind = kind

	var allowed []string
	if kind == KindStdio {
		allowed = []string{"name", "kind", "command", "args", "env", "cwd"}

		command, commandIssues := requireString(obj, "command", path+".command")
		issues = append(issues, commandIssues...)
		if commandIssues == nil {
			if len(command) < 1 {
				issues = append(issues, issue{path: path + ".command", message: "Too small: expected string to have >=1 characters"})
			}
			server.Command = command
		}

		if rawArgs, ok := obj["args"]; ok {
			args, argIssues := decodeStringArray(rawArgs, path+".args")
			issues = append(issues, argIssues...)
			server.Args = args
		} else {
			server.Args = []string{}
		}

		if rawEnv, ok := obj["env"]; ok {
			env, envIssues := decodeStringMap(rawEnv, path+".env")
			issues = append(issues, envIssues...)
			server.Env = env
		} else {
			server.Env = map[string]string{}
		}

		if rawCwd, ok := obj["cwd"]; ok {
			cwd, cwdIssues := decodeString(rawCwd, path+".cwd")
			issues = append(issues, cwdIssues...)
			server.Cwd = cwd
			server.HasCwd = cwdIssues == nil
		}
	} else {
		allowed = []string{"name", "kind", "url"}

		rawURL, urlIssues := requireString(obj, "url", path+".url")
		issues = append(issues, urlIssues...)
		if urlIssues == nil {
			// min, isHTTPOrHTTPSURL, and hasQueryString are three separate
			// zod refines and all three run unconditionally — not else-if.
			if len(rawURL) < 1 {
				issues = append(issues, issue{path: path + ".url", message: "Too small: expected string to have >=1 characters"})
			}
			if !isHTTPOrHTTPSURL(rawURL) {
				issues = append(issues, issue{path: path + ".url", message: "must be an http(s) URL like http://127.0.0.1:3000/mcp"})
			}
			if hasQueryString(rawURL) {
				issues = append(issues, issue{
					path:    path + ".url",
					message: "must not include a query string — mcpwarp merges each inbound request's own query onto this URL itself",
				})
			}
			server.URL = rawURL
		}
	}

	if extra := unrecognizedKeys(obj, allowed); len(extra) > 0 {
		issues = append(issues, issue{path: path, message: unrecognizedKeysMessage(extra)})
	}

	if len(issues) > 0 {
		return Server{}, issues
	}
	return server, nil
}

// discriminantKind reads obj["kind"] the way zod's discriminatedUnion reads
// its discriminant: valid only if the raw value is exactly the JSON string
// "stdio" or "http" — absent, null, a number, or any other string all fail.
func discriminantKind(obj map[string]json.RawMessage) (string, bool) {
	raw, ok := obj["kind"]
	if !ok || isJSONNull(raw) {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	if s != KindStdio && s != KindHTTP {
		return "", false
	}
	return s, true
}

func requireString(obj map[string]json.RawMessage, key, path string) (string, issueList) {
	raw, ok := obj[key]
	if !ok {
		return "", issueList{{path: path, message: "Invalid input: expected string, received undefined"}}
	}
	return decodeString(raw, path)
}

func decodeString(raw json.RawMessage, path string) (string, issueList) {
	if isJSONNull(raw) {
		return "", issueList{{path: path, message: fmt.Sprintf("Invalid input: expected string, received %s", jsonTypeName(raw))}}
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", issueList{{path: path, message: fmt.Sprintf("Invalid input: expected string, received %s", jsonTypeName(raw))}}
	}
	return s, nil
}

func decodeStringArray(raw json.RawMessage, path string) ([]string, issueList) {
	if isJSONNull(raw) {
		return nil, issueList{{path: path, message: fmt.Sprintf("Invalid input: expected array, received %s", jsonTypeName(raw))}}
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil, issueList{{path: path, message: "Invalid input: expected array"}}
	}
	var issues issueList
	out := make([]string, len(arr))
	for i, elem := range arr {
		s, elemIssues := decodeString(elem, fmt.Sprintf("%s[%d]", path, i))
		if elemIssues != nil {
			issues = append(issues, elemIssues...)
			continue
		}
		out[i] = s
	}
	if issues != nil {
		return nil, issues
	}
	return out, nil
}

func decodeStringMap(raw json.RawMessage, path string) (map[string]string, issueList) {
	if isJSONNull(raw) {
		return nil, issueList{{path: path, message: fmt.Sprintf("Invalid input: expected record, received %s", jsonTypeName(raw))}}
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, issueList{{path: path, message: "Invalid input: expected object"}}
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var issues issueList
	out := make(map[string]string, len(m))
	for _, k := range keys {
		s, elemIssues := decodeString(m[k], fmt.Sprintf("%s.%s", path, k))
		if elemIssues != nil {
			issues = append(issues, elemIssues...)
			continue
		}
		out[k] = s
	}
	if issues != nil {
		return nil, issues
	}
	return out, nil
}

// isJSONNull reports whether raw is the JSON literal `null`. Every caller
// below checks this explicitly before unmarshalling: json.Unmarshal of
// `null` into a non-pointer string/slice/map target is a documented no-op
// (leaves it at its zero value, no error), so without this check
// `"cwd": null`, `"args": null`, or `"env": null` would each silently pass
// as an empty value instead of reporting a type mismatch.
func isJSONNull(raw []byte) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func jsonTypeName(raw json.RawMessage) string {
	trimmed := strings.TrimSpace(string(raw))
	switch {
	case trimmed == "null":
		return "null"
	case trimmed == "true" || trimmed == "false":
		return "boolean"
	case len(trimmed) > 0 && (trimmed[0] == '{'):
		return "object"
	case len(trimmed) > 0 && trimmed[0] == '[':
		return "array"
	case len(trimmed) > 0 && (trimmed[0] == '"'):
		return "string"
	default:
		return "number"
	}
}

func unrecognizedKeys(obj map[string]json.RawMessage, allowed []string) []string {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, a := range allowed {
		allowedSet[a] = struct{}{}
	}
	var extra []string
	for k := range obj {
		if _, ok := allowedSet[k]; !ok {
			extra = append(extra, k)
		}
	}
	sort.Strings(extra)
	return extra
}

func unrecognizedKeysMessage(keys []string) string {
	quoted := make([]string, len(keys))
	for i, k := range keys {
		quoted[i] = fmt.Sprintf("%q", k)
	}
	label := "key"
	if len(keys) > 1 {
		label = "keys"
	}
	return fmt.Sprintf("Unrecognized %s: %s", label, strings.Join(quoted, ", "))
}

func isHTTPOrHTTPSURL(value string) bool {
	u, err := url.Parse(value)
	if err != nil {
		return false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	if !strings.HasPrefix(value, u.Scheme+"://") {
		return false
	}
	return u.Hostname() != ""
}

func hasQueryString(value string) bool {
	u, err := url.Parse(value)
	if err != nil {
		return false
	}
	return u.RawQuery != ""
}
