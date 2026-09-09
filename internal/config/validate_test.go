package config

import (
	"strings"
	"testing"
)

func mustValidate(t *testing.T, raw string) *Config {
	t.Helper()
	cfg, issues := validateConfig([]byte(raw))
	if issues != nil {
		t.Fatalf("expected success, got issues: %v", issues)
	}
	return cfg
}

func mustFail(t *testing.T, raw string) issueList {
	t.Helper()
	cfg, issues := validateConfig([]byte(raw))
	if issues == nil {
		t.Fatalf("expected failure, got success: %+v", cfg)
	}
	return issues
}

func containsPath(issues issueList, path string) bool {
	for _, i := range issues {
		if i.path == path {
			return true
		}
	}
	return false
}

func TestValidateConfig(t *testing.T) {
	t.Run("accepts a valid mixed stdio + http config", func(t *testing.T) {
		mustValidate(t, `{"servers":[
			{"name":"blender","kind":"stdio","command":"uvx","args":["blender-mcp"]},
			{"name":"notes","kind":"http","url":"http://127.0.0.1:8765/mcp"}
		]}`)
	})

	t.Run("defaults args, env for stdio servers and leaves cwd unset", func(t *testing.T) {
		cfg := mustValidate(t, `{"servers":[{"name":"blender","kind":"stdio","command":"uvx"}]}`)
		s := cfg.Servers[0]
		if len(s.Args) != 0 || s.Args == nil {
			t.Errorf("expected non-nil empty args, got %#v", s.Args)
		}
		if len(s.Env) != 0 || s.Env == nil {
			t.Errorf("expected non-nil empty env, got %#v", s.Env)
		}
		if s.HasCwd {
			t.Errorf("expected cwd unset")
		}
	})

	t.Run("rejects an empty servers array", func(t *testing.T) {
		mustFail(t, `{"servers":[]}`)
	})

	t.Run("rejects duplicate server names across kinds", func(t *testing.T) {
		issues := mustFail(t, `{"servers":[
			{"name":"dup","kind":"stdio","command":"uvx"},
			{"name":"dup","kind":"http","url":"http://127.0.0.1:8765/mcp"}
		]}`)
		if !containsPath(issues, "servers[1].name") {
			t.Errorf("expected issue at servers[1].name, got %v", issues)
		}
	})

	t.Run("rejects an uppercase or otherwise invalid name", func(t *testing.T) {
		issues := mustFail(t, `{"servers":[{"name":"Blender","kind":"stdio","command":"uvx"}]}`)
		if !containsPath(issues, "servers[0].name") {
			t.Errorf("expected issue at servers[0].name, got %v", issues)
		}
	})

	t.Run("rejects a name starting with a hyphen", func(t *testing.T) {
		mustFail(t, `{"servers":[{"name":"-blender","kind":"stdio","command":"uvx"}]}`)
	})

	t.Run("rejects a name ending with a hyphen", func(t *testing.T) {
		mustFail(t, `{"servers":[{"name":"blender-","kind":"stdio","command":"uvx"}]}`)
	})

	t.Run("accepts a single-character name", func(t *testing.T) {
		mustValidate(t, `{"servers":[{"name":"a","kind":"stdio","command":"uvx"}]}`)
	})

	t.Run("rejects a stdio server missing command", func(t *testing.T) {
		issues := mustFail(t, `{"servers":[{"name":"blender","kind":"stdio"}]}`)
		if !containsPath(issues, "servers[0].command") {
			t.Errorf("expected issue at servers[0].command, got %v", issues)
		}
	})

	t.Run("rejects an http server with a non-URL", func(t *testing.T) {
		issues := mustFail(t, `{"servers":[{"name":"notes","kind":"http","url":"not a url"}]}`)
		if !containsPath(issues, "servers[0].url") {
			t.Errorf("expected issue at servers[0].url, got %v", issues)
		}
	})

	t.Run("rejects an http server with a non-http(s) URL", func(t *testing.T) {
		mustFail(t, `{"servers":[{"name":"notes","kind":"http","url":"ftp://example.com/mcp"}]}`)
	})

	t.Run("rejects a URL missing the // after the scheme", func(t *testing.T) {
		issues := mustFail(t, `{"servers":[{"name":"notes","kind":"http","url":"http:8765"}]}`)
		if !containsPath(issues, "servers[0].url") {
			t.Errorf("expected issue at servers[0].url, got %v", issues)
		}
	})

	t.Run("accepts a bare-hostname http(s) URL", func(t *testing.T) {
		mustValidate(t, `{"servers":[{"name":"notes","kind":"http","url":"https://x"}]}`)
	})

	t.Run("rejects an http server url with a query string", func(t *testing.T) {
		issues := mustFail(t, `{"servers":[{"name":"notes","kind":"http","url":"http://127.0.0.1:8765/mcp?token=abc"}]}`)
		if !containsPath(issues, "servers[0].url") {
			t.Errorf("expected issue at servers[0].url, got %v", issues)
		}
	})

	t.Run("rejects an unknown kind", func(t *testing.T) {
		mustFail(t, `{"servers":[{"name":"notes","kind":"websocket","url":"http://127.0.0.1:8765/mcp"}]}`)
	})

	t.Run("rejects unrecognized keys on a server entry (strict)", func(t *testing.T) {
		mustFail(t, `{"servers":[{"name":"blender","kind":"stdio","command":"uvx","extra":true}]}`)
	})

	t.Run("rejects unrecognized top-level keys (strict)", func(t *testing.T) {
		mustFail(t, `{"servers":[{"name":"blender","kind":"stdio","command":"uvx"}],"extra":true}`)
	})

	t.Run("rejects a non-string env value", func(t *testing.T) {
		issues := mustFail(t, `{"servers":[{"name":"blender","kind":"stdio","command":"uvx","env":{"PORT":8765}}]}`)
		if !containsPath(issues, "servers[0].env.PORT") {
			t.Errorf("expected issue at servers[0].env.PORT, got %v", issues)
		}
	})

	t.Run("rejects a name longer than 30 characters", func(t *testing.T) {
		issues := mustFail(t, `{"servers":[{"name":"`+strings.Repeat("a", 31)+`","kind":"stdio","command":"uvx"}]}`)
		if !containsPath(issues, "servers[0].name") {
			t.Errorf("expected issue at servers[0].name, got %v", issues)
		}
	})

	t.Run("accepts a name exactly 30 characters long", func(t *testing.T) {
		mustValidate(t, `{"servers":[{"name":"`+strings.Repeat("a", 30)+`","kind":"stdio","command":"uvx"}]}`)
	})

	t.Run("rejects a non-string args element", func(t *testing.T) {
		issues := mustFail(t, `{"servers":[{"name":"blender","kind":"stdio","command":"uvx","args":["ok",5]}]}`)
		if !containsPath(issues, "servers[0].args[1]") {
			t.Errorf("expected issue at servers[0].args[1], got %v", issues)
		}
	})

	t.Run("accepts an explicit cwd for a stdio server", func(t *testing.T) {
		cfg := mustValidate(t, `{"servers":[{"name":"blender","kind":"stdio","command":"uvx","cwd":"/srv/blender"}]}`)
		if !cfg.Servers[0].HasCwd || cfg.Servers[0].Cwd != "/srv/blender" {
			t.Errorf("expected cwd /srv/blender, got %+v", cfg.Servers[0])
		}
	})

	t.Run("rejects a non-string cwd", func(t *testing.T) {
		issues := mustFail(t, `{"servers":[{"name":"blender","kind":"stdio","command":"uvx","cwd":123}]}`)
		if !containsPath(issues, "servers[0].cwd") {
			t.Errorf("expected issue at servers[0].cwd, got %v", issues)
		}
	})
}

// findIssue returns the first issue at path, for tests that need to assert
// on its message text rather than just its presence.
func findIssue(issues issueList, path string) (issue, bool) {
	for _, i := range issues {
		if i.path == path {
			return i, true
		}
	}
	return issue{}, false
}

// TestValidateConfigMessages asserts exact message text, not just issue
// paths — golden cases lifted from mcpwarp-cli's
// test/unit/config/schema.test.ts, which only asserts paths; the messages
// here were confirmed against the real zod schema (config/schema.ts) via
// `node -e` against the built CLI's zod dependency.
func TestValidateConfigMessages(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		path    string
		message string
	}{
		{
			name:    "uppercase name fails only the pattern refine",
			raw:     `{"servers":[{"name":"Blender","kind":"stdio","command":"uvx"}]}`,
			path:    "servers[0].name",
			message: "must be lowercase alphanumeric with hyphens, starting and ending with a lowercase letter or digit",
		},
		{
			name:    "missing command is a required-string issue",
			raw:     `{"servers":[{"name":"blender","kind":"stdio"}]}`,
			path:    "servers[0].command",
			message: "Invalid input: expected string, received undefined",
		},
		{
			name:    "a non-URL string fails the http(s) refine",
			raw:     `{"servers":[{"name":"notes","kind":"http","url":"not a url"}]}`,
			path:    "servers[0].url",
			message: "must be an http(s) URL like http://127.0.0.1:3000/mcp",
		},
		{
			name:    "a query string fails the no-query refine",
			raw:     `{"servers":[{"name":"notes","kind":"http","url":"http://127.0.0.1:8765/mcp?token=abc"}]}`,
			path:    "servers[0].url",
			message: "must not include a query string — mcpwarp merges each inbound request's own query onto this URL itself",
		},
		{
			name:    "a non-string env value reports the received type",
			raw:     `{"servers":[{"name":"blender","kind":"stdio","command":"uvx","env":{"PORT":8765}}]}`,
			path:    "servers[0].env.PORT",
			message: "Invalid input: expected string, received number",
		},
		{
			name:    "a 31-character name is too big, not a pattern mismatch",
			raw:     `{"servers":[{"name":"` + strings.Repeat("a", 31) + `","kind":"stdio","command":"uvx"}]}`,
			path:    "servers[0].name",
			message: "Too big: expected string to have <=30 characters",
		},
		{
			name:    "a non-string args element reports the received type",
			raw:     `{"servers":[{"name":"blender","kind":"stdio","command":"uvx","args":["ok",5]}]}`,
			path:    "servers[0].args[1]",
			message: "Invalid input: expected string, received number",
		},
		{
			name:    "a non-string cwd reports the received type",
			raw:     `{"servers":[{"name":"blender","kind":"stdio","command":"uvx","cwd":123}]}`,
			path:    "servers[0].cwd",
			message: "Invalid input: expected string, received number",
		},
		{
			name:    "a duplicate name names the first index it was seen at",
			raw:     `{"servers":[{"name":"dup","kind":"stdio","command":"uvx"},{"name":"dup","kind":"http","url":"http://127.0.0.1:8765/mcp"}]}`,
			path:    "servers[1].name",
			message: `duplicate server name "dup" (already used at servers[0])`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			issues := mustFail(t, tc.raw)
			got, ok := findIssue(issues, tc.path)
			if !ok {
				t.Fatalf("expected an issue at %s, got %v", tc.path, issues)
			}
			if got.message != tc.message {
				t.Errorf("got message %q, want %q", got.message, tc.message)
			}
		})
	}

	t.Run("a 31-character all-lowercase name reports only the too-big issue, not a pattern mismatch too", func(t *testing.T) {
		issues := mustFail(t, `{"servers":[{"name":"`+strings.Repeat("a", 31)+`","kind":"stdio","command":"uvx"}]}`)
		count := 0
		for _, i := range issues {
			if i.path == "servers[0].name" {
				count++
			}
		}
		if count != 1 {
			t.Errorf("expected exactly 1 issue at servers[0].name, got %d: %v", count, issues)
		}
	})
}

// TestValidateConfigNullValues covers json.Unmarshal's null handling: it's a
// silent no-op for a non-pointer string target and sets slice/map targets to
// nil, neither of which errors — so every null case needs its own explicit
// check to avoid `"args": null` etc. quietly passing as an empty value.
func TestValidateConfigNullValues(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		path    string
		message string
	}{
		{"whole config", `null`, "", "Invalid input: expected object, received null"},
		{"servers", `{"servers":null}`, "servers", "Invalid input: expected array, received null"},
		{"server name", `{"servers":[{"name":null,"kind":"stdio","command":"uvx"}]}`, "servers[0].name", "Invalid input: expected string, received null"},
		{"server kind", `{"servers":[{"name":"a","kind":null,"command":"uvx"}]}`, "servers[0].kind", "Invalid discriminator value. Expected 'stdio' | 'http'"},
		{"stdio command", `{"servers":[{"name":"a","kind":"stdio","command":null}]}`, "servers[0].command", "Invalid input: expected string, received null"},
		{"stdio args", `{"servers":[{"name":"a","kind":"stdio","command":"uvx","args":null}]}`, "servers[0].args", "Invalid input: expected array, received null"},
		{"stdio env", `{"servers":[{"name":"a","kind":"stdio","command":"uvx","env":null}]}`, "servers[0].env", "Invalid input: expected record, received null"},
		{"stdio cwd", `{"servers":[{"name":"a","kind":"stdio","command":"uvx","cwd":null}]}`, "servers[0].cwd", "Invalid input: expected string, received null"},
		{"http url", `{"servers":[{"name":"a","kind":"http","url":null}]}`, "servers[0].url", "Invalid input: expected string, received null"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			issues := mustFail(t, tc.raw)
			got, ok := findIssue(issues, tc.path)
			if !ok {
				t.Fatalf("expected an issue at %q, got %v", tc.path, issues)
			}
			if got.message != tc.message {
				t.Errorf("got message %q, want %q", got.message, tc.message)
			}
		})
	}
}

// TestValidateConfigMultipleIssuesPerField asserts that a field failing more
// than one check reports every failing check, not just the first — zod runs
// every chained refine on a field regardless of earlier failures.
func TestValidateConfigMultipleIssuesPerField(t *testing.T) {
	t.Run("a too-big name that also fails the pattern reports both", func(t *testing.T) {
		issues := mustFail(t, `{"servers":[{"name":"`+strings.Repeat("A", 70)+`","kind":"stdio","command":"uvx"}]}`)
		wantMessages := map[string]bool{
			"Too big: expected string to have <=30 characters":                                                  false,
			"must be lowercase alphanumeric with hyphens, starting and ending with a lowercase letter or digit": false,
		}
		count := 0
		for _, i := range issues {
			if i.path != "servers[0].name" {
				continue
			}
			if _, ok := wantMessages[i.message]; ok {
				wantMessages[i.message] = true
				count++
			}
		}
		if count != len(wantMessages) {
			t.Errorf("expected both name issues, got %v", issues)
		}
	})

	t.Run("an empty url fails both the min-length and http(s) refines", func(t *testing.T) {
		issues := mustFail(t, `{"servers":[{"name":"notes","kind":"http","url":""}]}`)
		wantMessages := map[string]bool{
			"Too small: expected string to have >=1 characters":     false,
			"must be an http(s) URL like http://127.0.0.1:3000/mcp": false,
		}
		count := 0
		for _, i := range issues {
			if i.path != "servers[0].url" {
				continue
			}
			if _, ok := wantMessages[i.message]; ok {
				wantMessages[i.message] = true
				count++
			}
		}
		if count != len(wantMessages) {
			t.Errorf("expected both url issues, got %v", issues)
		}
	})
}

// TestValidateConfigNonObjectTopLevel covers a syntactically valid JSON
// document whose top level isn't an object at all.
func TestValidateConfigNonObjectTopLevel(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		message string
	}{
		{"an array", `[]`, "Invalid input: expected object, received array"},
		{"a string", `"x"`, "Invalid input: expected object, received string"},
		{"a number", `5`, "Invalid input: expected object, received number"},
		{"a boolean", `true`, "Invalid input: expected object, received boolean"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			issues := mustFail(t, tc.raw)
			got, ok := findIssue(issues, "")
			if !ok {
				t.Fatalf("expected a top-level issue, got %v", issues)
			}
			if got.message != tc.message {
				t.Errorf("got message %q, want %q", got.message, tc.message)
			}
		})
	}
}

// TestValidateConfigDiscriminatorKind covers zod's discriminatedUnion
// behaviour for the "kind" field: an invalid discriminant fails routing
// before either branch schema is even entered, so it reports exactly one
// issue — never name, command, or url issues too, no matter how invalid
// those are.
func TestValidateConfigDiscriminatorKind(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"missing kind", `{"servers":[{"name":"a","command":"uvx"}]}`},
		{"numeric kind", `{"servers":[{"name":"a","kind":5,"command":"uvx"}]}`},
		{"bad kind with a bad name", `{"servers":[{"name":"Bad Name!","kind":"weird"}]}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			issues := mustFail(t, tc.raw)
			if len(issues) != 1 {
				t.Fatalf("expected exactly one issue, got %v", issues)
			}
			want := issue{path: "servers[0].kind", message: "Invalid discriminator value. Expected 'stdio' | 'http'"}
			if issues[0] != want {
				t.Errorf("got %+v, want %+v", issues[0], want)
			}
		})
	}
}
