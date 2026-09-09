package cli

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mcpwarp/cli/internal/auth"
	"github.com/mcpwarp/cli/internal/output"
)

func withCapturedStderr(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	orig := output.Stderr
	output.Stderr = &buf
	t.Cleanup(func() { output.Stderr = orig })
	return &buf
}

func TestRunLoginRequiresTTY(t *testing.T) {
	stderr := withCapturedStderr(t)
	ctx := &Context{Log: NewLogger(false)}
	err := runLogin(ctx, false, loginDeps{isTTY: func() bool { return false }})
	if err == nil {
		t.Fatal("expected an error")
	}
	if code := err.(ExitCoder).ExitCode(); code != 1 {
		t.Fatalf("got exit code %d", code)
	}
	if !strings.Contains(stderr.String(), "login requires an interactive terminal") {
		t.Fatalf("got %q", stderr.String())
	}
}

func newAuthServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	var issuer string
	mux.HandleFunc("/realms/mcpwarp/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer":                        issuer,
			"device_authorization_endpoint": issuer + "/device",
			"token_endpoint":                issuer + "/token",
		})
	})
	mux.HandleFunc("/realms/mcpwarp/device", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"device_code": "dc", "user_code": "ABCD-EFGH",
			"verification_uri": issuer + "/activate", "expires_in": 600, "interval": 1,
		})
	})
	mux.HandleFunc("/realms/mcpwarp/token", func(w http.ResponseWriter, r *http.Request) {
		accessToken := "header." + b64urlPayload(map[string]any{"sub": "user-1", "email": "user@example.com"}) + ".sig"
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": accessToken, "refresh_token": "rt-1", "expires_in": 3600,
			"token_type": "Bearer", "scope": "openid offline_access",
		})
	})
	srv := httptest.NewServer(mux)
	issuer = srv.URL + "/realms/mcpwarp"
	return srv
}

func b64urlPayload(m map[string]any) string {
	raw, _ := json.Marshal(m)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func TestRunLoginHappyPath(t *testing.T) {
	srv := newAuthServer(t)
	defer srv.Close()
	auth.ClearDiscoveryCache()

	dir := t.TempDir()
	paths, err := auth.CredentialsPaths(srv.URL+"/realms/mcpwarp", dir)
	if err != nil {
		t.Fatal(err)
	}

	stdout := withCapturedStdout(t)
	var browserURL string
	ctx := &Context{IssuerOverride: srv.URL, HomeDir: dir, Log: NewLogger(false)}
	deps := loginDeps{
		isTTY:       func() bool { return true },
		client:      srv.Client(),
		openBrowser: func(u string) { browserURL = u },
		paths:       &paths,
	}
	if err := runLogin(ctx, false, deps); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if browserURL == "" {
		t.Fatal("expected the browser to be opened")
	}
	if !strings.Contains(stdout.String(), "Logged in as user@example.com") {
		t.Fatalf("got %q", stdout.String())
	}

	creds := auth.Load(paths, nil)
	if creds == nil || creds.RefreshToken != "rt-1" || creds.Sub != "user-1" {
		t.Fatalf("got %#v", creds)
	}
}

func TestRunLoginNoBrowserSkipsOpen(t *testing.T) {
	srv := newAuthServer(t)
	defer srv.Close()
	auth.ClearDiscoveryCache()

	dir := t.TempDir()
	paths, _ := auth.CredentialsPaths(srv.URL+"/realms/mcpwarp", dir)
	withCapturedStdout(t)

	opened := false
	ctx := &Context{IssuerOverride: srv.URL, HomeDir: dir, Log: NewLogger(false)}
	deps := loginDeps{
		isTTY: func() bool { return true }, client: srv.Client(),
		openBrowser: func(u string) { opened = true }, paths: &paths,
	}
	if err := runLogin(ctx, true, deps); err != nil {
		t.Fatal(err)
	}
	if opened {
		t.Fatal("--no-browser must not open a browser")
	}
}

func TestRunLoginNoRefreshTokenIsAnError(t *testing.T) {
	mux := http.NewServeMux()
	var issuer string
	mux.HandleFunc("/realms/mcpwarp/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer": issuer, "device_authorization_endpoint": issuer + "/device", "token_endpoint": issuer + "/token",
		})
	})
	mux.HandleFunc("/realms/mcpwarp/device", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"device_code": "dc", "user_code": "ABCD-EFGH", "verification_uri": issuer + "/activate",
			"expires_in": 600, "interval": 1,
		})
	})
	mux.HandleFunc("/realms/mcpwarp/token", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "at", "expires_in": 3600, "token_type": "Bearer",
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	issuer = srv.URL + "/realms/mcpwarp"
	auth.ClearDiscoveryCache()

	dir := t.TempDir()
	stderr := withCapturedStderr(t)
	withCapturedStdout(t)
	ctx := &Context{IssuerOverride: srv.URL, HomeDir: dir, Log: NewLogger(false)}
	err := runLogin(ctx, true, loginDeps{isTTY: func() bool { return true }, client: srv.Client()})
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(stderr.String(), "offline_access") {
		t.Fatalf("got %q", stderr.String())
	}
}

func TestRunLoginNoSubClaimIsAnError(t *testing.T) {
	mux := http.NewServeMux()
	var issuer string
	mux.HandleFunc("/realms/mcpwarp/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer": issuer, "device_authorization_endpoint": issuer + "/device", "token_endpoint": issuer + "/token",
		})
	})
	mux.HandleFunc("/realms/mcpwarp/device", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"device_code": "dc", "user_code": "ABCD-EFGH", "verification_uri": issuer + "/activate",
			"expires_in": 600, "interval": 1,
		})
	})
	mux.HandleFunc("/realms/mcpwarp/token", func(w http.ResponseWriter, r *http.Request) {
		accessToken := "header." + b64urlPayload(map[string]any{"email": "user@example.com"}) + ".sig" // no sub
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": accessToken, "refresh_token": "rt-1", "expires_in": 3600,
			"token_type": "Bearer", "scope": "openid offline_access",
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	issuer = srv.URL + "/realms/mcpwarp"
	auth.ClearDiscoveryCache()

	dir := t.TempDir()
	stderr := withCapturedStderr(t)
	withCapturedStdout(t)
	ctx := &Context{IssuerOverride: srv.URL, HomeDir: dir, Log: NewLogger(false)}
	err := runLogin(ctx, true, loginDeps{isTTY: func() bool { return true }, client: srv.Client()})
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(stderr.String(), "no `sub` claim") {
		t.Fatalf("got %q", stderr.String())
	}
}

func TestRunLoginCredentialsWriteFailure(t *testing.T) {
	srv := newAuthServer(t)
	defer srv.Close()
	auth.ClearDiscoveryCache()

	dir := t.TempDir()
	// paths.Dir sits under a regular file, not a directory — Save's
	// os.MkdirAll must fail.
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	badPaths := auth.Paths{
		Dir:    filepath.Join(blocker, "credentials"),
		File:   filepath.Join(blocker, "credentials", "creds.json"),
		Issuer: srv.URL + "/realms/mcpwarp",
	}

	stderr := withCapturedStderr(t)
	withCapturedStdout(t)
	ctx := &Context{IssuerOverride: srv.URL, HomeDir: dir, Log: NewLogger(false)}
	deps := loginDeps{
		isTTY: func() bool { return true }, client: srv.Client(),
		openBrowser: func(string) {}, paths: &badPaths,
	}
	err := runLogin(ctx, false, deps)
	if err == nil {
		t.Fatal("expected an error")
	}
	if code := err.(ExitCoder).ExitCode(); code != 1 {
		t.Fatalf("got exit code %d", code)
	}
	if !strings.Contains(stderr.String(), "could not write credentials") {
		t.Fatalf("got %q", stderr.String())
	}
}

func TestRunLoginMalformedIssuerExitsUsageError(t *testing.T) {
	stderr := withCapturedStderr(t)
	ctx := &Context{IssuerOverride: "not a url", Log: NewLogger(false)}
	err := runLogin(ctx, false, loginDeps{isTTY: func() bool { return true }})
	if err == nil {
		t.Fatal("expected an error")
	}
	if code := err.(ExitCoder).ExitCode(); code != 2 {
		t.Fatalf("got exit code %d, want 2 (usage/config error)", code)
	}
	if !strings.Contains(stderr.String(), "invalid auth server URL") {
		t.Fatalf("got %q", stderr.String())
	}
}

func TestRunLoginDiscoveryFailure(t *testing.T) {
	auth.ClearDiscoveryCache()
	dir := t.TempDir()
	stderr := withCapturedStderr(t)
	withCapturedStdout(t)
	ctx := &Context{IssuerOverride: "http://127.0.0.1:1", HomeDir: dir, Log: NewLogger(false)}
	err := runLogin(ctx, true, loginDeps{isTTY: func() bool { return true }})
	if err == nil {
		t.Fatal("expected an error")
	}
	if stderr.Len() == 0 {
		t.Fatal("expected an error message")
	}
}
