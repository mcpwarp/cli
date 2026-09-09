package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mcpwarp/cli/internal/auth"
)

func TestRunWhoamiNotLoggedIn(t *testing.T) {
	dir := t.TempDir()
	stderr := withCapturedStderr(t)
	paths, _ := auth.CredentialsPaths("http://localhost/realms/mcpwarp", dir)
	ctx := &Context{HomeDir: dir, Log: NewLogger(false)}
	err := runWhoami(ctx, false, whoamiDeps{paths: &paths, issuer: "http://localhost/realms/mcpwarp"})
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(stderr.String(), "Not logged in") {
		t.Fatalf("got %q", stderr.String())
	}
}

func TestRunWhoamiPrintsIdentity(t *testing.T) {
	dir := t.TempDir()
	issuer := "http://localhost/realms/mcpwarp"
	paths, _ := auth.CredentialsPaths(issuer, dir)
	creds := auth.Credentials{
		AccessToken: "at", RefreshToken: "rt", ExpiresAt: 99_999_999_999_999, TokenType: "Bearer",
		Scope: "openid", Sub: "u1", Email: "u1@example.com", Issuer: issuer, ClientID: "mcpwarp-cli", SavedAt: 0,
	}
	if err := auth.Save(creds, paths); err != nil {
		t.Fatal(err)
	}

	stdout := withCapturedStdout(t)
	ctx := &Context{HomeDir: dir, Log: NewLogger(false)}
	if err := runWhoami(ctx, false, whoamiDeps{paths: &paths, issuer: issuer}); err != nil {
		t.Fatal(err)
	}
	got := stdout.String()
	for _, want := range []string{"user: u1@example.com", "issuer: " + issuer, "valid"} {
		if !strings.Contains(got, want) {
			t.Errorf("expected %q in %q", want, got)
		}
	}
}

func TestRunWhoamiExpired(t *testing.T) {
	dir := t.TempDir()
	issuer := "http://localhost/realms/mcpwarp"
	paths, _ := auth.CredentialsPaths(issuer, dir)
	creds := auth.Credentials{
		AccessToken: "at", RefreshToken: "rt", ExpiresAt: 1, TokenType: "Bearer",
		Scope: "openid", Sub: "u1", Issuer: issuer, ClientID: "mcpwarp-cli", SavedAt: 0,
	}
	if err := auth.Save(creds, paths); err != nil {
		t.Fatal(err)
	}
	stdout := withCapturedStdout(t)
	ctx := &Context{HomeDir: dir, Log: NewLogger(false)}
	if err := runWhoami(ctx, false, whoamiDeps{paths: &paths, issuer: issuer}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "expired") {
		t.Fatalf("got %q", stdout.String())
	}
}

func TestRunWhoamiRefreshExercisesProvider(t *testing.T) {
	mux := http.NewServeMux()
	var issuer string
	mux.HandleFunc("/realms/mcpwarp/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer": issuer, "device_authorization_endpoint": issuer + "/device", "token_endpoint": issuer + "/token",
		})
	})
	mux.HandleFunc("/realms/mcpwarp/token", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "at-2", "refresh_token": "rt-2", "expires_in": 3600, "token_type": "Bearer",
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	issuer = srv.URL + "/realms/mcpwarp"
	auth.ClearDiscoveryCache()

	dir := t.TempDir()
	paths, _ := auth.CredentialsPaths(issuer, dir)
	creds := auth.Credentials{
		AccessToken: "at-1", RefreshToken: "rt-1", ExpiresAt: 1, TokenType: "Bearer",
		Scope: "openid", Sub: "u1", Issuer: issuer, ClientID: "mcpwarp-cli", SavedAt: 0,
	}
	if err := auth.Save(creds, paths); err != nil {
		t.Fatal(err)
	}

	stdout := withCapturedStdout(t)
	ctx := &Context{HomeDir: dir, Log: NewLogger(false)}
	err := runWhoami(ctx, true, whoamiDeps{paths: &paths, issuer: issuer, client: srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "refreshed") {
		t.Fatalf("got %q", stdout.String())
	}
	if auth.Load(paths, nil).AccessToken != "at-2" {
		t.Fatal("expected the refreshed token to be persisted")
	}
}

func TestRunWhoamiStaticTokenMode(t *testing.T) {
	t.Setenv(auth.StaticTokenEnvVar, "mcpwarp_pat_abc")

	stdout := withCapturedStdout(t)
	ctx := &Context{Log: NewLogger(false)}
	if err := runWhoami(ctx, false, whoamiDeps{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "MCPWARP_TOKEN") {
		t.Fatalf("got %q", stdout.String())
	}
}

func TestRunWhoamiStaticTokenModeWarnsOnBadPrefix(t *testing.T) {
	t.Setenv(auth.StaticTokenEnvVar, "not-a-pat")

	stdout := withCapturedStdout(t)
	ctx := &Context{Log: NewLogger(false)}
	if err := runWhoami(ctx, false, whoamiDeps{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "!") {
		t.Fatalf("expected a warning glyph, got %q", stdout.String())
	}
}
