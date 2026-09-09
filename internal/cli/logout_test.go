package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mcpwarp/cli/internal/auth"
)

func TestRunLogoutNotLoggedIn(t *testing.T) {
	dir := t.TempDir()
	stdout := withCapturedStdout(t)
	paths, _ := auth.CredentialsPaths("http://localhost/realms/mcpwarp", dir)
	ctx := &Context{HomeDir: dir, Log: NewLogger(false)}
	if err := runLogout(ctx, logoutDeps{paths: &paths}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "Not logged in.") {
		t.Fatalf("got %q", stdout.String())
	}
}

func TestRunLogoutClearsCredsAndBestEffortRevokes(t *testing.T) {
	var revoked bool
	mux := http.NewServeMux()
	var issuer string
	mux.HandleFunc("/realms/mcpwarp/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer": issuer, "device_authorization_endpoint": issuer + "/device",
			"token_endpoint": issuer + "/token", "end_session_endpoint": issuer + "/logout",
		})
	})
	mux.HandleFunc("/realms/mcpwarp/logout", func(w http.ResponseWriter, r *http.Request) {
		revoked = true
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	issuer = srv.URL + "/realms/mcpwarp"
	auth.ClearDiscoveryCache()

	dir := t.TempDir()
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
	if err := runLogout(ctx, logoutDeps{paths: &paths, client: srv.Client()}); err != nil {
		t.Fatal(err)
	}
	if !revoked {
		t.Fatal("expected the revocation endpoint to be called")
	}
	if !strings.Contains(stdout.String(), "Logged out.") {
		t.Fatalf("got %q", stdout.String())
	}
	if auth.Load(paths, nil) != nil {
		t.Fatal("credentials should be cleared")
	}
}

func TestRunLogoutClearsCredsEvenWhenRevocationFails(t *testing.T) {
	dir := t.TempDir()
	issuer := "http://127.0.0.1:1/realms/mcpwarp" // unreachable
	auth.ClearDiscoveryCache()
	paths, _ := auth.CredentialsPaths(issuer, dir)
	creds := auth.Credentials{
		AccessToken: "at", RefreshToken: "rt", ExpiresAt: 1, TokenType: "Bearer",
		Scope: "openid", Sub: "u1", Issuer: issuer, ClientID: "mcpwarp-cli", SavedAt: 0,
	}
	if err := auth.Save(creds, paths); err != nil {
		t.Fatal(err)
	}

	withCapturedStdout(t)
	ctx := &Context{HomeDir: dir, Log: NewLogger(false)}
	if err := runLogout(ctx, logoutDeps{paths: &paths}); err != nil {
		t.Fatal(err)
	}
	if auth.Load(paths, nil) != nil {
		t.Fatal("credentials should still be cleared despite the revocation failure")
	}
}
