package auth

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sample() Credentials {
	refreshExpires := int64(2_000_000)
	return Credentials{
		AccessToken:       "at",
		RefreshToken:      "rt",
		ExpiresAt:         1_000_000,
		RefreshExpiresAt:  &refreshExpires,
		TokenType:         "Bearer",
		Scope:             "openid offline_access",
		Sub:               "user-1",
		Email:             "user@example.com",
		PreferredUsername: "user1",
		Issuer:            "http://localhost:9999/realms/mcpwarp",
		ClientID:          "mcpwarp-cli",
		SavedAt:           500_000,
	}
}

func writeCreds(t *testing.T, paths Paths, c Credentials) {
	t.Helper()
	if err := os.MkdirAll(paths.Dir, 0o700); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.File, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestCredentialsLoad(t *testing.T) {
	t.Run("round-trips credentials written to disk", func(t *testing.T) {
		dir := t.TempDir()
		c := sample()
		paths, err := CredentialsPaths(c.Issuer, dir)
		if err != nil {
			t.Fatal(err)
		}
		writeCreds(t, paths, c)

		got := Load(paths, func(string) {})
		if got == nil {
			t.Fatal("expected credentials, got nil")
		}
		if got.AccessToken != c.AccessToken || got.Sub != c.Sub {
			t.Errorf("got %+v", got)
		}
	})

	t.Run("returns nil when the file does not exist", func(t *testing.T) {
		dir := t.TempDir()
		paths, _ := CredentialsPaths("http://localhost/realms/mcpwarp", dir)
		if got := Load(paths, func(string) {}); got != nil {
			t.Errorf("expected nil, got %+v", got)
		}
	})

	t.Run("returns nil and warns once for corrupt JSON", func(t *testing.T) {
		ResetCorruptWarningForTests()
		dir := t.TempDir()
		c := sample()
		paths, _ := CredentialsPaths(c.Issuer, dir)
		if err := os.MkdirAll(paths.Dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(paths.File, []byte("{not json"), 0o600); err != nil {
			t.Fatal(err)
		}

		var warnings []string
		warn := func(m string) { warnings = append(warnings, m) }
		if got := Load(paths, warn); got != nil {
			t.Errorf("expected nil, got %+v", got)
		}
		if got := Load(paths, warn); got != nil {
			t.Errorf("expected nil, got %+v", got)
		}
		if len(warnings) != 1 {
			t.Errorf("expected exactly one warning, got %d: %v", len(warnings), warnings)
		}
	})

	t.Run("returns nil for a schema-invalid file", func(t *testing.T) {
		ResetCorruptWarningForTests()
		dir := t.TempDir()
		c := sample()
		paths, _ := CredentialsPaths(c.Issuer, dir)
		if err := os.MkdirAll(paths.Dir, 0o700); err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(map[string]string{"access_token": "only-this-field"})
		if err := os.WriteFile(paths.File, raw, 0o600); err != nil {
			t.Fatal(err)
		}

		if got := Load(paths, func(string) {}); got != nil {
			t.Errorf("expected nil, got %+v", got)
		}
	})

	t.Run("returns nil when the stored issuer doesn't match the resolved path's issuer", func(t *testing.T) {
		dir := t.TempDir()
		c := sample()
		paths, _ := CredentialsPaths(c.Issuer, dir)
		writeCreds(t, paths, c)

		mismatched := paths
		mismatched.Issuer = "http://other-issuer.test/realms/mcpwarp"
		if got := Load(mismatched, func(string) {}); got != nil {
			t.Errorf("expected nil, got %+v", got)
		}
	})

	t.Run("returns nil with a distinct message on permission denied", func(t *testing.T) {
		dir := t.TempDir()
		c := sample()
		paths, _ := CredentialsPaths(c.Issuer, dir)
		writeCreds(t, paths, c)
		if err := os.Chmod(paths.File, 0o000); err != nil {
			t.Fatal(err)
		}
		defer os.Chmod(paths.File, 0o600)

		var warnings []string
		got := Load(paths, func(m string) { warnings = append(warnings, m) })
		if got != nil {
			t.Errorf("expected nil, got %+v", got)
		}
		found := false
		for _, m := range warnings {
			if strings.Contains(m, "permission denied") {
				found = true
			}
		}
		if !found {
			t.Errorf("expected a permission-denied warning, got %v", warnings)
		}
	})
}

// TestCredentialsRequiredFieldsArePresenceChecked exercises the #2 fix: a
// required field is missing only when its key is absent from the JSON, not
// when json.Unmarshal happens to leave it at Go's zero value. scope, saved_at,
// and token_type carry no min/max in CredentialsSchema (auth/credentials.ts),
// so "" and 0 are legal there; only refresh_token has a min(1) rule of its
// own.
func TestCredentialsRequiredFieldsArePresenceChecked(t *testing.T) {
	writeRaw := func(t *testing.T, paths Paths, raw string) {
		t.Helper()
		if err := os.MkdirAll(paths.Dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(paths.File, []byte(raw), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("accepts present-but-empty scope, token_type, and a zero saved_at", func(t *testing.T) {
		ResetCorruptWarningForTests()
		dir := t.TempDir()
		c := sample()
		paths, _ := CredentialsPaths(c.Issuer, dir)
		raw := `{
			"access_token": "at", "refresh_token": "rt", "expires_at": 1000000,
			"token_type": "", "scope": "", "sub": "user-1",
			"issuer": "` + c.Issuer + `", "client_id": "mcpwarp-cli", "saved_at": 0
		}`
		writeRaw(t, paths, raw)

		got := Load(paths, func(string) {})
		if got == nil {
			t.Fatal("expected credentials, got nil")
		}
		if got.TokenType != "" || got.Scope != "" || got.SavedAt != 0 {
			t.Errorf("expected legal zero values preserved, got %+v", got)
		}
	})

	t.Run("rejects a present-but-empty refresh_token", func(t *testing.T) {
		ResetCorruptWarningForTests()
		dir := t.TempDir()
		c := sample()
		paths, _ := CredentialsPaths(c.Issuer, dir)
		raw := `{
			"access_token": "at", "refresh_token": "", "expires_at": 1000000,
			"token_type": "Bearer", "scope": "openid", "sub": "user-1",
			"issuer": "` + c.Issuer + `", "client_id": "mcpwarp-cli", "saved_at": 1
		}`
		writeRaw(t, paths, raw)

		if got := Load(paths, func(string) {}); got != nil {
			t.Errorf("expected nil, got %+v", got)
		}
	})

	for _, key := range []string{
		"access_token", "refresh_token", "expires_at", "token_type",
		"scope", "sub", "issuer", "client_id", "saved_at",
	} {
		t.Run("rejects a missing "+key+" key", func(t *testing.T) {
			ResetCorruptWarningForTests()
			dir := t.TempDir()
			c := sample()
			paths, _ := CredentialsPaths(c.Issuer, dir)

			fields := map[string]any{
				"access_token": "at", "refresh_token": "rt", "expires_at": 1_000_000,
				"token_type": "Bearer", "scope": "openid", "sub": "user-1",
				"issuer": c.Issuer, "client_id": "mcpwarp-cli", "saved_at": 500_000,
			}
			delete(fields, key)
			raw, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(paths.Dir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(paths.File, raw, 0o600); err != nil {
				t.Fatal(err)
			}

			if got := Load(paths, func(string) {}); got != nil {
				t.Errorf("expected nil for missing %s, got %+v", key, got)
			}
		})
	}
}

func TestIsAccessTokenStale(t *testing.T) {
	t.Run("is stale once now is within bufferMs of expires_at", func(t *testing.T) {
		c := sample()
		c.ExpiresAt = 100_000
		if !IsAccessTokenStale(&c, 60_000, 40_000) {
			t.Error("expected stale")
		}
		if IsAccessTokenStale(&c, 60_000, 39_999) {
			t.Error("expected not stale")
		}
	})

	t.Run("with a zero buffer (status's own call), staleness happens exactly at expiry", func(t *testing.T) {
		c := sample()
		c.ExpiresAt = 100_000
		if !IsAccessTokenStale(&c, 0, 100_000) {
			t.Error("expected stale exactly at expires_at")
		}
		if IsAccessTokenStale(&c, 0, 99_999) {
			t.Error("expected not stale one ms before expires_at")
		}
	})
}

func TestCredentialsPathFor(t *testing.T) {
	dir := t.TempDir()
	got, err := CredentialsPathFor("http://localhost/realms/mcpwarp", dir)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(dir, ".mcpwarp", "credentials")
	if filepath.Dir(got) != want {
		t.Errorf("got %q want dir %q", got, want)
	}
}
