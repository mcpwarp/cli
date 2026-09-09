package auth

import "testing"

func withEnv(t *testing.T, key, value string) {
	t.Helper()
	t.Setenv(key, value)
}

func TestResolveSettings(t *testing.T) {
	t.Run("defaults authUrl when MCPWARP_AUTH_URL is unset", func(t *testing.T) {
		t.Setenv("MCPWARP_AUTH_URL", "")
		settings, err := ResolveSettings("")
		if err != nil {
			t.Fatal(err)
		}
		if settings.AuthURL != DefaultAuthURL {
			t.Errorf("got %q", settings.AuthURL)
		}
	})

	t.Run("prefers MCPWARP_AUTH_URL over the default", func(t *testing.T) {
		withEnv(t, "MCPWARP_AUTH_URL", "https://auth.example.com")
		settings, err := ResolveSettings("")
		if err != nil {
			t.Fatal(err)
		}
		if settings.AuthURL != "https://auth.example.com" {
			t.Errorf("got %q", settings.AuthURL)
		}
	})

	t.Run("prefers the --issuer override over MCPWARP_AUTH_URL and the default", func(t *testing.T) {
		withEnv(t, "MCPWARP_AUTH_URL", "https://auth.example.com")
		settings, err := ResolveSettings("https://cli-override.example.com")
		if err != nil {
			t.Fatal(err)
		}
		if settings.AuthURL != "https://cli-override.example.com" {
			t.Errorf("got %q", settings.AuthURL)
		}
	})

	t.Run("errors for a malformed auth URL", func(t *testing.T) {
		_, err := ResolveSettings("not a url")
		if err == nil {
			t.Fatal("expected an error")
		}
		if _, ok := err.(*SettingsError); !ok {
			t.Errorf("expected *SettingsError, got %T", err)
		}
	})
}

func TestCurrentIssuer(t *testing.T) {
	t.Run("combines the resolved authUrl and realm", func(t *testing.T) {
		withEnv(t, "MCPWARP_AUTH_URL", "https://auth.example.com")
		issuer, err := CurrentIssuer("")
		if err != nil {
			t.Fatal(err)
		}
		want := BuildIssuer("https://auth.example.com", "mcpwarp")
		if issuer != want {
			t.Errorf("got %q want %q", issuer, want)
		}
	})

	t.Run("uses the default auth URL when nothing overrides it", func(t *testing.T) {
		t.Setenv("MCPWARP_AUTH_URL", "")
		issuer, err := CurrentIssuer("")
		if err != nil {
			t.Fatal(err)
		}
		want := BuildIssuer(DefaultAuthURL, "mcpwarp")
		if issuer != want {
			t.Errorf("got %q want %q", issuer, want)
		}
	})
}
