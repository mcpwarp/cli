// Package auth holds credential storage and issuer resolution shared by
// `login`, `logout`, `whoami`, and `status`. M0 implements only the
// read-only slice `status` needs: settings resolution and credential
// loading. M1 extends this package with the device flow, refresh lock, and
// token provider rather than replacing it.
package auth

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

const (
	DefaultAuthURL  = "https://auth.mcpwarp.io"
	DefaultRealm    = "mcpwarp"
	DefaultClientID = "mcpwarp-cli"
	DeviceFlowScope = "openid offline_access"
)

// Settings is the resolved auth-server configuration for this invocation.
type Settings struct {
	AuthURL  string
	Realm    string
	ClientID string
	Scope    string
}

// SettingsError is a malformed --issuer/MCPWARP_AUTH_URL value — usage-shaped
// (exit 2), matching Node's AuthSettingsError.
type SettingsError struct {
	message string
}

func (e *SettingsError) Error() string { return e.message }
func (e *SettingsError) ExitCode() int { return 2 }

// ResolveSettings applies Node's precedence: --issuer flag > MCPWARP_AUTH_URL
// > DefaultAuthURL.
func ResolveSettings(cliAuthURL string) (Settings, error) {
	return resolveSettings(cliAuthURL, os.Getenv)
}

// resolveSettings is ResolveSettings with an injectable environment lookup —
// a seam M1's auth-flow tests need, to avoid mutating process env.
func resolveSettings(cliAuthURL string, getenv func(string) string) (Settings, error) {
	authURL := cliAuthURL
	if authURL == "" {
		authURL = getenv("MCPWARP_AUTH_URL")
	}
	if authURL == "" {
		authURL = DefaultAuthURL
	}
	// net/url.Parse is lenient about missing schemes (e.g. "not a url"
	// parses as a relative reference) and about a missing host after
	// "scheme://" (e.g. "http://" parses with Host == ""), so both are
	// checked by hand — matching the JS `new URL(...)` throw Node relies on
	// to reject either case.
	u, err := url.Parse(authURL)
	if err != nil || u.Scheme == "" || (strings.Contains(authURL, "://") && u.Host == "") {
		return Settings{}, &SettingsError{message: fmt.Sprintf("invalid auth server URL: %s", authURL)}
	}

	realm := getenv("MCPWARP_AUTH_REALM")
	if realm == "" {
		realm = DefaultRealm
	}
	clientID := getenv("MCPWARP_AUTH_CLIENT_ID")
	if clientID == "" {
		clientID = DefaultClientID
	}

	return Settings{
		AuthURL:  strings.TrimRight(authURL, "/"),
		Realm:    realm,
		ClientID: clientID,
		Scope:    DeviceFlowScope,
	}, nil
}

// BuildIssuer returns "{authUrl}/realms/{realm}", the OIDC issuer.
func BuildIssuer(authURL, realm string) string {
	return strings.TrimRight(authURL, "/") + "/realms/" + realm
}

// CurrentIssuer resolves settings then builds the issuer — what every
// command resolves first to find its credentials file.
func CurrentIssuer(cliAuthURL string) (string, error) {
	settings, err := ResolveSettings(cliAuthURL)
	if err != nil {
		return "", err
	}
	return BuildIssuer(settings.AuthURL, settings.Realm), nil
}
