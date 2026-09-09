package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const discoveryTimeout = 10 * time.Second

// maxDiscoveryDocBytes bounds how much of the discovery response body is
// read — the endpoint is attacker-influenced (issuer is CLI-supplied), so a
// malicious or misconfigured server returning an unbounded body must not be
// read into memory in full.
const maxDiscoveryDocBytes = 1 << 20 // 1 MiB

// OidcConfig is the subset of the discovery document (§/.well-known/openid-configuration)
// this CLI needs — mirrors Node's discovery.ts OidcConfig.
type OidcConfig struct {
	Issuer                      string
	DeviceAuthorizationEndpoint string
	TokenEndpoint               string
	EndSessionEndpoint          string
	UserinfoEndpoint            string
}

// DiscoveryError wraps a discovery failure with a user-facing message.
type DiscoveryError struct {
	message string
	cause   error
}

func (e *DiscoveryError) Error() string { return e.message }
func (e *DiscoveryError) Unwrap() error { return e.cause }

var (
	discoveryCache   = map[string]OidcConfig{}
	discoveryCacheMu sync.Mutex
)

// ClearDiscoveryCache clears the in-process discovery cache — a test seam,
// same purpose as Node's clearDiscoveryCache().
func ClearDiscoveryCache() {
	discoveryCacheMu.Lock()
	defer discoveryCacheMu.Unlock()
	discoveryCache = map[string]OidcConfig{}
}

// HTTPDoer is the seam every network call in this package is made through —
// satisfied by *http.Client, so tests can swap in one pointed at an
// httptest.Server or one that returns canned errors.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

var defaultHTTPClient HTTPDoer = &http.Client{}

type discoveryDoc struct {
	Issuer                      string `json:"issuer"`
	DeviceAuthorizationEndpoint string `json:"device_authorization_endpoint"`
	TokenEndpoint               string `json:"token_endpoint"`
	EndSessionEndpoint          string `json:"end_session_endpoint"`
	UserinfoEndpoint            string `json:"userinfo_endpoint"`
}

// Discover fetches and validates {issuer}/.well-known/openid-configuration,
// caching the result by issuer for the process lifetime (or until
// ClearDiscoveryCache is called). client defaults to a plain *http.Client.
func Discover(ctx context.Context, issuer string, client HTTPDoer) (OidcConfig, error) {
	discoveryCacheMu.Lock()
	cached, ok := discoveryCache[issuer]
	discoveryCacheMu.Unlock()
	if ok {
		return cached, nil
	}

	if client == nil {
		client = defaultHTTPClient
	}

	url := strings.TrimRight(issuer, "/") + "/.well-known/openid-configuration"

	ctx, cancel := context.WithTimeout(ctx, discoveryTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return OidcConfig{}, &DiscoveryError{message: fmt.Sprintf("could not reach auth server at %s: %s", url, err.Error()), cause: err}
	}

	resp, err := client.Do(req)
	if err != nil {
		return OidcConfig{}, &DiscoveryError{message: fmt.Sprintf("could not reach auth server at %s: %s", url, err.Error()), cause: err}
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return OidcConfig{}, &DiscoveryError{message: fmt.Sprintf("auth server discovery failed: %d %s (%s)", resp.StatusCode, http.StatusText(resp.StatusCode), url)}
	}

	var doc discoveryDoc
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxDiscoveryDocBytes)).Decode(&doc); err != nil {
		return OidcConfig{}, &DiscoveryError{message: fmt.Sprintf("auth server discovery document at %s was not valid JSON", url), cause: err}
	}

	if doc.Issuer != issuer {
		return OidcConfig{}, &DiscoveryError{message: fmt.Sprintf("auth server discovery document at %s declares issuer %q, expected %s", url, doc.Issuer, issuer)}
	}
	if doc.DeviceAuthorizationEndpoint == "" {
		return OidcConfig{}, &DiscoveryError{message: fmt.Sprintf("auth server discovery document at %s is missing device_authorization_endpoint", url)}
	}
	if doc.TokenEndpoint == "" {
		return OidcConfig{}, &DiscoveryError{message: fmt.Sprintf("auth server discovery document at %s is missing token_endpoint", url)}
	}

	config := OidcConfig{
		Issuer:                      issuer,
		DeviceAuthorizationEndpoint: doc.DeviceAuthorizationEndpoint,
		TokenEndpoint:               doc.TokenEndpoint,
		EndSessionEndpoint:          doc.EndSessionEndpoint,
		UserinfoEndpoint:            doc.UserinfoEndpoint,
	}
	discoveryCacheMu.Lock()
	discoveryCache[issuer] = config
	discoveryCacheMu.Unlock()
	return config, nil
}
