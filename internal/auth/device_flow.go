package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Implemented by hand over net/http rather than golang.org/x/oauth2's
// Config.DeviceAuth/DeviceAccessToken: those poll internally with no
// injectable sleep/clock, no control over the slow_down backoff increment,
// and no way to tell a transient network/5xx error apart from a terminal
// OAuth error the way RFC 8628 §3.5 and Node's device-flow.ts do — all of
// which item 2 requires DI seams for and tests need to drive deterministically.

const (
	requestTimeout       = 10 * time.Second
	pollTimeout          = 10 * time.Second
	slowDownIncrement    = 5 * time.Second
	defaultPollIntervalS = 5 // RFC 8628 §3.2 default when the server omits interval
	maxResponseBodyBytes = 1 << 20
)

// DeviceAuthorizationResponse is RFC 8628 §3.2's response body.
type DeviceAuthorizationResponse struct {
	DeviceCode              string
	UserCode                string
	VerificationURI         string
	VerificationURIComplete string
	ExpiresIn               int64
	Interval                int64
}

// TokenResponse is the token endpoint's success body — access_token is
// always present; refresh_token is present unless the grant didn't request
// offline_access. Scope is a pointer: nil means the server omitted it
// (caller should keep whatever scope it already has), non-nil means the
// server sent one explicitly, even if that's "".
type TokenResponse struct {
	AccessToken      string
	TokenType        string
	ExpiresIn        int64
	RefreshToken     string
	RefreshExpiresIn *int64
	Scope            *string
	IDToken          string
}

// DeviceFlowErrorCode classifies a DeviceFlowError, mirroring Node's
// DeviceFlowErrorCode union.
type DeviceFlowErrorCode string

const (
	CodeAuthorizationPending DeviceFlowErrorCode = "authorization_pending"
	CodeSlowDown             DeviceFlowErrorCode = "slow_down"
	CodeExpiredToken         DeviceFlowErrorCode = "expired_token"
	CodeAccessDenied         DeviceFlowErrorCode = "access_denied"
	CodeInvalidGrant         DeviceFlowErrorCode = "invalid_grant"
	CodeNetworkError         DeviceFlowErrorCode = "network_error"
	CodeHTTPError            DeviceFlowErrorCode = "http_error"
	CodeInvalidResponse      DeviceFlowErrorCode = "invalid_response"
)

// DeviceFlowError is a typed device-flow/refresh failure.
type DeviceFlowError struct {
	Message     string
	Code        DeviceFlowErrorCode
	Description string
}

func (e *DeviceFlowError) Error() string { return e.Message }

func newDeviceFlowError(code DeviceFlowErrorCode, message string, description ...string) *DeviceFlowError {
	desc := ""
	if len(description) > 0 {
		desc = description[0]
	}
	return &DeviceFlowError{Message: message, Code: code, Description: desc}
}

// oauthErrorBody's Error is a pointer so an absent "error" key (not
// OAuth-error shaped, retry) can be told apart from `{"error":""}` (a
// terminal, if malformed, OAuth error response — must not be retried).
type oauthErrorBody struct {
	Error            *string `json:"error"`
	ErrorDescription string  `json:"error_description"`
}

func postForm(ctx context.Context, client HTTPDoer, urlStr string, body url.Values, timeout time.Duration) (status int, ok bool, raw []byte, doErr error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, urlStr, strings.NewReader(body.Encode()))
	if err != nil {
		return 0, false, nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return 0, false, nil, err
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes))
	return resp.StatusCode, resp.StatusCode >= 200 && resp.StatusCode < 300, data, nil
}

func parseOAuthError(raw []byte) (oauthErrorBody, bool) {
	var body oauthErrorBody
	if err := json.Unmarshal(raw, &body); err != nil {
		return oauthErrorBody{}, false
	}
	if body.Error == nil {
		return oauthErrorBody{}, false
	}
	return body, true
}

// RequestDeviceCodeParams is the input to RequestDeviceCode.
type RequestDeviceCodeParams struct {
	DeviceAuthorizationEndpoint string
	ClientID                    string
	Scope                       string
	CodeChallenge               string
}

// RequestDeviceCode is RFC 8628 §3.1-3.2's device authorization request,
// with PKCE's code_challenge/code_challenge_method=S256 attached (AUTH §2).
func RequestDeviceCode(ctx context.Context, params RequestDeviceCodeParams, client HTTPDoer) (DeviceAuthorizationResponse, error) {
	if client == nil {
		client = defaultHTTPClient
	}

	status, ok, raw, err := postForm(ctx, client, params.DeviceAuthorizationEndpoint, url.Values{
		"client_id":             {params.ClientID},
		"scope":                 {params.Scope},
		"code_challenge":        {params.CodeChallenge},
		"code_challenge_method": {"S256"},
	}, requestTimeout)
	if err != nil {
		return DeviceAuthorizationResponse{}, newDeviceFlowError(CodeNetworkError, fmt.Sprintf("could not reach %s: %s", params.DeviceAuthorizationEndpoint, err.Error()))
	}

	if !ok {
		if body, isOAuth := parseOAuthError(raw); isOAuth {
			msg := body.ErrorDescription
			if msg == "" {
				msg = fmt.Sprintf("device authorization failed: %s", *body.Error)
			}
			return DeviceAuthorizationResponse{}, newDeviceFlowError(CodeHTTPError, msg, body.ErrorDescription)
		}
		return DeviceAuthorizationResponse{}, newDeviceFlowError(CodeHTTPError, fmt.Sprintf("device authorization failed: HTTP %d", status))
	}

	var doc struct {
		DeviceCode              *string `json:"device_code"`
		UserCode                *string `json:"user_code"`
		VerificationURI         *string `json:"verification_uri"`
		VerificationURIComplete *string `json:"verification_uri_complete"`
		ExpiresIn               *int64  `json:"expires_in"`
		Interval                *int64  `json:"interval"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil || doc.DeviceCode == nil || doc.UserCode == nil || doc.VerificationURI == nil || doc.ExpiresIn == nil {
		return DeviceAuthorizationResponse{}, newDeviceFlowError(CodeInvalidResponse, "device authorization endpoint returned an invalid response")
	}

	interval := int64(defaultPollIntervalS)
	if doc.Interval != nil {
		interval = *doc.Interval
	}
	resp := DeviceAuthorizationResponse{
		DeviceCode:      *doc.DeviceCode,
		UserCode:        *doc.UserCode,
		VerificationURI: *doc.VerificationURI,
		ExpiresIn:       *doc.ExpiresIn,
		Interval:        interval,
	}
	if doc.VerificationURIComplete != nil {
		resp.VerificationURIComplete = *doc.VerificationURIComplete
	}
	return resp, nil
}

func parseTokenResponse(raw []byte) (TokenResponse, bool) {
	var doc struct {
		AccessToken      *string `json:"access_token"`
		TokenType        *string `json:"token_type"`
		ExpiresIn        *int64  `json:"expires_in"`
		RefreshToken     *string `json:"refresh_token"`
		RefreshExpiresIn *int64  `json:"refresh_expires_in"`
		Scope            *string `json:"scope"`
		IDToken          *string `json:"id_token"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return TokenResponse{}, false
	}
	if doc.AccessToken == nil || doc.TokenType == nil || doc.ExpiresIn == nil {
		return TokenResponse{}, false
	}
	if doc.RefreshToken != nil && *doc.RefreshToken == "" {
		return TokenResponse{}, false
	}
	tr := TokenResponse{AccessToken: *doc.AccessToken, TokenType: *doc.TokenType, ExpiresIn: *doc.ExpiresIn}
	if doc.RefreshToken != nil {
		tr.RefreshToken = *doc.RefreshToken
	}
	tr.RefreshExpiresIn = doc.RefreshExpiresIn
	tr.Scope = doc.Scope
	if doc.IDToken != nil {
		tr.IDToken = *doc.IDToken
	}
	return tr, true
}

type pollOutcomeKind int

const (
	pollOK pollOutcomeKind = iota
	pollRetry
	pollError
)

type pollOutcome struct {
	kind pollOutcomeKind
	raw  []byte
	body oauthErrorBody
}

// postFormPoll classifies network errors, HTTP >= 500, and an unparseable
// or non-OAuth-shaped body as retryable — only a well-formed OAuth error
// body from a non-5xx response is potentially terminal (RFC 8628 §3.5).
func postFormPoll(ctx context.Context, client HTTPDoer, urlStr string, body url.Values, timeout time.Duration) pollOutcome {
	status, ok, raw, err := postForm(ctx, client, urlStr, body, timeout)
	if err != nil {
		return pollOutcome{kind: pollRetry}
	}
	if ok {
		return pollOutcome{kind: pollOK, raw: raw}
	}
	if status >= 500 {
		return pollOutcome{kind: pollRetry}
	}
	errBody, isOAuth := parseOAuthError(raw)
	if !isOAuth {
		return pollOutcome{kind: pollRetry}
	}
	return pollOutcome{kind: pollError, body: errBody}
}

// PollForTokenParams is the input to PollForToken.
type PollForTokenParams struct {
	TokenEndpoint string
	ClientID      string
	DeviceCode    string
	CodeVerifier  string
	// Interval is the initial polling interval in seconds.
	Interval int64
	// ExpiresIn is the hard deadline in seconds from now.
	ExpiresIn int64
}

// PollForTokenDeps are the DI seams tests need: an HTTP client, sleep, and
// clock.
type PollForTokenDeps struct {
	Client HTTPDoer
	Sleep  func(context.Context, time.Duration) error
	Now    func() time.Time
}

func defaultSleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PollForToken polls the token endpoint per RFC 8628 §3.4-3.5 until success
// or a terminal error. The first attempt fires immediately; subsequent
// attempts sleep `interval` (growing +5s on slow_down) after the previous
// one. The deadline is checked right before every request, including the
// first.
func PollForToken(ctx context.Context, params PollForTokenParams, deps PollForTokenDeps) (TokenResponse, error) {
	client := deps.Client
	if client == nil {
		client = defaultHTTPClient
	}
	sleep := deps.Sleep
	if sleep == nil {
		sleep = defaultSleep
	}
	now := deps.Now
	if now == nil {
		now = time.Now
	}

	deadline := now().Add(time.Duration(params.ExpiresIn) * time.Second)
	interval := time.Duration(params.Interval) * time.Second

	for {
		if !now().Before(deadline) {
			return TokenResponse{}, newDeviceFlowError(CodeExpiredToken, "device code expired before authorization completed")
		}

		outcome := postFormPoll(ctx, client, params.TokenEndpoint, url.Values{
			"grant_type":    {"urn:ietf:params:oauth:grant-type:device_code"},
			"device_code":   {params.DeviceCode},
			"client_id":     {params.ClientID},
			"code_verifier": {params.CodeVerifier},
		}, pollTimeout)

		switch outcome.kind {
		case pollOK:
			tr, ok := parseTokenResponse(outcome.raw)
			if !ok {
				return TokenResponse{}, newDeviceFlowError(CodeInvalidResponse, "token endpoint returned an invalid response")
			}
			return tr, nil
		case pollRetry:
			if err := sleep(ctx, interval); err != nil {
				return TokenResponse{}, err
			}
			continue
		}

		switch *outcome.body.Error {
		case "authorization_pending":
			// fall through to sleep below
		case "slow_down":
			interval += slowDownIncrement
		case "expired_token":
			msg := outcome.body.ErrorDescription
			if msg == "" {
				msg = "device code expired"
			}
			return TokenResponse{}, newDeviceFlowError(CodeExpiredToken, msg, outcome.body.ErrorDescription)
		case "access_denied":
			msg := outcome.body.ErrorDescription
			if msg == "" {
				msg = "login was declined"
			}
			return TokenResponse{}, newDeviceFlowError(CodeAccessDenied, msg, outcome.body.ErrorDescription)
		case "invalid_grant":
			msg := outcome.body.ErrorDescription
			if msg == "" {
				msg = "device code is not valid"
			}
			return TokenResponse{}, newDeviceFlowError(CodeInvalidGrant, msg, outcome.body.ErrorDescription)
		default:
			msg := outcome.body.ErrorDescription
			if msg == "" {
				msg = fmt.Sprintf("authorization failed: %s", *outcome.body.Error)
			}
			return TokenResponse{}, newDeviceFlowError(CodeHTTPError, msg, outcome.body.ErrorDescription)
		}

		if err := sleep(ctx, interval); err != nil {
			return TokenResponse{}, err
		}
	}
}

// RefreshTokenParams is the input to RefreshToken.
type RefreshTokenParams struct {
	TokenEndpoint string
	ClientID      string
	RefreshToken  string
}

// RefreshToken exchanges grant_type=refresh_token (AUTH §5). Rotates on
// every use — the caller must persist the new refresh_token and discard the
// old one.
func RefreshToken(ctx context.Context, params RefreshTokenParams, client HTTPDoer) (TokenResponse, error) {
	if client == nil {
		client = defaultHTTPClient
	}

	_, ok, raw, err := postForm(ctx, client, params.TokenEndpoint, url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {params.RefreshToken},
		"client_id":     {params.ClientID},
	}, requestTimeout)
	if err != nil {
		return TokenResponse{}, newDeviceFlowError(CodeNetworkError, fmt.Sprintf("could not reach %s: %s", params.TokenEndpoint, err.Error()))
	}

	if !ok {
		body, isOAuth := parseOAuthError(raw)
		if isOAuth && *body.Error == "invalid_grant" {
			msg := body.ErrorDescription
			if msg == "" {
				msg = "refresh token is invalid or was already used (rotation)"
			}
			return TokenResponse{}, newDeviceFlowError(CodeInvalidGrant, msg, body.ErrorDescription)
		}
		if isOAuth {
			msg := body.ErrorDescription
			if msg == "" {
				msg = fmt.Sprintf("refresh failed: %s", *body.Error)
			}
			return TokenResponse{}, newDeviceFlowError(CodeHTTPError, msg, body.ErrorDescription)
		}
		return TokenResponse{}, newDeviceFlowError(CodeHTTPError, "refresh failed with an unrecognized error response")
	}

	tr, ok := parseTokenResponse(raw)
	if !ok {
		return TokenResponse{}, newDeviceFlowError(CodeInvalidResponse, "token endpoint returned an invalid response")
	}
	return tr, nil
}
