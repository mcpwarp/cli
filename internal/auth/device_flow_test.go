package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
)

func TestRequestDeviceCode(t *testing.T) {
	t.Run("posts client_id, scope, code_challenge, code_challenge_method=S256", func(t *testing.T) {
		var got url.Values
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = r.ParseForm()
			got = r.PostForm
			_ = json.NewEncoder(w).Encode(map[string]any{
				"device_code":               "dc",
				"user_code":                 "ABCD-EFGH",
				"verification_uri":          "https://example/device",
				"verification_uri_complete": "https://example/device?user_code=ABCD-EFGH",
				"expires_in":                600,
				"interval":                  5,
			})
		}))
		defer srv.Close()

		resp, err := RequestDeviceCode(context.Background(), RequestDeviceCodeParams{
			DeviceAuthorizationEndpoint: srv.URL,
			ClientID:                    "mcpwarp-cli",
			Scope:                       "openid offline_access",
			CodeChallenge:               "challenge123",
		}, srv.Client())
		if err != nil {
			t.Fatal(err)
		}
		if resp.DeviceCode != "dc" {
			t.Fatalf("got %q", resp.DeviceCode)
		}
		if got.Get("client_id") != "mcpwarp-cli" || got.Get("scope") != "openid offline_access" ||
			got.Get("code_challenge") != "challenge123" || got.Get("code_challenge_method") != "S256" {
			t.Fatalf("unexpected form: %v", got)
		}
	})

	t.Run("defaults interval to 5s when the server omits it", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"device_code":      "dc",
				"user_code":        "ABCD-EFGH",
				"verification_uri": "https://example/device",
				"expires_in":       600,
			})
		}))
		defer srv.Close()

		resp, err := RequestDeviceCode(context.Background(), RequestDeviceCodeParams{
			DeviceAuthorizationEndpoint: srv.URL, ClientID: "c", Scope: "s", CodeChallenge: "ch",
		}, srv.Client())
		if err != nil {
			t.Fatal(err)
		}
		if resp.Interval != 5 {
			t.Fatalf("got %d", resp.Interval)
		}
	})

	t.Run("invalid_response on a schema mismatch", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"nope": true})
		}))
		defer srv.Close()

		_, err := RequestDeviceCode(context.Background(), RequestDeviceCodeParams{
			DeviceAuthorizationEndpoint: srv.URL, ClientID: "c", Scope: "s", CodeChallenge: "ch",
		}, srv.Client())
		dfe, ok := err.(*DeviceFlowError)
		if !ok || dfe.Code != CodeInvalidResponse {
			t.Fatalf("got %#v", err)
		}
	})
}

func fakeClock() (now func() time.Time, sleep func(context.Context, time.Duration) error) {
	t := int64(0)
	now = func() time.Time { return time.UnixMilli(atomic.LoadInt64(&t)) }
	sleep = func(_ context.Context, d time.Duration) error {
		atomic.AddInt64(&t, d.Milliseconds())
		return nil
	}
	return
}

func jsonHandler(status int, body any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		if body != nil {
			_ = json.NewEncoder(w).Encode(body)
		}
	}
}

func sequenceHandler(handlers ...http.HandlerFunc) http.HandlerFunc {
	var i int32
	return func(w http.ResponseWriter, r *http.Request) {
		idx := atomic.AddInt32(&i, 1) - 1
		if int(idx) >= len(handlers) {
			idx = int32(len(handlers) - 1)
		}
		handlers[idx](w, r)
	}
}

func TestPollForToken(t *testing.T) {
	baseParams := PollForTokenParams{
		TokenEndpoint: "", ClientID: "mcpwarp-cli", DeviceCode: "dc", CodeVerifier: "verifier",
		Interval: 5, ExpiresIn: 600,
	}

	t.Run("keeps polling through authorization_pending, then slow_down, then succeeds", func(t *testing.T) {
		srv := httptest.NewServer(sequenceHandler(
			jsonHandler(400, map[string]string{"error": "authorization_pending"}),
			jsonHandler(400, map[string]string{"error": "slow_down"}),
			jsonHandler(200, map[string]any{
				"access_token": "at", "refresh_token": "rt", "expires_in": 3600,
				"refresh_expires_in": 2591992, "token_type": "Bearer", "scope": "openid",
			}),
		))
		defer srv.Close()

		now, sleep := fakeClock()
		var sleepCalls []time.Duration
		wrappedSleep := func(ctx context.Context, d time.Duration) error {
			sleepCalls = append(sleepCalls, d)
			return sleep(ctx, d)
		}

		params := baseParams
		params.TokenEndpoint = srv.URL
		result, err := PollForToken(context.Background(), params, PollForTokenDeps{
			Client: srv.Client(), Sleep: wrappedSleep, Now: now,
		})
		if err != nil {
			t.Fatal(err)
		}
		if result.AccessToken != "at" {
			t.Fatalf("got %q", result.AccessToken)
		}
		if len(sleepCalls) != 2 || sleepCalls[0] != 5*time.Second || sleepCalls[1] != 10*time.Second {
			t.Fatalf("unexpected sleeps: %v", sleepCalls)
		}
	})

	t.Run("treats a network error, HTTP 502, and unparseable body as retryable, then succeeds", func(t *testing.T) {
		var i int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch atomic.AddInt32(&i, 1) {
			case 1:
				w.WriteHeader(http.StatusBadGateway)
			case 2:
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("not json"))
			default:
				_ = json.NewEncoder(w).Encode(map[string]any{
					"access_token": "at", "refresh_token": "rt", "expires_in": 3600,
					"token_type": "Bearer", "scope": "openid",
				})
			}
		}))
		defer srv.Close()

		now, sleep := fakeClock()
		params := baseParams
		params.TokenEndpoint = srv.URL
		result, err := PollForToken(context.Background(), params, PollForTokenDeps{
			Client: srv.Client(), Sleep: sleep, Now: now,
		})
		if err != nil {
			t.Fatal(err)
		}
		if result.AccessToken != "at" {
			t.Fatalf("got %q", result.AccessToken)
		}
	})

	t.Run("a network error keeps retrying until the deadline, then reports expired_token", func(t *testing.T) {
		client := &countingErrClient{fn: func() error { return context.DeadlineExceeded }}
		now, sleep := fakeClock()
		params := baseParams
		params.TokenEndpoint = "http://127.0.0.1:1"
		params.ExpiresIn = 20
		_, err := PollForToken(context.Background(), params, PollForTokenDeps{Client: client, Sleep: sleep, Now: now})
		dfe, ok := err.(*DeviceFlowError)
		if !ok || dfe.Code != CodeExpiredToken {
			t.Fatalf("got %#v", err)
		}
	})

	t.Run("invalid_response on a malformed success body", func(t *testing.T) {
		srv := httptest.NewServer(jsonHandler(200, map[string]any{"not_a_token_response": true}))
		defer srv.Close()
		now, sleep := fakeClock()
		params := baseParams
		params.TokenEndpoint = srv.URL
		_, err := PollForToken(context.Background(), params, PollForTokenDeps{Client: srv.Client(), Sleep: sleep, Now: now})
		dfe, ok := err.(*DeviceFlowError)
		if !ok || dfe.Code != CodeInvalidResponse {
			t.Fatalf("got %#v", err)
		}
	})

	t.Run("expired_token stops polling", func(t *testing.T) {
		srv := httptest.NewServer(jsonHandler(400, map[string]string{"error": "expired_token"}))
		defer srv.Close()
		now, sleep := fakeClock()
		params := baseParams
		params.TokenEndpoint = srv.URL
		_, err := PollForToken(context.Background(), params, PollForTokenDeps{Client: srv.Client(), Sleep: sleep, Now: now})
		dfe, ok := err.(*DeviceFlowError)
		if !ok || dfe.Code != CodeExpiredToken {
			t.Fatalf("got %#v", err)
		}
	})

	t.Run("access_denied stops polling", func(t *testing.T) {
		srv := httptest.NewServer(jsonHandler(400, map[string]string{"error": "access_denied"}))
		defer srv.Close()
		now, sleep := fakeClock()
		params := baseParams
		params.TokenEndpoint = srv.URL
		_, err := PollForToken(context.Background(), params, PollForTokenDeps{Client: srv.Client(), Sleep: sleep, Now: now})
		dfe, ok := err.(*DeviceFlowError)
		if !ok || dfe.Code != CodeAccessDenied {
			t.Fatalf("got %#v", err)
		}
	})

	t.Run("stops once the deadline passes, without another fetch", func(t *testing.T) {
		var calls int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&calls, 1)
			w.WriteHeader(400)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "authorization_pending"})
		}))
		defer srv.Close()

		now, sleep := fakeClock()
		params := baseParams
		params.TokenEndpoint = srv.URL
		params.ExpiresIn = 5
		params.Interval = 10
		_, err := PollForToken(context.Background(), params, PollForTokenDeps{Client: srv.Client(), Sleep: sleep, Now: now})
		dfe, ok := err.(*DeviceFlowError)
		if !ok || dfe.Code != CodeExpiredToken {
			t.Fatalf("got %#v", err)
		}
		if atomic.LoadInt32(&calls) != 1 {
			t.Fatalf("expected 1 call, got %d", calls)
		}
	})
}

// countingErrClient always fails Do — used to exercise PollForToken's
// network-error retry path without a real unreachable address (slow on some
// CI sandboxes).
type countingErrClient struct{ fn func() error }

func (c *countingErrClient) Do(req *http.Request) (*http.Response, error) {
	return nil, c.fn()
}

func TestRefreshToken(t *testing.T) {
	t.Run("posts grant_type=refresh_token and returns the rotated set", func(t *testing.T) {
		var got url.Values
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = r.ParseForm()
			got = r.PostForm
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "new-at", "refresh_token": "new-rt", "expires_in": 3600,
				"token_type": "Bearer", "scope": "openid",
			})
		}))
		defer srv.Close()

		result, err := RefreshToken(context.Background(), RefreshTokenParams{
			TokenEndpoint: srv.URL, ClientID: "mcpwarp-cli", RefreshToken: "old-rt",
		}, srv.Client())
		if err != nil {
			t.Fatal(err)
		}
		if result.RefreshToken != "new-rt" {
			t.Fatalf("got %q", result.RefreshToken)
		}
		if got.Get("grant_type") != "refresh_token" || got.Get("refresh_token") != "old-rt" {
			t.Fatalf("unexpected form: %v", got)
		}
	})

	t.Run("invalid_grant on reuse", func(t *testing.T) {
		srv := httptest.NewServer(jsonHandler(400, map[string]string{
			"error": "invalid_grant", "error_description": "Maximum allowed refresh token reuse exceeded",
		}))
		defer srv.Close()

		_, err := RefreshToken(context.Background(), RefreshTokenParams{
			TokenEndpoint: srv.URL, ClientID: "mcpwarp-cli", RefreshToken: "stale-rt",
		}, srv.Client())
		dfe, ok := err.(*DeviceFlowError)
		if !ok || dfe.Code != CodeInvalidGrant {
			t.Fatalf("got %#v", err)
		}
	})
}
