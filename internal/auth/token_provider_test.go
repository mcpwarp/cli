package auth

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func tpCreds(issuer string, expiresAt int64) Credentials {
	return Credentials{
		AccessToken: "at-1", RefreshToken: "rt-1", ExpiresAt: expiresAt, TokenType: "Bearer",
		Scope: "openid offline_access", Sub: "u1", Issuer: issuer, ClientID: "mcpwarp-cli", SavedAt: 0,
	}
}

func TestTokenProviderFreshToken(t *testing.T) {
	dir := t.TempDir()
	issuer := "http://localhost/realms/mcpwarp"
	paths, _ := CredentialsPaths(issuer, dir)
	c := tpCreds(issuer, 1_000_000)
	if err := Save(c, paths); err != nil {
		t.Fatal(err)
	}

	clock := func() int64 { return 100_000 } // well under expiresAt-60s
	discoverCalled := false
	p := NewTokenProvider(TokenProviderOptions{
		Issuer: issuer, Paths: paths, Clock: clock,
		Discover: func(ctx context.Context, issuer string, c HTTPDoer) (OidcConfig, error) {
			discoverCalled = true
			return OidcConfig{}, nil
		},
	})
	token, err := p.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if token != "at-1" {
		t.Fatalf("got %q", token)
	}
	if discoverCalled {
		t.Fatal("should not have refreshed a fresh token")
	}
	if p.LastAction() != "unchanged" {
		t.Fatalf("got %q", p.LastAction())
	}
}

func TestTokenProviderRefreshesStaleToken(t *testing.T) {
	dir := t.TempDir()
	issuer := "http://localhost/realms/mcpwarp"
	paths, _ := CredentialsPaths(issuer, dir)
	c := tpCreds(issuer, 1000) // already expired relative to clock below
	if err := Save(c, paths); err != nil {
		t.Fatal(err)
	}

	clock := func() int64 { return 2_000_000 }
	p := NewTokenProvider(TokenProviderOptions{
		Issuer: issuer, Paths: paths, Clock: clock,
		Discover: func(ctx context.Context, issuer string, c HTTPDoer) (OidcConfig, error) {
			return OidcConfig{TokenEndpoint: "https://token"}, nil
		},
		RefreshToken: func(ctx context.Context, params RefreshTokenParams, c HTTPDoer) (TokenResponse, error) {
			if params.RefreshToken != "rt-1" {
				t.Fatalf("unexpected refresh token %q", params.RefreshToken)
			}
			return TokenResponse{AccessToken: "at-2", RefreshToken: "rt-2", ExpiresIn: 3600, TokenType: "Bearer"}, nil
		},
	})

	token, err := p.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if token != "at-2" {
		t.Fatalf("got %q", token)
	}
	if p.LastAction() != "refreshed" {
		t.Fatalf("got %q", p.LastAction())
	}

	onDisk := Load(paths, nil)
	if onDisk.RefreshToken != "rt-2" {
		t.Fatalf("rotation should persist the new refresh token, got %q", onDisk.RefreshToken)
	}
}

func TestTokenProviderNoRotationIsAnError(t *testing.T) {
	dir := t.TempDir()
	issuer := "http://localhost/realms/mcpwarp"
	paths, _ := CredentialsPaths(issuer, dir)
	Save(tpCreds(issuer, 1000), paths)

	p := NewTokenProvider(TokenProviderOptions{
		Issuer: issuer, Paths: paths, Clock: func() int64 { return 2_000_000 },
		Discover: func(ctx context.Context, issuer string, c HTTPDoer) (OidcConfig, error) {
			return OidcConfig{TokenEndpoint: "https://token"}, nil
		},
		RefreshToken: func(ctx context.Context, params RefreshTokenParams, c HTTPDoer) (TokenResponse, error) {
			return TokenResponse{AccessToken: "at-2", ExpiresIn: 3600, TokenType: "Bearer"}, nil // no refresh_token
		},
	})
	_, err := p.Token(context.Background())
	if _, ok := err.(*TokenRefreshError); !ok {
		t.Fatalf("got %#v", err)
	}
}

func TestTokenProviderInvalidGrantClearsCredsAndReportsSessionExpired(t *testing.T) {
	dir := t.TempDir()
	issuer := "http://localhost/realms/mcpwarp"
	paths, _ := CredentialsPaths(issuer, dir)
	Save(tpCreds(issuer, 1000), paths)

	p := NewTokenProvider(TokenProviderOptions{
		Issuer: issuer, Paths: paths, Clock: func() int64 { return 2_000_000 },
		Discover: func(ctx context.Context, issuer string, c HTTPDoer) (OidcConfig, error) {
			return OidcConfig{TokenEndpoint: "https://token"}, nil
		},
		RefreshToken: func(ctx context.Context, params RefreshTokenParams, c HTTPDoer) (TokenResponse, error) {
			return TokenResponse{}, &DeviceFlowError{Code: CodeInvalidGrant, Message: "reused"}
		},
	})
	_, err := p.Token(context.Background())
	if _, ok := err.(*SessionExpiredError); !ok {
		t.Fatalf("got %#v", err)
	}
	if Load(paths, nil) != nil {
		t.Fatal("credentials should have been cleared")
	}
}

func TestTokenProviderNotLoggedIn(t *testing.T) {
	dir := t.TempDir()
	issuer := "http://localhost/realms/mcpwarp"
	paths, _ := CredentialsPaths(issuer, dir)
	p := NewTokenProvider(TokenProviderOptions{Issuer: issuer, Paths: paths})
	_, err := p.Token(context.Background())
	if _, ok := err.(*NotLoggedInError); !ok {
		t.Fatalf("got %#v", err)
	}
}

func TestTokenProviderProactiveRefreshFailureReturnsStillValidToken(t *testing.T) {
	dir := t.TempDir()
	issuer := "http://localhost/realms/mcpwarp"
	paths, _ := CredentialsPaths(issuer, dir)
	// Stale under the 60s buffer, but still valid against a 0 buffer.
	Save(tpCreds(issuer, 2_030_000), paths)

	var warned string
	p := NewTokenProvider(TokenProviderOptions{
		Issuer: issuer, Paths: paths, Clock: func() int64 { return 2_000_000 },
		Warn: func(m string) { warned = m },
		Discover: func(ctx context.Context, issuer string, c HTTPDoer) (OidcConfig, error) {
			return OidcConfig{}, &DiscoveryError{}
		},
	})
	token, err := p.Token(context.Background())
	if err != nil {
		t.Fatalf("proactive refresh failure should fall back to the still-valid token, got %v", err)
	}
	if token != "at-1" {
		t.Fatalf("got %q", token)
	}
	if warned == "" {
		t.Fatal("expected a warning")
	}
}

func TestTokenProviderAcceptanceModelForcesRefreshWhenUnaccepted(t *testing.T) {
	dir := t.TempDir()
	issuer := "http://localhost/realms/mcpwarp"
	paths, _ := CredentialsPaths(issuer, dir)
	Save(tpCreds(issuer, 1_000_000), paths) // fresh, would not normally refresh

	refreshCalls := 0
	p := NewTokenProvider(TokenProviderOptions{
		Issuer: issuer, Paths: paths, Clock: func() int64 { return 100_000 },
		Discover: func(ctx context.Context, issuer string, c HTTPDoer) (OidcConfig, error) {
			return OidcConfig{TokenEndpoint: "https://token"}, nil
		},
		RefreshToken: func(ctx context.Context, params RefreshTokenParams, c HTTPDoer) (TokenResponse, error) {
			refreshCalls++
			return TokenResponse{AccessToken: "at-2", RefreshToken: "rt-2", ExpiresIn: 3600, TokenType: "Bearer"}, nil
		},
	})

	first, err := p.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first != "at-1" {
		t.Fatalf("got %q", first)
	}
	if refreshCalls != 0 {
		t.Fatal("first call should not refresh")
	}

	// Never called MarkAccepted — asking again must force a refresh.
	second, err := p.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second != "at-2" {
		t.Fatalf("expected forced refresh, got %q", second)
	}
	if refreshCalls != 1 {
		t.Fatalf("expected exactly one refresh, got %d", refreshCalls)
	}
}

func TestTokenProviderMarkAcceptedPreventsForceRefresh(t *testing.T) {
	dir := t.TempDir()
	issuer := "http://localhost/realms/mcpwarp"
	paths, _ := CredentialsPaths(issuer, dir)
	Save(tpCreds(issuer, 1_000_000), paths)

	refreshCalls := 0
	p := NewTokenProvider(TokenProviderOptions{
		Issuer: issuer, Paths: paths, Clock: func() int64 { return 100_000 },
		Discover: func(ctx context.Context, issuer string, c HTTPDoer) (OidcConfig, error) {
			return OidcConfig{TokenEndpoint: "https://token"}, nil
		},
		RefreshToken: func(ctx context.Context, params RefreshTokenParams, c HTTPDoer) (TokenResponse, error) {
			refreshCalls++
			return TokenResponse{AccessToken: "at-2", RefreshToken: "rt-2", ExpiresIn: 3600, TokenType: "Bearer"}, nil
		},
	})

	if _, err := p.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.MarkAccepted()

	second, err := p.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second != "at-1" || refreshCalls != 0 {
		t.Fatalf("accepted token should not force a refresh: token=%q calls=%d", second, refreshCalls)
	}
}

// TestTokenProviderForcedRefreshFailureDoesNotFallBackToStaleToken (S6): the
// proactive path (obtainFresh) falls back to a still-valid stale token when
// a refresh attempt fails — see
// TestTokenProviderProactiveRefreshFailureReturnsStillValidToken — but the
// forced path (obtainForced) must not: the caller explicitly rejected the
// last token it was handed, so silently handing back that same stale token
// again on failure would hide a refresh outage rather than reporting it.
func TestTokenProviderForcedRefreshFailureDoesNotFallBackToStaleToken(t *testing.T) {
	dir := t.TempDir()
	issuer := "http://localhost/realms/mcpwarp"
	paths, _ := CredentialsPaths(issuer, dir)
	Save(tpCreds(issuer, 1_000_000), paths) // fresh — the first call won't refresh at all

	p := NewTokenProvider(TokenProviderOptions{
		Issuer: issuer, Paths: paths, Clock: func() int64 { return 100_000 },
		Discover: func(ctx context.Context, issuer string, c HTTPDoer) (OidcConfig, error) {
			return OidcConfig{}, &DiscoveryError{}
		},
	})

	first, err := p.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first != "at-1" {
		t.Fatalf("got %q", first)
	}

	// Never called MarkAccepted, so this second call goes through
	// obtainForced.
	_, err = p.Token(context.Background())
	if err == nil {
		t.Fatal("expected the forced refresh failure to propagate, not fall back to the stale token")
	}
	if _, ok := err.(*TokenRefreshError); !ok {
		t.Fatalf("got %#v", err)
	}
}

func TestForceRefreshUsesAlreadyRotatedCredsInstead(t *testing.T) {
	dir := t.TempDir()
	issuer := "http://localhost/realms/mcpwarp"
	paths, _ := CredentialsPaths(issuer, dir)
	// Another process already rotated past "at-1" to "at-rotated".
	rotated := tpCreds(issuer, 9_999_999)
	rotated.AccessToken = "at-rotated"
	Save(rotated, paths)

	refreshCalls := 0
	opts := TokenProviderOptions{
		Issuer: issuer, Paths: paths, Clock: func() int64 { return 100_000 },
		RefreshToken: func(ctx context.Context, params RefreshTokenParams, c HTTPDoer) (TokenResponse, error) {
			refreshCalls++
			return TokenResponse{}, nil
		},
	}
	token, err := ForceRefresh(context.Background(), opts, "at-1")
	if err != nil {
		t.Fatal(err)
	}
	if token != "at-rotated" {
		t.Fatalf("got %q", token)
	}
	if refreshCalls != 0 {
		t.Fatal("should not have refreshed again")
	}
}

// TestTokenProviderConcurrentCallsShareOneRefresh pins the single-flight
// behaviour deterministically: the first call is held inside RefreshToken by
// a gate channel (not a sleep) until the second call has actually been
// scheduled, so the assertion that only one refresh happens never depends on
// out-running a fixed delay.
func TestTokenProviderConcurrentCallsShareOneRefresh(t *testing.T) {
	dir := t.TempDir()
	issuer := "http://localhost/realms/mcpwarp"
	paths, _ := CredentialsPaths(issuer, dir)
	Save(tpCreds(issuer, 1000), paths) // stale relative to the clock below

	var calls int32
	entered := make(chan struct{})
	proceed := make(chan struct{})
	p := NewTokenProvider(TokenProviderOptions{
		Issuer: issuer, Paths: paths, Clock: func() int64 { return 2_000_000 },
		Discover: func(ctx context.Context, issuer string, c HTTPDoer) (OidcConfig, error) {
			return OidcConfig{TokenEndpoint: "https://token"}, nil
		},
		RefreshToken: func(ctx context.Context, params RefreshTokenParams, c HTTPDoer) (TokenResponse, error) {
			atomic.AddInt32(&calls, 1)
			close(entered)
			<-proceed
			return TokenResponse{AccessToken: "at-2", RefreshToken: "rt-2", ExpiresIn: 3600, TokenType: "Bearer"}, nil
		},
	})

	var wg sync.WaitGroup
	results := make([]string, 2)
	errs := make([]error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		results[0], errs[0] = p.Token(context.Background())
	}()
	<-entered // the first call now holds the in-flight slot, parked in RefreshToken

	secondStarted := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(secondStarted)
		results[1], errs[1] = p.Token(context.Background())
	}()
	<-secondStarted // scheduled to call Token(); inFlight stays set until proceed below, so it must join rather than refresh again

	close(proceed)
	wg.Wait()

	for i := range 2 {
		if errs[i] != nil {
			t.Fatalf("call %d: %v", i, errs[i])
		}
		if results[i] != "at-2" {
			t.Fatalf("call %d: got %q", i, results[i])
		}
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected exactly one refresh, got %d", got)
	}
}

// TestTokenProviderThirdCallDuringPublishWindowStartsItsOwnFlight pins B2:
// Token() clears p.inFlight and releases p.mu before closing the completed
// flight's done channel, so a third caller arriving in that gap sees no
// in-flight operation and starts its own — which must find the
// just-refreshed, now-fresh credentials on disk and return them without
// triggering a second refresh. testHookAfterInFlightCleared pins the race
// window deterministically instead of guessing at scheduling.
func TestTokenProviderThirdCallDuringPublishWindowStartsItsOwnFlight(t *testing.T) {
	dir := t.TempDir()
	issuer := "http://localhost/realms/mcpwarp"
	paths, _ := CredentialsPaths(issuer, dir)
	Save(tpCreds(issuer, 1000), paths)

	var calls int32
	p := NewTokenProvider(TokenProviderOptions{
		Issuer: issuer, Paths: paths, Clock: func() int64 { return 2_000_000 },
		Discover: func(ctx context.Context, issuer string, c HTTPDoer) (OidcConfig, error) {
			return OidcConfig{TokenEndpoint: "https://token"}, nil
		},
		RefreshToken: func(ctx context.Context, params RefreshTokenParams, c HTTPDoer) (TokenResponse, error) {
			atomic.AddInt32(&calls, 1)
			return TokenResponse{AccessToken: "at-2", RefreshToken: "rt-2", ExpiresIn: 3600, TokenType: "Bearer"}, nil
		},
	})

	hookEntered := make(chan struct{})
	release := make(chan struct{})
	var first int32 = 1
	origHook := testHookAfterInFlightCleared
	testHookAfterInFlightCleared = func() {
		// Only the first flight (call 1) parks here; a fast second flight
		// completing while this one is still parked (call 3, below) must
		// pass straight through.
		if atomic.CompareAndSwapInt32(&first, 1, 0) {
			close(hookEntered)
			<-release
		}
	}
	t.Cleanup(func() { testHookAfterInFlightCleared = origHook })

	var wg sync.WaitGroup
	var res1 string
	var err1 error
	wg.Add(1)
	go func() {
		defer wg.Done()
		res1, err1 = p.Token(context.Background())
	}()
	<-hookEntered // call 1 published its result (inFlight cleared) but hasn't closed f.done yet

	// Accept the token call 1 already published (its lastReturned/hasReturned
	// were set under the same lock as clearing inFlight, before the hook)
	// so call 3 exercises the B2 window on its own, independent of the
	// separate "an unaccepted token forces the next call to refresh"
	// behaviour (Token/MarkAccepted).
	p.MarkAccepted()

	done3 := make(chan struct{})
	var res3 string
	var err3 error
	go func() {
		res3, err3 = p.Token(context.Background())
		close(done3)
	}()
	<-done3 // the B2 window: call 3 saw inFlight == nil and ran its own (fast, no-refresh) flight to completion

	close(release)
	wg.Wait()

	if err1 != nil || err3 != nil {
		t.Fatalf("errs: %v, %v", err1, err3)
	}
	if res1 != "at-2" || res3 != "at-2" {
		t.Fatalf("got res1=%q res3=%q", res1, res3)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected exactly one refresh despite the third call landing in the publish window, got %d", got)
	}
}

func TestTokenProviderNetworkErrorLeavesCredentialsIntact(t *testing.T) {
	dir := t.TempDir()
	issuer := "http://localhost/realms/mcpwarp"
	paths, _ := CredentialsPaths(issuer, dir)
	Save(tpCreds(issuer, 1000), paths)

	p := NewTokenProvider(TokenProviderOptions{
		Issuer: issuer, Paths: paths, Clock: func() int64 { return 2_000_000 },
		Discover: func(ctx context.Context, issuer string, c HTTPDoer) (OidcConfig, error) {
			return OidcConfig{TokenEndpoint: "https://token"}, nil
		},
		RefreshToken: func(ctx context.Context, params RefreshTokenParams, c HTTPDoer) (TokenResponse, error) {
			return TokenResponse{}, newDeviceFlowError(CodeNetworkError, "could not reach https://token: fetch failed")
		},
	})
	_, err := p.Token(context.Background())
	if _, ok := err.(*TokenRefreshError); !ok {
		t.Fatalf("got %#v", err)
	}
	if Load(paths, nil).AccessToken != "at-1" {
		t.Fatal("credentials should be untouched after a network error")
	}
}

func TestForceRefreshRefreshesEvenWhenStoredTokenLooksFresh(t *testing.T) {
	dir := t.TempDir()
	issuer := "http://localhost/realms/mcpwarp"
	paths, _ := CredentialsPaths(issuer, dir)
	Save(tpCreds(issuer, 9_999_999), paths) // looks fresh against any reasonable clock

	refreshCalls := 0
	opts := TokenProviderOptions{
		Issuer: issuer, Paths: paths, Clock: func() int64 { return 100_000 },
		Discover: func(ctx context.Context, issuer string, c HTTPDoer) (OidcConfig, error) {
			return OidcConfig{TokenEndpoint: "https://token"}, nil
		},
		RefreshToken: func(ctx context.Context, params RefreshTokenParams, c HTTPDoer) (TokenResponse, error) {
			refreshCalls++
			return TokenResponse{AccessToken: "at-2", RefreshToken: "rt-2", ExpiresIn: 3600, TokenType: "Bearer"}, nil
		},
	}
	token, err := ForceRefresh(context.Background(), opts, "")
	if err != nil {
		t.Fatal(err)
	}
	if token != "at-2" || refreshCalls != 1 {
		t.Fatalf("got token=%q calls=%d", token, refreshCalls)
	}
}

func TestForceRefreshNotLoggedIn(t *testing.T) {
	dir := t.TempDir()
	issuer := "http://localhost/realms/mcpwarp"
	paths, _ := CredentialsPaths(issuer, dir)
	_, err := ForceRefresh(context.Background(), TokenProviderOptions{Issuer: issuer, Paths: paths}, "")
	if _, ok := err.(*NotLoggedInError); !ok {
		t.Fatalf("got %#v", err)
	}
}

func TestForceRefreshWrapsLockTimeoutAsTokenRefreshError(t *testing.T) {
	dir := t.TempDir()
	issuer := "http://localhost/realms/mcpwarp"
	paths, _ := CredentialsPaths(issuer, dir)
	Save(tpCreds(issuer, 9_999_999), paths)

	raw, _ := json.Marshal(lockPayload{PID: 999999, CreatedAt: time.Now().UnixMilli(), Token: "someone-else"})
	if err := os.WriteFile(paths.File+".lock", raw, 0o600); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(paths.File + ".lock")

	opts := TokenProviderOptions{
		Issuer: issuer, Paths: paths,
		Lock: LockOptions{RetryMs: 5, TimeoutMs: 20, StaleMs: 60_000},
	}
	_, err := ForceRefresh(context.Background(), opts, "")
	if _, ok := err.(*TokenRefreshError); !ok {
		t.Fatalf("got %#v", err)
	}
}

func TestRefreshAndPersistInvalidGrantDoesNotClearAlreadyRotatedCreds(t *testing.T) {
	dir := t.TempDir()
	issuer := "http://localhost/realms/mcpwarp"
	paths, _ := CredentialsPaths(issuer, dir)
	current := tpCreds(issuer, 1000) // what this call started with: refresh_token "rt-1"

	rotated := tpCreds(issuer, 9_999_999)
	rotated.RefreshToken = "rt-rotated-by-another-process"
	Save(rotated, paths)

	opts := TokenProviderOptions{
		Issuer: issuer, Paths: paths, Clock: func() int64 { return 2_000_000 },
		Discover: func(ctx context.Context, issuer string, c HTTPDoer) (OidcConfig, error) {
			return OidcConfig{TokenEndpoint: "https://token"}, nil
		},
		RefreshToken: func(ctx context.Context, params RefreshTokenParams, c HTTPDoer) (TokenResponse, error) {
			return TokenResponse{}, &DeviceFlowError{Code: CodeInvalidGrant, Message: "reused"}
		},
	}.resolve()

	_, err := refreshAndPersist(context.Background(), opts, current)
	if _, ok := err.(*SessionExpiredError); !ok {
		t.Fatalf("got %#v", err)
	}
	onDisk := Load(paths, nil)
	if onDisk == nil || onDisk.RefreshToken != "rt-rotated-by-another-process" {
		t.Fatalf("another process's already-rotated credentials must not be cleared, got %#v", onDisk)
	}
}

func TestIsInvalidGrantOrTokenSniffsInvalidTokenMessage(t *testing.T) {
	if !isInvalidGrantOrToken(&DeviceFlowError{Code: CodeHTTPError, Message: "Invalid_Token: token expired"}) {
		t.Fatal("expected a case-insensitive invalid_token sniff to match")
	}
	if isInvalidGrantOrToken(&DeviceFlowError{Code: CodeHTTPError, Message: "server unavailable"}) {
		t.Fatal("an unrelated http_error message must not be treated as invalid_grant")
	}
	if isInvalidGrantOrToken(&DeviceFlowError{Code: CodeNetworkError, Message: "invalid_token"}) {
		t.Fatal("only CodeHTTPError (or CodeInvalidGrant) should ever match")
	}
	if !isInvalidGrantOrToken(&DeviceFlowError{Code: CodeInvalidGrant, Message: "anything"}) {
		t.Fatal("CodeInvalidGrant must always match regardless of message")
	}
}

func TestStaticTokenProvider(t *testing.T) {
	const goodShape = "mcpwarp_pat_abcdef123456_s3cr3t"

	t.Run("returns the token verbatim", func(t *testing.T) {
		p := NewStaticTokenProvider(goodShape, nil)
		if p.Token() != goodShape {
			t.Fatalf("got %q", p.Token())
		}
	})

	t.Run("good shape passes silently", func(t *testing.T) {
		warned := false
		NewStaticTokenProvider(goodShape, func(m string) { warned = true })
		if warned {
			t.Fatal("unexpected warning")
		}
	})

	t.Run("warns on the wrong prefix", func(t *testing.T) {
		var warned string
		NewStaticTokenProvider("not-a-pat", func(m string) { warned = m })
		if warned == "" {
			t.Fatal("expected a warning")
		}
	})

	t.Run("warns on a short id", func(t *testing.T) {
		var warned string
		NewStaticTokenProvider("mcpwarp_pat_abc_s3cr3t", func(m string) { warned = m })
		if warned == "" {
			t.Fatal("expected a warning")
		}
	})

	t.Run("warns on a missing secret", func(t *testing.T) {
		var warned string
		NewStaticTokenProvider("mcpwarp_pat_abcdef123456_", func(m string) { warned = m })
		if warned == "" {
			t.Fatal("expected a warning")
		}
	})

	t.Run("warns when over 200 bytes", func(t *testing.T) {
		tooLong := "mcpwarp_pat_abcdef123456_" + strings.Repeat("s", 200)
		var warned string
		NewStaticTokenProvider(tooLong, func(m string) { warned = m })
		if warned == "" {
			t.Fatal("expected a warning")
		}
	})

	t.Run("token is still passed verbatim despite the warning", func(t *testing.T) {
		bad := "not-a-pat"
		p := NewStaticTokenProvider(bad, func(string) {})
		if p.Token() != bad {
			t.Fatalf("got %q, want the token unchanged", p.Token())
		}
	})
}
