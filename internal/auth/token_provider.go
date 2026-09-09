package auth

import (
	"context"
	"fmt"
	"regexp"
	"sync"
)

// NotLoggedInError means no usable credentials were found.
type NotLoggedInError struct{ message string }

func (e *NotLoggedInError) Error() string {
	if e.message != "" {
		return e.message
	}
	return "not logged in, run `mcpwarp login`"
}

// SessionExpiredError means the refresh token was rejected (rotation
// reuse, revocation, or realm-side expiry) — the stored session is dead.
type SessionExpiredError struct{ message string }

func (e *SessionExpiredError) Error() string {
	if e.message != "" {
		return e.message
	}
	return "session expired, run `mcpwarp login`"
}

// TokenRefreshError is a retryable failure: discovery/token endpoint
// unreachable, a transient (non-invalid_grant) error, or the cross-process
// lock timed out. Credentials are left untouched.
type TokenRefreshError struct {
	message string
	cause   error
}

func (e *TokenRefreshError) Error() string   { return e.message }
func (e *TokenRefreshError) Unwrap() error   { return e.cause }
func (e *TokenRefreshError) Retryable() bool { return true }

// DiscoverFn and RefreshTokenFn are the seams TokenProviderOptions injects
// for tests; they default to Discover/RefreshToken.
type DiscoverFn func(ctx context.Context, issuer string, client HTTPDoer) (OidcConfig, error)
type RefreshTokenFn func(ctx context.Context, params RefreshTokenParams, client HTTPDoer) (TokenResponse, error)

// TokenProviderOptions configures createTokenProvider/ForceRefresh.
type TokenProviderOptions struct {
	Issuer       string
	Paths        Paths
	Discover     DiscoverFn
	RefreshToken RefreshTokenFn
	Client       HTTPDoer
	Clock        func() int64
	Warn         func(string)
	// BufferMs is the proactive-refresh buffer (AUTH §6): refresh once
	// expires_at - BufferMs has passed. Default 60s.
	BufferMs int64
	Lock     LockOptions
}

func (o TokenProviderOptions) resolve() TokenProviderOptions {
	if o.Discover == nil {
		o.Discover = Discover
	}
	if o.RefreshToken == nil {
		o.RefreshToken = RefreshToken
	}
	if o.Clock == nil {
		o.Clock = nowMillisFn
	}
	if o.BufferMs == 0 {
		o.BufferMs = DefaultStaleBufferMs
	}
	if o.Lock.Now == nil {
		clock := o.Clock
		o.Lock.Now = func() int64 { return clock() }
	}
	return o
}

func nowMillisFn() int64 { return nowMs() }

var invalidTokenPattern = regexp.MustCompile(`(?i)invalid_token`)

func isInvalidGrantOrToken(err error) bool {
	dfe, ok := err.(*DeviceFlowError)
	if !ok {
		return false
	}
	if dfe.Code == CodeInvalidGrant {
		return true
	}
	return dfe.Code == CodeHTTPError && invalidTokenPattern.MatchString(dfe.Message)
}

func withTokenLock[T any](ctx context.Context, path string, lock LockOptions, fn func() (T, error)) (T, error) {
	var zero T
	result, err := WithLock(ctx, path, lock, fn)
	if err != nil {
		if lte, ok := err.(*LockTimeoutError); ok {
			return zero, &TokenRefreshError{message: fmt.Sprintf("could not acquire the refresh lock: %s", lte.Error()), cause: lte}
		}
		return zero, err
	}
	return result, nil
}

// refreshAndPersist runs one discover+refresh+persist cycle and returns the
// new access token.
func refreshAndPersist(ctx context.Context, opts TokenProviderOptions, current Credentials) (string, error) {
	oidc, err := opts.Discover(ctx, opts.Issuer, opts.Client)
	if err != nil {
		return "", &TokenRefreshError{message: fmt.Sprintf("could not reach the auth server to refresh the session: %s", err.Error()), cause: err}
	}

	tokens, err := opts.RefreshToken(ctx, RefreshTokenParams{
		TokenEndpoint: oidc.TokenEndpoint,
		ClientID:      current.ClientID,
		RefreshToken:  current.RefreshToken,
	}, opts.Client)
	if err != nil {
		if isInvalidGrantOrToken(err) {
			paths := opts.Paths
			onDisk := Load(paths, nil)
			if onDisk == nil || onDisk.RefreshToken == current.RefreshToken {
				_ = Clear(paths)
			}
			return "", &SessionExpiredError{}
		}
		return "", &TokenRefreshError{message: fmt.Sprintf("token refresh failed: %s", err.Error()), cause: err}
	}

	if tokens.RefreshToken == "" {
		return "", &TokenRefreshError{message: "the auth server's refresh response carried no rotated refresh_token"}
	}

	savedAt := opts.Clock()
	next := current
	next.AccessToken = tokens.AccessToken
	next.RefreshToken = tokens.RefreshToken
	next.ExpiresAt = savedAt + tokens.ExpiresIn*1000
	if tokens.RefreshExpiresIn != nil {
		v := savedAt + *tokens.RefreshExpiresIn*1000
		next.RefreshExpiresAt = &v
	} else {
		next.RefreshExpiresAt = nil
	}
	next.TokenType = tokens.TokenType
	if tokens.Scope != nil {
		next.Scope = *tokens.Scope
	}
	next.SavedAt = savedAt

	if err := Save(next, opts.Paths); err != nil {
		return "", &TokenRefreshError{message: fmt.Sprintf("could not persist refreshed credentials: %s", err.Error()), cause: err}
	}
	return next.AccessToken, nil
}

// ForceRefresh acquires the lock, re-reads, refreshes regardless of
// staleness (unless another process already rotated past expectedAccessToken),
// persists, and returns the new access token.
func ForceRefresh(ctx context.Context, opts TokenProviderOptions, expectedAccessToken string) (string, error) {
	opts = opts.resolve()

	if Load(opts.Paths, opts.Warn) == nil {
		return "", &NotLoggedInError{}
	}

	return withTokenLock(ctx, opts.Paths.File, opts.Lock, func() (string, error) {
		current := Load(opts.Paths, opts.Warn)
		if current == nil {
			return "", &NotLoggedInError{}
		}
		if expectedAccessToken != "" && current.AccessToken != expectedAccessToken {
			return current.AccessToken, nil
		}
		return refreshAndPersist(ctx, opts, *current)
	})
}

// flight is one in-flight Token() call shared by concurrent callers — a
// fresh instance per call, so a later flight can never overwrite an earlier
// one's result out from under a waiter that already grabbed a reference to it.
type flight struct {
	done chan struct{}
	res  string
	err  error
}

// TokenProvider is the func handed to the SDK, widened with MarkAccepted
// and LastAction. Concurrent in-process calls share one in-flight
// operation (single-flight); the cross-process case is lock.go's job.
type TokenProvider struct {
	opts TokenProviderOptions

	mu       sync.Mutex
	inFlight *flight

	lastReturned string
	hasReturned  bool
	accepted     bool
	lastAction   string // "refreshed" | "unchanged"
}

// NewTokenProvider builds the provider.
func NewTokenProvider(opts TokenProviderOptions) *TokenProvider {
	return &TokenProvider{opts: opts.resolve(), lastAction: "unchanged"}
}

// LastAction reports whether the most recently completed call actually
// refreshed the token, or found it already usable. Display-only.
func (p *TokenProvider) LastAction() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastAction
}

// MarkAccepted confirms the token most recently returned was accepted —
// clears the "force refresh next call" flag set by returning it.
func (p *TokenProvider) MarkAccepted() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.accepted = true
}

func (p *TokenProvider) obtainFresh(ctx context.Context) (string, error) {
	opts := p.opts
	creds := Load(opts.Paths, opts.Warn)
	if creds == nil {
		return "", &NotLoggedInError{}
	}

	if !IsAccessTokenStale(creds, opts.BufferMs, opts.Clock()) {
		p.mu.Lock()
		p.lastAction = "unchanged"
		p.mu.Unlock()
		return creds.AccessToken, nil
	}

	token, err := withTokenLock(ctx, opts.Paths.File, opts.Lock, func() (string, error) {
		reloaded := Load(opts.Paths, nil)
		if reloaded == nil {
			return "", &NotLoggedInError{}
		}
		if !IsAccessTokenStale(reloaded, opts.BufferMs, opts.Clock()) {
			p.mu.Lock()
			p.lastAction = "unchanged"
			p.mu.Unlock()
			return reloaded.AccessToken, nil
		}
		refreshed, err := refreshAndPersist(ctx, opts, *reloaded)
		if err != nil {
			return "", err
		}
		p.mu.Lock()
		p.lastAction = "refreshed"
		p.mu.Unlock()
		return refreshed, nil
	})
	if err != nil {
		// Proactive refresh only: a failed *attempt* to renew a token that
		// hadn't actually expired yet shouldn't take the caller down — hand
		// back the still-good token and let the next call try again.
		if tre, ok := err.(*TokenRefreshError); ok {
			stillValid := Load(opts.Paths, opts.Warn)
			if stillValid != nil && !IsAccessTokenStale(stillValid, 0, opts.Clock()) {
				if opts.Warn != nil {
					opts.Warn(fmt.Sprintf("proactive token refresh failed (%s); returning the still-valid access token", tre.Error()))
				}
				p.mu.Lock()
				p.lastAction = "unchanged"
				p.mu.Unlock()
				return stillValid.AccessToken, nil
			}
		}
		return "", err
	}
	return token, nil
}

func (p *TokenProvider) obtainForced(ctx context.Context, expected string) (string, error) {
	token, err := ForceRefresh(ctx, p.opts, expected)
	if err != nil {
		return "", err
	}
	p.mu.Lock()
	p.lastAction = "refreshed"
	p.mu.Unlock()
	return token, nil
}

// testHookAfterInFlightCleared, when non-nil, runs after a completed
// flight's result is published (p.inFlight cleared, p.mu released) but
// before its done channel is closed — the race window a third Token() call
// can land in, pinned by
// TestTokenProviderThirdCallDuringPublishWindowStartsItsOwnFlight (B2). Nil
// in production; tests must restore it.
var testHookAfterInFlightCleared func()

// Token returns the current access token, refreshing if necessary. Safe
// for concurrent use: concurrent in-process calls share one in-flight
// operation.
func (p *TokenProvider) Token(ctx context.Context) (string, error) {
	p.mu.Lock()
	if p.inFlight != nil {
		f := p.inFlight
		p.mu.Unlock()
		// A waiter joining an in-flight call rides out that call's own ctx,
		// not its own — this ctx is never consulted again below. Same
		// tradeoff Node's provider makes: a single shared refresh can't
		// respect N different cancellations, so the flight owner's ctx wins.
		<-f.done
		return f.res, f.err
	}

	forceExpected := ""
	if p.hasReturned && !p.accepted {
		forceExpected = p.lastReturned
	}
	f := &flight{done: make(chan struct{})}
	p.inFlight = f
	p.mu.Unlock()

	var token string
	var err error
	if forceExpected != "" {
		token, err = p.obtainForced(ctx, forceExpected)
	} else {
		token, err = p.obtainFresh(ctx)
	}

	p.mu.Lock()
	p.inFlight = nil
	if err == nil {
		p.lastReturned = token
		p.hasReturned = true
		p.accepted = false
	}
	p.mu.Unlock()

	if testHookAfterInFlightCleared != nil {
		testHookAfterInFlightCleared()
	}

	f.res, f.err = token, err
	close(f.done)

	return token, err
}
