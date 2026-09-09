package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// LockTimeoutError is returned when acquiring the cross-process refresh
// lock exceeds its timeout.
type LockTimeoutError struct {
	message string
}

func (e *LockTimeoutError) Error() string { return e.message }

type lockPayload struct {
	PID       int    `json:"pid"`
	CreatedAt int64  `json:"createdAt"`
	Token     string `json:"token"`
}

// LockOptions tunes acquireLock's retry/stale/timeout behaviour and its DI
// seams — mirrors Node's lock.ts LockOptions.
type LockOptions struct {
	// StaleMs: a lock file older than this is assumed abandoned. Default 60s.
	StaleMs int64
	// RetryMs: delay between acquire attempts. Default 100ms.
	RetryMs int64
	// TimeoutMs: total time to keep retrying before giving up. Default 45s.
	TimeoutMs int64
	Now       func() int64
	Sleep     func(context.Context, time.Duration) error
}

const (
	defaultStaleMs   = 60_000
	defaultRetryMs   = 100
	defaultTimeoutMs = 45_000
)

func nowMs() int64 { return time.Now().UnixMilli() }

func (o LockOptions) resolve() LockOptions {
	if o.StaleMs == 0 {
		o.StaleMs = defaultStaleMs
	}
	if o.RetryMs == 0 {
		o.RetryMs = defaultRetryMs
	}
	if o.TimeoutMs == 0 {
		o.TimeoutMs = defaultTimeoutMs
	}
	if o.Now == nil {
		o.Now = nowMs
	}
	if o.Sleep == nil {
		o.Sleep = defaultSleep
	}
	return o
}

func lockPathFor(path string) string { return path + ".lock" }

func randomToken() string {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}

// lockPayloadShadow decodes the same JSON as lockPayload but with pid and
// createdAt as pointers, so a field that's genuinely absent (nil) can be
// told apart from one present with a zero value — a lock file missing
// createdAt must never be reaped as "very stale" (age computed against a
// zero timestamp) nor as "not stale" by accident; it's simply not a payload
// this process can judge the age of.
type lockPayloadShadow struct {
	PID       *int    `json:"pid"`
	CreatedAt *int64  `json:"createdAt"`
	Token     *string `json:"token"`
}

func readLockPayload(lockPath string) *lockPayload {
	raw, err := os.ReadFile(lockPath)
	if err != nil {
		return nil
	}
	var shadow lockPayloadShadow
	if err := json.Unmarshal(raw, &shadow); err != nil {
		return nil
	}
	if shadow.PID == nil || shadow.CreatedAt == nil {
		return nil
	}
	p := lockPayload{PID: *shadow.PID, CreatedAt: *shadow.CreatedAt}
	if shadow.Token != nil {
		p.Token = *shadow.Token
	}
	return &p
}

// tryCreate attempts one O_EXCL create; returns the new lock's token on
// success, "" if the lock file already exists.
func tryCreate(lockPath string, now int64) (string, error) {
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if os.IsExist(err) {
			return "", nil
		}
		return "", err
	}
	token := randomToken()
	payload := lockPayload{PID: os.Getpid(), CreatedAt: now, Token: token}
	raw, _ := json.Marshal(payload)
	_, werr := f.Write(raw)
	cerr := f.Close()
	if werr != nil {
		return "", werr
	}
	if cerr != nil {
		return "", cerr
	}
	return token, nil
}

// reapIfStale takes over lockPath if its payload is older than staleMs, via
// an atomic rename to a scratch path followed by unlink — so when two
// reapers race the same stale file, only the one whose rename succeeds
// proceeds to remove it.
func reapIfStale(lockPath string, staleMs int64, now int64) {
	payload := readLockPayload(lockPath)
	if payload == nil {
		return
	}
	if now-payload.CreatedAt <= staleMs {
		return
	}

	stalePath := fmt.Sprintf("%s.stale.%d.%d", lockPath, os.Getpid(), now)
	if err := os.Rename(lockPath, stalePath); err != nil {
		return // another reaper (or the original holder) already moved/removed it
	}
	_ = os.Remove(stalePath)
}

func releaseLock(lockPath, token string) {
	payload := readLockPayload(lockPath)
	if payload == nil || payload.Token != token {
		return
	}
	_ = os.Remove(lockPath)
}

// AcquireLock acquires the exclusive lock at <path>.lock, retrying every
// RetryMs until TimeoutMs elapses. A lock older than StaleMs is treated as
// abandoned and removed before the next attempt. Returns a release
// function; returns *LockTimeoutError naming the holder's pid on timeout.
func AcquireLock(ctx context.Context, path string, opts LockOptions) (func(), error) {
	opts = opts.resolve()
	lockPath := lockPathFor(path)
	deadline := opts.Now() + opts.TimeoutMs

	for {
		token, err := tryCreate(lockPath, opts.Now())
		if err != nil {
			return nil, err
		}
		if token != "" {
			return func() { releaseLock(lockPath, token) }, nil
		}

		reapIfStale(lockPath, opts.StaleMs, opts.Now())

		if opts.Now() >= deadline {
			holder := readLockPayload(lockPath)
			who := "another process"
			if holder != nil {
				who = fmt.Sprintf("pid %d", holder.PID)
			}
			return nil, &LockTimeoutError{message: fmt.Sprintf(
				"timed out waiting for the lock at %s (held by %s); if that process is gone, delete %s manually and retry",
				lockPath, who, lockPath,
			)}
		}

		if err := opts.Sleep(ctx, time.Duration(opts.RetryMs)*time.Millisecond); err != nil {
			return nil, err
		}
	}
}

// WithLock runs fn under the exclusive lock at <path>.lock. release is
// deferred right after acquiring, so it always runs before WithLock
// returns — whether fn returns an error or panics — not just on the
// success path.
func WithLock[T any](ctx context.Context, path string, opts LockOptions, fn func() (T, error)) (T, error) {
	var zero T
	release, err := AcquireLock(ctx, path, opts)
	if err != nil {
		return zero, err
	}
	defer release()
	return fn()
}
