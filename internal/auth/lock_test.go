package auth

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func lockFakeClock(startMs int64) (now func() int64, sleep func(context.Context, time.Duration) error) {
	t := startMs
	now = func() int64 { return atomic.LoadInt64(&t) }
	sleep = func(_ context.Context, d time.Duration) error {
		atomic.AddInt64(&t, d.Milliseconds())
		return nil
	}
	return
}

func TestAcquireLockUncontended(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "credentials.json")

	release, err := AcquireLock(context.Background(), target, LockOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(target + ".lock"); err != nil {
		t.Fatal("lock file should exist")
	}
	release()
	if _, err := os.Stat(target + ".lock"); !os.IsNotExist(err) {
		t.Fatal("lock file should be gone")
	}
}

func TestReleaseIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "credentials.json")
	release, err := AcquireLock(context.Background(), target, LockOptions{})
	if err != nil {
		t.Fatal(err)
	}
	release()
	release() // must not panic
}

func TestWithLockReleasesOnError(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "credentials.json")

	_, err := WithLock(context.Background(), target, LockOptions{}, func() (int, error) {
		return 0, os.ErrClosed
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if _, statErr := os.Stat(target + ".lock"); !os.IsNotExist(statErr) {
		t.Fatal("lock should be released even on error")
	}
}

func TestWithLockReturnsValue(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "credentials.json")
	v, err := WithLock(context.Background(), target, LockOptions{}, func() (int, error) { return 42, nil })
	if err != nil || v != 42 {
		t.Fatalf("got %d, %v", v, err)
	}
}

// TestAcquireLockContentionInProcess pins contention against a real lock
// file with no real clock or wall-clock wait anywhere: the second acquire's
// Sleep hook (its retry backoff) is the only thing standing between it and
// success, and on its Nth call it releases the first holder itself and
// advances the fake clock — so success is only possible after at least N
// retries, never sooner and never by outrunning a timer.
func TestAcquireLockContentionInProcess(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "credentials.json")

	var clockMs int64 = 1_000_000
	now := func() int64 { return atomic.LoadInt64(&clockMs) }

	release, err := AcquireLock(context.Background(), target, LockOptions{RetryMs: 5, Now: now})
	if err != nil {
		t.Fatal(err)
	}

	const wantRetries = 3
	var sleepCalls int32
	releaseOnceRetried := func(_ context.Context, d time.Duration) error {
		n := atomic.AddInt32(&sleepCalls, 1)
		atomic.AddInt64(&clockMs, d.Milliseconds())
		if n == wantRetries {
			release()
		}
		return nil
	}

	second, err := AcquireLock(context.Background(), target, LockOptions{
		RetryMs: 5, TimeoutMs: 10_000, Now: now, Sleep: releaseOnceRetried,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer second()

	if got := atomic.LoadInt32(&sleepCalls); got < wantRetries {
		t.Fatalf("expected the second acquire to have retried at least %d times, got %d", wantRetries, got)
	}
}

func TestAcquireLockTakesOverStaleLock(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "credentials.json")

	raw, _ := json.Marshal(lockPayload{PID: 999999, CreatedAt: 0})
	if err := os.WriteFile(target+".lock", raw, 0o600); err != nil {
		t.Fatal(err)
	}

	now, sleep := lockFakeClock(31_000)
	release, err := AcquireLock(context.Background(), target, LockOptions{
		StaleMs: 30_000, RetryMs: 1, TimeoutMs: 1_000, Now: now, Sleep: sleep,
	})
	if err != nil {
		t.Fatal(err)
	}
	release()
}

func TestAcquireLockTimesOut(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "credentials.json")
	now, sleep := lockFakeClock(1_000_000)
	raw, _ := json.Marshal(lockPayload{PID: 424242, CreatedAt: now()})
	if err := os.WriteFile(target+".lock", raw, 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := AcquireLock(context.Background(), target, LockOptions{
		StaleMs: 60_000, RetryMs: 5, TimeoutMs: 50, Now: now, Sleep: sleep,
	})
	lte, ok := err.(*LockTimeoutError)
	if !ok {
		t.Fatalf("got %#v", err)
	}
	if !contains(lte.Error(), "424242") {
		t.Fatalf("expected pid in message, got %s", lte.Error())
	}
}

func TestReleaseNoOpWhenTokenReplaced(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "credentials.json")

	release, err := AcquireLock(context.Background(), target, LockOptions{})
	if err != nil {
		t.Fatal(err)
	}

	raw, _ := json.Marshal(lockPayload{PID: 424242, CreatedAt: time.Now().UnixMilli(), Token: "someone-elses"})
	if err := os.WriteFile(target+".lock", raw, 0o600); err != nil {
		t.Fatal(err)
	}

	release()
	if _, err := os.Stat(target + ".lock"); err != nil {
		t.Fatal("lock file should still exist — release() must not touch a takeover")
	}
}

func TestReadLockPayloadRequiresBothPidAndCreatedAt(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "credentials.json.lock")

	t.Run("missing createdAt is not a valid payload", func(t *testing.T) {
		if err := os.WriteFile(lockPath, []byte(`{"pid":123}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if p := readLockPayload(lockPath); p != nil {
			t.Fatalf("expected nil, got %#v", p)
		}
	})

	t.Run("missing pid is not a valid payload", func(t *testing.T) {
		if err := os.WriteFile(lockPath, []byte(`{"createdAt":123}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if p := readLockPayload(lockPath); p != nil {
			t.Fatalf("expected nil, got %#v", p)
		}
	})

	t.Run("both present is a valid payload, even createdAt: 0", func(t *testing.T) {
		if err := os.WriteFile(lockPath, []byte(`{"pid":123,"createdAt":0}`), 0o600); err != nil {
			t.Fatal(err)
		}
		p := readLockPayload(lockPath)
		if p == nil || p.PID != 123 || p.CreatedAt != 0 {
			t.Fatalf("got %#v", p)
		}
	})
}

func TestLockWithoutCreatedAtIsNeverReaped(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "credentials.json")
	if err := os.WriteFile(target+".lock", []byte(`{"pid":999999}`), 0o600); err != nil {
		t.Fatal(err)
	}

	now, sleep := lockFakeClock(1_000_000)
	_, err := AcquireLock(context.Background(), target, LockOptions{
		StaleMs: 1, RetryMs: 5, TimeoutMs: 50, Now: now, Sleep: sleep,
	})
	if _, ok := err.(*LockTimeoutError); !ok {
		t.Fatalf("a payload missing createdAt must never be reaped as stale, got %#v", err)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (func() bool {
		for i := 0; i+len(substr) <= len(s); i++ {
			if s[i:i+len(substr)] == substr {
				return true
			}
		}
		return false
	})()
}
