package auth

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// TestHelperHoldLock is not a real test: it's re-exec'd as a subprocess (the
// Go re-exec trick, since there's no separate helper binary) via
// GO_WANT_LOCK_HOLDER, holds the lock at the target path (its last
// os.Args entry) for the given milliseconds (the entry before that), prints
// "locked" once acquired, and optionally exits without releasing
// (GO_LOCK_HOLDER_NO_RELEASE=1) to simulate a crash.
func TestHelperHoldLock(t *testing.T) {
	if os.Getenv("GO_WANT_LOCK_HOLDER") != "1" {
		t.Skip("only runs as a re-exec'd helper process")
	}
	target := os.Args[len(os.Args)-2]
	holdMs, _ := strconv.Atoi(os.Args[len(os.Args)-1])

	release, err := AcquireLock(context.Background(), target, LockOptions{})
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
	fmt.Println("locked")
	time.Sleep(time.Duration(holdMs) * time.Millisecond)
	if os.Getenv("GO_LOCK_HOLDER_NO_RELEASE") == "1" {
		os.Exit(0)
	}
	release()
}

func spawnLockHolder(t *testing.T, target string, holdMs int, noRelease bool) *exec.Cmd {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=TestHelperHoldLock", "-test.v", target, strconv.Itoa(holdMs))
	cmd.Env = append(os.Environ(), "GO_WANT_LOCK_HOLDER=1")
	if noRelease {
		cmd.Env = append(cmd.Env, "GO_LOCK_HOLDER_NO_RELEASE=1")
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// Guarantees the helper never outlives the (sub)test, including on a
	// t.Fatal from the "never reported locked" branch below.
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	locked := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if scanner.Text() == "locked" {
				close(locked)
				return
			}
		}
	}()

	select {
	case <-locked:
	case <-time.After(5 * time.Second):
		t.Fatal("helper never reported locked")
	}
	return cmd
}

func TestAcquireLockCrossProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a subprocess")
	}

	t.Run("times out while another process holds the lock", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(dir, "credentials.json")
		spawnLockHolder(t, target, 2000, false)

		_, err := AcquireLock(context.Background(), target, LockOptions{RetryMs: 50, TimeoutMs: 300})
		if _, ok := err.(*LockTimeoutError); !ok {
			t.Fatalf("got %#v", err)
		}
	})

	t.Run("acquires once the other process releases", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(dir, "credentials.json")
		spawnLockHolder(t, target, 300, false)

		release, err := AcquireLock(context.Background(), target, LockOptions{RetryMs: 20, TimeoutMs: 5000})
		if err != nil {
			t.Fatal(err)
		}
		release()
	})

	t.Run("recovers once an abandoned lock goes stale", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(dir, "credentials.json")
		cmd := spawnLockHolder(t, target, 200, true)
		_ = cmd.Wait()

		if _, err := os.Stat(target + ".lock"); err != nil {
			t.Fatal("lock file should be left behind by the crashed holder")
		}

		release, err := AcquireLock(context.Background(), target, LockOptions{StaleMs: 50, RetryMs: 20, TimeoutMs: 5000})
		if err != nil {
			t.Fatal(err)
		}
		release()
	})
}
