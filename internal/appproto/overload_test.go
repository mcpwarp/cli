package appproto

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOverloadQueueSendsImmediatelyWhenNotPaused(t *testing.T) {
	q := NewOverloadQueue()
	var ran int32
	err := q.Send(func() error {
		atomic.AddInt32(&ran, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ran != 1 {
		t.Fatalf("expected fn to run immediately")
	}
}

func TestOverloadQueueTriggerPausesAndDrainsInOrder(t *testing.T) {
	q := NewOverloadQueue()
	released := make(chan struct{})
	q.sleep = func(time.Duration) { <-released }

	q.Trigger()
	waitQueueLen(t, q, 0) // Trigger's goroutine has taken the lock at least once

	var mu sync.Mutex
	var order []int
	var wg sync.WaitGroup
	// Launched one at a time, each waited into the queue before the next
	// starts, so insertion order is deterministic — Send itself only
	// guarantees FIFO among calls that are actually queued, not among
	// goroutines racing to be the one that queues first.
	for i := 0; i < 3; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := q.Send(func() error {
				mu.Lock()
				order = append(order, i)
				mu.Unlock()
				return nil
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
		waitQueueLen(t, q, i+1)
	}

	close(released)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 3 {
		t.Fatalf("expected all 3 queued sends to run, got %v", order)
	}
	for i, v := range order {
		if v != i {
			t.Fatalf("expected FIFO order, got %v", order)
		}
	}
}

func TestOverloadQueueDropsOldestPastCap(t *testing.T) {
	q := NewOverloadQueue()
	released := make(chan struct{})
	q.sleep = func(time.Duration) { <-released }
	q.Trigger()
	waitQueueLen(t, q, 0)

	// Launched one at a time, each waited into the queue (or, past cap,
	// waited for the queue to stay at cap) before the next starts, so
	// insertion order — and therefore which one is "oldest" — is
	// deterministic.
	results := make([]chan error, MaxOverloadQueue+1)
	for i := range results {
		results[i] = make(chan error, 1)
		i := i
		go func() {
			results[i] <- q.Send(func() error { return nil })
		}()
		waitQueueLen(t, q, i+1)
	}

	close(released)

	if err := <-results[0]; !errors.Is(err, ErrOverloadQueueFull) {
		t.Fatalf("expected the oldest queued send to be dropped, got %v", err)
	}
	for i := 1; i < len(results); i++ {
		if err := <-results[i]; err != nil {
			t.Fatalf("send %d: unexpected error: %v", i, err)
		}
	}
}

// waitQueueLen polls q's cumulative enqueued counter (total calls ever
// appended to queue, not len(q.queue) — the latter shrinks as drop-oldest
// evicts past MaxOverloadQueue, so it can't be waited on for a specific
// value past the cap) until it reaches want, giving concurrent Send calls a
// bounded window to actually enqueue before the test proceeds.
func waitQueueLen(t *testing.T, q *OverloadQueue, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		q.mu.Lock()
		n := q.enqueued
		q.mu.Unlock()
		if n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("enqueued count never reached %d, stuck at %d", want, n)
		}
		time.Sleep(time.Millisecond)
	}
}
