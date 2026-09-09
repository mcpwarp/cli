package appproto

import (
	"errors"
	"sync"
	"time"
)

// MaxOverloadQueue bounds OverloadQueue: past this many pending sends,
// the oldest is dropped (its Send rejected) to make room for the new one —
// ports Node client.ts's MAX_OVERLOAD_QUEUE.
const MaxOverloadQueue = 64

// OverloadPause is how long a "OVERLOADED" app error pauses sends for.
const OverloadPause = time.Second

// ErrOverloadQueueFull is returned to a queued Send's caller when it gets
// dropped to make room for a newer one past MaxOverloadQueue.
var ErrOverloadQueueFull = errors.New("appproto: overload queue full; dropping oldest queued send")

// OverloadQueue holds back app-channel sends while the tunnel is
// OVERLOADED (DESIGN.md §8): Trigger starts (if not already running) a 1s
// pause, during which Send queues its call instead of running it
// immediately. Queued sends run in order once the pause lifts; past
// MaxOverloadQueue pending, the oldest queued call is dropped (and its
// Send returns ErrOverloadQueueFull) to make room.
type OverloadQueue struct {
	mu       sync.Mutex
	paused   bool
	queue    []queuedSend
	enqueued int // total calls ever appended to queue, for test synchronization

	// sleep is a test seam for the 1s pause.
	sleep func(time.Duration)
}

type queuedSend struct {
	fn     func() error
	result chan<- error
}

// NewOverloadQueue builds an OverloadQueue using the real clock.
func NewOverloadQueue() *OverloadQueue {
	return &OverloadQueue{sleep: time.Sleep}
}

// Trigger starts a pause if one isn't already running. Call on receiving an
// OVERLOADED app error.
func (q *OverloadQueue) Trigger() {
	q.mu.Lock()
	if q.paused {
		q.mu.Unlock()
		return
	}
	q.paused = true
	q.mu.Unlock()

	go func() {
		q.sleep(OverloadPause)
		q.mu.Lock()
		q.paused = false
		drained := q.queue
		q.queue = nil
		q.mu.Unlock()
		for _, item := range drained {
			item.result <- item.fn()
		}
	}()
}

// Send runs fn immediately if no pause is in effect, else queues it to run
// once the pause lifts (blocking the caller either way). Past
// MaxOverloadQueue pending, the oldest queued call is dropped in its favor.
func (q *OverloadQueue) Send(fn func() error) error {
	q.mu.Lock()
	if !q.paused {
		q.mu.Unlock()
		return fn()
	}

	result := make(chan error, 1)
	if len(q.queue) >= MaxOverloadQueue {
		dropped := q.queue[0]
		q.queue = q.queue[1:]
		dropped.result <- ErrOverloadQueueFull
	}
	q.queue = append(q.queue, queuedSend{fn: fn, result: result})
	q.enqueued++
	q.mu.Unlock()

	return <-result
}
