package tunnel

import "sync"

// dispatcher runs enqueued work on its own goroutine, one at a time, in
// order. wsmixer's OnConnect/OnApp/OnStream/OnDrain/OnDisconnect callbacks
// all fire on the SDK's own shared delivery goroutine and must never block
// (CLIENT-SDK.md); anything beyond a cheap decode — registry mutation,
// eventbus.Publish (control is lossless and can block until a consumer
// reads), calling a user callback — is enqueued here instead of run
// inline. The queue is a plain mutex-guarded slice, unbounded in practice
// (bounded only by how fast this goroutine can drain it), so enqueue can
// never itself block the caller.
type dispatcher struct {
	mu     sync.Mutex
	cond   *sync.Cond
	queue  []func()
	closed bool
	done   chan struct{}
}

func newDispatcher() *dispatcher {
	d := &dispatcher{done: make(chan struct{})}
	d.cond = sync.NewCond(&d.mu)
	go d.run()
	return d
}

func (d *dispatcher) enqueue(fn func()) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.queue = append(d.queue, fn)
	d.mu.Unlock()
	d.cond.Signal()
}

func (d *dispatcher) run() {
	defer close(d.done)
	for {
		d.mu.Lock()
		for len(d.queue) == 0 && !d.closed {
			d.cond.Wait()
		}
		if len(d.queue) == 0 {
			d.mu.Unlock()
			return
		}
		fn := d.queue[0]
		d.queue = d.queue[1:]
		d.mu.Unlock()
		fn()
	}
}

// close stops accepting new work and waits for the queue to drain (every
// already-enqueued fn still runs).
func (d *dispatcher) close() {
	d.mu.Lock()
	d.closed = true
	d.mu.Unlock()
	d.cond.Signal()
	<-d.done
}
