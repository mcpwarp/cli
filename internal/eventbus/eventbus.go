// Package eventbus defines the typed events the supervisor/bridge emit and
// the two-channel bus that carries them to a consumer (TUI or plain
// renderer), per DESIGN.md §3. Producers (bridge/supervisor) never import
// a consumer package — only this one.
package eventbus

import (
	"sync"
	"time"
)

// ServerStateChanged mirrors supervisor.ts's state transitions
// (healthy/restarting/failed/disabled/stopped) plus the restart count.
type ServerStateChanged struct {
	Name     string
	State    string
	Restarts int
}

// LogLine is a piece of telemetry (e.g. a child's stderr line), drop-oldest
// under backpressure like the rest of the telemetry channel (DESIGN.md §3).
type LogLine struct {
	Server string
	Level  string
	Text   string
}

// MetricSample is one point of tunnel/relay telemetry (bytes/latency
// counters etc.) — carried on Metrics, drop-oldest like Telemetry rather
// than the lossless Control channel.
type MetricSample struct {
	Kind     string
	Session  string
	StreamID uint32
	Value    float64
	At       time.Time
}

// ConnStateChanged, StreamOpened, StreamClosed and AppError are the
// canonical M3B/M4 control-channel event shapes (DESIGN.md §3/§8), published
// directly by internal/tunnel.
type ConnStateChanged struct {
	State   string
	Session string
	Reason  string
}

type StreamOpened struct {
	ID uint32
}

type StreamClosed struct {
	ID  uint32
	Err string
}

type AppError struct {
	Code    string
	Message string
	Service string
}

// Control events are lossless (backpressure the producer); Telemetry and
// Metrics events are drop-oldest, matching the OVERLOADED queue policy
// (DESIGN.md §3, §8).

// Bus is the two-channel shape DESIGN.md §3 calls for (a lossless control
// channel and a drop-oldest telemetry channel), plus a third drop-oldest
// channel for numeric metric samples.
type Bus struct {
	Control   chan any
	Telemetry chan LogLine
	Metrics   chan MetricSample

	// closeMu guards against Close() closing the channels while a
	// Publish/PublishTelemetry send is in flight (which would panic with
	// "send on closed channel"): publishers hold the read lock for the
	// duration of their send, Close takes the write lock.
	closeMu sync.RWMutex
	closed  bool
	// done is closed by Close() before it takes closeMu's write lock, so a
	// Publish already blocked mid-send (Control full, no consumer) has
	// somewhere to bail out to — otherwise it would hold the read lock
	// forever and Close's write lock would never be granted.
	done     chan struct{}
	closeOne sync.Once
}

// TelemetryCap bounds the telemetry channel; once full, sends drop the
// oldest queued item to make room, mirroring the OVERLOADED queue.
const TelemetryCap = 64

// New creates a Bus with a lossless control channel (capacity controlCap;
// 0 makes it unbuffered, in which case Publish blocks until a consumer
// reads) and a bounded drop-oldest telemetry channel.
func New(controlCap int) *Bus {
	return &Bus{
		Control:   make(chan any, controlCap),
		Telemetry: make(chan LogLine, TelemetryCap),
		Metrics:   make(chan MetricSample, TelemetryCap),
		done:      make(chan struct{}),
	}
}

// Publish sends a control event, blocking the caller if the channel is
// full — control events must never be dropped — but bailing out via done
// if Close() is trying to shut the bus down while the send is stuck.
func (b *Bus) Publish(evt any) {
	b.closeMu.RLock()
	defer b.closeMu.RUnlock()
	if b.closed {
		return
	}
	select {
	case b.Control <- evt:
	case <-b.done:
	}
}

// PublishTelemetry sends a telemetry event, dropping the oldest queued
// telemetry event if the channel is full rather than blocking the
// producer.
func (b *Bus) PublishTelemetry(evt LogLine) {
	b.closeMu.RLock()
	defer b.closeMu.RUnlock()
	if b.closed {
		return
	}
	for {
		select {
		case b.Telemetry <- evt:
			return
		default:
		}
		select {
		case <-b.Telemetry:
		default:
			// Someone drained it concurrently; loop and retry the send.
		}
	}
}

// PublishMetric sends a metric sample, dropping the oldest queued sample if
// the channel is full rather than blocking the producer — same drop-oldest
// policy as PublishTelemetry.
func (b *Bus) PublishMetric(evt MetricSample) {
	b.closeMu.RLock()
	defer b.closeMu.RUnlock()
	if b.closed {
		return
	}
	for {
		select {
		case b.Metrics <- evt:
			return
		default:
		}
		select {
		case <-b.Metrics:
		default:
			// Someone drained it concurrently; loop and retry the send.
		}
	}
}

// Close closes all three channels — safe to call concurrently with
// Publish/PublishTelemetry (unlike a bare close(chan), which would panic
// if a send raced it): closed is set, and the channels only actually
// close, once no publisher is still mid-send.
func (b *Bus) Close() {
	// Unblock any Publish already parked mid-send before taking the write
	// lock — otherwise Close would wait forever for a read lock that a
	// blocked send never releases.
	b.closeOne.Do(func() { close(b.done) })

	b.closeMu.Lock()
	defer b.closeMu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	close(b.Control)
	close(b.Telemetry)
	close(b.Metrics)
}
