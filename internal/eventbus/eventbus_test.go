package eventbus

import (
	"sync"
	"testing"
	"time"
)

func TestBus_ControlIsLossless(t *testing.T) {
	b := New(2)
	b.Publish(ServerStateChanged{Name: "a", State: "healthy"})
	b.Publish(ServerStateChanged{Name: "b", State: "restarting"})
	if len(b.Control) != 2 {
		t.Fatalf("want 2 buffered control events, got %d", len(b.Control))
	}
	first := (<-b.Control).(ServerStateChanged)
	second := (<-b.Control).(ServerStateChanged)
	if first.Name != "a" || second.Name != "b" {
		t.Fatalf("control channel reordered events: %+v %+v", first, second)
	}
}

// TestBus_CloseDoesNotPanicUnderLiveProducers races Publish/PublishTelemetry
// against Close — a bare close(chan) would panic ("send on closed
// channel") if a send lands after the close. Run with -race.
func TestBus_CloseDoesNotPanicUnderLiveProducers(t *testing.T) {
	for i := 0; i < 200; i++ {
		b := New(1)
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			b.Publish(ServerStateChanged{Name: "a"})
		}()
		go func() {
			defer wg.Done()
			b.PublishTelemetry(LogLine{Text: "x"})
		}()
		go func() {
			defer wg.Done()
			b.Close()
		}()
		wg.Wait()
	}
}

// TestBus_CloseReturnsPromptlyBehindABlockedPublish reproduces the Close()
// deadlock: a Publish blocked mid-send on a full Control channel with no
// consumer holds closeMu's read lock for as long as it's blocked, so
// Close()'s write lock must not simply wait that out. Close must return
// promptly, and a later Publish (post-close) must not block either.
func TestBus_CloseReturnsPromptlyBehindABlockedPublish(t *testing.T) {
	b := New(0) // unbuffered: the first Publish blocks with no consumer
	publishReturned := make(chan struct{})
	go func() {
		b.Publish(ServerStateChanged{Name: "stuck"})
		close(publishReturned)
	}()

	time.Sleep(50 * time.Millisecond) // let the goroutine above land inside Publish's blocking send

	closeReturned := make(chan struct{})
	go func() {
		b.Close()
		close(closeReturned)
	}()
	select {
	case <-closeReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("Close() deadlocked behind a blocked Publish")
	}

	select {
	case <-publishReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("the blocked Publish never returned once Close() ran")
	}

	laterReturned := make(chan struct{})
	go func() {
		b.Publish(ServerStateChanged{Name: "after-close"})
		close(laterReturned)
	}()
	select {
	case <-laterReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("a Publish after Close() must not block")
	}
}

func TestBus_MetricsDropsOldestWhenFull(t *testing.T) {
	b := New(0)
	for i := 0; i < TelemetryCap+5; i++ {
		b.PublishMetric(MetricSample{Kind: "latency", Value: float64(i)})
	}
	if len(b.Metrics) != TelemetryCap {
		t.Fatalf("want metrics channel full at cap %d, got %d", TelemetryCap, len(b.Metrics))
	}
}

func TestBus_TelemetryDropsOldestWhenFull(t *testing.T) {
	b := New(0)
	for i := 0; i < TelemetryCap+5; i++ {
		b.PublishTelemetry(LogLine{Text: string(rune('a' + i%26))})
	}
	if len(b.Telemetry) != TelemetryCap {
		t.Fatalf("want telemetry channel full at cap %d, got %d", TelemetryCap, len(b.Telemetry))
	}
}
