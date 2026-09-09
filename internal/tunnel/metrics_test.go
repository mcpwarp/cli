package tunnel

import (
	"testing"
	"time"

	"github.com/mcpwarp/cli/internal/eventbus"
)

// TestBusMetricsPingRTTPublishesMetricSample would fail on the old
// behaviour, which published PingRTT as an eventbus.LogLine on Telemetry
// instead of an eventbus.MetricSample on Metrics.
func TestBusMetricsPingRTTPublishesMetricSample(t *testing.T) {
	bus := eventbus.New(1)
	m := newBusMetrics(bus)

	m.PingRTT("sess-1", 42*time.Millisecond)

	select {
	case sample := <-bus.Metrics:
		if sample.Kind != "ping_rtt" || sample.Session != "sess-1" || sample.Value != 42 {
			t.Fatalf("got %+v, want Kind=ping_rtt Session=sess-1 Value=42", sample)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the MetricSample")
	}

	select {
	case line := <-bus.Telemetry:
		t.Fatalf("expected no LogLine fallback, got %+v", line)
	default:
	}
}
