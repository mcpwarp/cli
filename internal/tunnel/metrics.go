package tunnel

import (
	"fmt"
	"time"

	"github.com/mcpwarp/cli/internal/eventbus"
	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// busMetrics is a wsmixer.Metrics implementation that feeds the
// connection's ping latency and send-window stalls onto the eventbus (
// DESIGN.md §3: drop-oldest, same policy as the OVERLOADED queue — a slow
// TUI must never backpressure the connection's read/write loops, which is
// exactly what calls these methods synchronously).
//
// BytesTransferred fires once per DATA frame — on a busy stream that's far
// too hot a path to afford formatting and publishing a value per call, so
// it's a no-op here; the TUI derives its bytes-transferred figure by
// polling Client.Stats() instead (internal/cli's pollTunnelForTUI).
type busMetrics struct {
	bus *eventbus.Bus
}

func newBusMetrics(bus *eventbus.Bus) *busMetrics { return &busMetrics{bus: bus} }

func (m *busMetrics) emit(text string) {
	if m.bus == nil {
		return
	}
	m.bus.PublishTelemetry(eventbus.LogLine{Server: "tunnel", Level: "metric", Text: text})
}

func (m *busMetrics) PingRTT(session string, rtt time.Duration) {
	if m.bus == nil {
		return
	}
	m.bus.PublishMetric(eventbus.MetricSample{
		Kind:    "ping_rtt",
		Session: session,
		Value:   float64(rtt.Milliseconds()),
		At:      time.Now(),
	})
}

// BytesTransferred is a no-op: fires once per DATA frame, too hot a path to
// afford formatting/publishing a string per call. See the type doc comment.
func (m *busMetrics) BytesTransferred(session string, direction string, n int64) {}

func (m *busMetrics) SendWindowBlocked(session string, streamID uint32, d time.Duration) {
	m.emit(fmt.Sprintf("send_window_blocked session=%s stream=%d wait_ms=%d", session, streamID, d.Milliseconds()))
}

// Every other wsmixer.Metrics method is a no-op: DESIGN.md §3 calls out
// only PingRTT/BytesTransferred/SendWindowBlocked as feeding telemetry for
// this milestone.
func (m *busMetrics) ConnectionOpened(session, role string)                                {}
func (m *busMetrics) ConnectionClosed(session string, closeCode int, errorCode string)     {}
func (m *busMetrics) HandshakeFailed(stage string)                                         {}
func (m *busMetrics) StreamOpened(session string, streamID uint32)                         {}
func (m *busMetrics) StreamClosed(session string, streamID uint32, duration time.Duration) {}
func (m *busMetrics) StreamReset(session string, streamID uint32, code wsmixer.ErrorCode)  {}
func (m *busMetrics) KeepaliveTimeout(session string)                                      {}
func (m *busMetrics) DrainStarted(session string, reason string)                           {}
func (m *busMetrics) DrainCompleted(session string, reason string)                         {}
func (m *busMetrics) DrainReceived(session string, reason string)                          {}
func (m *busMetrics) ProtocolViolation(session string, code string)                        {}
func (m *busMetrics) StaleFrameDiscarded(session string, streamID uint32)                  {}
func (m *busMetrics) UnknownFrameType(session string, frameType uint8)                     {}
func (m *busMetrics) AppMessage(session string, direction string)                          {}
func (m *busMetrics) ControlMessage(session string, msgType string, direction string)      {}
func (m *busMetrics) WindowUpdate(session string, streamID uint32, increment uint32)       {}
func (m *busMetrics) DuplicatePong(session string, id int64)                               {}
func (m *busMetrics) StreamCancelledOnDrain(session string, streamID uint32)               {}
func (m *busMetrics) RecvWindowSample(session string, streamID uint32, remaining int64)    {}

var _ wsmixer.Metrics = (*busMetrics)(nil)
