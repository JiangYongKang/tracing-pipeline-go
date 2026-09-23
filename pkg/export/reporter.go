package export

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/highcumontoa/tracing-pipeline-go/pkg/model"
)

// FailureClass distinguishes reporter failure categories.
type FailureClass string

const (
	FailureNone        FailureClass = ""
	FailureTimeout     FailureClass = "timeout"
	FailureUnavailable FailureClass = "unavailable"
	FailureRejected    FailureClass = "rejected"
)

// ReportError carries the classified failure category so the exporter and
// test logs can explain every failed attempt distinctly.
type ReportError struct {
	Class FailureClass
	Msg   string
}

func (e *ReportError) Error() string { return string(e.Class) + ": " + e.Msg }

// AsClass extracts the FailureClass from a reporter error, or FailureNone.
func AsClass(err error) FailureClass {
	var re *ReportError
	if errors.As(err, &re) {
		return re.Class
	}
	return FailureNone
}

// SpanBatch is the unit of delivery: all spans of one trace travel together,
// which makes retries atomic and guarantees no duplicate partial reporting.
type SpanBatch struct {
	TraceID    model.TraceID
	Spans      []model.Span
	Attempt    int
	EnqueuedAt time.Time
}

// Reporter is the downstream reporting endpoint.
type Reporter interface {
	Report(ctx context.Context, b SpanBatch) error
}

// MockReportRule is a predicate used by the mock to decide per-batch outcomes.
type MockReportRule func(b SpanBatch) FailureClass

// MockReporter is a deterministic local stand-in for a remote collector.
//
// Behavior is configured through rules and switches so tests can exercise
// slowness, timeouts, unavailability and rejections deterministically. Every
// accepted delivery is recorded exactly once (successful reports only),
// enabling exact-set verification of no-loss/no-duplication.
type MockReporter struct {
	mu sync.Mutex

	latency   time.Duration
	available bool
	rule      MockReportRule

	// recv records the number of spans accepted per trace id; a trace id
	// appearing here means exactly one whole-batch success.
	recv       map[model.TraceID]int
	recvOrder  []model.TraceID
	totalOK    int
	totalCalls int
}

// MockReporterConfig configures a MockReporter.
type MockReporterConfig struct {
	Latency   time.Duration
	Available bool
	Rule      MockReportRule
}

// NewMockReporter constructs a mock reporter.
func NewMockReporter(cfg MockReporterConfig) *MockReporter {
	return &MockReporter{
		latency:   cfg.Latency,
		available: cfg.Available,
		rule:      cfg.Rule,
		recv:      make(map[model.TraceID]int),
	}
}

// Report applies latency and the configured outcome rule.
//
// Order of checks is deterministic: context cancellation first, then
// availability (FailureUnavailable), then the user rule (timeout/rejected),
// and only an all-clear result performs the sleep and records delivery.
func (m *MockReporter) Report(ctx context.Context, b SpanBatch) error {
	if err := ctx.Err(); err != nil {
		return &ReportError{Class: FailureTimeout, Msg: "context already done: " + err.Error()}
	}

	m.mu.Lock()
	m.totalCalls++
	latency := m.latency
	available := m.available
	rule := m.rule
	m.mu.Unlock()

	if !available {
		return &ReportError{Class: FailureUnavailable, Msg: "reporter is switched offline"}
	}
	if rule != nil {
		if class := rule(b); class != FailureNone {
			return &ReportError{Class: class, Msg: "forced by mock rule"}
		}
	}

	// Simulate work, but remain interruptible so shutdown/timeout are prompt.
	if latency > 0 {
		t := time.NewTimer(latency)
		select {
		case <-ctx.Done():
			t.Stop()
			return &ReportError{Class: FailureTimeout, Msg: "delivery interrupted: " + ctx.Err().Error()}
		case <-t.C:
		}
	}

	m.mu.Lock()
	if _, ok := m.recv[b.TraceID]; !ok {
		m.recvOrder = append(m.recvOrder, b.TraceID)
	}
	m.recv[b.TraceID] += len(b.Spans)
	m.totalOK++
	m.mu.Unlock()
	return nil
}

// SetAvailable flips endpoint availability at runtime.
func (m *MockReporter) SetAvailable(ok bool) {
	m.mu.Lock()
	m.available = ok
	m.mu.Unlock()
}

// SetLatency changes the simulated per-delivery latency at runtime.
func (m *MockReporter) SetLatency(d time.Duration) {
	m.mu.Lock()
	m.latency = d
	m.mu.Unlock()
}

// DeliveredTraceIDs returns trace ids accepted at least once, in first-delivery order.
func (m *MockReporter) DeliveredTraceIDs() []model.TraceID {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]model.TraceID, len(m.recvOrder))
	copy(out, m.recvOrder)
	return out
}

// DeliveredSpans returns the total number of spans ever accepted.
func (m *MockReporter) DeliveredSpans() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, c := range m.recv {
		n += c
	}
	return n
}

// DeliveredSpanCountFor reports spans accepted for one trace id (0 = none).
func (m *MockReporter) DeliveredSpanCountFor(id model.TraceID) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.recv[id]
}

// Calls returns the total number of delivery attempts observed.
func (m *MockReporter) Calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.totalCalls
}

// OKs returns the number of successful deliveries.
func (m *MockReporter) OKs() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.totalOK
}

// Reset clears delivery history.
func (m *MockReporter) Reset() {
	m.mu.Lock()
	m.recv = make(map[model.TraceID]int)
	m.recvOrder = nil
	m.totalOK = 0
	m.totalCalls = 0
	m.mu.Unlock()
}
