// Package export provides a locally simulated reporting endpoint and a
// bounded exporter with backpressure, deterministic retry/dead-letter
// handling and externally queryable counters.
package export

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/highcumontoa/tracing-pipeline-go/internal/model"
)

// Reporter is the exporter's sink. Implementations must be safe for
// concurrent use.
type Reporter interface {
	Report(ctx context.Context, batch []*model.Span) ReportResult
}

// ReportResult is the deterministic outcome of one delivery attempt.
type ReportResult struct {
	OK      bool
	Failure model.FailureClass
	Message string
}

// Mode selects the mock endpoint's scripted behavior.
type Mode int

const (
	// ModeOK: every batch is accepted.
	ModeOK Mode = iota
	// ModeSlow: every attempt sleeps Behavior.Delay before accepting.
	ModeSlow
	// ModeFail: attempts fail with Behavior.Failure, optionally on a
	// deterministic cadence (FailModulo).
	ModeFail
)

// Behavior configures the mock endpoint. When FailModulo > 0 in ModeFail,
// every FailModulo-th request (1-based) fails, the others succeed, which
// makes failure/retry behavior reproducible.
type Behavior struct {
	Mode       Mode
	Delay      time.Duration
	Failure    model.FailureClass // used when Mode == ModeFail
	InFlight   int                // simulated in-flight request cap (<=0 = unbounded)
	FailModulo int                // when >0, fail every Nth batch deterministically
}

// acquireTimeout bounds how long an attempt waits for an in-flight slot.
const acquireTimeout = 5 * time.Millisecond

// MockReporter is a local, scripted reporting endpoint.
type MockReporter struct {
	mu       sync.RWMutex
	behavior Behavior
	sem      chan struct{}
	requests atomic.Int64
	accepted atomic.Int64
	failures sync.Map // model.FailureClass -> *atomic.Int64
}

// NewMockReporter builds a healthy mock reporter.
func NewMockReporter() *MockReporter {
	return &MockReporter{}
}

// SetBehavior atomically swaps the endpoint behavior, including the
// simulated in-flight cap (InFlight<=0 means unbounded).
func (r *MockReporter) SetBehavior(b Behavior) {
	r.mu.Lock()
	r.behavior = b
	if b.InFlight > 0 {
		r.sem = make(chan struct{}, b.InFlight)
	} else {
		r.sem = nil
	}
	r.mu.Unlock()
}

// Report implements Reporter.
func (r *MockReporter) Report(ctx context.Context, batch []*model.Span) ReportResult {
	b, sem := r.current()
	n := r.requests.Add(1)

	// In-flight cap: do not queue indefinitely. Failure to acquire a slot
	// promptly is a deterministic rejection, not a goroutine leak.
	if sem != nil {
		timer := time.NewTimer(acquireTimeout)
		select {
		case sem <- struct{}{}:
			timer.Stop()
			defer func() { <-sem }()
		case <-timer.C:
			return r.record(ReportResult{Failure: model.FailEndpointRejected,
				Message: "in-flight capacity saturated"})
		case <-ctx.Done():
			timer.Stop()
			return r.record(ReportResult{Failure: model.FailEndpointTimeout,
				Message: "aborted while waiting for capacity: " + ctx.Err().Error()})
		}
	}

	switch b.Mode {
	case ModeOK:
		return r.record(ReportResult{OK: true})
	case ModeSlow:
		if b.Delay > 0 {
			timer := time.NewTimer(b.Delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return r.record(ReportResult{Failure: model.FailEndpointTimeout,
					Message: "endpoint slow, attempt canceled: " + ctx.Err().Error()})
			}
		}
		return r.record(ReportResult{OK: true})
	case ModeFail:
		fail := true
		if b.FailModulo > 0 {
			fail = n%int64(b.FailModulo) == 0
		}
		if fail {
			fc := b.Failure
			if fc == model.FailNone {
				fc = model.FailEndpointError
			}
			return r.record(ReportResult{Failure: fc, Message: "scripted endpoint failure"})
		}
		return r.record(ReportResult{OK: true})
	default:
		return r.record(ReportResult{Failure: model.FailEndpointError, Message: "unknown reporter mode"})
	}
}

func (r *MockReporter) record(res ReportResult) ReportResult {
	if res.OK {
		r.accepted.Add(1)
	} else {
		if c, ok := r.failures.Load(res.Failure); ok {
			c.(*atomic.Int64).Add(1)
		} else {
			var c atomic.Int64
			c.Store(1)
			actual, _ := r.failures.LoadOrStore(res.Failure, &c)
			if actual != &c {
				actual.(*atomic.Int64).Add(1)
			}
		}
	}
	return res
}

// current returns behavior and the matching capacity channel captured
// together under one lock, so one attempt never straddles two behaviors.
func (r *MockReporter) current() (Behavior, chan struct{}) {
	r.mu.RLock()
	b := r.behavior
	c := r.sem
	r.mu.RUnlock()
	return b, c
}

// RequestCount returns the number of delivery attempts observed.
func (r *MockReporter) RequestCount() int64 { return r.requests.Load() }

// AcceptedCount returns successful deliveries.
func (r *MockReporter) AcceptedCount() int64 { return r.accepted.Load() }

// FailureCounts returns a copy of per-failure-class attempt counters.
func (r *MockReporter) FailureCounts() map[model.FailureClass]int64 {
	out := map[model.FailureClass]int64{}
	r.failures.Range(func(k, v any) bool {
		out[k.(model.FailureClass)] = v.(*atomic.Int64).Load()
		return true
	})
	return out
}
