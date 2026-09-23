// Package sampler implements parent-based, per-trace-frozen sampling.
// The first decision for a trace is derived deterministically from the
// trace id and the currently configured ratio, then frozen: later ratio
// changes, and any order of parent/child arrival, can never flip it.
package sampler

import (
	"hash/fnv"
	"math"
	"sync"
	"sync/atomic"

	"github.com/highcumontoa/tracing-pipeline-go/internal/model"
)

// ratioBasis is the granularity of the sampling ratio (1/10000).
const ratioBasis = 10_000

// Sampler is safe for concurrent use.
type Sampler struct {
	mu sync.Mutex
	// ratio in [0, ratioBasis]; the sample probability is ratio/ratioBasis.
	ratio uint64
	// decisions is the frozen verdict per trace.
	decisions map[model.TraceID]model.Decision
	// spanVerdict remembers one accepted span's verdict, used to honor a
	// known parent when a trace is seen for the "first" time via a child.
	spanVerdict map[model.SpanKey]model.Decision

	sampled atomic.Int64
	dropped atomic.Int64
}

// Snapshot is a point-in-time view of sampler state.
type Snapshot struct {
	Ratio         float64
	TracesDecided int64
	Sampled       int64
	Dropped       int64
}

// New builds a sampler with the given initial ratio in [0.0, 1.0].
// Values outside the range are clamped; NaN becomes 0.
func New(ratio float64) *Sampler {
	return &Sampler{
		ratio:       clampRatio(ratio),
		decisions:   make(map[model.TraceID]model.Decision),
		spanVerdict: make(map[model.SpanKey]model.Decision),
	}
}

// Decide returns the frozen decision for the trace owning span, honoring
// a known parent's decision when one exists. Because the verdict is keyed
// by trace id and frozen at first sight, every parent/child pair in one
// trace always agrees regardless of arrival order or later ratio changes.
func (s *Sampler) Decide(span *model.Span) model.Decision {
	s.mu.Lock()
	defer s.mu.Unlock()

	if d, ok := s.decisions[span.TraceID]; ok {
		s.spanVerdict[span.Key()] = d
		return d
	}

	var d model.Decision
	if span.HasParent {
		if pd, ok := s.spanVerdict[model.SpanKey{Trace: span.TraceID, Span: span.Parent}]; ok {
			d = pd
		}
	}
	if d == model.DecisionUnknown {
		d = s.derive(span.TraceID)
	}

	s.decisions[span.TraceID] = d
	s.spanVerdict[span.Key()] = d
	if d == model.DecisionSample {
		s.sampled.Add(1)
	} else {
		s.dropped.Add(1)
	}
	return d
}

// derive maps the trace id to a stable verdict against the current ratio.
// FNV-1a gives uniform, dependency-free bucketing; the same trace id maps
// to the same bucket forever.
func (s *Sampler) derive(tid model.TraceID) model.Decision {
	if s.ratio == 0 {
		return model.DecisionDrop
	}
	if s.ratio >= ratioBasis {
		return model.DecisionSample
	}
	h := fnv.New64a()
	h.Write(tid[:])
	bucket := h.Sum64() % ratioBasis
	if bucket < s.ratio {
		return model.DecisionSample
	}
	return model.DecisionDrop
}

// SetRatio updates the ratio for traces not yet decided. Already started
// traces keep their frozen decision, so a mid-run change never flips them.
func (s *Sampler) SetRatio(ratio float64) {
	s.mu.Lock()
	s.ratio = clampRatio(ratio)
	s.mu.Unlock()
}

// Snapshot returns a point-in-time state copy.
func (s *Sampler) Snapshot() Snapshot {
	s.mu.Lock()
	ratio := float64(s.ratio) / ratioBasis
	decided := int64(len(s.decisions))
	s.mu.Unlock()
	return Snapshot{
		Ratio:         ratio,
		TracesDecided: decided,
		Sampled:       s.sampled.Load(),
		Dropped:       s.dropped.Load(),
	}
}

func clampRatio(ratio float64) uint64 {
	if math.IsNaN(ratio) || math.IsInf(ratio, 0) {
		return 0
	}
	if ratio <= 0 {
		return 0
	}
	if ratio >= 1 {
		return ratioBasis
	}
	return uint64(math.Round(ratio * ratioBasis))
}
