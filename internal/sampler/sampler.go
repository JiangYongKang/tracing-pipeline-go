// Package sampler implements parent-based, per-trace-frozen sampling.
// The first decision for a trace is derived deterministically from the
// trace id and the currently configured ratio, then frozen for the lifetime
// of the trace's retention lifecycle: later ratio changes, and any order of
// parent/child arrival, can never flip it. Decisions live in the bounded
// retention.Registry shared with the normalizer, so a long-running process
// keeps only a bounded number of frozen verdicts; a trace reclaimed and
// seen again starts a new lifecycle with a freshly derived (still
// deterministic) verdict.
package sampler

import (
	"hash/fnv"
	"math"
	"sync"
	"sync/atomic"

	"github.com/highcumontoa/tracing-pipeline-go/internal/model"
	"github.com/highcumontoa/tracing-pipeline-go/internal/retention"
)

// ratioBasis is the granularity of the sampling ratio (1/10000).
const ratioBasis = 10_000

// Sampler is safe for concurrent use.
type Sampler struct {
	reg *retention.Registry

	mu sync.Mutex
	// ratio in [0, ratioBasis]; the sample probability is ratio/ratioBasis.
	ratio uint64

	sampled atomic.Int64
	dropped atomic.Int64
}

// Snapshot is a point-in-time view of sampler state.
type Snapshot struct {
	Ratio         float64
	TracesDecided int64 // currently retained (live) trace lifecycles
	Sampled       int64
	Dropped       int64
}

// New builds a sampler with the given initial ratio in [0.0, 1.0] and its
// own bounded default registry. Values outside the range are clamped; NaN
// becomes 0.
func New(ratio float64) *Sampler {
	return NewWithRegistry(ratio, retention.New(retention.DefaultConfig()))
}

// NewWithRegistry builds a sampler whose frozen decisions live in reg,
// shared with the normalizer so both stages see one bounded lifecycle per
// trace.
func NewWithRegistry(ratio float64, reg *retention.Registry) *Sampler {
	return &Sampler{
		reg:   reg,
		ratio: clampRatio(ratio),
	}
}

// Decide returns the frozen decision for the trace owning span. Because the
// verdict is keyed by trace id and frozen at first sight of a lifecycle,
// every parent/child pair in one trace always agrees regardless of arrival
// order or later ratio changes.
func (s *Sampler) Decide(span *model.Span) model.Decision {
	d, _, fresh := s.reg.Decide(span.TraceID, func() model.Decision {
		s.mu.Lock()
		ratio := s.ratio
		s.mu.Unlock()
		return derive(ratio, span.TraceID)
	})
	if fresh {
		if d == model.DecisionSample {
			s.sampled.Add(1)
		} else {
			s.dropped.Add(1)
		}
	}
	return d
}

// derive maps the trace id to a stable verdict against the given ratio.
// FNV-1a gives uniform, dependency-free bucketing; the same trace id maps
// to the same bucket forever, so a re-derived verdict (new lifecycle after
// reclamation) is identical unless the ratio changed in between.
func derive(ratio uint64, tid model.TraceID) model.Decision {
	if ratio == 0 {
		return model.DecisionDrop
	}
	if ratio >= ratioBasis {
		return model.DecisionSample
	}
	h := fnv.New64a()
	h.Write(tid[:])
	bucket := h.Sum64() % ratioBasis
	if bucket < ratio {
		return model.DecisionSample
	}
	return model.DecisionDrop
}

// SetRatio updates the ratio for lifecycles not yet decided. Already frozen
// verdicts (live lifecycles) keep their decision, so a mid-run change never
// flips them.
func (s *Sampler) SetRatio(ratio float64) {
	s.mu.Lock()
	s.ratio = clampRatio(ratio)
	s.mu.Unlock()
}

// Snapshot returns a point-in-time state copy.
func (s *Sampler) Snapshot() Snapshot {
	s.mu.Lock()
	ratio := float64(s.ratio) / ratioBasis
	s.mu.Unlock()
	return Snapshot{
		Ratio:         ratio,
		TracesDecided: int64(s.reg.Stats().LiveTraces),
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
