// Package sampler implements parent-based, per-trace-frozen sampling.
// The first decision for a trace is derived deterministically from the
// trace id and the currently configured ratio, then frozen: later ratio
// changes, and any order of parent/child arrival, can never flip it.
//
// Runtime state is bounded by a retention.Store: while a trace's frozen
// decision is retained, all consistency guarantees hold. Once the
// decision is reclaimed (capacity/TTL), a re-arriving span of that trace
// deterministically starts a new lifecycle and is flagged as a revisit,
// so it is never silently dropped nor mixed into another trace's verdict.
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
	mu sync.Mutex
	// ratio in [0, ratioBasis]; the sample probability is ratio/ratioBasis.
	ratio uint64
	// decisions is the bounded store of frozen verdicts per trace.
	decisions *retention.Store[model.TraceID, model.Decision]
	// spanVerdict remembers accepted spans' verdicts in a bounded store,
	// used to honor a known parent when a trace is seen for the "first"
	// time via a child.
	spanVerdict *retention.Store[model.SpanKey, model.Decision]

	sampled  atomic.Int64
	dropped  atomic.Int64
	revisits atomic.Int64
}

// Snapshot is a point-in-time view of sampler state.
type Snapshot struct {
	Ratio         float64
	TracesDecided int64 // currently retained frozen decisions
	Sampled       int64
	Dropped       int64
	Revisits      int64 // traces that started a new lifecycle after eviction
	Evicted       int64 // frozen decisions reclaimed so far
}

// New builds a sampler with the given initial ratio in [0.0, 1.0] and
// bounded default retention. Values outside the range are clamped; NaN
// becomes 0.
func New(ratio float64) *Sampler {
	return NewWithRetention(ratio, retention.DefaultConfig(), nil)
}

// NewWithRetention builds a sampler with explicit retention bounds for
// the frozen-decision state. logf may be nil; when set, every reclamation
// is logged with the trace id and reason.
func NewWithRetention(ratio float64, cfg retention.Config, logf func(format string, args ...any)) *Sampler {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	s := &Sampler{ratio: clampRatio(ratio)}
	s.decisions = retention.NewStore(cfg, nil,
		func(tid model.TraceID, d model.Decision, r retention.Reason) {
			logf("retention evict stage=sampler-decisions trace=%s decision=%s reason=%s", tid, d, r)
		})
	s.spanVerdict = retention.NewStore(cfg, nil,
		func(k model.SpanKey, d model.Decision, r retention.Reason) {
			logf("retention evict stage=sampler-span-verdicts trace=%s span=%s decision=%s reason=%s",
				k.Trace, k.Span, d, r)
		})
	return s
}

// Decide returns the frozen decision for the trace owning span. It is
// DecideWithLifecycle without the revisit flag.
func (s *Sampler) Decide(span *model.Span) model.Decision {
	d, _ := s.DecideWithLifecycle(span)
	return d
}

// DecideWithLifecycle returns the frozen decision plus whether the span
// reopened a trace whose previous decision state was already reclaimed.
//
// Within the retention window the verdict is keyed by trace id and frozen
// at first sight, so every parent/child pair in one trace always agrees
// regardless of arrival order or later ratio changes. After reclamation
// the trace deterministically starts a new lifecycle: a still-retained
// parent verdict is inherited (the protected conclusion is reused),
// otherwise the verdict is re-derived from the trace id and the current
// ratio. Either way the revisit is flagged and counted, never silent.
func (s *Sampler) DecideWithLifecycle(span *model.Span) (model.Decision, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if d, ok := s.decisions.Get(span.TraceID); ok {
		s.spanVerdict.Put(span.Key(), d)
		return d, false
	}

	// The trace has no live frozen decision. If it was decided before and
	// reclaimed, this span reopens it under a new lifecycle.
	revisit := s.decisions.WasEvicted(span.TraceID)

	var d model.Decision
	if span.HasParent {
		if pd, ok := s.spanVerdict.Get(model.SpanKey{Trace: span.TraceID, Span: span.Parent}); ok {
			d = pd
		}
	}
	if d == model.DecisionUnknown {
		d = s.derive(span.TraceID)
	}

	s.decisions.Put(span.TraceID, d)
	s.spanVerdict.Put(span.Key(), d)
	if revisit {
		s.revisits.Add(1)
	}
	if d == model.DecisionSample {
		s.sampled.Add(1)
	} else {
		s.dropped.Add(1)
	}
	return d, revisit
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
// traces keep their frozen decision for as long as it is retained, so a
// mid-run change never flips them.
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
	dst := s.decisions.Stats()
	return Snapshot{
		Ratio:         ratio,
		TracesDecided: int64(dst.Retained),
		Sampled:       s.sampled.Load(),
		Dropped:       s.dropped.Load(),
		Revisits:      s.revisits.Load(),
		Evicted:       dst.EvictedTotal,
	}
}

// RetentionStats exposes the reclamation bookkeeping of both bounded
// stores for external queries.
func (s *Sampler) RetentionStats() (decisions, spanVerdicts retention.Stats) {
	return s.decisions.Stats(), s.spanVerdict.Stats()
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
