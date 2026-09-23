// Package sampling provides parent-based, trace-consistent sampling.
//
// Rules:
//   - The root span of a trace establishes the decision, derived
//     deterministically from its trace id and the ratio at that moment.
//   - Every child inherits the parent trace's recorded decision. A child can
//     never flip its trace's decision while the trace is in progress.
//   - Changing the ratio at runtime only affects traces whose root has not
//     yet been seen.
//   - The decision cache is bounded (FIFO eviction); ReleaseTrace removes a
//     finished trace promptly so in-progress traces are not evicted in
//     practice (cache capacity is configured >> expected concurrent traces;
//     eviction under gross overload is the documented, deterministic escape
//     hatch rather than unbounded memory).
package sampling

import (
	"hash/fnv"
	"math"
	"sync"

	"github.com/highcumontoa/tracing-pipeline-go/pkg/model"
)

// Decision is the sampling verdict for a trace.
type Decision bool

const (
	Drop Decision = false
	Keep Decision = true
)

func (d Decision) String() string {
	if d {
		return "KEEP"
	}
	return "DROP"
}

const defaultCacheCap = 1 << 16

// Sampler records one consistent decision per trace id.
type Sampler struct {
	mu    sync.Mutex
	ratio float64
	cap   int
	// order + cache together form a bounded insertion-ordered (FIFO) cache.
	order []model.TraceID
	cache map[model.TraceID]Decision
}

// Config configures a Sampler.
type Config struct {
	// Ratio is the initial keep probability in [0,1]; out-of-range clamps.
	Ratio float64
	// CacheCap bounds the number of remembered trace decisions (<=0 => default).
	CacheCap int
}

// New constructs a Sampler.
func New(cfg Config) *Sampler {
	r := clampRatio(cfg.Ratio)
	cap := cfg.CacheCap
	if cap <= 0 {
		cap = defaultCacheCap
	}
	return &Sampler{
		ratio: r,
		cap:   cap,
		order: make([]model.TraceID, 0, min(cap, 1024)),
		cache: make(map[model.TraceID]Decision),
	}
}

func clampRatio(r float64) float64 {
	if math.IsNaN(r) || r < 0 {
		return 0
	}
	if r > 1 {
		return 1
	}
	return r
}

// SetRatio atomically changes the sampling ratio. Existing trace decisions
// are never recomputed or flipped; the new ratio affects first-seen roots only.
func (s *Sampler) SetRatio(r float64) {
	s.mu.Lock()
	s.ratio = clampRatio(r)
	s.mu.Unlock()
}

// Ratio returns the current ratio.
func (s *Sampler) Ratio() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ratio
}

// hashTraceID maps a trace id deterministically to [0,1). FNV-1a 64 bit.
func hashTraceID(t model.TraceID) float64 {
	h := fnv.New64a()
	_, _ = h.Write(t[:])
	v := h.Sum64()
	// Divide by 2^64 to land in [0,1). v == max stays strictly below 1.
	return float64(v) / float64(uint64(1)<<63) / 2
}

// rootDecision computes the verdict a root establishes at the current ratio.
// Kept separate (no locking) so DecideBatch holds one lock for the whole batch.
func (s *Sampler) rootDecisionLocked(id model.TraceID) Decision {
	d := hashTraceID(id) < s.ratio
	dec := Decision(d)
	if _, ok := s.cache[id]; !ok {
		if len(s.order) >= s.cap {
			oldest := s.order[0]
			s.order = s.order[1:]
			delete(s.cache, oldest)
		}
		s.order = append(s.order, id)
		s.cache[id] = dec
	}
	return dec
}

// Decide returns the trace-consistent decision for one span: recorded for
// roots (deterministic hash of trace id), inherited for children.
func (s *Sampler) Decide(span model.Span) Decision {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d, ok := s.cache[span.TraceID]; ok {
		return d
	}
	return s.rootDecisionLocked(span.TraceID)
}

// DecideBatch resolves one decision per distinct trace in the batch and
// returns a parallel slice of decisions. It guarantees all spans of a trace
// share the same verdict, so a sampled-out trace never yields partial spans:
// the caller filters by whole trace, never by individual span.
func (s *Sampler) DecideBatch(spans []model.Span) []Decision {
	decisions := make([]Decision, len(spans))
	s.mu.Lock()
	defer s.mu.Unlock()
	traceDec := make(map[model.TraceID]Decision)
	for i, sp := range spans {
		d, ok := traceDec[sp.TraceID]
		if !ok {
			if cached, ok2 := s.cache[sp.TraceID]; ok2 {
				d = cached
			} else {
				d = s.rootDecisionLocked(sp.TraceID)
			}
			traceDec[sp.TraceID] = d
		}
		decisions[i] = d
	}
	return decisions
}

// ReleaseTrace forgets a finished trace's decision, freeing cache capacity.
func (s *Sampler) ReleaseTrace(id model.TraceID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.cache[id]; !ok {
		return
	}
	delete(s.cache, id)
	// Remove from the FIFO order slice (one linear scan; release is on
	// finished-trace boundaries, not per span).
	for i, t := range s.order {
		if t == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
}

// CachedDecisions reports how many trace decisions are currently remembered.
func (s *Sampler) CachedDecisions() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.cache)
}
