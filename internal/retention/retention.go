// Package retention bounds the runtime per-trace lifecycle state kept by
// the ingestion and sampling stages. State lives in per-trace lifecycles:
// a lifecycle holds the trace's frozen sampling decision and the set of
// span ids registered under it. The number of live lifecycles is hard-capped
// (capacity) and lifecycles idle beyond a TTL are reclaimed, so retained
// state never grows with the number of unique traces seen by a long-running
// process. Reclamation is deterministic (LRU + idle expiry), amortized so
// writers are never blocked for long, and fully observable via Stats.
//
// A reclaimed trace leaves a bounded tombstone. When the same trace id
// reappears later (late child span, duplicate report), the tombstone makes
// the revisit distinguishable: a new lifecycle (generation >= 2) begins,
// unless the re-arriving span is still inside the exporter's duplicate
// protection window, in which case no new lifecycle begins and the original
// still-protected conclusion stands.
package retention

import (
	"container/list"
	"sync"
	"time"

	"github.com/highcumontoa/tracing-pipeline-go/internal/model"
)

// Default bounds applied when a Config field is left zero.
const (
	DefaultMaxTraces    = 10_000
	DefaultTombstoneCap = 10_000
	DefaultSweepBatch   = 8
)

// DefaultTTL is the default idle retention window for a lifecycle.
const DefaultTTL = 10 * time.Minute

// Config configures the bounded lifecycle registry. The zero value is
// valid and yields the documented defaults, so retention is bounded out of
// the box.
type Config struct {
	// MaxTraces is the hard cap on simultaneously live trace lifecycles.
	// <=0 selects DefaultMaxTraces.
	MaxTraces int
	// TTL is the maximum idle time (no new span/decision) before a
	// lifecycle becomes reclaimable. <=0 selects DefaultTTL.
	TTL time.Duration
	// TombstoneCap bounds how many reclaimed traces are remembered for
	// revisit classification. <=0 selects DefaultTombstoneCap.
	TombstoneCap int
	// SweepBatch caps how many idle-expired lifecycles one write reclaims,
	// amortizing TTL work so writers never stall on a big sweep.
	// <=0 selects DefaultSweepBatch.
	SweepBatch int
	// Now optionally overrides the clock (tests). Nil means time.Now.
	Now func() time.Time
}

// DefaultConfig returns the bounded default configuration.
func DefaultConfig() Config {
	return Config{
		MaxTraces:    DefaultMaxTraces,
		TTL:          DefaultTTL,
		TombstoneCap: DefaultTombstoneCap,
		SweepBatch:   DefaultSweepBatch,
	}
}

// BeginStatus classifies what Register/Decide found for a trace.
type BeginStatus int

const (
	// BeginFresh: first lifecycle ever observed for this trace.
	BeginFresh BeginStatus = iota
	// BeginActive: the trace's lifecycle is still live; state was reused.
	BeginActive
	// BeginRevisit: a previous lifecycle was reclaimed; a new generation
	// begins now (or was suppressed by export protection).
	BeginRevisit
)

// String renders the begin status for logs.
func (b BeginStatus) String() string {
	switch b {
	case BeginFresh:
		return "fresh"
	case BeginActive:
		return "active"
	case BeginRevisit:
		return "revisit"
	default:
		return "unknown"
	}
}

// SpanStatus is the per-span registration verdict inside a lifecycle.
type SpanStatus int

const (
	// SpanNew: span id registered under this lifecycle for the first time.
	SpanNew SpanStatus = iota
	// SpanDuplicate: span id already registered under this same trace.
	SpanDuplicate
	// SpanConflict: span id already registered under a different live trace.
	SpanConflict
	// SpanProtected: the trace's previous lifecycle was reclaimed, but the
	// span is still inside the exporter's duplicate-protection window, so
	// no new lifecycle was begun and nothing was registered.
	SpanProtected
)

// RegisterResult reports the outcome of one Register call.
type RegisterResult struct {
	Begin      BeginStatus
	Span       SpanStatus
	Generation uint64 // lifecycle generation the span belongs to; 0 when SpanProtected/SpanConflict
}

// Stats is a point-in-time, externally queryable view of the registry.
type Stats struct {
	LiveTraces      int   // currently retained lifecycles (<= MaxTraces)
	LiveSpans       int   // currently retained span registrations
	Tombstones      int   // reclaimed traces still remembered for revisit detection
	EvictedCapacity int64 // reclamations forced by the capacity cap
	EvictedTTL      int64 // reclamations forced by idle expiry
	Revisits        int64 // traces that began a new lifecycle after reclamation
	Lifecycles      int64 // total lifecycles begun
}

// entry is one live trace lifecycle.
type entry struct {
	trace    model.TraceID
	gen      uint64
	decision model.Decision // frozen sampling verdict of this lifecycle
	spans    map[model.SpanID]struct{}
	touched  time.Time
}

// tombstone remembers one reclaimed trace for revisit classification.
type tombstone struct {
	trace model.TraceID
	gen   uint64
}

// Registry is the bounded lifecycle store. It is safe for concurrent use.
type Registry struct {
	cfg Config
	now func() time.Time

	mu        sync.Mutex
	lru       *list.List // front = most recently touched; value = *entry
	byTrace   map[model.TraceID]*list.Element
	spanIndex map[model.SpanID]model.TraceID // live spans -> owning trace
	liveSpans int
	tombs     map[model.TraceID]uint64
	tombOrder []tombstone // FIFO, bounded by TombstoneCap

	evictedCapacity int64
	evictedTTL      int64
	revisits        int64
	lifecycles      int64
}

// New builds a bounded registry. Zero Config fields select defaults.
func New(cfg Config) *Registry {
	if cfg.MaxTraces <= 0 {
		cfg.MaxTraces = DefaultMaxTraces
	}
	if cfg.TTL <= 0 {
		cfg.TTL = DefaultTTL
	}
	if cfg.TombstoneCap <= 0 {
		cfg.TombstoneCap = DefaultTombstoneCap
	}
	if cfg.SweepBatch <= 0 {
		cfg.SweepBatch = DefaultSweepBatch
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Registry{
		cfg:       cfg,
		now:       now,
		lru:       list.New(),
		byTrace:   make(map[model.TraceID]*list.Element),
		spanIndex: make(map[model.SpanID]model.TraceID),
		tombs:     make(map[model.TraceID]uint64),
	}
}

// Register records one span id under its trace's lifecycle, beginning a new
// lifecycle when none is live. guard (may be nil) reports whether a span key
// is still inside the exporter's duplicate-protection window; it is consulted
// only on the revisit path, before any state is mutated, and a protected
// revisit begins nothing.
func (r *Registry) Register(trace model.TraceID, span model.SpanID, guard func(model.SpanKey) bool) RegisterResult {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.sweepLocked()

	// Live lifecycle: reuse it. The span may still be new, a duplicate of
	// this trace, or a conflict with another live trace.
	if el, ok := r.byTrace[trace]; ok {
		e := el.Value.(*entry)
		r.touchLocked(el, e)
		res := r.registerSpanLocked(e, span)
		res.Begin = BeginActive
		res.Generation = e.gen
		return res
	}

	// No live lifecycle. A span id owned by a *different* live trace is a
	// conflict and must not begin a lifecycle for this trace.
	if owner, ok := r.spanIndex[span]; ok && owner != trace {
		return RegisterResult{Begin: BeginFresh, Span: SpanConflict}
	}

	// Reclaimed before? The tombstone makes the revisit distinguishable and
	// the new lifecycle a new generation.
	gen := uint64(1)
	begin := BeginFresh
	if g, ok := r.tombs[trace]; ok {
		gen = g + 1
		begin = BeginRevisit
		if guard != nil && guard(model.SpanKey{Trace: trace, Span: span}) {
			// Still protected by the exporter's bounded dedup window: the
			// original conclusion stands, nothing is registered, no new
			// lifecycle begins. The caller classifies this deterministically.
			return RegisterResult{Begin: BeginRevisit, Span: SpanProtected}
		}
		delete(r.tombs, trace)
		r.revisits++
	}

	e := r.beginLocked(trace, gen)
	res := r.registerSpanLocked(e, span)
	res.Begin = begin
	res.Generation = gen
	return res
}

// Decide returns the frozen sampling decision of the trace's live lifecycle,
// deriving and freezing it on first use via derive (called at most once per
// lifecycle, while the registry lock is held; derive must be pure and fast).
// It reports the lifecycle generation and whether this call froze the
// decision.
func (r *Registry) Decide(trace model.TraceID, derive func() model.Decision) (decision model.Decision, generation uint64, fresh bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.sweepLocked()

	var e *entry
	if el, ok := r.byTrace[trace]; ok {
		e = el.Value.(*entry)
		r.touchLocked(el, e)
	} else {
		// Sampler used standalone (no prior Register): begin a lifecycle so
		// the decision still has a bounded home and revisit semantics hold.
		gen := uint64(1)
		if g, ok := r.tombs[trace]; ok {
			gen = g + 1
			delete(r.tombs, trace)
			r.revisits++
		}
		e = r.beginLocked(trace, gen)
	}

	if e.decision != model.DecisionUnknown {
		return e.decision, e.gen, false
	}
	e.decision = derive()
	return e.decision, e.gen, true
}

// Stats returns a point-in-time snapshot of the bounded state.
func (r *Registry) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Stats{
		LiveTraces:      len(r.byTrace),
		LiveSpans:       r.liveSpans,
		Tombstones:      len(r.tombs),
		EvictedCapacity: r.evictedCapacity,
		EvictedTTL:      r.evictedTTL,
		Revisits:        r.revisits,
		Lifecycles:      r.lifecycles,
	}
}

// registerSpanLocked adds span to a live entry, or reports duplicate/conflict.
func (r *Registry) registerSpanLocked(e *entry, span model.SpanID) RegisterResult {
	if owner, ok := r.spanIndex[span]; ok {
		if owner == e.trace {
			return RegisterResult{Span: SpanDuplicate}
		}
		return RegisterResult{Span: SpanConflict}
	}
	e.spans[span] = struct{}{}
	r.spanIndex[span] = e.trace
	r.liveSpans++
	return RegisterResult{Span: SpanNew}
}

// beginLocked starts a new lifecycle, evicting LRU lifecycles first when the
// capacity cap is reached. Eviction of one entry is O(its span count), so a
// writer never pays more than one lifecycle's worth of work per insert.
func (r *Registry) beginLocked(trace model.TraceID, gen uint64) *entry {
	for len(r.byTrace) >= r.cfg.MaxTraces {
		r.evictLocked(r.lru.Back(), &r.evictedCapacity)
	}
	e := &entry{trace: trace, gen: gen, spans: make(map[model.SpanID]struct{}), touched: r.now()}
	r.byTrace[trace] = r.lru.PushFront(e)
	r.lifecycles++
	return e
}

// touchLocked marks an entry most-recently-used and slides its idle window.
func (r *Registry) touchLocked(el *list.Element, e *entry) {
	e.touched = r.now()
	r.lru.MoveToFront(el)
}

// sweepLocked reclaims up to SweepBatch idle-expired lifecycles from the LRU
// tail. Because the list is touch-ordered, the first non-expired tail entry
// ends the sweep: work per write is bounded by SweepBatch.
func (r *Registry) sweepLocked() {
	now := r.now()
	for i := 0; i < r.cfg.SweepBatch; i++ {
		back := r.lru.Back()
		if back == nil {
			return
		}
		e := back.Value.(*entry)
		if now.Sub(e.touched) < r.cfg.TTL {
			return
		}
		r.evictLocked(back, &r.evictedTTL)
	}
}

// evictLocked removes one lifecycle deterministically and records a bounded
// tombstone so a later revisit can be classified as a new generation.
func (r *Registry) evictLocked(el *list.Element, counter *int64) {
	e := el.Value.(*entry)
	r.lru.Remove(el)
	delete(r.byTrace, e.trace)
	for sid := range e.spans {
		delete(r.spanIndex, sid)
	}
	r.liveSpans -= len(e.spans)
	*counter++

	r.tombs[e.trace] = e.gen
	r.tombOrder = append(r.tombOrder, tombstone{trace: e.trace, gen: e.gen})
	for len(r.tombs) > r.cfg.TombstoneCap {
		old := r.tombOrder[0]
		r.tombOrder = r.tombOrder[1:]
		// A tombstone is consumed when its trace begins a new lifecycle;
		// only drop entries still pointing at the recorded generation.
		if g, ok := r.tombs[old.trace]; ok && g == old.gen {
			delete(r.tombs, old.trace)
		}
	}
}
