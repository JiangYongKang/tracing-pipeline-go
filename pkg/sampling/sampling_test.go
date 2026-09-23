package sampling

import (
	"fmt"
	"log"
	"sync"
	"testing"

	"github.com/highcumontoa/tracing-pipeline-go/pkg/model"
)

func mustTrace(t *testing.T, s string) model.TraceID {
	t.Helper()
	id, err := model.ParseTraceID(s)
	if err != nil {
		t.Fatalf("trace id %q: %v", s, err)
	}
	return id
}
func mustSpan(t *testing.T, s string) model.SpanID {
	t.Helper()
	id, err := model.ParseSpanID(s)
	if err != nil {
		t.Fatalf("span id %q: %v", s, err)
	}
	return id
}

func mkSpan(t *testing.T, tid model.TraceID, sid, pid string) model.Span {
	t.Helper()
	s := model.Span{TraceID: tid, SpanID: mustSpan(t, sid)}
	if pid != "" {
		s.ParentID = mustSpan(t, pid)
	}
	return s
}

// TestDeterministicRootDecision: same trace id + ratio yields same verdict in
// two independent samplers (pure hash, no ordering dependence).
func TestDeterministicRootDecision(t *testing.T) {
	tid := mustTrace(t, "0af7651916cd43dd8448eb211c80319c")
	a := New(Config{Ratio: 0.5})
	b := New(Config{Ratio: 0.5})
	if a.Decide(mkSpan(t, tid, "0000000000000001", "")) != b.Decide(mkSpan(t, tid, "0000000000000002", "")) {
		t.Fatal("root decision must be deterministic from trace id")
	}
}

// TestRatioBounds: ratio 0 drops everything, ratio 1 keeps everything.
func TestRatioBounds(t *testing.T) {
	all := New(Config{Ratio: 1.0, CacheCap: 1000})
	none := New(Config{Ratio: 0, CacheCap: 1000})
	for i := 0; i < 200; i++ {
		id := mustTrace(t, decID(i))
		if all.Decide(mkSpan(t, id, sidN(i), "")) != Keep {
			t.Fatalf("ratio=1 must keep trace %d", i)
		}
		if none.Decide(mkSpan(t, id, sidN(i+5000), "")) != Drop {
			t.Fatalf("ratio=0 must drop trace %d", i)
		}
	}
	log.Printf("decision bounds checked: ratio=1 keeps 200, ratio=0 drops 200")
}

// decID is a bijective mapping i -> unique 32-hex trace id (two 64-bit words).
func decID(i int) string {
	lo := uint64(i)
	hi := lo ^ 0x9e3779b97f4a7c15
	return fmt.Sprintf("%016x%016x", hi, lo)
}

// sidN is a bijective mapping i -> unique, non-zero 16-hex span id.
func sidN(i int) string {
	v := uint64(i)*2654435761 + 1 // multiplication by odd constant is bijective mod 2^64
	if v == 0 {
		v = 1
	}
	return fmt.Sprintf("%016x", v)
}

// TestParentChildConsistency: regardless of decision order (child first or
// root first), all spans of one trace share the root's verdict.
func TestParentChildConsistency(t *testing.T) {
	s := New(Config{Ratio: 0.5})
	tid := mustTrace(t, "4bf92f3577b34da6a3ce929d0e0e4736")
	root := mkSpan(t, tid, "1111111111111111", "")
	child := mkSpan(t, tid, "2222222222222222", "1111111111111111")
	grand := mkSpan(t, tid, "3333333333333333", "2222222222222222")

	// Ask for the grandchild first — it establishes the root decision.
	d := s.Decide(grand)
	if s.Decide(child) != d || s.Decide(root) != d {
		t.Fatalf("family verdicts diverge: %v", d)
	}
	log.Printf("trace=%s decision=%s parent-child consistency ok", tid, d)
}

// TestRatioChangeNeverFlipsStartedTrace is the core runtime-adjustment rule.
func TestRatioChangeNeverFlipsStartedTrace(t *testing.T) {
	s := New(Config{Ratio: 1.0})
	started := make([]model.TraceID, 30)
	spans := make([]model.Span, 0, 60)
	for i := range started {
		started[i] = mustTrace(t, decID(100+i))
		r := mkSpan(t, started[i], sidN(100+i), "")
		spans = append(spans, r, mkSpan(t, started[i], sidN(200+i), r.SpanID.String()))
	}
	before := s.DecideBatch(spans)
	for i, d := range before {
		if d != Keep {
			t.Fatalf("setup: expected keep, got drop at %d", i)
		}
	}

	// Flip the ratio to 0 while traces are in progress.
	s.SetRatio(0)
	if s.Ratio() != 0 {
		t.Fatal("ratio update not visible")
	}
	for i, tid := range started {
		d := s.Decide(mkSpan(t, tid, sidN(300+i), sidN(100+i)))
		if d != Keep {
			t.Fatalf("trace %s flipped to %s after ratio change", tid, d)
		}
		log.Printf("trace=%s decision=%s unchanged after ratio 1->0", tid, d)
	}

	// A genuinely new trace observed only after the change follows new ratio.
	fresh := mustTrace(t, decID(999))
	if d := s.Decide(mkSpan(t, fresh, sidN(999), "")); d != Drop {
		t.Fatalf("new trace must follow new ratio, got %v", d)
	}
}

// TestNoPartialSpansOnDrop: DecideBatch on a multi-span DROP trace returns all
// DROP so the caller can discard the whole trace atomically.
func TestNoPartialSpansOnDrop(t *testing.T) {
	// ratio 0 guarantees drop; construct a large family.
	s := New(Config{Ratio: 0})
	tid := mustTrace(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	spans := []model.Span{mkSpan(t, tid, "1111111111111111", "")}
	for i := 0; i < 20; i++ {
		spans = append(spans, mkSpan(t, tid, sidN(i+100), spans[0].SpanID.String()))
	}
	ds := s.DecideBatch(spans)
	if len(ds) != len(spans) {
		t.Fatalf("decision count %d != span count %d", len(ds), len(spans))
	}
	for i, d := range ds {
		if d != Drop {
			t.Fatalf("span %d kept inside dropped trace => partial export possible", i)
		}
	}
	log.Printf("trace=%s decision=DROP spans=%d whole-trace drop, no partial spans", tid, len(spans))
}

// TestBoundedCacheEviction: a tiny cache never grows beyond its cap, and
// ReleaseTrace frees an entry.
func TestBoundedCacheEviction(t *testing.T) {
	s := New(Config{Ratio: 1, CacheCap: 4})
	for i := 0; i < 10; i++ {
		id := mustTrace(t, decID(i))
		s.Decide(mkSpan(t, id, sidN(i), ""))
	}
	if n := s.CachedDecisions(); n != 4 {
		t.Fatalf("cache should be bounded at 4, got %d", n)
	}
	id := mustTrace(t, decID(0))
	s.ReleaseTrace(id) // may already be evicted; must not panic
	if n := s.CachedDecisions(); n > 4 {
		t.Fatalf("cache grew after release: %d", n)
	}
}

// TestConcurrentDecisionsSameTrace: many goroutines deciding spans of one
// trace must all observe a single identical verdict.
func TestConcurrentDecisionsSameTrace(t *testing.T) {
	s := New(Config{Ratio: 0.5})
	tid := mustTrace(t, "cccccccccccccccccccccccccccccccc")
	var wg sync.WaitGroup
	results := make([]Decision, 200)
	for g := 0; g < 200; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			parent := ""
			if g > 0 {
				parent = "1111111111111111"
			}
			results[g] = s.Decide(mkSpan(t, tid, sidN(7000+g), parent))
		}(g)
	}
	wg.Wait()
	for _, d := range results {
		if d != results[0] {
			t.Fatalf("concurrent verdicts diverge: %v vs %v", d, results[0])
		}
	}
	log.Printf("trace=%s decision=%s under 200 concurrent goroutines, unanimous", tid, results[0])
}
