package retention

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/tracing-pipeline-go/internal/model"
)

func traceN(t *testing.T, n int) model.TraceID {
	t.Helper()
	id, err := model.ParseTraceID(fmt.Sprintf("%032x", n))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func spanN(t *testing.T, n int) model.SpanID {
	t.Helper()
	id, err := model.ParseSpanID(fmt.Sprintf("%016x", n))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestCapacityEvictionIsDeterministicLRU(t *testing.T) {
	r := New(Config{MaxTraces: 3, TombstoneCap: 16})
	// Insert 1,2,3; touch 1 so 2 becomes the LRU victim.
	for i := 1; i <= 3; i++ {
		r.Register(traceN(t, i), spanN(t, i), nil)
	}
	r.Register(traceN(t, 1), spanN(t, 100), nil) // touch trace 1

	r.Register(traceN(t, 4), spanN(t, 4), nil)

	st := r.Stats()
	if st.LiveTraces != 3 || st.EvictedCapacity != 1 {
		t.Fatalf("stats=%+v, want 3 live and 1 capacity eviction", st)
	}
	// Trace 2 was the LRU victim: it must carry a tombstone, trace 1 must not.
	if _, ok := r.tombs[traceN(t, 2)]; !ok {
		t.Fatal("expected tombstone for LRU victim trace 2")
	}
	if _, ok := r.tombs[traceN(t, 1)]; ok {
		t.Fatal("recently touched trace 1 must not be evicted")
	}
	// Identity: lifecycles begun == live + evicted.
	if st.Lifecycles != int64(st.LiveTraces)+st.EvictedCapacity+st.EvictedTTL {
		t.Fatalf("lifecycle accounting broken: %+v", st)
	}
}

func TestTTLSweepWithFakeClock(t *testing.T) {
	now := time.Unix(1_000, 0)
	clock := func() time.Time { return now }
	r := New(Config{MaxTraces: 100, TTL: time.Minute, SweepBatch: 4, Now: clock})

	r.Register(traceN(t, 1), spanN(t, 1), nil)
	now = now.Add(2 * time.Minute)             // trace 1 idles out
	r.Register(traceN(t, 2), spanN(t, 2), nil) // write triggers amortized sweep

	st := r.Stats()
	if st.EvictedTTL != 1 || st.LiveTraces != 1 {
		t.Fatalf("stats=%+v, want 1 TTL eviction and 1 live", st)
	}
	if _, ok := r.tombs[traceN(t, 1)]; !ok {
		t.Fatal("TTL-evicted trace must leave a tombstone")
	}
}

func TestRevisitBeginsNewGeneration(t *testing.T) {
	r := New(Config{MaxTraces: 1, TombstoneCap: 16})
	tid := traceN(t, 7)

	res := r.Register(tid, spanN(t, 1), nil)
	if res.Begin != BeginFresh || res.Generation != 1 || res.Span != SpanNew {
		t.Fatalf("first register=%+v", res)
	}
	r.Register(traceN(t, 8), spanN(t, 8), nil) // evicts trace 7 (capacity)

	res = r.Register(tid, spanN(t, 2), nil)
	if res.Begin != BeginRevisit || res.Generation != 2 || res.Span != SpanNew {
		t.Fatalf("revisit register=%+v, want revisit gen=2 new", res)
	}
	st := r.Stats()
	if st.Revisits != 1 {
		t.Fatalf("revisits=%d want 1", st.Revisits)
	}
	// The old span id was reclaimed with generation 1: re-registering it in
	// generation 2 is a fresh registration, not a duplicate.
	res = r.Register(tid, spanN(t, 1), nil)
	if res.Span != SpanNew || res.Generation != 2 {
		t.Fatalf("old span in new generation=%+v, want new gen=2", res)
	}
}

func TestProtectedRevisitBeginsNothing(t *testing.T) {
	r := New(Config{MaxTraces: 1, TombstoneCap: 16})
	tid := traceN(t, 7)
	protectedKey := model.SpanKey{Trace: tid, Span: spanN(t, 1)}

	r.Register(tid, spanN(t, 1), nil)
	r.Register(traceN(t, 8), spanN(t, 8), nil) // evicts trace 7
	before := r.Stats()

	guard := func(k model.SpanKey) bool { return k == protectedKey }
	res := r.Register(tid, spanN(t, 1), guard)
	if res.Span != SpanProtected || res.Begin != BeginRevisit || res.Generation != 0 {
		t.Fatalf("protected revisit=%+v", res)
	}
	after := r.Stats()
	// No lifecycle begun, nothing registered, tombstone retained, and the
	// visit is not counted as a started new lifecycle.
	if after.Lifecycles != before.Lifecycles || after.LiveTraces != before.LiveTraces ||
		after.LiveSpans != before.LiveSpans || after.Revisits != before.Revisits {
		t.Fatalf("protected revisit mutated state: before=%+v after=%+v", before, after)
	}
	if _, ok := r.tombs[tid]; !ok {
		t.Fatal("tombstone must survive a protected revisit")
	}

	// Once protection expires, the same register begins generation 2.
	res = r.Register(tid, spanN(t, 1), func(model.SpanKey) bool { return false })
	if res.Span != SpanNew || res.Generation != 2 {
		t.Fatalf("unprotected revisit=%+v, want new gen=2", res)
	}
}

func TestDuplicateAndConflictInsideLifecycle(t *testing.T) {
	r := New(Config{MaxTraces: 8, TombstoneCap: 16})
	r.Register(traceN(t, 1), spanN(t, 1), nil)

	if res := r.Register(traceN(t, 1), spanN(t, 1), nil); res.Span != SpanDuplicate || res.Begin != BeginActive {
		t.Fatalf("duplicate=%+v", res)
	}
	if res := r.Register(traceN(t, 2), spanN(t, 1), nil); res.Span != SpanConflict {
		t.Fatalf("conflict=%+v", res)
	}
	// A conflict must not begin a lifecycle for the losing trace.
	if st := r.Stats(); st.LiveTraces != 1 {
		t.Fatalf("conflict began a lifecycle: %+v", st)
	}
}

func TestDecideFreezesPerLifecycle(t *testing.T) {
	r := New(Config{MaxTraces: 1, TombstoneCap: 16})
	tid := traceN(t, 9)
	calls := 0
	derive := func() model.Decision { calls++; return model.DecisionSample }

	d, gen, fresh := r.Decide(tid, derive)
	if d != model.DecisionSample || gen != 1 || !fresh {
		t.Fatalf("first decide=%s gen=%d fresh=%v", d, gen, fresh)
	}
	d, gen, fresh = r.Decide(tid, derive)
	if d != model.DecisionSample || gen != 1 || fresh {
		t.Fatalf("second decide must reuse frozen verdict: %s gen=%d fresh=%v", d, gen, fresh)
	}
	if calls != 1 {
		t.Fatalf("derive called %d times, want exactly 1 per lifecycle", calls)
	}

	r.Decide(traceN(t, 10), derive) // evicts trace 9; derives once for trace 10
	d, gen, fresh = r.Decide(tid, derive)
	if gen != 2 || !fresh {
		t.Fatalf("post-eviction decide=%s gen=%d fresh=%v, want new generation", d, gen, fresh)
	}
	if calls != 3 {
		t.Fatalf("new lifecycle must re-derive: calls=%d want 3", calls)
	}
}

// Concurrent writers against a tiny cap: invariants must hold at every
// observable point and the race detector must stay quiet.
func TestConcurrentRegisterDecideUnderConstantEviction(t *testing.T) {
	const cap = 32
	r := New(Config{MaxTraces: cap, TombstoneCap: 64, SweepBatch: 4})

	const workers = 16
	const perWorker = 500
	const idSpace = 200 // deliberately smaller than total writes: traces recur
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				n := (w*perWorker+i)%idSpace + 1
				tid := traceNT(n)
				r.Register(tid, spanNT(w*perWorker+i+1), nil)
				d, _, _ := r.Decide(tid, func() model.Decision { return model.DecisionSample })
				if d != model.DecisionSample {
					t.Errorf("derived decision changed: %s", d)
				}
				if st := r.Stats(); st.LiveTraces > cap {
					t.Errorf("live traces %d exceed cap %d", st.LiveTraces, cap)
				}
			}
		}()
	}
	wg.Wait()

	st := r.Stats()
	if st.LiveTraces > cap || st.Tombstones > 64 {
		t.Fatalf("bounds violated: %+v", st)
	}
	if st.EvictedCapacity == 0 {
		t.Fatal("expected continuous capacity eviction under load")
	}
	if st.Lifecycles != int64(st.LiveTraces)+st.EvictedCapacity+st.EvictedTTL {
		t.Fatalf("lifecycle accounting broken: %+v", st)
	}
	if st.Revisits == 0 {
		t.Fatal("recurring trace ids must produce revisits after eviction")
	}
}

// helpers without *testing.T for goroutines
func traceNT(n int) model.TraceID {
	id, _ := model.ParseTraceID(fmt.Sprintf("%032x", n))
	return id
}
func spanNT(n int) model.SpanID {
	id, _ := model.ParseSpanID(fmt.Sprintf("%016x", n))
	return id
}
