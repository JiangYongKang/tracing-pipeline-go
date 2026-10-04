package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/tracing-pipeline-go/internal/export"
	"github.com/highcumontoa/tracing-pipeline-go/internal/model"
	"github.com/highcumontoa/tracing-pipeline-go/internal/retention"
)

// Concurrent heavy writes against a tiny retention cap: reclamation runs
// continuously, retained state stays within its hard bound, writers are not
// pinned, and every admitted span is still delivered exactly once.
func TestConcurrentWritesWithContinuousReclaim(t *testing.T) {
	cfg := export.Config{
		QueueCapacity: 256, BatchMax: 32, FlushInterval: 2 * time.Millisecond,
		MaxAttempts: 1, DeadLetterCapacity: 256,
	}
	ret := retention.Config{MaxTraces: 64, TTL: time.Hour, TombstoneCap: 128, SweepBatch: 4}
	logger := newLogger(t)
	rep := newCaptureReporter()
	p := newWithReporterRetention(logger.logf, rep, cfg, 0.5, ret)
	p.Start()
	defer p.Close(context.Background())

	const workers = 16
	const perWorker = 250 // 4000 unique traces against a 64-trace cap
	start := time.Now()
	var wg sync.WaitGroup
	var enqueued atomic.Int64
	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				n := w*perWorker + i + 1
				res := submitAdmit(p, raw(fmt.Sprintf("%032x", n), fmt.Sprintf("%016x", n), ""))
				if res.Enqueued {
					enqueued.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if elapsed := time.Since(start); elapsed > 30*time.Second {
		t.Fatalf("writers pinned by reclamation: %s", elapsed)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	st := p.Stats()
	if st.Retention.LiveTraces > 64 {
		t.Fatalf("retained traces %d exceed hard cap 64", st.Retention.LiveTraces)
	}
	if st.Retention.EvictedCapacity == 0 {
		t.Fatal("expected continuous capacity reclamation under unique-trace load")
	}
	if got := st.Retention.Lifecycles - st.Retention.EvictedCapacity - st.Retention.EvictedTTL; got != int64(st.Retention.LiveTraces) {
		t.Fatalf("lifecycle accounting broken: lifecycles-evicted=%d live=%d stats=%+v",
			got, st.Retention.LiveTraces, st.Retention)
	}
	// No admitted span is lost or duplicated, even while reclamation runs.
	seen, _ := rep.counts()
	if int64(len(seen)) != enqueued.Load() {
		t.Fatalf("delivered=%d admitted=%d (admitted must not be lost)", len(seen), enqueued.Load())
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("span %v delivered %d times, want exactly 1", k, n)
		}
	}
	if !strings.Contains(logger.String(), "decision=") {
		t.Fatal("logs missing per-span decision markers")
	}
	t.Logf("retention after load: %+v", st.Retention)
}

// A trace reclaimed by retention and seen again must be deterministic and
// distinguishable: while its span is still export-protected the original
// conclusion stands (no resubmission); once protection expires a new
// lifecycle (generation 2) begins and the span may be exported again.
func TestRevisitAfterReclaim(t *testing.T) {
	cfg := export.Config{
		QueueCapacity: 4, BatchMax: 4, FlushInterval: 2 * time.Millisecond,
		MaxAttempts: 1, DeadLetterCapacity: 64,
	}
	// recentCap = 4*QueueCapacity = 16 delivered keys of dedup protection.
	ret := retention.Config{MaxTraces: 2, TTL: time.Hour, TombstoneCap: 64, SweepBatch: 4}
	logger := newLogger(t)
	rep := newCaptureReporter()
	p := newWithReporterRetention(logger.logf, rep, cfg, 1.0, ret)
	p.Start()
	defer p.Close(context.Background())

	flush := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := p.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	delivered := func(tidHex, sidHex string) int {
		seen, _ := rep.counts()
		tid, _ := model.ParseTraceID(tidHex)
		sid, _ := model.ParseSpanID(sidHex)
		return seen[model.SpanKey{Trace: tid, Span: sid}]
	}

	const tidT = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
	const sidS1 = "00000000000000a1"

	// Lifecycle 1: T/S1 is sampled and delivered.
	if res := p.Submit(raw(tidT, sidS1, "")); !res.Enqueued || res.Generation != 1 {
		t.Fatalf("first submit=%+v", res)
	}
	flush()
	if got := delivered(tidT, sidS1); got != 1 {
		t.Fatalf("S1 delivered %d times, want 1", got)
	}

	// Reclaim T's lifecycle via the capacity cap (MaxTraces=2).
	p.Submit(raw("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb1", "00000000000000b1", ""))
	p.Submit(raw("ccccccccccccccccccccccccccccccc1", "00000000000000c1", ""))
	flush()
	if st := p.Stats(); st.Retention.EvictedCapacity == 0 {
		t.Fatal("expected T's lifecycle to be reclaimed by the capacity cap")
	}

	// Revisit while S1 is still inside the exporter's dedup window: the
	// original conclusion stands, nothing is resubmitted, no new lifecycle.
	res := p.Submit(raw(tidT, sidS1, ""))
	if !res.Outcome.Revisit || !res.Outcome.Protected || res.Outcome.Status != model.ClassDuplicate {
		t.Fatalf("protected revisit outcome=%+v", res.Outcome)
	}
	if res.Enqueued {
		t.Fatal("protected revisit must not be resubmitted")
	}
	flush()
	if got := delivered(tidT, sidS1); got != 1 {
		t.Fatalf("protected revisit resubmitted S1: delivered=%d", got)
	}
	if st := p.Stats(); st.ProtectedRevisits != 1 {
		t.Fatalf("protected revisits=%d want 1", st.ProtectedRevisits)
	}

	// Push enough distinct spans through the exporter to age S1 out of the
	// bounded recent-delivered ring (16 keys).
	for i := 0; i < 20; i++ {
		p.Submit(raw(fmt.Sprintf("dddddddddddddddddddddddddddd%04x", i+1), fmt.Sprintf("000000000000d%03x", i+1), ""))
	}
	flush()
	if p.Exporter().IsProtected(model.SpanKey{Trace: mustTrace(t, tidT), Span: mustSpan(t, sidS1)}) {
		t.Fatal("S1 should have aged out of the dedup protection window")
	}

	// Revisit after protection expiry: a new lifecycle (generation 2)
	// begins, externally distinguishable, and the span may export again.
	res = p.Submit(raw(tidT, sidS1, ""))
	if res.Outcome.Status != model.ClassRevisit || !res.Revisit || res.Generation != 2 {
		t.Fatalf("revisit outcome=%+v result=%+v", res.Outcome, res)
	}
	if !res.Enqueued || res.Decision != model.DecisionSample {
		t.Fatalf("new lifecycle must be sampled and enqueued: %+v", res)
	}
	flush()
	if got := delivered(tidT, sidS1); got != 2 {
		t.Fatalf("S1 delivered %d times across two lifecycles, want 2", got)
	}

	// Within generation 2 the span is a plain duplicate again: no third
	// delivery, no confusion with other traces.
	res = p.Submit(raw(tidT, sidS1, ""))
	if res.Outcome.Status != model.ClassDuplicate || res.Outcome.Revisit {
		t.Fatalf("duplicate inside generation 2: %+v", res.Outcome)
	}
	flush()
	if got := delivered(tidT, sidS1); got != 2 {
		t.Fatalf("duplicate inside lifecycle delivered again: %d", got)
	}

	st := p.Stats()
	if st.Revisits != 1 || st.Retention.Revisits != 1 {
		t.Fatalf("revisits pipeline=%d retention=%d, want 1/1", st.Revisits, st.Retention.Revisits)
	}
	logs := logger.String()
	for _, marker := range []string{"revisit=protected", "revisit=new-lifecycle", "gen=2"} {
		if !strings.Contains(logs, marker) {
			t.Fatalf("logs missing %q:\n%s", marker, logs)
		}
	}
	t.Logf("final retention stats: %+v", st.Retention)
}

func mustTrace(t *testing.T, hex string) model.TraceID {
	t.Helper()
	id, err := model.ParseTraceID(hex)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func mustSpan(t *testing.T, hex string) model.SpanID {
	t.Helper()
	id, err := model.ParseSpanID(hex)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
