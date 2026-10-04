package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/tracing-pipeline-go/internal/export"
	"github.com/highcumontoa/tracing-pipeline-go/internal/model"
	"github.com/highcumontoa/tracing-pipeline-go/internal/retention"
)

// smallRetention returns tight retention bounds so reclamation happens
// continuously during the test.
func smallRetention() retention.Config {
	return retention.Config{MaxEntries: 64, TTL: time.Hour, JournalCapacity: 512}
}

// Concurrent heavy writes of unique traces while retention continuously
// reclaims: the hard state bound holds at all times, reclamation is
// counted and queryable, and admitted spans are still delivered exactly
// once (no loss, no duplication) despite the churn.
func TestConcurrentWritesWithContinuousEviction(t *testing.T) {
	cfg := export.Config{
		QueueCapacity: 512, BatchMax: 64, FlushInterval: 2 * time.Millisecond,
		MaxAttempts: 1, DeadLetterCapacity: 512,
	}
	logger := newLogger(t)
	rep := newCaptureReporter()
	p := newWithReporterRetention(logger.logf, rep, cfg, 1.0, smallRetention())
	p.Start()
	defer p.Close(context.Background())

	const spans = 3000 // far beyond the retention cap of 64
	var wg sync.WaitGroup
	for i := 1; i <= spans; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			tid := fmt.Sprintf("%032x", 50000+n)
			sid := fmt.Sprintf("%016x", 50000+n)
			submitAdmit(p, raw(tid, sid, ""))
		}(i)
	}
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Exactly-once delivery is unaffected by reclamation churn.
	seen, _ := rep.counts()
	st := p.Stats()
	if int64(len(seen)) != st.Export.Enqueued {
		t.Fatalf("delivered=%d admitted=%d (admitted must not be lost)", len(seen), st.Export.Enqueued)
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("span %v delivered %d times, want exactly 1", k, n)
		}
	}

	// The hard bound held throughout: retained never exceeds capacity,
	// and reclamation ran continuously under the unique-trace flood.
	rt := st.Retention
	for name, s := range map[string]retention.Stats{
		"ingest": rt.IngestSpans, "sampler-decisions": rt.SamplerDecisions,
		"sampler-span-verdicts": rt.SamplerSpanVerdicts,
	} {
		if s.Retained > s.Capacity {
			t.Fatalf("%s retained=%d exceeds capacity=%d", name, s.Retained, s.Capacity)
		}
		if s.EvictedTotal == 0 {
			t.Fatalf("%s must have reclaimed under %d unique traces", name, spans)
		}
	}
	if !strings.Contains(logger.String(), "retention evict") {
		t.Fatal("logs missing reclamation lines")
	}
	t.Logf("retention: ingest=%+v decisions=%+v", rt.IngestSpans, rt.SamplerDecisions)
}

// Revisit after reclamation: a trace whose runtime state was reclaimed
// deterministically starts a new, externally distinguishable lifecycle.
// While the exporter's duplicate protection still covers the span it
// cannot be resubmitted; only after that protection expires may the new
// lifecycle be admitted again.
func TestRevisitAfterEviction(t *testing.T) {
	cfg := export.Config{
		QueueCapacity: 64, BatchMax: 8, FlushInterval: 2 * time.Millisecond,
		MaxAttempts: 1, DeadLetterCapacity: 64,
	}
	rcfg := retention.Config{MaxEntries: 8, TTL: time.Hour, JournalCapacity: 4096}
	logger := newLogger(t)
	rep := newCaptureReporter()
	p := newWithReporterRetention(logger.logf, rep, cfg, 1.0, rcfg)
	p.Start()
	defer p.Close(context.Background())

	tidA := fmt.Sprintf("%032x", 777)
	sidA1 := fmt.Sprintf("%016x", 77701)

	// Trace A, span 1: first lifecycle, delivered once.
	res := p.Submit(raw(tidA, sidA1, ""))
	if !res.Enqueued || res.Revisit {
		t.Fatalf("first submit: %+v", res)
	}
	flush(t, p)

	// Churn enough unique traces to reclaim A's ingest/sampler state
	// (cap 8), but far fewer than the exporter's recent ring
	// (4*QueueCapacity = 256), so A's span stays duplicate-protected.
	for i := 1; i <= 16; i++ {
		submitAdmit(p, raw(fmt.Sprintf("%032x", 800+i), fmt.Sprintf("%016x", 80000+i), ""))
	}
	flush(t, p)

	// Revisit while still inside export-side duplicate protection:
	// classified as a revisit (new lifecycle at ingest/sampling), but the
	// exporter refuses to re-deliver the still-protected span.
	res = p.Submit(raw(tidA, sidA1, ""))
	if !res.Revisit {
		t.Fatalf("expected revisit classification, got %+v", res)
	}
	if res.Enqueued || res.ExportFailure != model.FailDuplicate {
		t.Fatalf("protected span must not be resubmittable: %+v", res)
	}
	assertDelivered(t, rep, tidA, sidA1, 1)

	// The protected resubmit above reopened A's lifecycle at the sampler;
	// churn once more so its runtime state is reclaimed again before the
	// late child arrives.
	for i := 1; i <= 16; i++ {
		submitAdmit(p, raw(fmt.Sprintf("%032x", 850+i), fmt.Sprintf("%016x", 85000+i), ""))
	}
	flush(t, p)

	// A late child span of the reclaimed trace (never seen before):
	// sampled under the new lifecycle and delivered; the revisit is
	// distinguishable in result, stats and logs.
	sidA2 := fmt.Sprintf("%016x", 77702)
	res = submitAdmit(p, raw(tidA, sidA2, sidA1))
	if !res.Revisit || !res.Enqueued {
		t.Fatalf("late child must reopen a sampled lifecycle: %+v", res)
	}
	flush(t, p)
	assertDelivered(t, rep, tidA, sidA2, 1)

	// Push the original span beyond the exporter's duplicate-protection
	// horizon (recent ring holds 4*QueueCapacity = 256 keys).
	for i := 1; i <= 300; i++ {
		submitAdmit(p, raw(fmt.Sprintf("%032x", 900+i), fmt.Sprintf("%016x", 90000+i), ""))
	}
	flush(t, p)

	// Now the same span may start a genuinely new lifecycle end-to-end:
	// admitted and delivered a second time, still flagged as a revisit.
	res = p.Submit(raw(tidA, sidA1, ""))
	if !res.Revisit || !res.Enqueued {
		t.Fatalf("post-protection revisit must be admitted: %+v", res)
	}
	flush(t, p)
	assertDelivered(t, rep, tidA, sidA1, 2)

	st := p.Stats()
	if st.Retention.Revisits < 3 {
		t.Fatalf("revisits=%d want >= 3", st.Retention.Revisits)
	}
	if st.Retention.SamplerDecisions.EvictedTotal == 0 || st.Retention.IngestSpans.EvictedTotal == 0 {
		t.Fatalf("reclamation counters must be queryable: %+v", st.Retention)
	}
	logs := logger.String()
	if !strings.Contains(logs, "lifecycle=revisit") || !strings.Contains(logs, "retention evict") {
		t.Fatalf("logs missing revisit/reclamation markers:\n%s", logs)
	}
	t.Logf("revisits=%d retention=%+v", st.Retention.Revisits, st.Retention.SamplerDecisions)
}

// Default configuration must already satisfy the hard state bound, with
// retention queryable through the existing Stats entry point.
func TestDefaultRetentionIsBounded(t *testing.T) {
	p, _, _ := buildPipeline(t, export.DefaultConfig(), 1.0)
	p.Start()
	defer p.Close(context.Background())

	for i := 1; i <= 500; i++ {
		p.Submit(raw(fmt.Sprintf("%032x", 60000+i), fmt.Sprintf("%016x", 60000+i), ""))
	}
	rt := p.Stats().Retention
	for name, s := range map[string]retention.Stats{
		"ingest": rt.IngestSpans, "sampler-decisions": rt.SamplerDecisions,
		"sampler-span-verdicts": rt.SamplerSpanVerdicts,
	} {
		if s.Capacity <= 0 {
			t.Fatalf("%s: default retention must be bounded", name)
		}
		if s.Retained > s.Capacity {
			t.Fatalf("%s retained=%d exceeds capacity=%d", name, s.Retained, s.Capacity)
		}
	}
	ex := p.Stats().Export
	if ex.RecentCap <= 0 || ex.PendingLen < 0 || ex.RecentLen > ex.RecentCap {
		t.Fatalf("export dedup bookkeeping must be bounded and queryable: %+v", ex)
	}
}

func flush(t *testing.T, p *Pipeline) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}

func assertDelivered(t *testing.T, rep *captureReporter, tidHex, sidHex string, want int) {
	t.Helper()
	tid, _ := model.ParseTraceID(tidHex)
	sid, _ := model.ParseSpanID(sidHex)
	seen, _ := rep.counts()
	if got := seen[model.SpanKey{Trace: tid, Span: sid}]; got != want {
		t.Fatalf("span %s/%s delivered %d times, want %d", tidHex, sidHex, got, want)
	}
}
