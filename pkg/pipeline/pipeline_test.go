package pipeline_test

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/tracing-pipeline-go/pkg/export"
	"github.com/highcumontoa/tracing-pipeline-go/pkg/model"
	"github.com/highcumontoa/tracing-pipeline-go/pkg/normalize"
	"github.com/highcumontoa/tracing-pipeline-go/pkg/pipeline"
)

// ---- trace fixtures -----------------------------------------------------

func traceIDHex(n int) string { return fmt.Sprintf("%032x", uint64(n)*6364136223846793005+1) }
func spanIDHex(n int) string  { return fmt.Sprintf("%016x", uint64(n)*2654435761+1) }

func mustTID(n int) model.TraceID {
	id, err := model.ParseTraceID(traceIDHex(n))
	if err != nil {
		panic(err)
	}
	return id
}

// familyBatch builds a deterministic whole trace: 1 root + children spans.
func familyBatch(n, children int) []normalize.HTTPSpanV1 {
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC).UnixNano() + int64(n)
	tid := traceIDHex(n)
	root := normalize.HTTPSpanV1{
		TraceID: tid, SpanID: spanIDHex(n*100 + 1), Name: "root", Kind: "server",
		StartUnixNano: base, EndUnixNano: base + 1_000,
	}
	out := []normalize.HTTPSpanV1{root}
	for c := 0; c < children; c++ {
		out = append(out, normalize.HTTPSpanV1{
			TraceID:       tid,
			SpanID:        spanIDHex(n*100 + c + 2),
			ParentSpanID:  root.SpanID,
			Name:          fmt.Sprintf("child-%d", c),
			Kind:          "client",
			StartUnixNano: base + int64(c+1),
			EndUnixNano:   base + int64(c+1) + 10,
		})
	}
	return out
}

func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting: %s", msg)
}

func startPipeline(t *testing.T, cfg pipeline.Config, rep *export.MockReporter) (*pipeline.Pipeline, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	p := pipeline.New(cfg, rep)
	p.Start(ctx)
	return p, cancel
}

// TestConcurrentSamplingConsistencyAndNoLossNoDup is the headline concurrency
// test: many goroutines across all three ingestion entries write whole traces;
// every trace is either delivered whole exactly once or delivered zero times
// (sampled out / back-pressured), and per-trace span counts match.
func TestConcurrentSamplingConsistencyAndNoLossNoDup(t *testing.T) {
	const goroutines = 24
	const tracesPerG = 40
	const children = 4

	rep := export.NewMockReporter(export.MockReporterConfig{Available: true})
	p, cancel := startPipeline(t, pipeline.Config{
		Sampling: cfgSampling(1.0, 0), // keep everything to check exact delivery
		Export:   export.Config{QueueCap: 256, DLQCap: 64, MaxAttempts: 1},
	}, rep)
	defer func() {
		cancel()
		_ = p.Shutdown(shutdownCtx())
	}()

	var wg sync.WaitGroup
	var expectedKept, expectedSpans int64
	var mu sync.Mutex
	expectedKeptTraces := map[string]bool{}

	total := goroutines * tracesPerG
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < tracesPerG; i++ {
				n := g*tracesPerG + i + 1
				batch := familyBatch(n, children) // 1 root + children
				// Blocking-but-interruptible submission: producers wait for
				// capacity instead of losing data, so exact delivery is
				// checkable even with a small bounded queue.
				var err error
				switch g % 3 { // exercise every entry point
				case 0:
					_, err = p.SubmitHTTP(context.Background(), batch)
				case 1:
					_, err = p.SubmitW3C(context.Background(), httpToW3C(t, batch))
				case 2:
					_, err = p.SubmitZipkin(context.Background(), httpToZipkin(t, batch))
				}
				if err != nil {
					t.Errorf("submit failed: %v", err)
					return
				}
				mu.Lock()
				expectedKept++
				expectedSpans += int64(children + 1)
				expectedKeptTraces[traceIDHex(n)] = true
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()

	waitFor(t, 5*time.Second, func() bool { return int64(rep.OKs()) == expectedKept },
		fmt.Sprintf("all %d traces delivered (got %d)", expectedKept, rep.OKs()))

	if got := rep.DeliveredSpans(); int64(got) != expectedSpans {
		t.Fatalf("span count: delivered=%d expected=%d (loss or duplication)", got, expectedSpans)
	}
	// Exact set equality: each expected trace delivered once, no phantom trace.
	gotIDs := rep.DeliveredTraceIDs()
	if len(gotIDs) != total {
		t.Fatalf("unique delivered traces %d != expected %d", len(gotIDs), total)
	}
	for _, id := range gotIDs {
		if !expectedKeptTraces[id.String()] {
			t.Fatalf("unexpected/duplicate trace delivered: %s", id)
		}
		if n := rep.DeliveredSpanCountFor(id); n != children+1 {
			t.Fatalf("trace %s delivered partial spans: %d", id, n)
		}
	}
	// Every family delivered whole => parent/child sampling consistency under
	// concurrency is implied (children never lost independently).
	snap := p.Counters().Snapshot()
	if snap.Accepted != expectedSpans || snap.SampledIn != expectedSpans || snap.SampledOut != 0 {
		t.Fatalf("counter accounting wrong: %+v expectedSpans=%d", snap, expectedSpans)
	}
	log.Printf("CONCURRENCY: goroutines=%d traces=%d spans=%d delivered=%d unique=%d duplicate_batches=%d",
		goroutines, expectedKept, expectedSpans, rep.OKs(), len(gotIDs), snap.Duplicates)
}

// TestSampledOutTracesProduceNoSpans verifies with ratio 0 that nothing at all
// is exported, counters partition accepted == in + out, and no partial trace.
func TestSampledOutTracesProduceNoSpans(t *testing.T) {
	rep := export.NewMockReporter(export.MockReporterConfig{Available: true})
	p, cancel := startPipeline(t, pipeline.Config{
		Sampling: cfgSampling(0, 0),
		Export:   export.Config{QueueCap: 16, DLQCap: 16, MaxAttempts: 1},
	}, rep)
	defer func() { cancel(); _ = p.Shutdown(shutdownCtx()) }()

	const children = 3
	for n := 1; n <= 50; n++ {
		r := p.IngestHTTP(familyBatch(n, children))
		if len(r.EnqueuedTraces) != 0 {
			t.Fatalf("dropped trace %d enqueued spans", n)
		}
	}
	time.Sleep(50 * time.Millisecond)
	if rep.OKs() != 0 || rep.DeliveredSpans() != 0 {
		t.Fatalf("sampled-out data reached reporter: oks=%d spans=%d", rep.OKs(), rep.DeliveredSpans())
	}
	snap := p.Counters().Snapshot()
	if snap.Accepted != snap.SampledIn+snap.SampledOut {
		t.Fatalf("accepted(%d) != in(%d)+out(%d)", snap.Accepted, snap.SampledIn, snap.SampledOut)
	}
	log.Printf("trace=* decision=DROP traces=50 spans=%d exporter_deliveries=0 (no partial spans)", snap.SampledOut)
}

// TestReporterOutageThenRecoveryWalksTheFailureChain: offline reporter ->
// retries exhaust -> DLQ bounded (oldest dropped, counted) -> reporter recovers
// -> DrainDLQ redelivers survivors exactly once; counters reconcile.
func TestReporterOutageThenRecoveryWalksTheFailureChain(t *testing.T) {
	rep := export.NewMockReporter(export.MockReporterConfig{Available: false})
	const maxAttempts = 2
	const dlqCap = 3
	p, cancel := startPipeline(t, pipeline.Config{
		Sampling: cfgSampling(1.0, 0),
		Export: export.Config{
			QueueCap: 16, DLQCap: dlqCap, MaxAttempts: maxAttempts,
			Backoff: time.Millisecond,
		},
	}, rep)
	defer func() { cancel(); _ = p.Shutdown(shutdownCtx()) }()

	const traces = 6
	for n := 1; n <= traces; n++ {
		r := p.IngestHTTP(familyBatch(n, 1))
		if len(r.EnqueuedTraces) != 1 {
			t.Fatalf("trace %d not enqueued", n)
		}
	}

	// All fail maxAttempts times; DLQ keeps only the newest dlqCap; oldest
	// traces are deterministically dropped and counted.
	waitFor(t, 3*time.Second, func() bool {
		s := p.Counters().Snapshot()
		return s.ExportFailures == traces && s.DLQEnqueued == traces
	}, "all failures accounted")

	snap := p.Counters().Snapshot()
	if snap.ExportAttempts != traces*maxAttempts {
		t.Fatalf("attempts=%d want %d", snap.ExportAttempts, traces*maxAttempts)
	}
	if snap.DroppedDLQFull != traces-dlqCap {
		t.Fatalf("dropped=%d want %d", snap.DroppedDLQFull, traces-dlqCap)
	}
	if len(p.Exporter().DLQ()) != dlqCap {
		t.Fatalf("dlq len=%d want %d", len(p.Exporter().DLQ()), dlqCap)
	}
	log.Printf("failure_class=unavailable attempts=%d failures=%d dlq=%d dropped_oldest=%d",
		snap.ExportAttempts, snap.ExportFailures, dlqCap, snap.DroppedDLQFull)

	// Recover and redrive; the three retained traces deliver exactly once.
	rep.SetAvailable(true)
	red, rem := p.Exporter().DrainDLQ(context.Background())
	if red != dlqCap || rem != 0 {
		t.Fatalf("drain redelivered=%d remaining=%d", red, rem)
	}
	red2, _ := p.Exporter().DrainDLQ(context.Background())
	if red2 != 0 {
		t.Fatalf("DLQ redelivery duplicated %d batches", red2)
	}
	survivors := map[string]bool{}
	for n := traces - dlqCap + 1; n <= traces; n++ {
		survivors[traceIDHex(n)] = true
	}
	for _, id := range rep.DeliveredTraceIDs() {
		if !survivors[id.String()] {
			t.Fatalf("non-survivor trace %s delivered (wrong drop policy)", id)
		}
		log.Printf("trace=%s failure_class=unavailable -> recovered, redelivered=once spans=2", id)
	}
	snap = p.Counters().Snapshot()
	if snap.DLQDrained != int64(dlqCap) || snap.DLQDepth != 0 || rep.OKs() != dlqCap {
		t.Fatalf("post-recovery accounting wrong: %+v oks=%d", snap, rep.OKs())
	}
	// Global reconciliation identity.
	if snap.Ingested != snap.Accepted+snap.Rejected {
		t.Fatalf("ingested=%d != accepted=%d+rejected=%d", snap.Ingested, snap.Accepted, snap.Rejected)
	}
}

// TestBackpressureVisibleAtPipelineEntry: a slow reporter fills the bounded
// queue; further ingests report QueueFullTraces instead of blocking forever;
// admitted data later arrives exactly once.
func TestBackpressureVisibleAtPipelineEntry(t *testing.T) {
	rep := export.NewMockReporter(export.MockReporterConfig{
		Available: true, Latency: 30 * time.Millisecond,
	})
	const qcap = 4
	p, cancel := startPipeline(t, pipeline.Config{
		Sampling: cfgSampling(1.0, 0),
		Export:   export.Config{QueueCap: qcap, DLQCap: 4, MaxAttempts: 1},
	}, rep)
	defer func() { cancel(); _ = p.Shutdown(shutdownCtx()) }()

	queueFull := 0
	enqueued := map[string]bool{}
	for n := 1; n <= 30; n++ {
		r := p.IngestHTTP(familyBatch(n, 1))
		if len(r.QueueFullTraces) > 0 {
			queueFull++
		}
		for _, id := range r.EnqueuedTraces {
			enqueued[id.String()] = true
		}
	}
	if queueFull == 0 {
		t.Fatal("expected back-pressure (QueueFullTraces) under slow reporter")
	}
	rep.SetLatency(0)
	waitFor(t, 5*time.Second, func() bool { return rep.OKs() == len(enqueued) }, "drain after recovery")
	for _, id := range rep.DeliveredTraceIDs() {
		if !enqueued[id.String()] {
			t.Fatalf("unadmitted trace %s delivered", id)
		}
	}
	log.Printf("backpressure entry: enqueued=%d refused_queue_full=%d delivered_exact_once=%d",
		len(enqueued), queueFull, rep.OKs())
}

// TestClassificationAtEntry logs and verifies the distinguishable rejection
// classes surface through the pipeline facade and counters.
func TestClassificationAtEntry(t *testing.T) {
	rep := export.NewMockReporter(export.MockReporterConfig{Available: true})
	p, cancel := startPipeline(t, pipeline.Config{
		Sampling: cfgSampling(1.0, 0),
		Export:   export.Config{QueueCap: 8, DLQCap: 4, MaxAttempts: 1},
	}, rep)
	defer func() { cancel(); _ = p.Shutdown(shutdownCtx()) }()

	base := time.Date(2026, 9, 23, 9, 0, 0, 0, time.UTC).UnixNano()
	batch := []normalize.HTTPSpanV1{
		{TraceID: traceIDHex(1), SpanID: spanIDHex(1), Name: "ok", Kind: "server",
			StartUnixNano: base, EndUnixNano: base + 1},
		{TraceID: "nope", SpanID: spanIDHex(2), Name: "no-ctx",
			StartUnixNano: base, EndUnixNano: base + 1},
		{TraceID: traceIDHex(1), SpanID: spanIDHex(3), Kind: "weird", Name: "bad-field",
			StartUnixNano: base, EndUnixNano: base + 1},
		{TraceID: traceIDHex(1), SpanID: spanIDHex(4), ParentSpanID: spanIDHex(999),
			Name: "orphan", StartUnixNano: base, EndUnixNano: base + 1},
		{TraceID: traceIDHex(1), SpanID: spanIDHex(1), Name: "dup",
			StartUnixNano: base, EndUnixNano: base + 1},
	}
	r := p.IngestHTTP(batch)
	classes := map[normalize.RejectClass]int{}
	for _, rj := range r.Normalized.Rejected {
		classes[rj.Reason]++
	}
	for _, want := range []normalize.RejectClass{
		normalize.RejectMissingContext, normalize.RejectInvalidFields,
		normalize.RejectBrokenLink, normalize.RejectDuplicateSpan,
	} {
		if classes[want] == 0 {
			t.Fatalf("missing rejection class %s: %v", want, classes)
		}
	}
	snap := p.Counters().Snapshot()
	if snap.Rejected != 4 || snap.Accepted != 1 {
		t.Fatalf("rejected=%d accepted=%d", snap.Rejected, snap.Accepted)
	}
	log.Printf("entry classification: 1 healthy delivered, 4 rejected in distinct classes=%v", classes)
}

// ---- helpers -------------------------------------------------------------
