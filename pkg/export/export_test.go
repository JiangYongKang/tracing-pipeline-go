package export

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/tracing-pipeline-go/pkg/metrics"
	"github.com/highcumontoa/tracing-pipeline-go/pkg/model"
)

func mkTraceID(t *testing.T, n int) model.TraceID {
	t.Helper()
	id, err := model.ParseTraceID(id32(n))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func id32(n int) string {
	const hex = "0123456789abcdef"
	b := make([]byte, 32)
	x := uint64(n)*6364136223846793005 + 1442695040888963407
	for i := 0; i < 16; i++ {
		b[i] = hex[(x>>(uint(i)*4))&0xf]
	}
	for i := 0; i < 16; i++ {
		b[16+i] = hex[(x>>(uint(15-i)*4))&0xf]
	}
	if string(b) == "00000000000000000000000000000000" {
		b[31] = '1'
	}
	return string(b)
}

func mkBatch(t *testing.T, n int, spans int) (model.TraceID, []model.Span) {
	tid := mkTraceID(t, n)
	out := make([]model.SpanID, spans)
	var err error
	for i := range out {
		out[i], err = model.ParseSpanID(span16(n*1000 + i + 1))
		if err != nil {
			t.Fatal(err)
		}
	}
	s := make([]model.Span, spans)
	for i := range s {
		s[i] = model.Span{TraceID: tid, SpanID: out[i], Name: "s"}
	}
	return tid, s
}

func span16(n int) string {
	v := uint64(n)*2654435761 + 1
	const hex = "0123456789abcdef"
	b := make([]byte, 16)
	for i := range b {
		b[i] = hex[(v>>(uint(i)*4))&0xf]
	}
	return string(b)
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond * 2)
	}
	t.Fatalf("condition not met within %v", d)
}

// TestOfferBoundedQueueBackpressure: with the worker not running, filling a
// capacity-N queue causes the (N+1)th offer to fail immediately; producers
// are never blocked and memory is bounded.
func TestOfferBoundedQueueBackpressure(t *testing.T) {
	rep := NewMockReporter(MockReporterConfig{Available: true})
	exp := New(Config{QueueCap: 4, DLQCap: 2, MaxAttempts: 1}, rep, metrics.New())

	admitted := 0
	for i := 0; i < 10; i++ {
		tid, spans := mkBatch(t, i, 1)
		if exp.OfferBatch(tid, spans) {
			admitted++
		}
	}
	if admitted != 4 {
		t.Fatalf("bounded queue should admit exactly cap=4 offers, admitted=%d", admitted)
	}
	if exp.QueueDepth() != 4 {
		t.Fatalf("queue depth %d", exp.QueueDepth())
	}
	log.Printf("backpressure: 10 offers into cap=4 -> admitted=4 refused=6, producer never blocked")
}

// TestSubmitBlocksButInterruptibly: Submit blocks on a full queue and returns
// when the context is canceled, so no unbounded blocking is possible.
func TestSubmitBlocksButInterruptibly(t *testing.T) {
	rep := NewMockReporter(MockReporterConfig{Available: true})
	exp := New(Config{QueueCap: 1, DLQCap: 1, MaxAttempts: 1}, rep, metrics.New())
	tid, spans := mkBatch(t, 0, 1)
	if !exp.OfferBatch(tid, spans) {
		t.Fatal("first offer should fit")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	tid2, spans2 := mkBatch(t, 1, 1)
	start := time.Now()
	err := exp.SubmitBatch(ctx, tid2, spans2)
	if err == nil {
		t.Fatal("Submit should return ctx error on full queue")
	}
	if time.Since(start) > time.Second {
		t.Fatalf("Submit blocked too long: %v", time.Since(start))
	}
	log.Printf("trace=%s submit interrupted on full queue after %v", tid2, time.Since(start).Round(time.Millisecond))
}

// TestRetryThenDLQ: an always-failing reporter consumes MaxAttempts then the
// batch lands in the bounded DLQ; counters explain the classified failure.
func TestRetryThenDLQ(t *testing.T) {
	var calls atomic.Int32
	rep := NewMockReporter(MockReporterConfig{
		Available: true,
		Rule: func(b SpanBatch) FailureClass {
			calls.Add(1)
			return FailureUnavailable
		},
	})
	mc := metrics.New()
	exp := New(Config{QueueCap: 2, DLQCap: 4, MaxAttempts: 3, Backoff: time.Millisecond}, rep, mc)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = exp.Run(ctx)

	tid, spans := mkBatch(t, 5, 3)
	if !exp.OfferBatch(tid, spans) {
		t.Fatal("enqueue failed")
	}
	waitFor(t, time.Second, func() bool { return len(exp.DLQ()) == 1 })
	if calls.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", calls.Load())
	}
	snap := mc.Snapshot()
	if snap.ExportAttempts != 3 || snap.ExportFailures != 1 || snap.DLQEnqueued != 1 || snap.Retried != 2 {
		t.Fatalf("counters wrong: %+v", snap)
	}
	log.Printf("trace=%s failure_class=%s attempts=%d result=DLQ retried=%d",
		tid, FailureUnavailable, snap.ExportAttempts, snap.Retried)
	cancel()
	_ = exp.Close(context.Background())
}

// TestDLQOverflowDropsOldest: DLQ cap exceeded => oldest sacrificed, newest
// retained; drop counter and gauge reflect exactly that.
func TestDLQOverflowDropsOldest(t *testing.T) {
	fail := NewMockReporter(MockReporterConfig{Available: false})
	mc := metrics.New()
	exp := New(Config{QueueCap: 8, DLQCap: 2, MaxAttempts: 1}, fail, mc)
	ctx, cancel := context.WithCancel(context.Background())
	_ = exp.Run(ctx)

	for i := 0; i < 3; i++ {
		tid, spans := mkBatch(t, 10+i, 1)
		exp.OfferBatch(tid, spans)
	}
	waitFor(t, time.Second, func() bool { return mc.Snapshot().DroppedDLQFull == 1 })

	dlq := exp.DLQ()
	if len(dlq) != 2 {
		t.Fatalf("DLQ must stay bounded at 2, got %d", len(dlq))
	}
	// Oldest (n=10) was sacrificed; 11 and 12 remain (deterministic order).
	wantOldest := mkTraceID(t, 11)
	if dlq[0].TraceID != wantOldest {
		t.Fatalf("drop-oldest violated: head=%s want %s", dlq[0].TraceID, wantOldest)
	}
	snap := mc.Snapshot()
	if snap.DroppedDLQFull != 1 || snap.DLQDepth != 2 || snap.ExportFailures != 3 {
		t.Fatalf("overflow counters wrong: %+v", snap)
	}
	log.Printf("dlq overflow: oldest trace=%s dropped, retained traces=%s,%s (counted, not silent)",
		mkTraceID(t, 10), dlq[0].TraceID, dlq[1].TraceID)
	cancel()
	_ = exp.Close(context.Background())
}

// TestDrainDLQAtMostOnce: a batch in DLQ is redelivered exactly once when the
// reporter recovers; repeated draining does not duplicate it.
func TestDrainDLQAtMostOnce(t *testing.T) {
	rep := NewMockReporter(MockReporterConfig{Available: false})
	mc := metrics.New()
	exp := New(Config{QueueCap: 4, DLQCap: 4, MaxAttempts: 1}, rep, mc)
	ctx, cancel := context.WithCancel(context.Background())
	_ = exp.Run(ctx)

	tid, spans := mkBatch(t, 20, 2)
	exp.OfferBatch(tid, spans)
	waitFor(t, time.Second, func() bool { return len(exp.DLQ()) == 1 })

	// Reporter recovers; one drain delivers, the second drain finds nothing.
	rep.SetAvailable(true)
	red, rem := exp.DrainDLQ(context.Background())
	if red != 1 || rem != 0 {
		t.Fatalf("drain redelivered=%d remaining=%d", red, rem)
	}
	red2, rem2 := exp.DrainDLQ(context.Background())
	if red2 != 0 || rem2 != 0 {
		t.Fatalf("second drain must not redeliver: %d,%d", red2, rem2)
	}
	if rep.OKs() != 1 || rep.DeliveredSpanCountFor(tid) != 2 {
		t.Fatalf("reporter delivery state wrong: oks=%d spans=%d", rep.OKs(), rep.DeliveredSpanCountFor(tid))
	}
	snap := mc.Snapshot()
	if snap.DLQDrained != 1 || snap.DLQDepth != 0 {
		t.Fatalf("drain counters wrong: %+v", snap)
	}
	log.Printf("trace=%s decision=n/a redelivered=once spans=2 no-duplicate", tid)
	cancel()
	_ = exp.Close(context.Background())
}

// TestDuplicateSubmissionSuppressed: offering the same trace id twice only
// admits the first (even across queue flush), and counts the duplicate.
func TestDuplicateSubmissionSuppressed(t *testing.T) {
	rep := NewMockReporter(MockReporterConfig{Available: true})
	mc := metrics.New()
	exp := New(Config{QueueCap: 4, DLQCap: 4, MaxAttempts: 1}, rep, mc)
	ctx, cancel := context.WithCancel(context.Background())
	_ = exp.Run(ctx)

	tid, spans := mkBatch(t, 30, 1)
	if !exp.OfferBatch(tid, spans) {
		t.Fatal("first offer")
	}
	waitFor(t, time.Second, func() bool { return rep.OKs() == 1 })
	// Same trace again after successful export: suppressed as duplicate.
	if exp.OfferBatch(tid, spans) {
		t.Fatal("duplicate trace must be refused")
	}
	if snap := mc.Snapshot(); snap.Duplicates != 1 || rep.OKs() != 1 {
		t.Fatalf("duplicate accounting wrong: %+v oks=%d", snap, rep.OKs())
	}
	log.Printf("trace=%s duplicate offer suppressed, reporter deliveries=%d", tid, rep.OKs())
	cancel()
	_ = exp.Close(context.Background())
}

// TestReporterSlowBackpressure: a slow reporter fills the queue and offers
// start returning false immediately — producer never unbounded-blocked,
// memory stays at queue cap; after recovery the admitted traces are delivered
// exactly once each (no loss/no dup among admitted data).
func TestReporterSlowBackpressure(t *testing.T) {
	rep := NewMockReporter(MockReporterConfig{Available: true, Latency: 20 * time.Millisecond})
	mc := metrics.New()
	const cap = 8
	exp := New(Config{QueueCap: cap, DLQCap: 4, MaxAttempts: 1}, rep, mc)
	ctx, cancel := context.WithCancel(context.Background())
	_ = exp.Run(ctx)

	admittedIDs := map[model.TraceID]bool{}
	refused := 0
	for i := 0; i < 40; i++ {
		tid, spans := mkBatch(t, 40+i, 1)
		if exp.OfferBatch(tid, spans) {
			admittedIDs[tid] = true
		} else {
			refused++
		}
	}
	if refused == 0 {
		t.Fatal("slow reporter must cause visible back-pressure (some refuses)")
	}
	if exp.QueueDepth() > cap {
		t.Fatalf("memory unbounded: depth=%d cap=%d", exp.QueueDepth(), cap)
	}
	log.Printf("slow reporter: admitted=%d refused-by-backpressure=%d depth<=cap", len(admittedIDs), refused)

	// Recover and verify exact-once delivery of everything admitted.
	rep.SetLatency(0)
	waitFor(t, 3*time.Second, func() bool { return rep.OKs() == len(admittedIDs) })
	if rep.OKs() != len(admittedIDs) {
		t.Fatalf("delivery count %d != admitted %d", rep.OKs(), len(admittedIDs))
	}
	delivered := rep.DeliveredTraceIDs()
	if len(delivered) != len(admittedIDs) {
		t.Fatalf("unique delivered traces %d != admitted %d", len(delivered), len(admittedIDs))
	}
	for _, id := range delivered {
		if !admittedIDs[id] {
			t.Fatalf("delivered trace %s was never admitted (duplicate/phantom)", id)
		}
		if rep.DeliveredSpanCountFor(id) != 1 {
			t.Fatalf("trace %s delivered with wrong span count", id)
		}
	}
	log.Printf("exact-once verified: %d admitted traces, %d unique deliveries, zero duplicates",
		len(admittedIDs), len(delivered))
	cancel()
	if err := exp.Close(contextWithWait()); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func contextWithWait() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = cancel
	return ctx
}

// TestCloseStopsAndPreservesDLQ: shutdown drains in-flight buffers, stops the
// worker, and leaves retained DLQ entries inspectable (not reported).
func TestCloseStopsAndPreservesDLQ(t *testing.T) {
	rep := NewMockReporter(MockReporterConfig{Available: false})
	mc := metrics.New()
	exp := New(Config{QueueCap: 4, DLQCap: 4, MaxAttempts: 1}, rep, mc)
	ctx, cancel := context.WithCancel(context.Background())
	_ = exp.Run(ctx)
	tid, spans := mkBatch(t, 50, 1)
	exp.OfferBatch(tid, spans)
	waitFor(t, time.Second, func() bool { return len(exp.DLQ()) == 1 })
	cancel()

	cctx, ccancel := context.WithTimeout(context.Background(), time.Second)
	defer ccancel()
	if err := exp.Close(cctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	if len(exp.DLQ()) != 1 {
		t.Fatalf("DLQ must be preserved on shutdown, got %d", len(exp.DLQ()))
	}
	// Intake after close must fail fast with ErrClosed.
	if err := exp.SubmitBatch(context.Background(), tid, spans); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close intake err=%v want ErrClosed", err)
	}
	log.Printf("trace=%s shutdown: worker stopped, DLQ retained=1, post-close intake rejected", tid)
}

// TestConcurrentOfferNoRace is a smoke test under -race.
func TestConcurrentOfferNoRace(t *testing.T) {
	rep := NewMockReporter(MockReporterConfig{Available: true})
	mc := metrics.New()
	exp := New(Config{QueueCap: 16, DLQCap: 16, MaxAttempts: 1}, rep, mc)
	ctx, cancel := context.WithCancel(context.Background())
	_ = exp.Run(ctx)

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				tid, spans := mkBatch(t, g*1000+i, 1)
				exp.OfferBatch(tid, spans)
			}
		}(g)
	}
	wg.Wait()
	waitFor(t, 3*time.Second, func() bool { return rep.OKs() == int(mc.Snapshot().ExportSuccesses) && mc.Snapshot().QueueDepth == 0 })
	cancel()
	_ = exp.Close(contextWithWait())
	snap := mc.Snapshot()
	if snap.Duplicates < 0 || snap.ExportSuccesses != int64(rep.OKs()) {
		t.Fatalf("counter sanity failed: %+v oks=%d", snap, rep.OKs())
	}
	log.Printf("concurrent offers ok: deliveries=%d duplicates(if any)=%d", rep.OKs(), snap.Duplicates)
}

// TestSubmitDuplicateReturnsSentinel: blocking submit of an already-admitted
// trace id returns ErrDuplicate and counts the event, rather than queue-full.
func TestSubmitDuplicateReturnsSentinel(t *testing.T) {
	rep := NewMockReporter(MockReporterConfig{Available: true})
	mc := metrics.New()
	exp := New(Config{QueueCap: 4, DLQCap: 4, MaxAttempts: 1}, rep, mc)
	ctx, cancel := context.WithCancel(context.Background())
	_ = exp.Run(ctx)

	tid, spans := mkBatch(t, 70, 1)
	if err := exp.SubmitBatch(ctx, tid, spans); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	waitFor(t, time.Second, func() bool { return rep.OKs() == 1 })
	if err := exp.SubmitBatch(ctx, tid, spans); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("second submit err=%v want ErrDuplicate", err)
	}
	if mc.Snapshot().Duplicates != 1 {
		t.Fatalf("duplicate counter=%d", mc.Snapshot().Duplicates)
	}
	log.Printf("trace=%s blocking duplicate submit -> ErrDuplicate, counted", tid)
	cancel()
	_ = exp.Close(contextWithWait())
}
