package export

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/tracing-pipeline-go/internal/model"
)

// recordingReporter is a deterministic reporter for exporter tests.
type recordingReporter struct {
	mu       sync.Mutex
	batches  [][]*model.Span
	failNext map[int]bool // batch attempt failures are scripted per call
	calls    int
}

func (r *recordingReporter) Report(ctx context.Context, batch []*model.Span) ReportResult {
	r.mu.Lock()
	r.calls++
	call := r.calls
	r.batches = append(r.batches, append([]*model.Span(nil), batch...))
	fail := r.failNext[call]
	r.mu.Unlock()
	if fail {
		return ReportResult{Failure: model.FailEndpointError, Message: "scripted"}
	}
	return ReportResult{OK: true}
}

func (r *recordingReporter) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *recordingReporter) totalSpansSeen() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, b := range r.batches {
		n += len(b)
	}
	return n
}

func mustSpan(t *testing.T, n int) *model.Span {
	t.Helper()
	tid, err := model.ParseTraceID(hex32(n))
	if err != nil {
		t.Fatal(err)
	}
	sid, err := model.ParseSpanID(hex16(n))
	if err != nil {
		t.Fatal(err)
	}
	return &model.Span{TraceID: tid, SpanID: sid}
}

func hex32(n int) string {
	const digits = "0123456789abcdef"
	b := make([]byte, 32)
	for i := 31; i >= 0; i-- {
		b[i] = digits[n&0xf]
		n >>= 4
	}
	return string(b)
}

func hex16(n int) string {
	s := hex32(n)
	return s[16:]
}

func TestEnqueueDeliversExactlyOnce(t *testing.T) {
	rep := &recordingReporter{}
	e := New(Config{QueueCapacity: 8, BatchMax: 4, FlushInterval: 50 * time.Millisecond, MaxAttempts: 1, RetryBackoff: 0, DeadLetterCapacity: 8}, rep, nil)
	e.Start()
	for i := 1; i <= 4; i++ {
		if r := e.Enqueue(context.Background(), mustSpan(t, i)); !r.Accepted {
			t.Fatalf("enqueue %d rejected: %s", i, r.Reason)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if rep.totalSpansSeen() != 4 {
		t.Fatalf("reporter saw %d spans, want 4", rep.totalSpansSeen())
	}

	// Duplicate re-enqueue after delivery: rejected, never sent twice.
	if r := e.Enqueue(context.Background(), mustSpan(t, 1)); r.Accepted || r.Reason != model.FailDuplicate {
		t.Fatalf("dup enqueue result=%+v", r)
	}
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if rep.totalSpansSeen() != 4 {
		t.Fatalf("reporter saw %d after duplicate, want still 4", rep.totalSpansSeen())
	}
	if e.Stats().RejectedDup != 1 {
		t.Fatalf("dup counter=%d", e.Stats().RejectedDup)
	}
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// A slow/failing endpoint with a tiny queue must produce bounded
// FailQueueFull rejections instead of blocking producers forever, and no
// accepted span may be lost silently.
func TestBoundedBackpressure(t *testing.T) {
	release := make(chan struct{})
	blocking := &blockingReporter{gate: release}
	cfg := Config{QueueCapacity: 4, BatchMax: 1, FlushInterval: time.Hour, MaxAttempts: 1, DeadLetterCapacity: 4, RetryBackoff: 0}
	e := New(cfg, blocking, nil)
	e.Start()

	// Fill worker (1) + queue (4). The first Enqueue triggers immediate
	// delivery because BatchMax=1 once the worker picks it; to force
	// saturation we admit 5 and then attempt more.
	for i := 1; i <= 5; i++ {
		e.Enqueue(context.Background(), mustSpan(t, i))
	}
	// Give the worker a moment to pick one span, then hammer the queue.
	time.Sleep(10 * time.Millisecond)
	full := 0
	for i := 6; i <= 40; i++ {
		r := e.Enqueue(context.Background(), mustSpan(t, i))
		if !r.Accepted {
			if r.Reason != model.FailQueueFull {
				t.Fatalf("unexpected reject reason %s", r.Reason)
			}
			full++
		}
	}
	if full == 0 {
		t.Fatal("expected bounded FailQueueFull rejections while endpoint stuck")
	}

	// Producers must never be blocked for long: the loop above finished
	// quickly. Now release and verify admitted spans all get accounted.
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	st := e.Stats()
	admitted := st.SentSpans + st.DeadLettered + st.DeadLetterDrops
	if admitted != st.Enqueued {
		t.Fatalf("accounting: admitted=%d enqueued=%d stats=%+v", admitted, st.Enqueued, st)
	}
	if st.RejectedFull != int64(full) {
		t.Fatalf("rejectedFull=%d full=%d", st.RejectedFull, full)
	}
}

type blockingReporter struct {
	gate <-chan struct{}
}

func (b *blockingReporter) Report(ctx context.Context, batch []*model.Span) ReportResult {
	select {
	case <-b.gate:
		return ReportResult{OK: true}
	case <-ctx.Done():
		return ReportResult{Failure: model.FailEndpointTimeout, Message: ctx.Err().Error()}
	}
}

// Exhausting attempts moves spans deterministically into the bounded
// dead-letter ring exactly once and counts them.
func TestRetryThenDeadLetter(t *testing.T) {
	rep := &recordingReporter{failNext: map[int]bool{1: true, 2: true, 3: true}}
	cfg := Config{QueueCapacity: 8, BatchMax: 2, FlushInterval: time.Hour, MaxAttempts: 3, RetryBackoff: time.Millisecond, DeadLetterCapacity: 8}
	e := New(cfg, rep, nil)
	e.Start()
	for i := 1; i <= 2; i++ {
		e.Enqueue(context.Background(), mustSpan(t, i))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	st := e.Stats()
	if st.DeadLettered != 2 || st.SentSpans != 0 || st.Retries != 2 {
		t.Fatalf("stats=%+v", st)
	}
	dls := e.DeadLetters()
	if len(dls) != 2 {
		t.Fatalf("dead letters=%d", len(dls))
	}
	for _, dl := range dls {
		if dl.Failure != model.FailEndpointError || dl.Attempts != 3 {
			t.Fatalf("dl=%+v", dl)
		}
	}
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// The dead-letter ring is bounded; overflow drops the oldest entry
// deterministically and counts it.
func TestDeadLetterRingBounded(t *testing.T) {
	cfg := Config{QueueCapacity: 8, BatchMax: 2, FlushInterval: time.Hour, MaxAttempts: 1, RetryBackoff: 0, DeadLetterCapacity: 2}
	e := New(cfg, alwaysFailReporter{}, nil)
	e.Start()
	for i := 1; i <= 6; i++ {
		e.Enqueue(context.Background(), mustSpan(t, i))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	st := e.Stats()
	if st.DeadLettered != 6 || st.DeadLetterDrops != 4 {
		t.Fatalf("stats=%+v", st)
	}
	if len(e.DeadLetters()) != 2 {
		t.Fatal("dead-letter ring must stay bounded at 2")
	}
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

type alwaysFailReporter struct{}

func (alwaysFailReporter) Report(context.Context, []*model.Span) ReportResult {
	return ReportResult{Failure: model.FailEndpointError}
}

// After a span has been dead-lettered it cannot be enqueued again.
func TestDeadLetterIdempotent(t *testing.T) {
	rep := &recordingReporter{failNext: map[int]bool{1: true}}
	cfg := Config{QueueCapacity: 8, BatchMax: 1, FlushInterval: time.Hour, MaxAttempts: 1, RetryBackoff: 0, DeadLetterCapacity: 8}
	e := New(cfg, rep, nil)
	e.Start()
	sp := mustSpan(t, 1)
	e.Enqueue(context.Background(), sp)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if r := e.Enqueue(context.Background(), sp); r.Accepted || r.Reason != model.FailDuplicate {
		t.Fatalf("re-enqueue of dead letter = %+v", r)
	}
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
