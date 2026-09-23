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
)

// testLogger captures log lines while still echoing them, so the unit-test
// log contains trace id / decision / failure classification.
type testLogger struct {
	mu sync.Mutex
	b  strings.Builder
	t  *testing.T
}

func newLogger(t *testing.T) *testLogger { return &testLogger{t: t} }

func (l *testLogger) logf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	l.mu.Lock()
	l.b.WriteString(line)
	l.b.WriteByte('\n')
	l.mu.Unlock()
	l.t.Log(line)
}

func (l *testLogger) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// captureReporter records exactly which span keys the endpoint accepted,
// and can be scripted to fail.
type captureReporter struct {
	mu      sync.Mutex
	seen    map[model.SpanKey]int
	deliver func(batch []*model.Span) export.ReportResult
	batches int
}

func newCaptureReporter() *captureReporter {
	return &captureReporter{seen: map[model.SpanKey]int{}}
}

func (c *captureReporter) Report(_ context.Context, batch []*model.Span) export.ReportResult {
	c.mu.Lock()
	fn := c.deliver
	c.mu.Unlock()
	if fn != nil {
		if res := fn(batch); !res.OK {
			return res
		}
	}
	c.mu.Lock()
	c.batches++
	for _, sp := range batch {
		c.seen[sp.Key()]++
	}
	c.mu.Unlock()
	return export.ReportResult{OK: true}
}

func (c *captureReporter) counts() (map[model.SpanKey]int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[model.SpanKey]int, len(c.seen))
	for k, v := range c.seen {
		out[k] = v
	}
	return out, c.batches
}

func (c *captureReporter) setDeliver(fn func([]*model.Span) export.ReportResult) {
	c.mu.Lock()
	c.deliver = fn
	c.mu.Unlock()
}

// buildPipeline constructs a pipeline backed by a capture reporter so
// accepted spans can be counted exactly.
func buildPipeline(t *testing.T, cfg export.Config, ratio float64) (*Pipeline, *captureReporter, *testLogger) {
	t.Helper()
	logger := newLogger(t)
	rep := newCaptureReporter()
	return newWithReporter(logger.logf, rep, cfg, ratio), rep, logger
}

// submitAdmit models a producer that respects bounded backpressure: a
// queue_full verdict is retried briefly while the worker drains; any other
// verdict (duplicate, or non-running) is returned immediately. The retry
// re-enqueues the already-normalized span rather than resubmitting the raw
// record (which would be a normalization duplicate): the span was valid and
// its sampling decision is frozen, only bounded admission was deferred.
func submitAdmit(p *Pipeline, r model.RawRecord) Result {
	res := p.Submit(r)
	for !res.Enqueued && res.ExportFailure == model.FailQueueFull {
		time.Sleep(time.Millisecond)
		er := p.EnqueueSpan(context.Background(), res.Outcome.Span)
		res.Enqueued = er.Accepted
		res.ExportFailure = er.Reason
	}
	return res
}

func raw(tidHex, sidHex, parentHex string) model.RawRecord {
	return model.RawRecord{
		Source: model.SourceInproc, TraceID: tidHex, SpanID: sidHex,
		ParentSpanID: parentHex, Name: "span", StartNano: 1, EndNano: 2,
	}
}

func TestEndToEndHappyPath(t *testing.T) {
	cfg := export.DefaultConfig()
	p, rep, logger := buildPipeline(t, cfg, 1.0)
	p.Start()
	defer p.Close(context.Background())

	p.Submit(raw("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "1111111111111111", ""))
	p.Submit(raw("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "2222222222222222", "1111111111111111"))
	p.Submit(raw("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "zzzzzzzzzzzzzzzz", "1111111111111111")) // invalid
	p.Submit(raw("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "3333333333333333", ""))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	st := p.Stats()
	if st.ByClass[model.ClassOK] != 3 || st.ByClass[model.ClassInvalidField] != 1 {
		t.Fatalf("class counts=%+v", st.ByClass)
	}
	seen, batches := rep.counts()
	if len(seen) != 3 {
		t.Fatalf("accepted spans=%d want 3: %+v", len(seen), seen)
	}
	if batches < 1 {
		t.Fatal("expected at least one batch")
	}
	if !strings.Contains(logger.String(), "decision=sample") ||
		!strings.Contains(logger.String(), "class=invalid_field") {
		t.Fatalf("logs missing decision/class markers:\n%s", logger.String())
	}
}

// High-concurrency core acceptance: under ratio 1, every distinct sampled
// span is delivered exactly once (no duplicates, none lost).
func TestConcurrentNoDuplicateNoLoss(t *testing.T) {
	cfg := export.Config{
		QueueCapacity: 256, BatchMax: 32, FlushInterval: 5 * time.Millisecond,
		MaxAttempts: 1, DeadLetterCapacity: 256,
	}
	p, rep, _ := buildPipeline(t, cfg, 1.0)
	p.Start()
	defer p.Close(context.Background())

	const traces = 200
	const spansPerTrace = 5
	var wg sync.WaitGroup
	for tr := 1; tr <= traces; tr++ {
		tid := fmt.Sprintf("%032x", tr)
		for i := 1; i <= spansPerTrace; i++ {
			wg.Add(1)
			sid := fmt.Sprintf("%016x", tr*100+i)
			parent := ""
			if i > 1 {
				parent = fmt.Sprintf("%016x", tr*100+i-1)
			}
			go func(r model.RawRecord) {
				defer wg.Done()
				submitAdmit(p, r)
			}(raw(tid, sid, parent))
		}
	}
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	want := traces * spansPerTrace
	seen, _ := rep.counts()
	if len(seen) != want {
		t.Fatalf("distinct delivered=%d want %d", len(seen), want)
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("span %v delivered %d times, want exactly 1", k, n)
		}
	}
	st := p.Stats()
	if st.Export.SentSpans != int64(want) {
		t.Fatalf("sent spans=%d want %d stats=%+v", st.Export.SentSpans, want, st.Export)
	}
	if st.Export.RejectedDup != 0 {
		t.Fatalf("unexpected duplicate rejections=%d", st.Export.RejectedDup)
	}
}

// Endpoint fault: when the reporter fails all attempts, sampled spans are
// moved to the bounded dead-letter ring exactly once, counters are
// queryable, and nothing is reported twice.
func TestEndpointFailureDeadLettersAndCounters(t *testing.T) {
	cfg := export.Config{
		QueueCapacity: 64, BatchMax: 8, FlushInterval: 5 * time.Millisecond,
		MaxAttempts: 2, RetryBackoff: time.Millisecond, DeadLetterCapacity: 64,
	}
	p, rep, logger := buildPipeline(t, cfg, 1.0)
	// Every delivery attempt fails: all sampled spans must dead-letter
	// exactly once across the bounded number of attempts.
	rep.setDeliver(func(batch []*model.Span) export.ReportResult {
		return export.ReportResult{Failure: model.FailEndpointError, Message: "boom"}
	})
	p.Start()
	defer p.Close(context.Background())

	const n = 40
	for i := 1; i <= n; i++ {
		submitAdmit(p, raw(fmt.Sprintf("%032x", 7000+i), fmt.Sprintf("%016x", 7000+i), ""))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	st := p.Stats().Export
	// First 8 batches of 8 = 64 span-attempts fail; we only have 40 spans,
	// so they all dead-letter (5 failing batches x 8). Then none remain.
	if st.DeadLettered != int64(n) {
		t.Fatalf("deadlettered=%d want %d stats=%+v", st.DeadLettered, n, st)
	}
	if st.SentSpans != 0 {
		t.Fatalf("sent=%d want 0", st.SentSpans)
	}
	if st.Retries != int64(5) {
		t.Fatalf("retries=%d want 5", st.Retries)
	}
	if got := len(p.Exporter().DeadLetters()); got != n {
		t.Fatalf("dead letters held=%d want %d", got, n)
	}
	if !strings.Contains(logger.String(), "failure=endpoint_error") &&
		!strings.Contains(logger.String(), "class=endpoint_error") {
		// exporter logs use class=; pipeline logs use failure=
		if !strings.Contains(logger.String(), "endpoint_error") {
			t.Fatalf("logs missing endpoint_error:\n%s", logger.String())
		}
	}
	seen, _ := rep.counts()
	if len(seen) != 0 {
		t.Fatalf("failed spans must not be reported, got %d", len(seen))
	}
}

// Concurrency + sampling consistency: for each trace, the set of exported
// spans must be all-or-nothing and match the single frozen decision.
func TestConcurrentSamplingAllOrNothing(t *testing.T) {
	cfg := export.Config{
		QueueCapacity: 512, BatchMax: 64, FlushInterval: 2 * time.Millisecond,
		MaxAttempts: 1, DeadLetterCapacity: 512,
	}
	p, rep, _ := buildPipeline(t, cfg, 0.5)
	p.Start()
	defer p.Close(context.Background())

	const traces = 100
	const width = 6 // spans per trace, submitted in shuffled parent/child order
	var wg sync.WaitGroup
	for tr := 1; tr <= traces; tr++ {
		tid := fmt.Sprintf("%032x", 9000+tr)
		order := []int{3, 1, 5, 2, 6, 4} // deliberate out-of-order arrival
		for _, i := range order {
			wg.Add(1)
			sid := fmt.Sprintf("%016x", 9000*100+tr*10+i)
			parent := ""
			if i > 1 {
				parent = fmt.Sprintf("%016x", 9000*100+tr*10+i-1)
			}
			go func(r model.RawRecord) {
				defer wg.Done()
				submitAdmit(p, r)
			}(raw(tid, sid, parent))
		}
	}
	wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	seen, _ := rep.counts()
	// Group delivered spans by trace and require 0 or exactly width.
	perTrace := map[string]int{}
	for k := range seen {
		perTrace[k.Trace.String()]++
	}
	for tr := 1; tr <= traces; tr++ {
		tid := fmt.Sprintf("%032x", 9000+tr)
		got := perTrace[tid]
		if got != 0 && got != width {
			t.Fatalf("trace %s exported %d spans, want 0 or %d (all-or-nothing)", tid, got, width)
		}
	}
	sst := p.sampler.Snapshot()
	sampledTraces := 0
	for _, c := range perTrace {
		if c == width {
			sampledTraces++
		}
	}
	if int64(sampledTraces) != sst.Sampled {
		t.Fatalf("sampled traces=%d but sampler says %d (snapshot=%+v)", sampledTraces, sst.Sampled, sst)
	}
	if int64(sampledTraces)+sst.Dropped != traces {
		t.Fatalf("sampled+dropped=%d want %d", int64(sampledTraces)+sst.Dropped, traces)
	}
}

// Bounded backpressure under a stuck endpoint: producers are rejected with
// queue_full quickly (never hang), rejected count is queryable, and all
// eventually-admitted spans are later delivered without loss/duplication.
func TestBackpressureUnderStuckEndpoint(t *testing.T) {
	cfg := export.Config{
		QueueCapacity: 16, BatchMax: 4, FlushInterval: time.Hour,
		MaxAttempts: 1, DeadLetterCapacity: 256,
	}
	p, rep, logger := buildPipeline(t, cfg, 1.0)
	gate := make(chan struct{})
	rep.setDeliver(func(batch []*model.Span) export.ReportResult {
		<-gate
		return export.ReportResult{OK: true}
	})
	p.Start()
	defer p.Close(context.Background())

	// Saturate with a burst far beyond capacity; none of these calls may
	// block for long. Submits that are admitted will sit in the worker/queue.
	full := 0
	start := time.Now()
	for i := 1; i <= 400; i++ {
		res := p.Submit(raw(fmt.Sprintf("%032x", 12000+i), fmt.Sprintf("%016x", 12000+i), ""))
		if !res.Enqueued && res.ExportFailure == model.FailQueueFull {
			full++
		}
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("producers blocked for %s under stuck endpoint", elapsed)
	}
	if full == 0 {
		t.Fatal("expected queue_full rejections")
	}
	if !strings.Contains(logger.String(), "queue_full") {
		t.Fatal("logs missing queue_full classification")
	}

	// Recover endpoint, drain everything that was admitted.
	close(gate)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	st := p.Stats().Export
	if st.RejectedFull != int64(full) {
		t.Fatalf("rejectedFull=%d observed=%d", st.RejectedFull, full)
	}
	seen, _ := rep.counts()
	if int64(len(seen)) != st.Enqueued {
		t.Fatalf("delivered=%d admitted=%d (admitted must not be lost)", len(seen), st.Enqueued)
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("span %v delivered %d times", k, n)
		}
	}
}
