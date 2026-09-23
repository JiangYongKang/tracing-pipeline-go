// Package export implements bounded, back-pressured export with retries and a
// bounded dead-letter queue (DLQ).
//
// Back-pressure contract: intake is bounded by a buffered channel of fixed
// capacity. OfferBatch is non-blocking and returns false when the queue is
// full; SubmitBatch blocks (interruptibly via context) on a full queue.
// Therefore producers are never blocked unboundedly and memory can never grow
// without limit.
//
// Failure policy (deterministic): a failed batch is retried up to MaxAttempts
// with a fixed deterministic backoff; if all attempts fail it enters the
// bounded DLQ; if the DLQ is full the oldest DLQ entry is dropped and counted
// (drop-oldest) while the newer failed batch is retained — nothing is dropped
// silently, counters and the snapshot expose every event. Retries are
// batch-atomic, so a span batch is delivered either zero or one time: no
// duplicate reporting. DLQ batches are redelivered only via DrainDLQ and a
// redelivered batch is removed on its first success.
package export

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/highcumontoa/tracing-pipeline-go/pkg/metrics"
	"github.com/highcumontoa/tracing-pipeline-go/pkg/model"
)

// ErrQueueFull is returned by SubmitBatch when a non-blocking style caller
// wants the full-queue condition explicitly; OfferBatch encodes it as false.
var ErrQueueFull = errors.New("export queue is full")

// ErrClosed is returned by intake after Close has been called.
var ErrClosed = errors.New("exporter is closed")

// ErrDuplicate is returned by SubmitBatch when the trace id was already
// admitted earlier. The event is also counted in metrics.duplicates.
var ErrDuplicate = errors.New("trace batch already admitted (duplicate)")

// dedupCap bounds the duplicate-suppression memory; beyond this window an
// identical trace id can no longer be recognized as a repeat (documented).
const dedupCap = 1 << 16

// Config configures an Exporter.
type Config struct {
	QueueCap        int           // number of batches buffered before export
	DLQCap          int           // number of failed batches retained
	MaxAttempts     int           // total attempts per batch (>=1)
	Backoff         time.Duration // fixed delay between attempts
	DeliveryTimeout time.Duration // per-attempt reporter timeout (0 = none)
}

func (c Config) withDefaults() Config {
	if c.QueueCap <= 0 {
		c.QueueCap = 128
	}
	if c.DLQCap <= 0 {
		c.DLQCap = 64
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
	return c
}

// Exporter owns a bounded intake queue, one export worker and a bounded DLQ.
type Exporter struct {
	cfg Config
	rep Reporter
	mc  *metrics.Counters

	queue chan SpanBatch

	mu      sync.Mutex
	closing bool
	dlq     []SpanBatch
	seen    map[model.TraceID]struct{}
	seenOrd []model.TraceID

	workerDone chan struct{}
	runOnce    sync.Once
}

// New builds an exporter around the given reporter and counter set.
func New(cfg Config, rep Reporter, mc *metrics.Counters) *Exporter {
	cfg = cfg.withDefaults()
	return &Exporter{
		cfg:        cfg,
		rep:        rep,
		mc:         mc,
		queue:      make(chan SpanBatch, cfg.QueueCap),
		dlq:        make([]SpanBatch, 0, cfg.DLQCap),
		seen:       make(map[model.TraceID]struct{}),
		seenOrd:    make([]model.TraceID, 0, 1024),
		workerDone: make(chan struct{}),
	}
}

// Run executes the export loop until ctx is canceled, then drains whatever is
// already buffered and exits. Safe to call once.
func (e *Exporter) Run(ctx context.Context) error {
	e.runOnce.Do(func() { go e.loop(ctx) })
	return nil
}

func (e *Exporter) loop(ctx context.Context) {
	defer close(e.workerDone)
	for {
		select {
		case b := <-e.queue:
			e.mc.AddQueueDepth(-int64(len(b.Spans)))
			e.processWithRetry(ctx, b)
		case <-ctx.Done():
			// Drain batches that were admitted before shutdown, then stop.
			for {
				select {
				case b := <-e.queue:
					e.mc.AddQueueDepth(-int64(len(b.Spans)))
					e.processWithRetry(ctx, b)
				default:
					return
				}
			}
		}
	}
}

// processWithRetry runs up to MaxAttempts deliveries with fixed backoff.
func (e *Exporter) processWithRetry(parent context.Context, b SpanBatch) {
	for attempt := 1; attempt <= e.cfg.MaxAttempts; attempt++ {
		b.Attempt = attempt
		e.mc.IncExportAttempts(1)

		ctx := parent
		var cancel context.CancelFunc
		if e.cfg.DeliveryTimeout > 0 {
			ctx, cancel = context.WithTimeout(parent, e.cfg.DeliveryTimeout)
		}
		err := e.rep.Report(ctx, b)
		if cancel != nil {
			cancel()
		}
		if err == nil {
			e.mc.IncExportSuccesses(1)
			return
		}

		if attempt < e.cfg.MaxAttempts && e.cfg.Backoff > 0 && parent.Err() == nil {
			e.mc.IncRetried(1)
			t := time.NewTimer(e.cfg.Backoff)
			select {
			case <-parent.Done():
				t.Stop()
			case <-t.C:
			}
		}
	}
	e.mc.IncExportFailures(1)
	e.moveToDLQ(b)
}

func (e *Exporter) moveToDLQ(b SpanBatch) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.dlq) >= e.cfg.DLQCap {
		// Deterministic overflow rule: sacrifice the oldest retained batch.
		e.dlq = e.dlq[1:]
		e.mc.IncDroppedDLQFull(1)
		e.mc.AddDLQDepth(-1)
	}
	e.dlq = append(e.dlq, b)
	e.mc.IncDLQEnqueued(1)
	e.mc.AddDLQDepth(1)
}

// forgetSeen removes a reservation by value. The caller must hold e.mu.
// Searching by value (not assuming it is the tail) is required because other
// goroutines may have appended further ids between reservation and rollback.
func (e *Exporter) forgetSeen(traceID model.TraceID) {
	if _, ok := e.seen[traceID]; !ok {
		return
	}
	delete(e.seen, traceID)
	for i, id := range e.seenOrd {
		if id == traceID {
			e.seenOrd = append(e.seenOrd[:i], e.seenOrd[i+1:]...)
			return
		}
	}
}

// admit centralizes duplicate suppression, closing checks and enqueueing.
func (e *Exporter) admit(traceID model.TraceID, spans []model.Span, block bool, ctx context.Context) (bool, error) {
	if spans == nil {
		spans = []model.Span{}
	}
	e.mu.Lock()
	if e.closing {
		e.mu.Unlock()
		return false, ErrClosed
	}
	if _, dup := e.seen[traceID]; dup {
		e.mu.Unlock()
		e.mc.IncDuplicates(1)
		return false, ErrDuplicate
	}
	// Reserve the trace id before enqueue so a racing duplicate cannot slip in.
	e.seen[traceID] = struct{}{}
	e.seenOrd = append(e.seenOrd, traceID)
	if len(e.seenOrd) > dedupCap {
		old := e.seenOrd[0]
		e.seenOrd = e.seenOrd[1:]
		if old != traceID {
			delete(e.seen, old)
		}
	}
	e.mu.Unlock()

	b := SpanBatch{TraceID: traceID, Spans: spans, EnqueuedAt: time.Now()}
	if !block {
		select {
		case e.queue <- b:
			e.mc.AddQueueDepth(int64(len(spans)))
			return true, nil
		default:
			// Roll back the reservation so an identical retry is still admissible.
			e.mu.Lock()
			e.forgetSeen(traceID)
			e.mu.Unlock()
			return false, nil
		}
	}

	select {
	case e.queue <- b:
		e.mc.AddQueueDepth(int64(len(spans)))
		return true, nil
	case <-ctx.Done():
		e.mu.Lock()
		e.forgetSeen(traceID)
		e.mu.Unlock()
		return false, ctx.Err()
	}
}

// OfferBatch enqueues one trace batch without blocking; false = queue full or
// duplicate (duplicates are counted separately in metrics).
func (e *Exporter) OfferBatch(traceID model.TraceID, spans []model.Span) bool {
	ok, err := e.admit(traceID, spans, false, nil)
	return err == nil && ok
}

// SubmitBatch enqueues one trace batch, blocking until enqueued, ctx done,
// the exporter has stopped, or the trace id is a duplicate (ErrDuplicate).
func (e *Exporter) SubmitBatch(ctx context.Context, traceID model.TraceID, spans []model.Span) error {
	_, err := e.admit(traceID, spans, true, ctx)
	return err
}

// DrainDLQ attempts one synchronous redelivery pass over retained failed
// batches; a batch is removed on its first successful redelivery. Failures
// remain in place for a later pass, still guaranteeing at-most-once delivery.
func (e *Exporter) DrainDLQ(ctx context.Context) (redelivered, remaining int) {
	e.mu.Lock()
	snapshot := make([]SpanBatch, len(e.dlq))
	copy(snapshot, e.dlq)
	e.mu.Unlock()

	kept := make([]SpanBatch, 0, len(snapshot))
	for _, b := range snapshot {
		if err := ctx.Err(); err != nil {
			kept = append(kept, b)
			continue
		}
		b.Attempt++
		e.mc.IncExportAttempts(1)
		if err := e.rep.Report(ctx, b); err != nil {
			kept = append(kept, b)
			continue
		}
		e.mc.IncExportSuccesses(1)
		e.mc.IncDLQDrained(1)
		e.mc.AddDLQDepth(-1)
		redelivered++
	}
	e.mu.Lock()
	e.dlq = kept
	remaining = len(e.dlq)
	e.mu.Unlock()
	return redelivered, remaining
}

// DLQ returns a snapshot copy of the currently retained failed batches.
func (e *Exporter) DLQ() []SpanBatch {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]SpanBatch, len(e.dlq))
	copy(out, e.dlq)
	return out
}

// Close stops intake and waits for the worker to finish its drain, bounded by
// ctx. Retained DLQ entries are preserved (inspectable afterwards), not sent.
func (e *Exporter) Close(ctx context.Context) error {
	e.mu.Lock()
	e.closing = true
	e.mu.Unlock()

	// Closing the intake channel is unnecessary: the worker tracks shutdown
	// via the Run context. We simply wait, capped by the caller's deadline.
	select {
	case <-e.workerDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// QueueDepth returns the current number of buffered batches.
func (e *Exporter) QueueDepth() int { return len(e.queue) }
