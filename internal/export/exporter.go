package export

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/highcumontoa/tracing-pipeline-go/internal/model"
)

// attemptTimeout bounds one endpoint call so a stuck endpoint cannot pin
// the worker forever.
const attemptTimeout = 50 * time.Millisecond

// enqueueWait caps how long a producer may wait for a queue slot before
// the bounded backpressure policy rejects the span.
const enqueueWait = 2 * time.Millisecond

// Config configures the bounded exporter.
type Config struct {
	QueueCapacity      int
	BatchMax           int
	FlushInterval      time.Duration
	MaxAttempts        int
	RetryBackoff       time.Duration
	DeadLetterCapacity int
}

// DefaultConfig returns conservative local defaults.
func DefaultConfig() Config {
	return Config{
		QueueCapacity:      128,
		BatchMax:           16,
		FlushInterval:      20 * time.Millisecond,
		MaxAttempts:        3,
		RetryBackoff:       5 * time.Millisecond,
		DeadLetterCapacity: 64,
	}
}

// EnqueueResult tells the producer what happened at enqueue time.
type EnqueueResult struct {
	Accepted bool
	Reason   model.FailureClass
}

// DeadLetter is a span that exhausted all delivery attempts.
type DeadLetter struct {
	Span     *model.Span
	Failure  model.FailureClass
	Attempts int
	AtNano   int64
}

// Stats is a point-in-time snapshot of exporter counters and state.
type Stats struct {
	Running         bool
	QueueLen        int
	QueueCapacity   int
	Enqueued        int64
	RejectedFull    int64
	RejectedDup     int64
	BatchesSent     int64
	SentSpans       int64
	Retries         int64
	DeadLettered    int64
	DeadLetterDrops int64
	DeadLetterLen   int
	InFlight        int
	// Duplicate-protection bookkeeping, all hard-bounded:
	// PendingLen <= QueueCapacity+BatchMax, RecentLen <= RecentCap,
	// DeadLetterLen <= DeadLetterCapacity.
	PendingLen int
	RecentLen  int
	RecentCap  int
}

// Exporter is a bounded, single-worker exporting stage. A single worker
// makes delivery order and attempt accounting deterministic.
type Exporter struct {
	cfg Config
	rep Reporter
	log func(format string, args ...any)

	queue chan *model.Span

	// Bounded duplicate protection keyed by span lifecycle. Tracked under
	// one lock so the sets and the dead-letter ring stay consistent:
	//   pending -> admitted to the queue or inside a batch being delivered
	//   dead    -> currently held in the bounded dead-letter ring
	//   recent  -> FIFO ring of recently delivered keys (idempotency tail)
	// Every set is size-bounded, hence total bookkeeping memory is bounded
	// by QueueCapacity + BatchMax + DeadLetterCapacity + RecentCap.
	trackMu   sync.Mutex
	pending   map[model.SpanKey]struct{}
	dead      map[model.SpanKey]struct{}
	recent    []model.SpanKey
	recentCap int
	dl        []DeadLetter

	inFlight atomic.Int64

	enqueued        atomic.Int64
	rejectedFull    atomic.Int64
	rejectedDup     atomic.Int64
	batchesSent     atomic.Int64
	sentSpans       atomic.Int64
	retries         atomic.Int64
	deadLettered    atomic.Int64
	deadLetterDrops atomic.Int64

	startOnce sync.Once
	stopCh    chan struct{}
	doneCh    chan struct{}
	flushReqs chan chan struct{}
	running   atomic.Bool
}

// New builds an exporter around the given reporter. logf may be nil.
func New(cfg Config, rep Reporter, logf func(format string, args ...any)) *Exporter {
	if cfg.QueueCapacity <= 0 {
		cfg = DefaultConfig()
	}
	if cfg.BatchMax <= 0 {
		cfg.BatchMax = 16
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 20 * time.Millisecond
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 1
	}
	if cfg.DeadLetterCapacity <= 0 {
		cfg.DeadLetterCapacity = 64
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Exporter{
		cfg:       cfg,
		rep:       rep,
		log:       logf,
		queue:     make(chan *model.Span, cfg.QueueCapacity),
		pending:   make(map[model.SpanKey]struct{}),
		dead:      make(map[model.SpanKey]struct{}),
		recentCap: cfg.QueueCapacity * 4,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
		flushReqs: make(chan chan struct{}),
	}
}

// Start launches the background worker.
func (e *Exporter) Start() {
	e.startOnce.Do(func() {
		e.running.Store(true)
		go e.loop()
	})
}

// isKnown reports whether the key is currently protected (queued, in
// delivery, dead-lettered or still in the delivered idempotency tail).
func (e *Exporter) isKnownLocked(k model.SpanKey) bool {
	if _, ok := e.pending[k]; ok {
		return true
	}
	if _, ok := e.dead[k]; ok {
		return true
	}
	for _, rk := range e.recent {
		if rk == k {
			return true
		}
	}
	return false
}

// Enqueue never blocks beyond ctx/enqueueWait: a full queue is rejected
// with FailQueueFull instead of unbounded blocking or buffering. A span
// already accepted once (and still within bounded memory) is rejected with
// FailDuplicate, so the endpoint never receives the same span twice.
func (e *Exporter) Enqueue(ctx context.Context, span *model.Span) EnqueueResult {
	if span == nil || !e.running.Load() {
		return EnqueueResult{Accepted: false, Reason: model.FailEndpointError}
	}
	key := span.Key()

	e.trackMu.Lock()
	if e.isKnownLocked(key) {
		e.trackMu.Unlock()
		e.rejectedDup.Add(1)
		return EnqueueResult{Accepted: false, Reason: model.FailDuplicate}
	}
	// Reserve pending before sending so two producers racing with the same
	// key cannot both enter the queue.
	e.pending[key] = struct{}{}
	e.trackMu.Unlock()

	reject := func() EnqueueResult {
		e.trackMu.Lock()
		delete(e.pending, key) // never sent, reservation is ours alone
		e.trackMu.Unlock()
		e.rejectedFull.Add(1)
		return EnqueueResult{Accepted: false, Reason: model.FailQueueFull}
	}

	// Immediate non-blocking fast path.
	select {
	case e.queue <- span:
		e.enqueued.Add(1)
		return EnqueueResult{Accepted: true}
	default:
	}

	// Bounded backpressure: wait briefly for a slot, but never without
	// limit. Memory still cannot exceed QueueCapacity admitted spans.
	timer := time.NewTimer(enqueueWait)
	select {
	case e.queue <- span:
		timer.Stop()
		e.enqueued.Add(1)
		return EnqueueResult{Accepted: true}
	case <-timer.C:
		timer.Stop()
		return reject()
	case <-ctx.Done():
		timer.Stop()
		return reject()
	}
}

// loop is the single delivery worker.
func (e *Exporter) loop() {
	defer close(e.doneCh)
	ticker := time.NewTicker(e.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]*model.Span, 0, e.cfg.BatchMax)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		out := batch
		batch = make([]*model.Span, 0, e.cfg.BatchMax)
		e.deliver(out)
	}
	// drainFlush delivers everything already admitted to the queue, not
	// only the spans gathered so far. Used for explicit flush/shutdown so
	// their semantics are deterministic: after it returns, no admitted
	// span remains buffered.
	drainFlush := func() {
		for {
			select {
			case sp := <-e.queue:
				batch = append(batch, sp)
				if len(batch) >= e.cfg.BatchMax {
					flush()
				}
			default:
				flush()
				return
			}
		}
	}

	for {
		select {
		case sp := <-e.queue:
			batch = append(batch, sp)
			if len(batch) >= e.cfg.BatchMax {
				flush()
			}
		case <-ticker.C:
			flush()
		case ack := <-e.flushReqs:
			drainFlush()
			close(ack)
		case <-e.stopCh:
			drainFlush()
			return
		}
	}
}

// deliver sends one batch with bounded deterministic retries. A span is
// removed from pending exactly once: into the recent ring on success, or
// into the dead-letter ring after exhausting attempts.
func (e *Exporter) deliver(batch []*model.Span) {
	e.inFlight.Add(int64(len(batch)))
	defer e.inFlight.Add(-int64(len(batch)))

	var last ReportResult
	for attempt := 1; attempt <= e.cfg.MaxAttempts; attempt++ {
		if attempt > 1 {
			e.retries.Add(1)
			if e.cfg.RetryBackoff > 0 {
				timer := time.NewTimer(e.cfg.RetryBackoff)
				select {
				case <-timer.C:
				case <-e.stopCh:
					timer.Stop()
				}
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
		last = e.rep.Report(ctx, batch)
		cancel()
		if last.OK {
			e.markDelivered(batch)
			e.batchesSent.Add(1)
			e.sentSpans.Add(int64(len(batch)))
			e.log("export ok batch=%d spans=%d trace=%s", len(batch), len(batch), traceHint(batch))
			return
		}
		e.log("export attempt=%d/%d failed class=%s spans=%d trace=%s",
			attempt, e.cfg.MaxAttempts, last.Failure, len(batch), traceHint(batch))
	}

	e.markDead(batch, last.Failure, e.cfg.MaxAttempts)
}

// markDelivered moves batch keys from pending into the bounded recent ring.
func (e *Exporter) markDelivered(batch []*model.Span) {
	e.trackMu.Lock()
	for _, sp := range batch {
		k := sp.Key()
		delete(e.pending, k)
		e.recent = append(e.recent, k)
	}
	if len(e.recent) > e.recentCap {
		e.recent = e.recent[len(e.recent)-e.recentCap:]
	}
	e.trackMu.Unlock()
}

// markDead moves batch keys from pending into the bounded dead-letter ring.
func (e *Exporter) markDead(batch []*model.Span, f model.FailureClass, attempts int) {
	now := time.Now().UnixNano()
	e.trackMu.Lock()
	for _, sp := range batch {
		k := sp.Key()
		delete(e.pending, k)
		if len(e.dl) >= e.cfg.DeadLetterCapacity {
			// Deterministic bounded policy: discard the oldest dead letter.
			old := e.dl[0]
			e.dl = e.dl[1:]
			delete(e.dead, old.Span.Key())
			e.deadLetterDrops.Add(1)
		}
		e.dl = append(e.dl, DeadLetter{Span: sp, Failure: f, Attempts: attempts, AtNano: now})
		e.dead[k] = struct{}{}
		e.deadLettered.Add(1)
	}
	e.trackMu.Unlock()
}

func traceHint(batch []*model.Span) string {
	if len(batch) == 0 {
		return ""
	}
	return batch[0].TraceID.String()
}

// Stats returns a point-in-time counter snapshot.
func (e *Exporter) Stats() Stats {
	e.trackMu.Lock()
	dlLen := len(e.dl)
	pendingLen := len(e.pending)
	recentLen := len(e.recent)
	e.trackMu.Unlock()
	return Stats{
		Running:         e.running.Load(),
		QueueLen:        len(e.queue),
		QueueCapacity:   e.cfg.QueueCapacity,
		Enqueued:        e.enqueued.Load(),
		RejectedFull:    e.rejectedFull.Load(),
		RejectedDup:     e.rejectedDup.Load(),
		BatchesSent:     e.batchesSent.Load(),
		SentSpans:       e.sentSpans.Load(),
		Retries:         e.retries.Load(),
		DeadLettered:    e.deadLettered.Load(),
		DeadLetterDrops: e.deadLetterDrops.Load(),
		DeadLetterLen:   dlLen,
		InFlight:        int(e.inFlight.Load()),
		PendingLen:      pendingLen,
		RecentLen:       recentLen,
		RecentCap:       e.recentCap,
	}
}

// DeadLetters returns a copy of the current dead-letter contents.
func (e *Exporter) DeadLetters() []DeadLetter {
	e.trackMu.Lock()
	out := make([]DeadLetter, len(e.dl))
	copy(out, e.dl)
	e.trackMu.Unlock()
	return out
}

// Flush waits until every span admitted before the call has been handed
// to the reporter (success or dead-letter). It is driven by the single
// worker, hence deterministic and safe to call concurrently.
func (e *Exporter) Flush(ctx context.Context) error {
	if !e.running.Load() {
		return nil
	}
	ack := make(chan struct{})
	select {
	case e.flushReqs <- ack:
	case <-ctx.Done():
		return ctx.Err()
	case <-e.doneCh:
		return nil
	}
	select {
	case <-ack:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-e.doneCh:
		return nil
	}
}

// Close flushes buffered spans and stops the worker.
func (e *Exporter) Close(ctx context.Context) error {
	if !e.running.CompareAndSwap(true, false) {
		return nil
	}
	close(e.stopCh)
	select {
	case <-e.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
