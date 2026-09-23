// Package pipeline is the facade wiring normalization, sampling and bounded
// export into one concurrency-safe in-process tracing pipeline.
package pipeline

import (
	"context"
	"errors"
	"log"
	"sort"

	"github.com/highcumontoa/tracing-pipeline-go/pkg/export"
	"github.com/highcumontoa/tracing-pipeline-go/pkg/metrics"
	"github.com/highcumontoa/tracing-pipeline-go/pkg/model"
	"github.com/highcumontoa/tracing-pipeline-go/pkg/normalize"
	"github.com/highcumontoa/tracing-pipeline-go/pkg/sampling"
)

// Config is the full pipeline configuration.
type Config struct {
	Sampling sampling.Config
	Export   export.Config
}

// IngestResult is the per-batch result returned to callers: classification
// details plus the traces admitted to (or refused by) the export queue.
type IngestResult struct {
	Normalized normalize.Result
	Decisions  map[model.TraceID]sampling.Decision
	// EnqueuedTraces lists sampled-in traces admitted to the export queue.
	EnqueuedTraces []model.TraceID
	// QueueFullTraces lists sampled-in traces rejected due to back-pressure.
	QueueFullTraces []model.TraceID
}

// Pipeline coordinates all stages.
type Pipeline struct {
	cfg      Config
	norm     *normalize.Normalizer
	sampler  *sampling.Sampler
	exporter *export.Exporter
	reporter export.Reporter
	mc       *metrics.Counters
	logger   *log.Logger
}

// New constructs a pipeline around an explicit reporter.
func New(cfg Config, rep export.Reporter) *Pipeline {
	mc := metrics.New()
	return &Pipeline{
		cfg:      cfg,
		norm:     normalize.New(),
		sampler:  sampling.New(cfg.Sampling),
		exporter: export.New(cfg.Export, rep, mc),
		reporter: rep,
		mc:       mc,
		logger:   log.Default(),
	}
}

// Start launches background workers. The returned cancellation is tied to the
// caller-provided context; Shutdown additionally drains and waits.
func (p *Pipeline) Start(ctx context.Context) {
	_ = p.exporter.Run(ctx)
}

// ingest is the common path for all three sources: normalize, account,
// sample per trace, enqueue whole traces or account for back-pressure.
//
// block=false uses the non-blocking offer: a full queue surfaces immediately
// in QueueFullTraces (producers never block). block=true uses the interruptible
// blocking submit (waits for capacity or ctx) and therefore never reports a
// queue-full trace while ctx is alive.
func (p *Pipeline) ingest(ctx context.Context, block bool, source string, res normalize.Result) (IngestResult, error) {
	out := IngestResult{Normalized: res, Decisions: map[model.TraceID]sampling.Decision{}}

	p.mc.IncIngested(int64(len(res.Accepted) + len(res.Rejected)))
	p.mc.IncRejected(int64(len(res.Rejected)))
	p.mc.IncAccepted(int64(len(res.Accepted)))

	// Stable ordering per trace id makes all downstream behavior deterministic
	// regardless of source record ordering within a batch.
	traceOrder := make([]model.TraceID, 0)
	byTrace := make(map[model.TraceID][]model.Span)
	for _, sp := range res.Accepted {
		if _, ok := byTrace[sp.TraceID]; !ok {
			traceOrder = append(traceOrder, sp.TraceID)
		}
		byTrace[sp.TraceID] = append(byTrace[sp.TraceID], sp)
	}
	sort.Slice(traceOrder, func(i, j int) bool { return traceOrder[i].String() < traceOrder[j].String() })

	for _, tid := range traceOrder {
		spans := byTrace[tid]
		// One decision for the whole trace; the sampler also pins it for
		// future batches (not used cross-batch, but guarantees consistency
		// if the same trace is seen concurrently at multiple entries).
		dec := p.sampler.DecideBatch(spans)[0]
		out.Decisions[tid] = dec

		if dec == sampling.Drop {
			// Whole trace out: no span is enqueued, so no partial trace can
			// ever be exported.
			p.mc.IncSampledOut(int64(len(spans)))
			p.logDecision(source, tid, dec, len(spans), "dropped")
			continue
		}
		p.mc.IncSampledIn(int64(len(spans)))
		if block {
			err := p.exporter.SubmitBatch(ctx, tid, spans)
			if errors.Is(err, export.ErrDuplicate) {
				// Idempotent: already admitted earlier and counted as dup.
				p.logDecision(source, tid, dec, len(spans), "duplicate")
				continue
			}
			if err != nil {
				// ctx cancellation / shutdown — surface immediately.
				return out, err
			}
			out.EnqueuedTraces = append(out.EnqueuedTraces, tid)
			p.logDecision(source, tid, dec, len(spans), "enqueued")
			continue
		}
		if p.exporter.OfferBatch(tid, spans) {
			out.EnqueuedTraces = append(out.EnqueuedTraces, tid)
			p.logDecision(source, tid, dec, len(spans), "enqueued")
		} else {
			out.QueueFullTraces = append(out.QueueFullTraces, tid)
			p.logDecision(source, tid, dec, len(spans), "queue_full")
		}
	}

	for _, rj := range res.Rejected {
		p.logger.Printf("trace=%s source=%s reject_index=%d class=%s detail=%q",
			orDash(rj.TraceHint), rj.Source, rj.Index, rj.Reason, rj.Detail)
	}
	return out, nil
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func (p *Pipeline) logDecision(source string, tid model.TraceID, d sampling.Decision, n int, action string) {
	p.logger.Printf("trace=%s source=%s spans=%d decision=%s action=%s ratio=%.4f",
		tid, source, n, d, action, p.sampler.Ratio())
}

// IngestHTTP is the non-blocking HTTP JSON reporter entry point. A full queue
// is reported via IngestResult.QueueFullTraces rather than blocking.
func (p *Pipeline) IngestHTTP(batch []normalize.HTTPSpanV1) IngestResult {
	out, _ := p.ingest(context.Background(), false, normalize.SourceHTTP, p.norm.IngestHTTP(batch))
	return out
}

// IngestW3C is the non-blocking W3C-like reporter entry point.
func (p *Pipeline) IngestW3C(batch []normalize.W3CSpanV1) IngestResult {
	out, _ := p.ingest(context.Background(), false, normalize.SourceW3C, p.norm.IngestW3C(batch))
	return out
}

// IngestZipkin is the non-blocking Zipkin-like reporter entry point.
func (p *Pipeline) IngestZipkin(batch []normalize.ZipkinSpanV1) IngestResult {
	out, _ := p.ingest(context.Background(), false, normalize.SourceZipkin, p.norm.IngestZipkin(batch))
	return out
}

// SubmitHTTP is the blocking-but-interruptible HTTP entry point: it waits for
// queue capacity or ctx, so admitted data is never lost to back-pressure while
// producers still cannot block unboundedly.
func (p *Pipeline) SubmitHTTP(ctx context.Context, batch []normalize.HTTPSpanV1) (IngestResult, error) {
	return p.ingest(ctx, true, normalize.SourceHTTP, p.norm.IngestHTTP(batch))
}

// SubmitW3C is the blocking-but-interruptible W3C-like entry point.
func (p *Pipeline) SubmitW3C(ctx context.Context, batch []normalize.W3CSpanV1) (IngestResult, error) {
	return p.ingest(ctx, true, normalize.SourceW3C, p.norm.IngestW3C(batch))
}

// SubmitZipkin is the blocking-but-interruptible Zipkin-like entry point.
func (p *Pipeline) SubmitZipkin(ctx context.Context, batch []normalize.ZipkinSpanV1) (IngestResult, error) {
	return p.ingest(ctx, true, normalize.SourceZipkin, p.norm.IngestZipkin(batch))
}

// Counters exposes the externally queryable counters/gauges.
func (p *Pipeline) Counters() *metrics.Counters { return p.mc }

// Reporter returns the configured reporter.
func (p *Pipeline) Reporter() export.Reporter { return p.reporter }

// MockReporter returns the reporter as a *export.MockReporter when it is one.
func (p *Pipeline) MockReporter() *export.MockReporter {
	if m, ok := p.reporter.(*export.MockReporter); ok {
		return m
	}
	return nil
}

// Sampler exposes the sampler for ratio adjustments and inspection.
func (p *Pipeline) Sampler() *sampling.Sampler { return p.sampler }

// Exporter exposes the exporter (DLQ inspection and redelivery).
func (p *Pipeline) Exporter() *export.Exporter { return p.exporter }

// Shutdown drains and stops the pipeline within the ctx deadline; retained
// DLQ batches are preserved and remain inspectable after shutdown.
func (p *Pipeline) Shutdown(ctx context.Context) error {
	return p.exporter.Close(ctx)
}
