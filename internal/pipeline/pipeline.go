// Package pipeline wires ingestion, sampling and bounded export into the
// in-process tracing pipeline used by every entry point.
package pipeline

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/highcumontoa/tracing-pipeline-go/internal/export"
	"github.com/highcumontoa/tracing-pipeline-go/internal/ingest"
	"github.com/highcumontoa/tracing-pipeline-go/internal/model"
	"github.com/highcumontoa/tracing-pipeline-go/internal/retention"
	"github.com/highcumontoa/tracing-pipeline-go/internal/sampler"
)

// Result is what Submit returns to the caller: normalization class,
// sampling decision and export failure class are all independently
// observable and explainable.
type Result struct {
	Outcome       model.Outcome
	Decision      model.Decision
	ExportFailure model.FailureClass // FailNone when accepted/exported or not applicable
	Enqueued      bool
	// Revisit is true when the record reopened a trace/span identity
	// whose runtime state had already been reclaimed by retention: it
	// starts a new, externally distinguishable lifecycle.
	Revisit bool
}

// RetentionStats aggregates the bounded runtime-state bookkeeping of
// every stage, so retention, reclamation and revisit classification are
// externally queryable through the existing Stats entry point.
type RetentionStats struct {
	IngestSpans         retention.Stats // normalizer span-identity registry
	SamplerDecisions    retention.Stats // frozen per-trace verdicts
	SamplerSpanVerdicts retention.Stats // per-span parent verdicts
	// Revisits counts accepted records that reopened a reclaimed
	// trace/span identity under a new lifecycle.
	Revisits int64
}

// Stats aggregates all stage counters for external queries.
type Stats struct {
	Received     int64
	ByClass      map[model.Class]int64
	Sampled      int64
	Dropped      int64
	NormalizedOK int64
	Export       export.Stats
	Retention    RetentionStats
}

// Pipeline is safe for concurrent use.
type Pipeline struct {
	log      func(format string, args ...any)
	norm     *ingest.Normalizer
	sampler  *sampler.Sampler
	exporter *export.Exporter
	reporter *export.MockReporter

	received atomic.Int64
	sampled  atomic.Int64
	dropped  atomic.Int64
	revisits atomic.Int64

	closeOnce sync.Once
}

// New constructs a pipeline with a local mock reporting endpoint and
// bounded default retention for runtime trace state. logf may be nil.
func New(cfg export.Config, ratio float64, logf func(format string, args ...any)) *Pipeline {
	return NewWithRetention(cfg, ratio, retention.DefaultConfig(), logf)
}

// NewWithRetention constructs a pipeline with explicit retention bounds
// for the runtime trace state (ingest identity registry and frozen
// sampling decisions), so a long-running process keeps a hard upper
// bound on retained state no matter how many unique traces it ingests.
func NewWithRetention(cfg export.Config, ratio float64, rcfg retention.Config, logf func(format string, args ...any)) *Pipeline {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	rep := export.NewMockReporter()
	return &Pipeline{
		log:      logf,
		norm:     ingest.NewWithRetention(rcfg, logf),
		sampler:  sampler.NewWithRetention(ratio, rcfg, logf),
		exporter: export.New(cfg, rep, logf),
		reporter: rep,
	}
}

// newWithReporter constructs a pipeline around an arbitrary reporter. It is
// used by tests that need to observe exactly which spans were delivered.
func newWithReporter(logf func(string, ...any), rep export.Reporter, cfg export.Config, ratio float64) *Pipeline {
	return newWithReporterRetention(logf, rep, cfg, ratio, retention.DefaultConfig())
}

// newWithReporterRetention is newWithReporter with explicit retention
// bounds, for tests that exercise reclamation and revisits.
func newWithReporterRetention(logf func(string, ...any), rep export.Reporter, cfg export.Config, ratio float64, rcfg retention.Config) *Pipeline {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	mock, _ := rep.(*export.MockReporter)
	return &Pipeline{
		log:      logf,
		norm:     ingest.NewWithRetention(rcfg, logf),
		sampler:  sampler.NewWithRetention(ratio, rcfg, logf),
		exporter: export.New(cfg, rep, logf),
		reporter: mock,
	}
}

// Start opens the pipeline.
func (p *Pipeline) Start() { p.exporter.Start() }

// Submit pushes one raw record through normalize -> sample -> export.
//
// Normalization verdicts are distinguishable: malformed or missing records
// never reach sampling/export and cannot pollute valid ones. Sampling is
// decided once per trace and frozen, so an unsampled trace yields zero
// exported spans and a sampled trace never flips mid-run.
func (p *Pipeline) Submit(raw model.RawRecord) Result {
	p.received.Add(1)
	out := p.norm.Normalize(raw)

	res := Result{Outcome: out, Decision: model.DecisionUnknown, ExportFailure: model.FailNone}
	if out.Status != model.ClassOK {
		p.log("ingest reject source=%s hint=%s class=%s reason=%q",
			out.Source, out.RawHint, out.Status, out.Reason)
		return res
	}

	span := out.Span
	d, samplerRevisit := p.sampler.DecideWithLifecycle(span)
	res.Decision = d
	res.Revisit = out.Revisit || samplerRevisit
	if res.Revisit {
		p.revisits.Add(1)
	}
	lifecycle := ""
	if res.Revisit {
		lifecycle = " lifecycle=revisit"
	}
	if d == model.DecisionDrop {
		p.dropped.Add(1)
		p.log("trace=%s span=%s decision=drop class=ok%s", span.TraceID, span.SpanID, lifecycle)
		return res
	}
	p.sampled.Add(1)

	er := p.exporter.Enqueue(context.Background(), span)
	res.Enqueued = er.Accepted
	res.ExportFailure = er.Reason
	if er.Accepted {
		p.log("trace=%s span=%s decision=sample export=enqueued%s", span.TraceID, span.SpanID, lifecycle)
	} else {
		p.log("trace=%s span=%s decision=sample export=rejected failure=%s%s",
			span.TraceID, span.SpanID, er.Reason, lifecycle)
	}
	return res
}

// Reporter exposes the mock endpoint for test/demo fault injection.
func (p *Pipeline) Reporter() *export.MockReporter { return p.reporter }

// Exporter exposes the bounded exporter (e.g. for deterministic Flush).
func (p *Pipeline) Exporter() *export.Exporter { return p.exporter }

// SetSamplingRatio adjusts sampling for traces not yet started.
func (p *Pipeline) SetSamplingRatio(ratio float64) { p.sampler.SetRatio(ratio) }

// Flush blocks until all spans admitted before the call are delivered or
// dead-lettered.
func (p *Pipeline) Flush(ctx context.Context) error { return p.exporter.Flush(ctx) }

// EnqueueSpan retries bounded admission for an already normalized and
// sampled span (e.g. after a Submit returned FailQueueFull). It exists so a
// producer can honor backpressure without resubmitting the raw record,
// which would correctly be rejected as a normalization duplicate. No new
// normalization or sampling decision is made.
func (p *Pipeline) EnqueueSpan(ctx context.Context, span *model.Span) export.EnqueueResult {
	return p.exporter.Enqueue(ctx, span)
}

// Stats returns an aggregated snapshot, including the bounded retention
// bookkeeping (retained vs. capacity, reclamation counters, revisits).
func (p *Pipeline) Stats() Stats {
	classCounts := p.norm.ClassCounts()
	dec, verdicts := p.sampler.RetentionStats()
	return Stats{
		Received:     p.received.Load(),
		ByClass:      classCounts,
		Sampled:      p.sampled.Load(),
		Dropped:      p.dropped.Load(),
		NormalizedOK: classCounts[model.ClassOK],
		Export:       p.exporter.Stats(),
		Retention: RetentionStats{
			IngestSpans:         p.norm.RetentionStats(),
			SamplerDecisions:    dec,
			SamplerSpanVerdicts: verdicts,
			Revisits:            p.revisits.Load(),
		},
	}
}

// Close flushes and stops the pipeline.
func (p *Pipeline) Close(ctx context.Context) error {
	var err error
	p.closeOnce.Do(func() { err = p.exporter.Close(ctx) })
	return err
}
