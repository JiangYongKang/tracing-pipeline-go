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
	// Revisit is true when the record's trace had been reclaimed by
	// retention and this record was classified against a new (or
	// suppressed) lifecycle. Generation identifies the lifecycle.
	Revisit    bool
	Generation uint64
}

// Stats aggregates all stage counters for external queries.
type Stats struct {
	Received     int64
	ByClass      map[model.Class]int64
	Sampled      int64
	Dropped      int64
	NormalizedOK int64
	// Revisits counts records that began a new lifecycle after their
	// trace's previous lifecycle was reclaimed.
	Revisits int64
	// ProtectedRevisits counts reclaimed-trace records that were kept on
	// their original conclusion because the span is still inside the
	// exporter's duplicate-protection window.
	ProtectedRevisits int64
	Export            export.Stats
	Retention         retention.Stats
}

// Pipeline is safe for concurrent use.
type Pipeline struct {
	log      func(format string, args ...any)
	norm     *ingest.Normalizer
	sampler  *sampler.Sampler
	exporter *export.Exporter
	reporter *export.MockReporter
	reg      *retention.Registry

	received          atomic.Int64
	sampled           atomic.Int64
	dropped           atomic.Int64
	revisits          atomic.Int64
	protectedRevisits atomic.Int64

	closeOnce sync.Once
}

// New constructs a pipeline with a local mock reporting endpoint and the
// bounded default retention configuration. logf may be nil.
func New(cfg export.Config, ratio float64, logf func(format string, args ...any)) *Pipeline {
	return NewWithRetention(cfg, ratio, retention.DefaultConfig(), logf)
}

// NewWithRetention constructs a pipeline with an explicit retention
// configuration (bounds on the runtime per-trace lifecycle state).
func NewWithRetention(cfg export.Config, ratio float64, ret retention.Config, logf func(format string, args ...any)) *Pipeline {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	rep := export.NewMockReporter()
	exporter := export.New(cfg, rep, logf)
	reg := retention.New(ret)
	return &Pipeline{
		log:      logf,
		norm:     ingest.NewWithRegistry(reg, exporter.IsProtected),
		sampler:  sampler.NewWithRegistry(ratio, reg),
		exporter: exporter,
		reporter: rep,
		reg:      reg,
	}
}

// newWithReporter constructs a pipeline around an arbitrary reporter. It is
// used by tests that need to observe exactly which spans were delivered.
func newWithReporter(logf func(string, ...any), rep export.Reporter, cfg export.Config, ratio float64) *Pipeline {
	return newWithReporterRetention(logf, rep, cfg, ratio, retention.DefaultConfig())
}

// newWithReporterRetention is newWithReporter with explicit retention bounds.
func newWithReporterRetention(logf func(string, ...any), rep export.Reporter, cfg export.Config, ratio float64, ret retention.Config) *Pipeline {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	mock, _ := rep.(*export.MockReporter)
	exporter := export.New(cfg, rep, logf)
	reg := retention.New(ret)
	return &Pipeline{
		log:      logf,
		norm:     ingest.NewWithRegistry(reg, exporter.IsProtected),
		sampler:  sampler.NewWithRegistry(ratio, reg),
		exporter: exporter,
		reporter: mock,
		reg:      reg,
	}
}

// Start opens the pipeline.
func (p *Pipeline) Start() { p.exporter.Start() }

// Submit pushes one raw record through normalize -> sample -> export.
//
// Normalization verdicts are distinguishable: malformed or missing records
// never reach sampling/export and cannot pollute valid ones. Sampling is
// decided once per trace lifecycle and frozen, so an unsampled trace yields
// zero exported spans and a sampled trace never flips mid-run. A record
// whose trace was reclaimed by retention is never silently dropped: it
// either begins a distinguishable new lifecycle (ClassRevisit, generation
// >= 2) or, while its span is still export-protected, stays on the original
// conclusion (duplicate + Revisit/Protected flags).
func (p *Pipeline) Submit(raw model.RawRecord) Result {
	p.received.Add(1)
	out := p.norm.Normalize(raw)

	res := Result{Outcome: out, Decision: model.DecisionUnknown, ExportFailure: model.FailNone,
		Revisit: out.Revisit, Generation: out.Generation}
	if out.Status != model.ClassOK && out.Status != model.ClassRevisit {
		if out.Protected {
			p.protectedRevisits.Add(1)
			p.log("ingest revisit=protected source=%s hint=%s class=%s reason=%q",
				out.Source, out.RawHint, out.Status, out.Reason)
		} else {
			p.log("ingest reject source=%s hint=%s class=%s reason=%q",
				out.Source, out.RawHint, out.Status, out.Reason)
		}
		return res
	}
	if out.Status == model.ClassRevisit {
		p.revisits.Add(1)
		p.log("ingest revisit=new-lifecycle trace=%s span=%s gen=%d",
			out.Span.TraceID, out.Span.SpanID, out.Generation)
	}

	span := out.Span
	d := p.sampler.Decide(span)
	res.Decision = d
	if d == model.DecisionDrop {
		p.dropped.Add(1)
		p.log("trace=%s span=%s gen=%d decision=drop class=%s", span.TraceID, span.SpanID, out.Generation, out.Status)
		return res
	}
	p.sampled.Add(1)

	er := p.exporter.Enqueue(context.Background(), span)
	res.Enqueued = er.Accepted
	res.ExportFailure = er.Reason
	if er.Accepted {
		p.log("trace=%s span=%s gen=%d decision=sample export=enqueued", span.TraceID, span.SpanID, out.Generation)
	} else {
		p.log("trace=%s span=%s gen=%d decision=sample export=rejected failure=%s",
			span.TraceID, span.SpanID, out.Generation, er.Reason)
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

// Stats returns an aggregated snapshot.
func (p *Pipeline) Stats() Stats {
	classCounts := p.norm.ClassCounts()
	return Stats{
		Received:          p.received.Load(),
		ByClass:           classCounts,
		Sampled:           p.sampled.Load(),
		Dropped:           p.dropped.Load(),
		NormalizedOK:      classCounts[model.ClassOK] + classCounts[model.ClassRevisit],
		Revisits:          p.revisits.Load(),
		ProtectedRevisits: p.protectedRevisits.Load(),
		Export:            p.exporter.Stats(),
		Retention:         p.reg.Stats(),
	}
}

// Close flushes and stops the pipeline.
func (p *Pipeline) Close(ctx context.Context) error {
	var err error
	p.closeOnce.Do(func() { err = p.exporter.Close(ctx) })
	return err
}
