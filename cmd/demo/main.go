// Command demo drives the local tracing pipeline with scripted sources,
// sampling changes and endpoint failures for manual verification.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/highcumontoa/tracing-pipeline-go/internal/export"
	"github.com/highcumontoa/tracing-pipeline-go/internal/model"
	"github.com/highcumontoa/tracing-pipeline-go/internal/pipeline"
)

func main() {
	cfg := export.Config{
		QueueCapacity:      32,
		BatchMax:           8,
		FlushInterval:      10 * time.Millisecond,
		MaxAttempts:        2,
		RetryBackoff:       2 * time.Millisecond,
		DeadLetterCapacity: 32,
	}
	p := pipeline.New(cfg, 1.0, log.Printf)
	p.Start()
	defer func() {
		// Bounded shutdown: backpressure policy already permits rejecting
		// data, so a stuck endpoint must not make the process hang.
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		_ = p.Close(ctx)
	}()

	const t1 = "11111111111111111111111111111111"
	const t2 = "22222222222222222222222222222222"
	const t3 = "33333333333333333333333333333333"

	fmt.Println("== phase 1: normalized traffic from all three sources (ratio=1) ==")
	p.Submit(model.RawRecord{Source: model.SourceHTTPHeaders, TraceID: t1, SpanID: "aaaaaaaaaaaaaaaa", Name: "http", StartNano: 1, EndNano: 2})
	// binary_v2 carries numeric ids; parent references the numeric form of
	// the root (0xaa..aa == "aaaaaaaaaaaaaaaa"), demonstrating cross-source
	// normalization into the same unified trace.
	p.Submit(model.RawRecord{Source: model.SourceBinary,
		TraceNumHi: 0x1111111111111111, TraceNumLo: 0x1111111111111111,
		SpanNum: 0xbbbbbbbbbbbbbbbb, ParentSpanNum: 0xaaaaaaaaaaaaaaaa,
		Name: "bin", StartNano: 2, EndNano: 3})
	p.Submit(model.RawRecord{Source: model.SourceInproc, TraceID: t1, SpanID: "cccccccccccccccc", ParentSpanID: "bbbbbbbbbbbbbbbb", Name: "inproc", StartNano: 3, EndNano: 4})

	fmt.Println("== phase 2: distinct, non-silent rejections ==")
	p.Submit(model.RawRecord{Source: model.SourceHTTPHeaders, TraceID: "  ", SpanID: "dddddddddddddddd", Name: "missing", StartNano: 1, EndNano: 2})
	p.Submit(model.RawRecord{Source: model.SourceHTTPHeaders, TraceID: t2, SpanID: "not-hex", Name: "invalid", StartNano: 1, EndNano: 2})
	p.Submit(model.RawRecord{Source: model.SourceBinary, Name: "binary-missing-ids", StartNano: 1, EndNano: 2})
	p.Submit(model.RawRecord{Source: model.SourceInproc, TraceID: t2, SpanID: "eeeeeeeeeeeeeeee", ParentSpanID: "ffffffffffffffff", ParentTraceID: t1, Name: "xparent", StartNano: 1, EndNano: 2})

	fmt.Println("== phase 3: ratio changes 1 -> 0 mid-run ==")
	p.SetSamplingRatio(0)
	// A late child of the already-started trace t1 must stay sampled...
	p.Submit(model.RawRecord{Source: model.SourceInproc, TraceID: t1, SpanID: "1212121212121212", ParentSpanID: "cccccccccccccccc", Name: "late-child", StartNano: 4, EndNano: 5})
	// ...while a brand-new trace t3 must drop entirely (all spans).
	p.Submit(model.RawRecord{Source: model.SourceInproc, TraceID: t3, SpanID: "3131313131313131", Name: "dropped-root", StartNano: 1, EndNano: 2})
	p.Submit(model.RawRecord{Source: model.SourceInproc, TraceID: t3, SpanID: "3232323232323232", ParentSpanID: "3131313131313131", Name: "dropped-child", StartNano: 2, EndNano: 3})

	flush(p)

	fmt.Println("== phase 4: endpoint fails -> bounded retry then dead letters ==")
	p.SetSamplingRatio(1) // affects only traces not yet started
	p.Reporter().SetBehavior(export.Behavior{Mode: export.ModeFail, Failure: model.FailEndpointError})
	for i := 0; i < 16; i++ {
		p.Submit(model.RawRecord{
			Source:  model.SourceInproc,
			TraceID: fmt.Sprintf("%032x", 4000+i), SpanID: fmt.Sprintf("%016x", 4000+i),
			Name: "will-fail", StartNano: 1, EndNano: 2,
		})
	}
	flush(p)

	fmt.Println("== phase 5: stuck/slow endpoint -> bounded backpressure (queue_full) ==")
	p.Reporter().SetBehavior(export.Behavior{Mode: export.ModeSlow, Delay: 200 * time.Millisecond})
	full := 0
	for i := 0; i < 500; i++ {
		res := p.Submit(model.RawRecord{
			Source:  model.SourceInproc,
			TraceID: fmt.Sprintf("%032x", 500000+i), SpanID: fmt.Sprintf("%016x", 500000+i),
			Name: "burst", StartNano: 1, EndNano: 2,
		})
		if !res.Enqueued && res.ExportFailure == model.FailQueueFull {
			full++
		}
	}
	fmt.Printf("burst rejections due to bounded backpressure: %d\n", full)

	printStats(p)
}

func flush(p *pipeline.Pipeline) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.Flush(ctx); err != nil {
		log.Printf("flush: %v", err)
	}
}

func printStats(p *pipeline.Pipeline) {
	st := p.Stats()
	fmt.Println("== stats ==")
	fmt.Printf("received=%d normalized_ok=%d sampled=%d dropped=%d\n",
		st.Received, st.NormalizedOK, st.Sampled, st.Dropped)
	for c, n := range st.ByClass {
		fmt.Printf("  class %-16s %d\n", c, n)
	}
	e := st.Export
	fmt.Printf("export: enqueued=%d sent_spans=%d batches=%d retries=%d rejected_full=%d rejected_dup=%d deadlettered=%d dl_dropped=%d dl_held=%d\n",
		e.Enqueued, e.SentSpans, e.BatchesSent, e.Retries, e.RejectedFull, e.RejectedDup,
		e.DeadLettered, e.DeadLetterDrops, e.DeadLetterLen)
	for i, dl := range p.Exporter().DeadLetters() {
		if i >= 5 {
			fmt.Printf("  ... %d more dead letters\n", e.DeadLetterLen-5)
			break
		}
		fmt.Printf("  deadletter trace=%s span=%s failure=%s attempts=%d\n",
			dl.Span.TraceID, dl.Span.SpanID, dl.Failure, dl.Attempts)
	}
}
