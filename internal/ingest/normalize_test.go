package ingest

import (
	"fmt"
	"testing"

	"github.com/highcumontoa/tracing-pipeline-go/internal/model"
)

const (
	tidA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	tidB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	sid1 = "1111111111111111"
	sid2 = "2222222222222222"
	sid3 = "3333333333333333"
)

func valid(source model.Source, tid, sid, parent string) model.RawRecord {
	return model.RawRecord{
		Source: source, TraceID: tid, SpanID: sid, ParentSpanID: parent,
		Name: "op", StartNano: 1, EndNano: 2,
	}
}

func TestNormalizeAcrossSources(t *testing.T) {
	n := New()

	// Hex-string sources. Distinct span ids since the registry is shared.
	hexSources := []struct {
		src model.Source
		sid string
	}{
		{model.SourceHTTPHeaders, sid1},
		{model.SourceInproc, sid2},
	}
	for _, hs := range hexSources {
		out := n.Normalize(valid(hs.src, tidA, hs.sid, ""))
		if out.Status != model.ClassOK {
			t.Fatalf("source %s: status=%s reason=%s", hs.src, out.Status, out.Reason)
		}
		if out.Span.TraceID.String() != tidA || out.Span.SpanID.String() != hs.sid || out.Span.HasParent {
			t.Fatalf("source %s: bad normalized span %+v", hs.src, out.Span)
		}
	}

	// binary_v2 carries numeric ids; they normalize into the same model.
	b := model.RawRecord{Source: model.SourceBinary, TraceNumHi: 0x0a0a, TraceNumLo: 1,
		SpanNum: 0x0101, ParentSpanNum: 0, Name: "bin", StartNano: 1, EndNano: 2}
	out := n.Normalize(b)
	if out.Status != model.ClassOK {
		t.Fatalf("binary numeric: %s %s", out.Status, out.Reason)
	}
	wantTID := model.TraceIDFromNum(0x0a0a, 1)
	wantSID := model.SpanIDFromNum(0x0101)
	if out.Span.TraceID != wantTID || out.Span.SpanID != wantSID || out.Span.HasParent {
		t.Fatalf("binary numeric normalized span=%+v", out.Span)
	}

	// numeric parent is honored.
	out = n.Normalize(model.RawRecord{Source: model.SourceBinary, TraceNumHi: 0x0a0a, TraceNumLo: 1,
		SpanNum: 0x0102, ParentSpanNum: 0x0101, StartNano: 2, EndNano: 3})
	if out.Status != model.ClassOK || !out.Span.HasParent || out.Span.Parent != wantSID {
		t.Fatalf("binary numeric parent: %+v", out)
	}
}

func TestBinaryNumericMissingAndSelfParent(t *testing.T) {
	n := New()
	// all-zero numeric ids -> missing context.
	out := n.Normalize(model.RawRecord{Source: model.SourceBinary, StartNano: 1, EndNano: 2})
	if out.Status != model.ClassMissingContext {
		t.Fatalf("zero numeric: %s", out.Status)
	}
	// zero span with a present trace -> still missing.
	out = n.Normalize(model.RawRecord{Source: model.SourceBinary, TraceNumLo: 1, StartNano: 1, EndNano: 2})
	if out.Status != model.ClassMissingContext {
		t.Fatalf("zero span num: %s", out.Status)
	}
	// numeric self-parent -> broken.
	out = n.Normalize(model.RawRecord{Source: model.SourceBinary, TraceNumLo: 1, SpanNum: 7, ParentSpanNum: 7, StartNano: 1, EndNano: 2})
	if out.Status != model.ClassBrokenParent {
		t.Fatalf("numeric self parent: %s", out.Status)
	}
}

func TestNormalizeClassifications(t *testing.T) {
	cases := []struct {
		name string
		raw  model.RawRecord
		want model.Class
	}{
		{"missing", model.RawRecord{Source: model.SourceInproc, TraceID: "  ", SpanID: sid1, StartNano: 1, EndNano: 2}, model.ClassMissingContext},
		{"bad_trace_hex", valid(model.SourceInproc, "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", sid1, ""), model.ClassInvalidField},
		{"bad_span_len", valid(model.SourceInproc, tidA, "12", ""), model.ClassInvalidField},
		{"bad_time", model.RawRecord{Source: model.SourceInproc, TraceID: tidA, SpanID: sid1, StartNano: 9, EndNano: 1}, model.ClassInvalidField},
		{"unknown_source", valid(model.Source("bogus"), tidA, sid1, ""), model.ClassInvalidField},
		{"bad_parent_id", valid(model.SourceInproc, tidA, sid1, "xx"), model.ClassBrokenParent},
		{"self_parent", valid(model.SourceInproc, tidA, sid1, sid1), model.ClassBrokenParent},
		{"cross_trace_parent", func() model.RawRecord {
			r := valid(model.SourceInproc, tidA, sid1, sid2)
			r.ParentTraceID = tidB
			return r
		}(), model.ClassBrokenParent},
		{"parent_trace_without_span", model.RawRecord{Source: model.SourceInproc, TraceID: tidA, SpanID: sid1, ParentTraceID: tidB, StartNano: 1, EndNano: 2}, model.ClassBrokenParent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := New()
			out := n.Normalize(tc.raw)
			if out.Status != tc.want {
				t.Fatalf("got class=%s reason=%q, want %s", out.Status, out.Reason, tc.want)
			}
			if out.Span != nil {
				t.Fatal("rejected record must not produce a span")
			}
		})
	}
}

func TestDuplicateAndConflictDoNotPollute(t *testing.T) {
	n := New()
	// First valid span in trace A.
	if out := n.Normalize(valid(model.SourceInproc, tidA, sid1, "")); out.Status != model.ClassOK {
		t.Fatalf("first: %s %s", out.Status, out.Reason)
	}
	// Same identity again: duplicate, must not overwrite or double-count OK.
	if out := n.Normalize(valid(model.SourceInproc, tidA, sid1, "")); out.Status != model.ClassDuplicate {
		t.Fatalf("dup: %s", out.Status)
	}
	// Same span id under trace B: conflict, trace A mapping must stay intact.
	if out := n.Normalize(valid(model.SourceInproc, tidB, sid1, "")); out.Status != model.ClassConflict {
		t.Fatalf("conflict: %s", out.Status)
	}
	// A fresh span in trace A still works: no pollution.
	if out := n.Normalize(valid(model.SourceInproc, tidA, sid2, sid1)); out.Status != model.ClassOK {
		t.Fatalf("fresh after conflict: %s %s", out.Status, out.Reason)
	}
	if got := n.KnownSpanCount(); got != 2 {
		t.Fatalf("known spans=%d want 2", got)
	}
}

// A parent that has not arrived yet must not be called broken: classification
// has to be independent of arrival order.
func TestLateArrivingParentNotBroken(t *testing.T) {
	n := New()
	child := n.Normalize(valid(model.SourceInproc, tidA, sid2, sid1))
	if child.Status != model.ClassOK {
		t.Fatalf("child before parent: %s %s", child.Status, child.Reason)
	}
	parent := n.Normalize(valid(model.SourceInproc, tidA, sid1, ""))
	if parent.Status != model.ClassOK {
		t.Fatalf("parent: %s %s", parent.Status, parent.Reason)
	}
}

func TestNormalizeConcurrentIsolation(t *testing.T) {
	n := New()
	const workers = 16
	const perWorker = 200
	done := make(chan model.Class, workers*perWorker)
	for w := 0; w < workers; w++ {
		w := w
		go func() {
			for i := 0; i < perWorker; i++ {
				// Unique trace+span per (w,i): all must be OK, proving
				// concurrent valid records don't corrupt each other.
				tid := fmt.Sprintf("%032x", w*perWorker+i+1)
				sid := fmt.Sprintf("%016x", w*perWorker+i+1)
				done <- n.Normalize(valid(model.SourceInproc, tid, sid, "")).Status
			}
		}()
	}
	for i := 0; i < workers*perWorker; i++ {
		if c := <-done; c != model.ClassOK {
			t.Fatalf("concurrent normalize produced %s", c)
		}
	}
	if got := n.KnownSpanCount(); got != workers*perWorker {
		t.Fatalf("known=%d want %d", got, workers*perWorker)
	}
}
