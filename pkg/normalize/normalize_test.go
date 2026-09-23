package normalize

import (
	"log"
	"testing"
	"time"
)

// Fixed, readable ids reused across tests.
const (
	tidA = "0af7651916cd43dd8448eb211c80319c" // 32 hex
	tidB = "4bf92f3577b34da6a3ce929d0e0e4736"
	sid1 = "00f067aa0ba902b7" // 16 hex
	sid2 = "b7ad6b7169203331"
	sid3 = "b9c7c989f97918e1"
)

func nano(y int, mo time.Month, d, h, mi int) int64 {
	return time.Date(y, mo, d, h, mi, 0, 0, time.UTC).UnixNano()
}

// TestHTTPHappyPath verifies a root+child batch normalizes from the HTTP source
// with identity, parent link, kind and status preserved.
func TestHTTPHappyPath(t *testing.T) {
	n := New()
	start := nano(2026, 9, 23, 10, 0)
	res := n.IngestHTTP([]HTTPSpanV1{
		{TraceID: tidA, SpanID: sid1, Name: "root", Kind: "server",
			StartUnixNano: start, EndUnixNano: start + 1_000, Status: "OK"},
		{TraceID: tidA, SpanID: sid2, ParentSpanID: sid1, Name: "child",
			Kind: "client", StartUnixNano: start + 100, EndUnixNano: start + 500},
	})
	if len(res.Rejected) != 0 {
		t.Fatalf("expected no rejects, got %+v", res.Rejected)
	}
	if len(res.Accepted) != 2 {
		t.Fatalf("expected 2 accepted, got %d", len(res.Accepted))
	}
	root, child := res.Accepted[0], res.Accepted[1]
	if !root.IsRoot() || child.IsRoot() {
		t.Fatalf("root/child link wrong: root=%v child-parent=%s", root.IsRoot(), child.ParentID)
	}
	if child.ParentID != root.SpanID {
		t.Fatalf("child parent %s != root %s", child.ParentID, root.SpanID)
	}
	if root.Source != SourceHTTP || child.Source != SourceHTTP {
		t.Fatalf("source tag missing")
	}
	log.Printf("trace=%s decision=n/a normalized=2 rejected=0 happy-path http", tidA)
}

// TestRejectClasses checks every distinguishable rejection reason and that a
// bad record never contaminates the healthy record in the same batch.
func TestRejectClasses(t *testing.T) {
	n := New()
	start := nano(2026, 9, 23, 11, 0)

	batch := []HTTPSpanV1{
		// 0: healthy
		{TraceID: tidA, SpanID: sid1, Name: "good-root", Kind: "server",
			StartUnixNano: start, EndUnixNano: start + 10},
		// 1: missing context (bad trace id)
		{TraceID: "zzz", SpanID: sid2, Name: "x", StartUnixNano: start, EndUnixNano: start + 1},
		// 2: missing context (all-zero span id)
		{TraceID: tidA, SpanID: "0000000000000000", Name: "x", StartUnixNano: start, EndUnixNano: start + 1},
		// 3: invalid fields (bad kind)
		{TraceID: tidA, SpanID: sid3, Name: "x", Kind: "nope",
			StartUnixNano: start, EndUnixNano: start + 1},
		// 4: invalid fields (end before start)
		{TraceID: tidA, SpanID: "aaaaaaaaaaaaaaaa", Name: "x",
			StartUnixNano: start + 10, EndUnixNano: start},
		// 5: broken link (parent not in batch)
		{TraceID: tidA, SpanID: "cccccccccccccccc", ParentSpanID: "dddddddddddddddd",
			Name: "orphan", StartUnixNano: start, EndUnixNano: start + 1},
		// 6: duplicate of index 0
		{TraceID: tidA, SpanID: sid1, Name: "dup", Kind: "server",
			StartUnixNano: start, EndUnixNano: start + 1},
	}
	res := n.IngestHTTP(batch)

	counts := map[RejectClass]int{}
	for _, r := range res.Rejected {
		counts[r.Reason]++
		log.Printf("trace=%s decision=n/a reject_index=%d class=%s detail=%q",
			orEmpty(r.TraceHint), r.Index, r.Reason, r.Detail)
	}
	for _, want := range []RejectClass{RejectMissingContext, RejectInvalidFields, RejectBrokenLink, RejectDuplicateSpan} {
		if counts[want] == 0 {
			t.Fatalf("expected at least one %s rejection, counts=%v", want, counts)
		}
	}
	// Exactly one healthy span survives; bad records did not pollute it.
	if len(res.Accepted) != 1 {
		t.Fatalf("expected exactly 1 clean span, got %d: %+v", len(res.Accepted), res.Accepted)
	}
	if res.Accepted[0].SpanID.String() != sid1 {
		t.Fatalf("healthy span was contaminated: %s", res.Accepted[0].SpanID)
	}
	// Every input record accounted exactly once.
	if len(res.Accepted)+len(res.Rejected) != len(batch) {
		t.Fatalf("records lost: %d+%d != %d", len(res.Accepted), len(res.Rejected), len(batch))
	}
}

func orEmpty(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// TestBrokenSubtreeClassified verifies a child of a broken parent is itself
// classified as broken rather than silently accepted as a pseudo-root.
func TestBrokenSubtreeClassified(t *testing.T) {
	n := New()
	start := nano(2026, 9, 23, 12, 0)
	res := n.IngestHTTP([]HTTPSpanV1{
		{TraceID: tidB, SpanID: sid1, ParentSpanID: "1111111111111111", Name: "broken-root",
			StartUnixNano: start, EndUnixNano: start + 1},
		{TraceID: tidB, SpanID: sid2, ParentSpanID: sid1, Name: "grandchild-of-missing",
			StartUnixNano: start, EndUnixNano: start + 1},
	})
	if len(res.Accepted) != 0 {
		t.Fatalf("expected whole detached subtree rejected, accepted=%d", len(res.Accepted))
	}
	if len(res.Rejected) != 2 {
		t.Fatalf("expected 2 broken_link rejects, got %d: %+v", len(res.Rejected), res.Rejected)
	}
	for _, r := range res.Rejected {
		if r.Reason != RejectBrokenLink {
			t.Fatalf("unexpected class %s", r.Reason)
		}
		log.Printf("trace=%s class=%s index=%d detached-subtree", tidB, r.Reason, r.Index)
	}
}

// TestCrossTraceParentIsBroken ensures a parent id from another trace cannot
// link spans across traces.
func TestCrossTraceParentIsBroken(t *testing.T) {
	n := New()
	start := nano(2026, 9, 23, 13, 0)
	res := n.IngestHTTP([]HTTPSpanV1{
		{TraceID: tidA, SpanID: sid1, Name: "a-root",
			StartUnixNano: start, EndUnixNano: start + 1},
		{TraceID: tidB, SpanID: sid2, ParentSpanID: sid1, Name: "b-cross-ref",
			StartUnixNano: start, EndUnixNano: start + 1},
	})
	if len(res.Accepted) != 1 || res.Accepted[0].TraceID.String() != tidA {
		t.Fatalf("only trace A root should survive, accepted=%+v", res.Accepted)
	}
	if len(res.Rejected) != 1 || res.Rejected[0].Reason != RejectBrokenLink {
		t.Fatalf("expected one cross-trace broken link, got %+v", res.Rejected)
	}
}

// TestW3CAndZipkinSources exercises the other two parsing entry points.
func TestW3CAndZipkinSources(t *testing.T) {
	n := New()
	w3c := W3CSpanV1{
		Version:     0,
		TraceParent: "00-" + tidA + "-" + sid1 + "-01",
		Name:        "w3c-root", Kind: int(2 /*server*/),
		StartNano: nano(2026, 9, 23, 14, 0), DurationNano: 100,
	}
	r1 := n.IngestW3C([]W3CSpanV1{w3c})
	if len(r1.Rejected) != 0 || len(r1.Accepted) != 1 {
		t.Fatalf("w3c happy path failed: %+v", r1)
	}
	bad := w3c
	bad.TraceParent = "garbage"
	r1b := n.IngestW3C([]W3CSpanV1{bad})
	if len(r1b.Rejected) != 1 {
		t.Fatalf("malformed traceparent must be rejected: %+v", r1b)
	}

	// Zipkin 64-bit trace id (16 hex) maps to the low 8 bytes.
	z := ZipkinSpanV1{
		TraceID: "0000000000000001", ID: sid1, Name: "z-root", Kind: "SERVER",
		TimestampMicro: time.Date(2026, 9, 23, 15, 0, 0, 0, time.UTC).UnixMicro(),
		DurationMicro:  250, Error: "boom",
	}
	r2 := n.IngestZipkin([]ZipkinSpanV1{z})
	if len(r2.Rejected) != 0 || len(r2.Accepted) != 1 {
		t.Fatalf("zipkin happy path failed: %+v", r2)
	}
	if r2.Accepted[0].StatusCode != 2 /*StatusError*/ || r2.Accepted[0].StatusMsg != "boom" {
		t.Fatalf("zipkin error mapping failed: %+v", r2.Accepted[0])
	}
	log.Printf("trace=%s decision=n/a normalized sources w3c+zipkin ok", r2.Accepted[0].TraceID)
}

// TestNoSilentDropRequiresTotalAccounting verifies the sum invariant on a mixed
// batch for every source.
func TestNoSilentDropRequiresTotalAccounting(t *testing.T) {
	n := New()
	start := nano(2026, 9, 23, 16, 0)
	batch := []W3CSpanV1{
		{Version: 0, TraceParent: "00-" + tidA + "-" + sid1 + "-01", Name: "ok",
			StartNano: start, DurationNano: 1},
		{Version: 0, TraceParent: "", Name: "no-ctx"},
	}
	res := n.IngestW3C(batch)
	if len(res.Accepted)+len(res.Rejected) != len(batch) {
		t.Fatalf("accounting broken: %d+%d != %d", len(res.Accepted), len(res.Rejected), len(batch))
	}
	if res.Rejected[0].Reason != RejectMissingContext {
		t.Fatalf("empty traceparent => missing_context, got %s", res.Rejected[0].Reason)
	}
}
