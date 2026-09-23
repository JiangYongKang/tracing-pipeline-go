package model

import (
	"testing"
)

func TestParseIDs(t *testing.T) {
	tid, err := ParseTraceID("0af7651916cd43dd8448eb211c80319c")
	if err != nil || tid.IsZero() || tid.String() != "0af7651916cd43dd8448eb211c80319c" {
		t.Fatalf("trace id round-trip failed: %v %s", err, tid)
	}
	sid, err := ParseSpanID("00f067aa0ba902b7")
	if err != nil || sid.IsZero() || sid.String() != "00f067aa0ba902b7" {
		t.Fatalf("span id round-trip failed: %v %s", err, sid)
	}
	// Upper-case hex is accepted and normalized to lower-case.
	if up, err := ParseTraceID("0AF7651916CD43DD8448EB211C80319C"); err != nil || up != tid {
		t.Fatalf("uppercase trace id mismatch: %v", err)
	}
	// All zero and malformed lengths are illegal (missing context, not a
	// silently-generated id).
	for _, bad := range []string{"", "00000000000000000000000000000000", "abc", "zzzz651916cd43dd8448eb211c80319c"} {
		if _, err := ParseTraceID(bad); err == nil {
			t.Fatalf("bad trace id %q accepted", bad)
		}
	}
	for _, bad := range []string{"", "0000000000000000", "abcd", "gggg67aa0ba902b7"} {
		if _, err := ParseSpanID(bad); err == nil {
			t.Fatalf("bad span id %q accepted", bad)
		}
	}
}

func TestKindParsing(t *testing.T) {
	cases := map[string]SpanKind{
		"":         KindUnspecified,
		"server":   KindServer,
		"SERVER":   KindServer,
		"client":   KindClient,
		"consumer": KindConsumer,
	}
	for in, want := range cases {
		got, err := ParseSpanKind(in)
		if err != nil || got != want {
			t.Fatalf("kind %q => %v,%v want %v", in, got, err, want)
		}
	}
	if _, err := ParseSpanKind("bogus"); err == nil {
		t.Fatal("bogus kind must error, not silently map to unspecified")
	}
	if KindServer.String() != "server" {
		t.Fatalf("kind string %q", KindServer.String())
	}
}

func TestSpanIsRoot(t *testing.T) {
	var root Span
	if !root.IsRoot() {
		t.Fatal("zero parent id must mean root")
	}
	pid, _ := ParseSpanID("00f067aa0ba902b7")
	child := Span{ParentID: pid}
	if child.IsRoot() {
		t.Fatal("non-zero parent id must mean child")
	}
}
