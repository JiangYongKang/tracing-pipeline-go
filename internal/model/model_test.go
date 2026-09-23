package model

import "testing"

func TestParseAndRenderIDs(t *testing.T) {
	tidStr := "4bf92f3577b34da6a3ce929d0e0e4736"
	sidStr := "00f067aa0ba902b7"
	tid, err := ParseTraceID(tidStr)
	if err != nil {
		t.Fatalf("ParseTraceID: %v", err)
	}
	sid, err := ParseSpanID(sidStr)
	if err != nil {
		t.Fatalf("ParseSpanID: %v", err)
	}
	if got := tid.String(); got != tidStr {
		t.Fatalf("trace render = %q, want %q", got, tidStr)
	}
	if got := sid.String(); got != sidStr {
		t.Fatalf("span render = %q, want %q", got, sidStr)
	}
	if tid.IsZero() || sid.IsZero() {
		t.Fatal("non-zero ids reported zero")
	}
}

func TestParseIDRejects(t *testing.T) {
	bad := []string{"", "short", "4bf92f3577b34da6a3ce929d0e0e47zz", "00000000000000000000000000000000"}
	for _, s := range bad {
		if _, err := ParseTraceID(s); err == nil {
			t.Fatalf("ParseTraceID(%q) unexpectedly ok", s)
		}
	}
	badSpan := []string{"", "00", "00f067aa0ba902zz", "0000000000000000"}
	for _, s := range badSpan {
		if _, err := ParseSpanID(s); err == nil {
			t.Fatalf("ParseSpanID(%q) unexpectedly ok", s)
		}
	}
}

func TestZeroIDRendersEmpty(t *testing.T) {
	if (TraceID{}).String() != "" || (SpanID{}).String() != "" {
		t.Fatal("zero id must render empty")
	}
}
