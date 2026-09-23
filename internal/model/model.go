// Package model defines the unified in-process trace model shared by
// ingestion, sampling and export, plus the deterministic classification
// vocabulary used across the pipeline.
package model

import (
	"encoding/hex"
	"errors"
	"fmt"
)

// Source identifies where a raw trace record originates.
type Source string

// Supported ingestion sources. Each source has its own wire shape; the
// ingest package is responsible for normalizing them into Span.
const (
	SourceHTTPHeaders Source = "http_headers" // W3C-traceparent-like headers, hex strings
	SourceBinary      Source = "binary_v2"    // numeric/encoded ids from a binary protocol
	SourceInproc      Source = "inproc"       // in-process propagated context
)

// TraceID is a 16-byte trace identifier (W3C style).
type TraceID [16]byte

// SpanID is an 8-byte span identifier.
type SpanID [8]byte

// SpanKey uniquely identifies a span inside the pipeline.
type SpanKey struct {
	Trace TraceID
	Span  SpanID
}

var (
	zeroTraceID TraceID
	zeroSpanID  SpanID
)

// ErrMalformedID is returned when an id cannot be parsed.
var ErrMalformedID = errors.New("malformed trace/span id")

// ParseTraceID parses a 32-hex-char trace id. It rejects wrong length,
// non-hex content and the all-zero form.
func ParseTraceID(s string) (TraceID, error) {
	var id TraceID
	if len(s) != len(id)*2 {
		return zeroTraceID, fmt.Errorf("%w: trace id %q: want %d hex chars", ErrMalformedID, s, len(id)*2)
	}
	if _, err := hex.Decode(id[:], []byte(s)); err != nil {
		return zeroTraceID, fmt.Errorf("%w: trace id %q: %v", ErrMalformedID, s, err)
	}
	if id == zeroTraceID {
		return zeroTraceID, fmt.Errorf("%w: trace id must not be all zero", ErrMalformedID)
	}
	return id, nil
}

// ParseSpanID parses a 16-hex-char span id. It rejects wrong length,
// non-hex content and the all-zero form.
func ParseSpanID(s string) (SpanID, error) {
	var id SpanID
	if len(s) != len(id)*2 {
		return zeroSpanID, fmt.Errorf("%w: span id %q: want %d hex chars", ErrMalformedID, s, len(id)*2)
	}
	if _, err := hex.Decode(id[:], []byte(s)); err != nil {
		return zeroSpanID, fmt.Errorf("%w: span id %q: %v", ErrMalformedID, s, err)
	}
	if id == zeroSpanID {
		return zeroSpanID, fmt.Errorf("%w: span id must not be all zero", ErrMalformedID)
	}
	return id, nil
}

const hexdigits = "0123456789abcdef"

func renderHex(b []byte) string {
	out := make([]byte, len(b)*2)
	for i, x := range b {
		out[i*2] = hexdigits[x>>4]
		out[i*2+1] = hexdigits[x&0x0f]
	}
	return string(out)
}

// String renders the id as lowercase hex; the zero id renders as "".
func (id TraceID) String() string {
	if id == zeroTraceID {
		return ""
	}
	return renderHex(id[:])
}

// String renders the id as lowercase hex; the zero id renders as "".
func (id SpanID) String() string {
	if id == zeroSpanID {
		return ""
	}
	return renderHex(id[:])
}

// IsZero reports whether the id is all zeroes.
func (id TraceID) IsZero() bool { return id == zeroTraceID }

// IsZero reports whether the id is all zeroes.
func (id SpanID) IsZero() bool { return id == zeroSpanID }

// RawRecord is the loose, source-specific shape accepted at ingress.
// Identifier fields stay raw strings/numbers so that malformed input can
// be classified instead of panicking deep inside the pipeline.
type RawRecord struct {
	Source        Source
	TraceID       string
	SpanID        string
	ParentSpanID  string
	ParentTraceID string // empty when the source does not carry it
	Name          string
	StartNano     int64
	EndNano       int64
	Attrs         map[string]string

	// Numeric identifiers carried by the binary_v2 protocol. They are used
	// for SourceBinary when the corresponding hex string field is empty.
	// TraceNumHi/TraceNumLo form the high/low 8 bytes of the trace id;
	// SpanNum/ParentSpanNum encode the span ids. A zero value means absent.
	TraceNumHi    uint64
	TraceNumLo    uint64
	SpanNum       uint64
	ParentSpanNum uint64
}

// TraceIDFromNum builds a 16-byte trace id from high/low 8-byte words.
func TraceIDFromNum(hi, lo uint64) TraceID {
	var id TraceID
	for i := 0; i < 8; i++ {
		id[i] = byte(hi >> (56 - 8*uint(i)))
		id[8+i] = byte(lo >> (56 - 8*uint(i)))
	}
	return id
}

// SpanIDFromNum builds an 8-byte span id from a word.
func SpanIDFromNum(n uint64) SpanID {
	var id SpanID
	for i := 0; i < 8; i++ {
		id[i] = byte(n >> (56 - 8*uint(i)))
	}
	return id
}

// Span is the normalized, pipeline-wide representation of one span.
type Span struct {
	TraceID   TraceID
	SpanID    SpanID
	Parent    SpanID
	HasParent bool
	Name      string
	StartNano int64
	EndNano   int64
	Attrs     map[string]string
}

// Key returns the identity key of the span.
func (s *Span) Key() SpanKey { return SpanKey{Trace: s.TraceID, Span: s.SpanID} }

// Class is the normalization verdict of one raw record.
type Class int

const (
	// ClassOK: record normalized into a valid Span.
	ClassOK Class = iota
	// ClassMissingContext: required context (trace id / span id) is absent
	// or empty.
	ClassMissingContext
	// ClassInvalidField: present but illegal, e.g. malformed id or bad time range.
	ClassInvalidField
	// ClassBrokenParent: parent link is malformed, self-referential or
	// points into a different trace.
	ClassBrokenParent
	// ClassConflict: span id is already registered under another trace.
	ClassConflict
	// ClassDuplicate: exact same (trace id, span id) was already accepted.
	ClassDuplicate
)

// String renders the classification for logs and metrics.
func (c Class) String() string {
	switch c {
	case ClassOK:
		return "ok"
	case ClassMissingContext:
		return "missing_context"
	case ClassInvalidField:
		return "invalid_field"
	case ClassBrokenParent:
		return "broken_parent"
	case ClassConflict:
		return "conflict"
	case ClassDuplicate:
		return "duplicate"
	default:
		return "unknown"
	}
}

// Outcome is the result of normalizing a single raw record.
type Outcome struct {
	Source  Source
	RawHint string // raw identifier hint as received, for log correlation
	Status  Class
	Reason  string
	Span    *Span // non-nil iff Status == ClassOK
}

// Decision is a per-trace sampling verdict.
type Decision int

const (
	DecisionUnknown Decision = iota
	DecisionSample
	DecisionDrop
)

// String renders the decision for logs and metrics.
func (d Decision) String() string {
	switch d {
	case DecisionSample:
		return "sample"
	case DecisionDrop:
		return "drop"
	default:
		return "unknown"
	}
}

// FailureClass is the deterministic failure vocabulary for export.
type FailureClass int

const (
	FailNone             FailureClass = iota
	FailEndpointTimeout               // simulated endpoint was too slow
	FailEndpointRejected              // endpoint refused the batch deterministically
	FailEndpointError                 // endpoint returned a server error
	FailQueueFull                     // bounded queue was full at enqueue
	FailAttemptsExhausted
	FailDuplicate // span already accepted once
)

// String renders the failure class for logs and metrics.
func (f FailureClass) String() string {
	switch f {
	case FailNone:
		return "none"
	case FailEndpointTimeout:
		return "endpoint_timeout"
	case FailEndpointRejected:
		return "endpoint_rejected"
	case FailEndpointError:
		return "endpoint_error"
	case FailQueueFull:
		return "queue_full"
	case FailAttemptsExhausted:
		return "attempts_exhausted"
	case FailDuplicate:
		return "duplicate"
	default:
		return "unknown"
	}
}
