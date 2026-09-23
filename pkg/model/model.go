// Package model defines the unified in-process trace model shared by every
// ingestion entry point (HTTP JSON, W3C-like, Zipkin-like sources).
package model

import (
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

// TraceID is a 16-byte (128-bit) trace identifier, W3C trace-identity shaped.
type TraceID [16]byte

// SpanID is an 8-byte (64-bit) span identifier.
type SpanID [8]byte

var (
	// ErrBadTraceID means a trace id was absent or not 32 hex chars / all zero.
	ErrBadTraceID = errors.New("illegal trace id: require 32 lowercase-or-uppercase hex chars, non-zero")
	// ErrBadSpanID means a span id was absent or not 16 hex chars / all zero.
	ErrBadSpanID = errors.New("illegal span id: require 16 hex chars, non-zero")
)

// parseHexBytes decodes exactly want bytes of hex (allowing upper/lower case)
// into dst and verifies the value is not all-zero.
func parseHexBytes(s string, dst []byte, want int, zeroErr error) error {
	if len(s) != want*2 {
		return zeroErr
	}
	if _, err := hex.Decode(dst, []byte(s)); err != nil {
		return zeroErr
	}
	for _, b := range dst {
		if b != 0 {
			return nil
		}
	}
	return zeroErr
}

// ParseTraceID parses a 32-hex-char trace id; the all-zero id is illegal.
func ParseTraceID(s string) (TraceID, error) {
	var t TraceID
	if err := parseHexBytes(strings.TrimSpace(s), t[:], 16, ErrBadTraceID); err != nil {
		return TraceID{}, err
	}
	return t, nil
}

// ParseSpanID parses a 16-hex-char span id; the all-zero id is illegal.
func ParseSpanID(s string) (SpanID, error) {
	var id SpanID
	if err := parseHexBytes(strings.TrimSpace(s), id[:], 8, ErrBadSpanID); err != nil {
		return SpanID{}, err
	}
	return id, nil
}

func (t TraceID) String() string { return hex.EncodeToString(t[:]) }
func (s SpanID) String() string  { return hex.EncodeToString(s[:]) }

func (t TraceID) IsZero() bool {
	for _, b := range t {
		if b != 0 {
			return false
		}
	}
	return true
}

func (s SpanID) IsZero() bool {
	for _, b := range s {
		if b != 0 {
			return false
		}
	}
	return true
}

// SpanKind is the unified span kind across ingestion sources.
type SpanKind int

const (
	KindUnspecified SpanKind = iota
	KindInternal
	KindServer
	KindClient
	KindProducer
	KindConsumer
)

// ErrBadKind is returned for an unrecognised, non-empty kind value.
var ErrBadKind = errors.New("illegal span kind")

var kindWords = map[string]SpanKind{
	"unspecified": KindUnspecified,
	"internal":    KindInternal,
	"server":      KindServer,
	"client":      KindClient,
	"producer":    KindProducer,
	"consumer":    KindConsumer,
	// common W3C / Zipkin-ish spellings
	"":                      KindUnspecified,
	"span_kind_unspecified": KindUnspecified,
	"span_kind_internal":    KindInternal,
	"span_kind_server":      KindServer,
	"span_kind_client":      KindClient,
	"span_kind_producer":    KindProducer,
	"span_kind_consumer":    KindConsumer,
}

// ParseSpanKind maps a source-specific kind string onto the unified kind.
// An empty string is legal (KindUnspecified); any other unknown value errors.
func ParseSpanKind(s string) (SpanKind, error) {
	k, ok := kindWords[strings.ToLower(strings.TrimSpace(s))]
	if !ok {
		return KindUnspecified, ErrBadKind
	}
	return k, nil
}

func (k SpanKind) String() string {
	switch k {
	case KindInternal:
		return "internal"
	case KindServer:
		return "server"
	case KindClient:
		return "client"
	case KindProducer:
		return "producer"
	case KindConsumer:
		return "consumer"
	default:
		return "unspecified"
	}
}

// SpanStatusCode is the unified status code.
type SpanStatusCode int

const (
	StatusUnset SpanStatusCode = iota
	StatusOK
	StatusError
)

// Span is the normalized, source-independent span representation.
type Span struct {
	TraceID    TraceID
	SpanID     SpanID
	ParentID   SpanID // zero value marks a root span
	Name       string
	Kind       SpanKind
	Start      time.Time
	End        time.Time
	Attributes map[string]string
	StatusCode SpanStatusCode
	StatusMsg  string
	Source     string // ingestion source tag, never propagated across records
}

// IsRoot reports whether the span is a root (has no parent).
func (s *Span) IsRoot() bool { return s.ParentID.IsZero() }
