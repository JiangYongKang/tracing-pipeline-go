package normalize

// Raw record shapes emulate three local reporter formats. Fields stay in
// their source-native encodings so that the normalizer alone is responsible
// for parsing and validation.

// HTTPSpanV1 is the JSON-over-HTTP reporter shape.
type HTTPSpanV1 struct {
	TraceID       string            `json:"trace_id"`
	SpanID        string            `json:"span_id"`
	ParentSpanID  string            `json:"parent_span_id,omitempty"`
	Name          string            `json:"name"`
	Kind          string            `json:"kind,omitempty"`
	StartUnixNano int64             `json:"start_unix_nano"`
	EndUnixNano   int64             `json:"end_unix_nano"`
	Attributes    map[string]string `json:"attributes,omitempty"`
	Status        string            `json:"status,omitempty"`
	StatusMsg     string            `json:"status_message,omitempty"`
}

// W3CSpanV1 is a W3C-trace-context-like reporter shape.
type W3CSpanV1 struct {
	Version      int               `json:"ver"`
	TraceParent  string            `json:"traceparent"` // version-traceid-spanid-flags
	Parent       string            `json:"parent,omitempty"`
	Name         string            `json:"name"`
	Kind         int               `json:"kind,omitempty"`
	StartNano    int64             `json:"ts"`
	DurationNano int64             `json:"dur"`
	Tags         map[string]string `json:"tags,omitempty"`
	OK           *bool             `json:"ok,omitempty"`
}

// ZipkinSpanV1 is a Zipkin-JSON-like reporter shape.
type ZipkinSpanV1 struct {
	TraceID        string
	ID             string
	ParentID       string
	Name           string
	Kind           string // "SERVER"/"CLIENT"/""
	TimestampMicro int64
	DurationMicro  int64
	LocalTags      map[string]string
	Error          string
}
