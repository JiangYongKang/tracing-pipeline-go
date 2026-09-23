// Package normalize converts spans from heterogeneous local reporter formats
// into the unified model.Span and classifies every unprocessable record with
// a specific, distinguishable reason. No record is ever silently dropped:
// each input record appears exactly once in either Accepted or Rejected.
package normalize

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/highcumontoa/tracing-pipeline-go/pkg/model"
)

// RejectClass is the distinguishable classification of an invalid record.
type RejectClass string

const (
	// RejectMissingContext: trace id or span id absent / illegal.
	RejectMissingContext RejectClass = "missing_context"
	// RejectInvalidFields: ids present but a field is unparsable/illegal
	// (bad kind, bad timestamps, malformed traceparent, unknown status...).
	RejectInvalidFields RejectClass = "invalid_fields"
	// RejectBrokenLink: a syntactically valid parent id does not resolve to a
	// span of the same trace (unknown id, cross-trace reference, cycle, or
	// parent record itself invalid).
	RejectBrokenLink RejectClass = "broken_link"
	// RejectDuplicateSpan: same trace id + span id appears more than once.
	RejectDuplicateSpan RejectClass = "duplicate_span"
)

// Source tags carried on every accepted span / rejection.
const (
	SourceHTTP   = "http-json-v1"
	SourceW3C    = "w3c-context-v1"
	SourceZipkin = "zipkin-json-v1"
)

// RejectedRecord retains the original position and a specific reason.
type RejectedRecord struct {
	Source    string
	Index     int
	Reason    RejectClass
	Detail    string
	TraceHint string // best-effort trace id for log correlation
}

// Result is the deterministic outcome of normalizing one input batch.
//
// Batch contract: all spans belonging to the same trace MUST be submitted in
// the same batch. Broken parent/child links are detected within a batch, so
// cross-batch traces are out of scope by design (documented, deterministic).
// Every input record lands exactly once: either as an Accepted span or as a
// Rejected entry. Rejected is ordered by input index.
type Result struct {
	Accepted []model.Span
	Rejected []RejectedRecord
}

// Normalizer is stateless across batches; per-batch validation only.
type Normalizer struct{}

// New returns a Normalizer.
func New() *Normalizer { return &Normalizer{} }

// candidate is one syntactically parsed record awaiting link validation.
type candidate struct {
	index     int
	source    string
	traceHint string
	span      model.Span
}

// parseOutcome is either a candidate or a per-record rejection.
type parseOutcome struct {
	cand candidate
	rej  *RejectedRecord
}

func rejectAt(idx int, source, hint string, class RejectClass, format string, args ...any) *RejectedRecord {
	return &RejectedRecord{
		Source:    source,
		Index:     idx,
		Reason:    class,
		Detail:    fmt.Sprintf(format, args...),
		TraceHint: hint,
	}
}

func parseStatus(s string) (model.SpanStatusCode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "unset", "status_code_unset":
		return model.StatusUnset, nil
	case "ok", "status_code_ok":
		return model.StatusOK, nil
	case "error", "status_code_error":
		return model.StatusError, nil
	default:
		return model.StatusUnset, fmt.Errorf("unknown status %q", s)
	}
}

func cloneAttrs(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func parseHTTP(idx int, r HTTPSpanV1) parseOutcome {
	src := SourceHTTP
	hint := strings.TrimSpace(r.TraceID)
	tid, err := model.ParseTraceID(r.TraceID)
	if err != nil {
		return parseOutcome{rej: rejectAt(idx, src, hint, RejectMissingContext, "trace_id: %v", err)}
	}
	sid, err := model.ParseSpanID(r.SpanID)
	if err != nil {
		return parseOutcome{rej: rejectAt(idx, src, hint, RejectMissingContext, "span_id: %v", err)}
	}
	var pid model.SpanID
	if strings.TrimSpace(r.ParentSpanID) != "" {
		pid, err = model.ParseSpanID(r.ParentSpanID)
		if err != nil {
			return parseOutcome{rej: rejectAt(idx, src, hint, RejectInvalidFields, "parent_span_id: %v", err)}
		}
	}
	kind, err := model.ParseSpanKind(r.Kind)
	if err != nil {
		return parseOutcome{rej: rejectAt(idx, src, hint, RejectInvalidFields, "kind: %v", err)}
	}
	status, err := parseStatus(r.Status)
	if err != nil {
		return parseOutcome{rej: rejectAt(idx, src, hint, RejectInvalidFields, "%v", err)}
	}
	if r.StartUnixNano <= 0 {
		return parseOutcome{rej: rejectAt(idx, src, hint, RejectInvalidFields, "start_unix_nano must be > 0, got %d", r.StartUnixNano)}
	}
	if r.EndUnixNano < r.StartUnixNano {
		return parseOutcome{rej: rejectAt(idx, src, hint, RejectInvalidFields, "end_unix_nano %d before start_unix_nano %d", r.EndUnixNano, r.StartUnixNano)}
	}
	sp := model.Span{
		TraceID:    tid,
		SpanID:     sid,
		ParentID:   pid,
		Name:       r.Name,
		Kind:       kind,
		Start:      time.Unix(0, r.StartUnixNano).UTC(),
		End:        time.Unix(0, r.EndUnixNano).UTC(),
		Attributes: cloneAttrs(r.Attributes),
		StatusCode: status,
		StatusMsg:  r.StatusMsg,
		Source:     src,
	}
	return parseOutcome{cand: candidate{index: idx, source: src, traceHint: hint, span: sp}}
}

// parseTraceparent decodes `vv-tttt...-ssss-ff` (W3C traceparent shape).
func parseTraceparent(tp string) (tid model.TraceID, sid model.SpanID, err error) {
	parts := strings.Split(strings.TrimSpace(tp), "-")
	if len(parts) != 4 {
		return tid, sid, fmt.Errorf("traceparent must have 4 dash-separated fields, got %d", len(parts))
	}
	if len(parts[0]) != 2 || len(parts[3]) != 2 {
		return tid, sid, fmt.Errorf("traceparent version/flags must be 2 hex chars each")
	}
	if _, e := strconv.ParseUint(parts[0], 16, 8); e != nil {
		return tid, sid, fmt.Errorf("traceparent version not hex")
	}
	if _, e := strconv.ParseUint(parts[3], 16, 8); e != nil {
		return tid, sid, fmt.Errorf("traceparent flags not hex")
	}
	tid, err = model.ParseTraceID(parts[1])
	if err != nil {
		return tid, sid, fmt.Errorf("traceparent trace id: %w", err)
	}
	sid, err = model.ParseSpanID(parts[2])
	if err != nil {
		return tid, sid, fmt.Errorf("traceparent span id: %w", err)
	}
	return tid, sid, nil
}

func parseW3C(idx int, r W3CSpanV1) parseOutcome {
	src := SourceW3C
	tid, sid, err := parseTraceparent(r.TraceParent)
	hint := ""
	if p := strings.SplitN(strings.TrimSpace(r.TraceParent), "-", 3); len(p) >= 2 {
		hint = p[1]
	}
	if err != nil {
		// Missing identity vs. malformed fields: no/illegal id => missing context.
		if strings.TrimSpace(r.TraceParent) == "" {
			return parseOutcome{rej: rejectAt(idx, src, hint, RejectMissingContext, "traceparent empty")}
		}
		class := RejectInvalidFields
		if strings.Contains(err.Error(), "trace id") || strings.Contains(err.Error(), "span id") {
			class = RejectMissingContext
		}
		return parseOutcome{rej: rejectAt(idx, src, hint, class, "%v", err)}
	}
	var pid model.SpanID
	if strings.TrimSpace(r.Parent) != "" {
		pid, err = model.ParseSpanID(r.Parent)
		if err != nil {
			return parseOutcome{rej: rejectAt(idx, src, hint, RejectInvalidFields, "parent: %v", err)}
		}
	}
	if r.Version != 0 {
		return parseOutcome{rej: rejectAt(idx, src, hint, RejectInvalidFields, "unsupported ver %d (want 0)", r.Version)}
	}
	if r.Kind < 0 || r.Kind > int(model.KindConsumer) {
		return parseOutcome{rej: rejectAt(idx, src, hint, RejectInvalidFields, "kind %d out of range [0,5]", r.Kind)}
	}
	kind := model.SpanKind(r.Kind)
	if r.StartNano <= 0 {
		return parseOutcome{rej: rejectAt(idx, src, hint, RejectInvalidFields, "ts must be > 0, got %d", r.StartNano)}
	}
	if r.DurationNano < 0 {
		return parseOutcome{rej: rejectAt(idx, src, hint, RejectInvalidFields, "dur must be >= 0, got %d", r.DurationNano)}
	}
	status := model.StatusUnset
	if r.OK != nil {
		if *r.OK {
			status = model.StatusOK
		} else {
			status = model.StatusError
		}
	}
	sp := model.Span{
		TraceID:    tid,
		SpanID:     sid,
		ParentID:   pid,
		Name:       r.Name,
		Kind:       kind,
		Start:      time.Unix(0, r.StartNano).UTC(),
		End:        time.Unix(0, r.StartNano+r.DurationNano).UTC(),
		Attributes: cloneAttrs(r.Tags),
		StatusCode: status,
		Source:     src,
	}
	return parseOutcome{cand: candidate{index: idx, source: src, traceHint: hint, span: sp}}
}

// parseZipkinTraceID accepts the 16-hex (64-bit) or 32-hex (128-bit) form.
func parseZipkinTraceID(s string) (model.TraceID, error) {
	s = strings.TrimSpace(s)
	var t model.TraceID
	switch len(s) {
	case 16:
		raw := make([]byte, 8)
		if _, err := decodeHex(raw, s); err != nil {
			return t, model.ErrBadTraceID
		}
		copy(t[8:], raw) // high 8 bytes are zero
	case 32:
		if _, err := decodeHex(t[:], s); err != nil {
			return t, model.ErrBadTraceID
		}
	default:
		return t, model.ErrBadTraceID
	}
	if t.IsZero() {
		return t, model.ErrBadTraceID
	}
	return t, nil
}

func decodeHex(dst []byte, s string) (int, error) {
	if len(s) != len(dst)*2 {
		return 0, fmt.Errorf("bad length")
	}
	const hexd = "0123456789abcdefABCDEF"
	for i := 0; i < len(dst); i++ {
		hi := strings.IndexByte(hexd, s[i*2])
		lo := strings.IndexByte(hexd, s[i*2+1])
		if hi < 0 || lo < 0 {
			return 0, fmt.Errorf("not hex")
		}
		if hi > 15 {
			hi -= 6
		}
		if lo > 15 {
			lo -= 6
		}
		dst[i] = byte(hi<<4 | lo)
	}
	return len(dst), nil
}

func parseZipkin(idx int, r ZipkinSpanV1) parseOutcome {
	src := SourceZipkin
	hint := strings.TrimSpace(r.TraceID)
	tid, err := parseZipkinTraceID(r.TraceID)
	if err != nil {
		return parseOutcome{rej: rejectAt(idx, src, hint, RejectMissingContext, "traceId: %v", err)}
	}
	sid, err := model.ParseSpanID(r.ID)
	if err != nil {
		return parseOutcome{rej: rejectAt(idx, src, hint, RejectMissingContext, "id: %v", err)}
	}
	var pid model.SpanID
	if strings.TrimSpace(r.ParentID) != "" {
		pid, err = model.ParseSpanID(r.ParentID)
		if err != nil {
			return parseOutcome{rej: rejectAt(idx, src, hint, RejectInvalidFields, "parentId: %v", err)}
		}
	}
	kind, err := model.ParseSpanKind(r.Kind)
	if err != nil {
		return parseOutcome{rej: rejectAt(idx, src, hint, RejectInvalidFields, "kind: %v", err)}
	}
	if r.TimestampMicro <= 0 {
		return parseOutcome{rej: rejectAt(idx, src, hint, RejectInvalidFields, "timestamp must be > 0, got %d", r.TimestampMicro)}
	}
	if r.DurationMicro < 0 {
		return parseOutcome{rej: rejectAt(idx, src, hint, RejectInvalidFields, "duration must be >= 0, got %d", r.DurationMicro)}
	}
	status := model.StatusUnset
	statusMsg := ""
	if r.Error != "" {
		status = model.StatusError
		statusMsg = r.Error
	}
	sp := model.Span{
		TraceID:    tid,
		SpanID:     sid,
		ParentID:   pid,
		Name:       r.Name,
		Kind:       kind,
		Start:      time.UnixMicro(r.TimestampMicro).UTC(),
		End:        time.UnixMicro(r.TimestampMicro + r.DurationMicro).UTC(),
		Attributes: cloneAttrs(r.LocalTags),
		StatusCode: status,
		StatusMsg:  statusMsg,
		Source:     src,
	}
	return parseOutcome{cand: candidate{index: idx, source: src, traceHint: hint, span: sp}}
}

// validateLinks performs the cross-record checks in index order and splits
// candidates into final accepted spans / rejections.
func validateLinks(outcomes []parseOutcome) Result {
	var res Result

	// Pass 1: keep syntactically valid candidates; surface per-record rejects.
	cands := make([]candidate, 0, len(outcomes))
	rejected := make([]RejectedRecord, 0)
	for _, o := range outcomes {
		if o.rej != nil {
			rejected = append(rejected, *o.rej)
			continue
		}
		cands = append(cands, o.cand)
	}

	// Pass 2: duplicates — key (trace, span); first occurrence wins by index.
	type key struct {
		t model.TraceID
		s model.SpanID
	}
	seen := make(map[key]int) // key -> position in cands
	dup := make(map[int]bool) // positions rejected as duplicates
	for i, c := range cands {
		k := key{c.span.TraceID, c.span.SpanID}
		if first, ok := seen[k]; ok {
			dup[i] = true
			rejected = append(rejected, RejectedRecord{
				Source:    c.source,
				Index:     c.index,
				Reason:    RejectDuplicateSpan,
				Detail:    fmt.Sprintf("span %s repeats earlier span at index %d in trace %s", c.span.SpanID, cands[first].index, c.span.TraceID),
				TraceHint: c.traceHint,
			})
			continue
		}
		seen[k] = i
	}

	// Pass 3: build identity index of surviving spans for link checks.
	idPos := make(map[key]int)
	for i, c := range cands {
		if dup[i] {
			continue
		}
		idPos[key{c.span.TraceID, c.span.SpanID}] = i
	}

	// Pass 4: resolve every parent chain; unknown parents and cycles are
	// broken links. Mark transitively-bad positions so a broken ancestor
	// classifies its whole detached subtree (never silently mixed with the
	// healthy part of the trace).
	bad := make(map[int]*RejectedRecord)
	for i, c := range cands {
		if dup[i] {
			continue
		}
		path := map[int]bool{i: true}
		cur := c.span
		chain := []model.SpanID{}
		for !cur.IsRoot() {
			pp, ok := idPos[key{cur.TraceID, cur.ParentID}]
			if !ok {
				bad[i] = &RejectedRecord{
					Source:    c.source,
					Index:     c.index,
					Reason:    RejectBrokenLink,
					Detail:    fmt.Sprintf("span %s references parent %s absent from trace %s", c.span.SpanID, cur.ParentID, c.span.TraceID),
					TraceHint: c.traceHint,
				}
				break
			}
			if path[pp] {
				// Every span on the cycle is itself broken.
				bad[i] = &RejectedRecord{
					Source:    c.source,
					Index:     c.index,
					Reason:    RejectBrokenLink,
					Detail:    fmt.Sprintf("span %s is part of a parent cycle %v in trace %s", c.span.SpanID, append(chain, cur.SpanID), c.span.TraceID),
					TraceHint: c.traceHint,
				}
				break
			}
			path[pp] = true
			chain = append(chain, cur.SpanID)
			cur = cands[pp].span
		}
	}

	// Pass 5: transitive contamination — children of a broken/duplicate
	// parent are explicitly classified (never silently merged into healthy
	// data). A direct chain check already covers dangling references to
	// removed duplicates; this loop catches ancestry transitively.
changed:
	for {
		for i, c := range cands {
			if dup[i] || bad[i] != nil {
				continue
			}
			if c.span.IsRoot() {
				continue
			}
			pp := idPos[key{c.span.TraceID, c.span.ParentID}]
			if b := bad[pp]; b != nil {
				bad[i] = &RejectedRecord{
					Source:    c.source,
					Index:     c.index,
					Reason:    RejectBrokenLink,
					Detail:    fmt.Sprintf("span %s descends from broken/duplicate parent %s in trace %s", c.span.SpanID, c.span.ParentID, c.span.TraceID),
					TraceHint: c.traceHint,
				}
				continue changed
			}
		}
		break
	}

	// Emit results in input-index order for deterministic, explainable output.
	type item struct {
		idx int
		sp  model.Span
		rj  *RejectedRecord
	}
	items := make([]item, 0, len(cands))
	for i, c := range cands {
		if dup[i] {
			continue
		}
		if r := bad[i]; r != nil {
			items = append(items, item{idx: c.index, rj: r})
		} else {
			items = append(items, item{idx: c.index, sp: c.span})
		}
	}
	for _, r := range rejected {
		items = append(items, item{idx: r.Index, rj: &r})
	}
	// items are mostly ordered; sort stable by index (insertion sort, batches
	// are typically small; avoids pulling in sort for clarity of ordering).
	for i := 1; i < len(items); i++ {
		for j := i; j > 0 && items[j-1].idx > items[j].idx; j-- {
			items[j-1], items[j] = items[j], items[j-1]
		}
	}
	for _, it := range items {
		if it.rj != nil {
			res.Rejected = append(res.Rejected, *it.rj)
		} else {
			res.Accepted = append(res.Accepted, it.sp)
		}
	}
	return res
}

// IngestHTTP normalizes a batch from the HTTP JSON reporter.
func (n *Normalizer) IngestHTTP(batch []HTTPSpanV1) Result {
	out := make([]parseOutcome, len(batch))
	for i, r := range batch {
		out[i] = parseHTTP(i, r)
	}
	return validateLinks(out)
}

// IngestW3C normalizes a batch from the W3C-like reporter.
func (n *Normalizer) IngestW3C(batch []W3CSpanV1) Result {
	out := make([]parseOutcome, len(batch))
	for i, r := range batch {
		out[i] = parseW3C(i, r)
	}
	return validateLinks(out)
}

// IngestZipkin normalizes a batch from the Zipkin-like reporter.
func (n *Normalizer) IngestZipkin(batch []ZipkinSpanV1) Result {
	out := make([]parseOutcome, len(batch))
	for i, r := range batch {
		out[i] = parseZipkin(i, r)
	}
	return validateLinks(out)
}
