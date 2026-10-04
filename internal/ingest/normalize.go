// Package ingest normalizes trace records coming from heterogeneous
// sources into the unified model.Span shape. Invalid input is classified,
// never silently dropped or allowed to corrupt other records.
package ingest

import (
	"strings"
	"sync"

	"github.com/highcumontoa/tracing-pipeline-go/internal/model"
	"github.com/highcumontoa/tracing-pipeline-go/internal/retention"
)

// knownSources is the set of ingress channels the normalizer understands.
var knownSources = map[model.Source]struct{}{
	model.SourceHTTPHeaders: {},
	model.SourceBinary:      {},
	model.SourceInproc:      {},
}

// Normalizer is safe for concurrent use. Identity state (which span id
// belongs to which trace) lives in a bounded retention.Registry shared with
// the sampler, so duplicate/conflict detection and the frozen sampling
// decision share one bounded per-trace lifecycle. Reclaimed traces are
// remembered as bounded tombstones, which makes a later revisit of the same
// trace distinguishable instead of silently looking brand new.
type Normalizer struct {
	reg *retention.Registry
	// protected, when non-nil, reports whether a span key is still inside
	// the exporter's duplicate-protection window. It is consulted only when
	// a reclaimed trace reappears: a protected span must not begin a new
	// lifecycle nor be re-registered.
	protected func(model.SpanKey) bool

	mu sync.Mutex
	// counts per normalization class, externally queryable.
	counts map[model.Class]int64
}

// New builds a Normalizer with its own bounded default registry.
func New() *Normalizer {
	return NewWithRegistry(retention.New(retention.DefaultConfig()), nil)
}

// NewWithRegistry builds a Normalizer sharing reg with the sampler.
// protected may be nil (standalone use: revisits always begin a new
// lifecycle).
func NewWithRegistry(reg *retention.Registry, protected func(model.SpanKey) bool) *Normalizer {
	return &Normalizer{
		reg:       reg,
		protected: protected,
		counts:    make(map[model.Class]int64),
	}
}

// Normalize converts one raw record into a model.Outcome.
//
// Classification is deterministic and independent of arrival order:
// a parent that simply has not arrived yet is never flagged as broken,
// because it may arrive later. Only structurally certain faults are.
func (n *Normalizer) Normalize(raw model.RawRecord) model.Outcome {
	hint := string(raw.Source) + ":" + strings.TrimSpace(raw.TraceID) + "/" + strings.TrimSpace(raw.SpanID)
	bad := func(c model.Class, reason string) model.Outcome {
		n.bump(c)
		return model.Outcome{Source: raw.Source, RawHint: hint, Status: c, Reason: reason}
	}

	if _, ok := knownSources[raw.Source]; !ok {
		return bad(model.ClassInvalidField, "unknown source: "+string(raw.Source))
	}

	// Field validity: time range.
	if raw.StartNano < 0 || raw.EndNano < 0 || raw.EndNano < raw.StartNano {
		return bad(model.ClassInvalidField, "invalid time range: start/end negative or end before start")
	}

	// Resolve the four identity elements. binary_v2 carries numeric ids; the
	// other sources carry hex strings. Missing required context is distinct
	// from present-but-illegal values.
	tid, sid, parent, hasParent, oc, reason := resolveIDs(raw)
	if oc != model.ClassOK {
		return bad(oc, reason)
	}
	if hasParent && parent == sid {
		return bad(model.ClassBrokenParent, "span is its own parent")
	}

	span := &model.Span{
		TraceID:   tid,
		SpanID:    sid,
		Parent:    parent,
		HasParent: hasParent,
		Name:      raw.Name,
		StartNano: raw.StartNano,
		EndNano:   raw.EndNano,
		Attrs:     cloneAttrs(raw.Attrs),
	}

	// Identity checks happen last and only for otherwise valid records, so a
	// malformed duplicate is reported by its malformation first. Registration
	// lives in the bounded lifecycle registry: duplicates/conflicts never
	// overwrite accepted data, and a trace reclaimed by retention revisits as
	// a distinguishable new generation instead of silently resetting.
	res := n.reg.Register(tid, sid, n.protected)
	switch res.Span {
	case retention.SpanConflict:
		n.bump(model.ClassConflict)
		return model.Outcome{Source: raw.Source, RawHint: hint, Status: model.ClassConflict,
			Reason: "span id already registered under another live trace"}
	case retention.SpanDuplicate:
		n.bump(model.ClassDuplicate)
		return model.Outcome{Source: raw.Source, RawHint: hint, Status: model.ClassDuplicate,
			Reason: "span already registered in this trace"}
	case retention.SpanProtected:
		// The trace's previous lifecycle was reclaimed, but this exact span
		// is still protected by the exporter's bounded dedup bookkeeping.
		// The original conclusion stands; nothing is re-registered and no
		// new lifecycle begins. Externally distinguishable via Revisit+Protected.
		n.bump(model.ClassDuplicate)
		return model.Outcome{Source: raw.Source, RawHint: hint, Status: model.ClassDuplicate,
			Reason:    "trace reclaimed but span still under export duplicate protection; original conclusion stands",
			Revisit:   true,
			Protected: true}
	}

	if res.Begin == retention.BeginRevisit {
		n.bump(model.ClassRevisit)
		return model.Outcome{Source: raw.Source, RawHint: hint, Status: model.ClassRevisit,
			Span: span, Revisit: true, Generation: res.Generation}
	}
	n.bump(model.ClassOK)
	return model.Outcome{Source: raw.Source, RawHint: hint, Status: model.ClassOK,
		Span: span, Generation: res.Generation}
}

func (n *Normalizer) bump(c model.Class) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.counts == nil {
		n.counts = make(map[model.Class]int64)
	}
	n.counts[c]++
}

// KnownSpanCount reports how many valid, distinct spans are currently
// retained (bounded by the registry's lifecycle capacity).
func (n *Normalizer) KnownSpanCount() int {
	return n.reg.Stats().LiveSpans
}

// ClassCounts returns a copy of per-class normalization counters.
func (n *Normalizer) ClassCounts() map[model.Class]int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make(map[model.Class]int64, len(n.counts))
	for k, v := range n.counts {
		out[k] = v
	}
	return out
}

func cloneAttrs(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// resolveIDs maps a source-specific raw record onto (trace id, span id,
// parent id) in the unified model. It returns a non-OK Class/Reason for
// missing context, illegal identifiers, or a structurally broken parent.
func resolveIDs(raw model.RawRecord) (tid model.TraceID, sid model.SpanID, parent model.SpanID, hasParent bool, class model.Class, reason string) {
	class = model.ClassOK

	if raw.Source == model.SourceBinary {
		// Numeric form. Zero means absent; all-zero ids are rejected as
		// missing (required context), not treated as a valid zero id.
		if raw.TraceNumHi == 0 && raw.TraceNumLo == 0 || raw.SpanNum == 0 {
			class = model.ClassMissingContext
			reason = "numeric trace id or span id is zero/absent"
			return
		}
		tid = model.TraceIDFromNum(raw.TraceNumHi, raw.TraceNumLo)
		sid = model.SpanIDFromNum(raw.SpanNum)
		if raw.ParentSpanNum != 0 {
			hasParent = true
			parent = model.SpanIDFromNum(raw.ParentSpanNum)
		}
		// Parent trace for binary is the same numeric trace (parent and
		// child are delivered in one trace context); nothing more to check.
		return
	}

	tidStr := strings.TrimSpace(raw.TraceID)
	sidStr := strings.TrimSpace(raw.SpanID)
	if tidStr == "" || sidStr == "" {
		class = model.ClassMissingContext
		reason = "trace id or span id is empty"
		return
	}
	var err error
	if tid, err = model.ParseTraceID(tidStr); err != nil {
		class = model.ClassInvalidField
		reason = err.Error()
		return
	}
	if sid, err = model.ParseSpanID(sidStr); err != nil {
		class = model.ClassInvalidField
		reason = err.Error()
		return
	}
	if pidStr := strings.TrimSpace(raw.ParentSpanID); pidStr != "" {
		hasParent = true
		pid, perr := model.ParseSpanID(pidStr)
		if perr != nil {
			class = model.ClassBrokenParent
			reason = "parent span id malformed: " + perr.Error()
			return
		}
		if ptStr := strings.TrimSpace(raw.ParentTraceID); ptStr != "" {
			ptid, pterr := model.ParseTraceID(ptStr)
			if pterr != nil {
				class = model.ClassBrokenParent
				reason = "parent trace id malformed: " + pterr.Error()
				return
			}
			if ptid != tid {
				class = model.ClassBrokenParent
				reason = "parent belongs to a different trace"
				return
			}
		}
		parent = pid
	} else if strings.TrimSpace(raw.ParentTraceID) != "" {
		class = model.ClassBrokenParent
		reason = "parent trace id present without parent span id"
	}
	return
}
