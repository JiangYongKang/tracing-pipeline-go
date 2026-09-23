// Package metrics holds the externally queryable counters and gauges of the
// tracing pipeline. All operations are concurrency safe.
package metrics

import "sync/atomic"

// Field names identify a counter; they are also used as log/snapshot labels.
const (
	FieldIngested        = "ingested"
	FieldRejected        = "rejected"
	FieldAccepted        = "accepted"
	FieldSampledIn       = "sampled_in"
	FieldSampledOut      = "sampled_out"
	FieldExportAttempts  = "export_attempts"
	FieldExportSuccesses = "export_successes"
	FieldExportFailures  = "export_failures"
	FieldRetried         = "retried"
	FieldDLQEnqueued     = "dlq_enqueued"
	FieldDLQDrained      = "dlq_drained"
	FieldDroppedDLQFull  = "dropped_dlq_full"
	FieldDuplicates      = "duplicates"
	FieldQueueDepth      = "queue_depth"
	FieldDLQDepth        = "dlq_depth"
)

// Counters is a flat set of monotonic counters plus bounded gauges.
// Every field is an atomic int64, so a running system can be inspected at any
// time without stopping ingestion or export.
type Counters struct {
	ingested        atomic.Int64 // valid raw records that entered normalization
	rejected        atomic.Int64 // records rejected by normalization
	accepted        atomic.Int64 // spans that passed normalization
	sampledIn       atomic.Int64 // spans belonging to sampled-in traces
	sampledOut      atomic.Int64 // spans belonging to sampled-out traces
	exportAttempts  atomic.Int64 // attempts handed to the reporter (incl. retries)
	exportSuccesses atomic.Int64 // batches acknowledged by the reporter
	exportFailures  atomic.Int64 // batches whose final attempt failed
	retried         atomic.Int64 // extra attempts performed beyond the first
	dlqEnqueued     atomic.Int64 // batches moved into the DLQ
	dlqDrained      atomic.Int64 // batches successfully redelivered from DLQ
	droppedDLQFull  atomic.Int64 // batches lost because the DLQ needed eviction
	duplicates      atomic.Int64 // duplicate submissions suppressed at export

	queueDepth atomic.Int64 // spans currently buffered for export
	dlqDepth   atomic.Int64 // batches currently held in the DLQ
}

// New creates an empty counter set.
func New() *Counters { return &Counters{} }

// Snapshot is a point-in-time, externally observable view of the counters.
type Snapshot struct {
	Ingested        int64
	Rejected        int64
	Accepted        int64
	SampledIn       int64
	SampledOut      int64
	ExportAttempts  int64
	ExportSuccesses int64
	ExportFailures  int64
	Retried         int64
	DLQEnqueued     int64
	DLQDrained      int64
	DroppedDLQFull  int64
	Duplicates      int64
	QueueDepth      int64
	DLQDepth        int64
}

// Snapshot returns a point-in-time copy. Each field is read atomically; the
// snapshot is field-wise consistent (counters are monotonic, so callers can
// safely derive inequalities such as accepted == sampledIn + sampledOut).
func (c *Counters) Snapshot() Snapshot {
	return Snapshot{
		Ingested:        c.ingested.Load(),
		Rejected:        c.rejected.Load(),
		Accepted:        c.accepted.Load(),
		SampledIn:       c.sampledIn.Load(),
		SampledOut:      c.sampledOut.Load(),
		ExportAttempts:  c.exportAttempts.Load(),
		ExportSuccesses: c.exportSuccesses.Load(),
		ExportFailures:  c.exportFailures.Load(),
		Retried:         c.retried.Load(),
		DLQEnqueued:     c.dlqEnqueued.Load(),
		DLQDrained:      c.dlqDrained.Load(),
		DroppedDLQFull:  c.droppedDLQFull.Load(),
		Duplicates:      c.duplicates.Load(),
		QueueDepth:      c.queueDepth.Load(),
		DLQDepth:        c.dlqDepth.Load(),
	}
}

// --- increment helpers (monotonic counters) ---

func (c *Counters) IncIngested(d int64)        { c.ingested.Add(d) }
func (c *Counters) IncRejected(d int64)        { c.rejected.Add(d) }
func (c *Counters) IncAccepted(d int64)        { c.accepted.Add(d) }
func (c *Counters) IncSampledIn(d int64)       { c.sampledIn.Add(d) }
func (c *Counters) IncSampledOut(d int64)      { c.sampledOut.Add(d) }
func (c *Counters) IncExportAttempts(d int64)  { c.exportAttempts.Add(d) }
func (c *Counters) IncExportSuccesses(d int64) { c.exportSuccesses.Add(d) }
func (c *Counters) IncExportFailures(d int64)  { c.exportFailures.Add(d) }
func (c *Counters) IncRetried(d int64)         { c.retried.Add(d) }
func (c *Counters) IncDLQEnqueued(d int64)     { c.dlqEnqueued.Add(d) }
func (c *Counters) IncDLQDrained(d int64)      { c.dlqDrained.Add(d) }
func (c *Counters) IncDroppedDLQFull(d int64)  { c.droppedDLQFull.Add(d) }
func (c *Counters) IncDuplicates(d int64)      { c.duplicates.Add(d) }

// --- gauge helpers (may move both ways) ---

func (c *Counters) AddQueueDepth(d int64) int64 { return c.queueDepth.Add(d) }
func (c *Counters) AddDLQDepth(d int64) int64   { return c.dlqDepth.Add(d) }
