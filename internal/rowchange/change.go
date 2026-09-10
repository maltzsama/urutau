// Package change defines the change event that flows from source decoding
// to sink writing: one row-level operation with its before/after images,
// primary key, and source position.
package rowchange

import (
	"time"

	"github.com/maltzsama/urutau/dataplane"
)

type Op uint8

const (
	OpInsert Op = iota
	OpUpdate
	OpDelete
)

func (o Op) String() string {
	switch o {
	case OpInsert:
		return "insert"
	case OpUpdate:
		return "update"
	case OpDelete:
		return "delete"
	default:
		return "unknown"
	}
}

// Change mirrors a single row-level event. Key holds the primary key values
// in table-spec order; After is nil for deletes and Before is only carried
// when needed (mutable-column filters). Position is the source coordinate
// (GTID | LSN) the event came from; CommitTS is the source commit timestamp.
//
// DELETE IMAGE CONTRACT (RV-11): a delete's row image lives in Before when
// the change comes from an in-process source decoder, and in After when the
// change was decoded from the wire — the wire carries the before image in
// the flat columns (CR-021), so DecodeBatch fills After and leaves Before
// nil. Consumers selecting the delete image must handle BOTH: prefer
// Before when non-empty, else After. Two consumers assuming one location
// produced mirrored data-loss bugs (enrich #4, sink.go RV-02).
//
// Value contract (row universe): Arrow has no Go-exact float16/float32
// value type in map[string]any, so a Float32 column DECODES as float64
// (promotion, M-3). Re-encoding a promoted value back into a Float32
// column is accepted WITHOUT a precision guard (C-7): the round-trip of
// the column depends on it. Float64 columns keep the exact-precision
// guard for integer sources.
type Change struct {
	Op       Op
	Table    string // target table ("namespace.name")
	Key      []any
	After    map[string]any
	Before   map[string]any
	Position string
	CommitTS time.Time
	// IngestTS is when the pipeline processed the event. The difference
	// between CommitTS and IngestTS is the per-row replication lag.
	IngestTS time.Time
	// Snapshot marks rows read by a DBLog chunk SELECT (true) versus live
	// stream events (false). Read by name on the collapse/window paths.
	Snapshot bool
	// Phase is the __phase wire column: "snapshot" for DBLog chunk rows,
	// "stream" for live events, "" when the producer did not set it. An axis
	// orthogonal to Op — a snapshot row is semantically an insert. Sinks
	// materialize it only when the table declares the phase metadata column.
	Phase string
	// Window tags the change as part of a DBLog snapshot window. Nil for
	// plain stream events.
	Window *Window
	// Transport is the message-queue envelope when the event came from a
	// message log (Kafka today, Kinesis next). CDC sources leave it nil and
	// their transport metadata (stream = source table, sequence = position)
	// is derived at projection time.
	Transport *Transport
	// EnrichMiss marks a left-join reference miss set by the enrichment
	// stage: the event passed with NULL reference columns. Materialized
	// only when the table declares the enrich_miss metadata column.
	EnrichMiss bool
}

// Transport is the envelope of a message-log event. The coordinate names
// are deliberately transport-neutral so Kafka, Kinesis, NATS and Pulsar fit
// without renaming. Shard and Seq are strings because a Kinesis sequence
// number is a decimal too large for int64.
type Transport struct {
	Stream  string // topic / stream name
	Shard   string // partition / shard id
	Seq     string // offset / sequence number
	MsgTS   time.Time
	MsgKey  string
	Headers string // JSON-serialized headers
}

// Window carries DBLog snapshot-window signaling on a change. The runner
// tags live events that fall inside [low, high] with InWindow so the
// batcher discards the superseded snapshot row, and emits a Closes marker
// (with no row payload) once the reader has provably caught up past high.
type Window struct {
	ChunkID  uint32
	InWindow bool
	Closes   bool
}

// Collapsed is the reduced state of a batch after per-key collapse: the last
// operation for each key wins, winners keep first-appearance order (D-6).
type Collapsed struct {
	// Changes holds the surviving rows in first-appearance order. A key
	// whose last operation is a delete appears here AS a delete (equality
	// delete only — a data row must never be emitted for it, since a delete
	// file committed together with the data it means to remove is
	// unreliable across Iceberg implementations, see the spike finding).
	Changes []Change
}

// Keys returns every primary key in the batch, in collapse order.
func (c Collapsed) Keys() [][]any {
	keys := make([][]any, 0, len(c.Changes))
	for _, ch := range c.Changes {
		keys = append(keys, ch.Key)
	}
	return keys
}

// Count returns (upserts, deletes) — winners split by final operation.
func (c Collapsed) Count() (upserts, deletes int) {
	for _, ch := range c.Changes {
		if ch.Op == OpDelete {
			deletes++
		} else {
			upserts++
		}
	}
	return upserts, deletes
}

// WriteMode controls how the worker and writer handle a batch.
type WriteMode uint8

const (
	// UpsertMode collapses batches per-key and applies equality deletes.
	UpsertMode WriteMode = iota
	// AppendMode passes every change through without collapse. Deletes are
	// emitted as upserts with op='delete' so the row carries the operation.
	AppendMode
)

// Batch is the unit handed to a committer: the changes of one table plus
// the source position reached when the batch closed.
//
// D-6: Changes holds ONE slice in ARRIVAL order — order is the input of
// last-write-wins. Splitting into upsert/delete buckets before the wire
// reorders a delete after a later insert of the same key and resurrects
// the row (T-13). Consumers that need the split call ByOp().
type Batch struct {
	Table    string
	Changes  []Change
	Position string
	Mode     WriteMode
	// SnapshotState is the durable snapshot state machine for resumable
	// backfill. When non-empty, the writer persists it atomically with
	// position.
	SnapshotState   string
	SnapshotPending []uint32 // chunk IDs still pending after this batch
}

// ByOp partitions the batch's changes by operation, preserving arrival
// order within each partition. Derived view — never written back.
func (b Batch) ByOp() (upserts, deletes []Change) {
	for _, c := range b.Changes {
		switch c.Op {
		case OpDelete:
			deletes = append(deletes, c)
		case OpInsert, OpUpdate:
			upserts = append(upserts, c)
		}
	}
	return upserts, deletes
}

// ToDataplaneMode maps a row-layer write mode to the public data-plane enum.
// The enums have different zero values (row layer 0 = upsert, data plane 0 =
// ModeUnset), so a raw uint8 cast would silently misread across the boundary.
func ToDataplaneMode(m WriteMode) dataplane.WriteMode {
	switch m {
	case AppendMode:
		return dataplane.AppendMode
	default:
		return dataplane.UpsertMode
	}
}

// ToRowMode maps a data-plane write mode to the row-layer enum. ModeUnset is
// mapped to upsert only because the caller validates it first; it must never
// reach here from production code.
func ToRowMode(m dataplane.WriteMode) WriteMode {
	switch m {
	case dataplane.AppendMode:
		return AppendMode
	default:
		return UpsertMode
	}
}
