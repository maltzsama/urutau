// Package spec defines the resolvedSpec: the contract between the planner,
// the admission webhook, and coordinator boot. Validation is single and
// server-side. JSON tags are the wire contract with the planner and do not
// change.
package spec

import (
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
)

// WriteMode selects how a table is written: upsert reflects state through
// the primary key; append emits every change as a new row.

type WriteMode string

const (
	WriteModeUpsert WriteMode = "upsert"
	WriteModeAppend WriteMode = "append"
	// WriteModeAppendIdempotent is physically append (zero equality
	// deletes) but declares the transport coordinate that makes the table
	// logically idempotent: on a message log the coordinate never reappears,
	// so duplicates are provably absent and cheaply removable if a
	// re-partitioned batch ever overlaps. The identity must be transport
	// metadata (shard/sequence/msg_key/…), never a data column — the
	// guarantee comes from the transport, not the content.
	WriteModeAppendIdempotent WriteMode = "append-idempotent"
)

// ChangeMode maps the spec's declared write mode onto the engine's write
// shape. Append-idempotent is physically append — its identity is a declared
// transport coordinate for downstream dedup and verification, not a
// write-path difference. An empty declaration is upsert: reflecting state
// is the default.
func (m WriteMode) ChangeMode() dataplane.WriteMode {
	if m == WriteModeAppend || m == WriteModeAppendIdempotent {
		return dataplane.AppendMode
	}
	return dataplane.UpsertMode
}

type Spec struct {
	Pipeline string  `json:"pipeline"`
	Source   Source  `json:"source"`
	Sink     Sink    `json:"sink"`
	Tables   []Table `json:"tables"`
}

type Source struct {
	Kind string `json:"kind"`
	URI  string `json:"uri"`
	// SnapshotURI is a READ-ONLY connection string the workers use for the
	// snapshot chunk SELECT. The full replication URI stays coordinator-only;
	// the worker never needs replication credentials, so a deployment can
	// grant the worker a SELECT-only user. When empty, URI is used (the
	// pre-scoping behavior).
	SnapshotURI   string `json:"snapshotUri,omitempty"`
	ServerID      string `json:"serverId,omitempty"`
	SlotName      string `json:"slotName,omitempty"`
	SnapshotMode  string `json:"snapshotMode,omitempty"`
	BootstrapAdds string `json:"bootstrapServers,omitempty"`
	GroupID       string `json:"groupId,omitempty"`
	// PartitionedByPrimaryKey declares the Kafka topics are partitioned by
	// the key, so ordering (and thus upsert correctness) holds within a
	// key. Kafka only orders inside a partition: if the same key landed in
	// different partitions, an upsert could apply a stale version silently.
	// The engine cannot verify this — it must be a conscious operator
	// assertion. Required for writeMode: upsert on a Kafka source.
	PartitionedByPrimaryKey bool `json:"partitionedByPrimaryKey,omitempty"`
	// Format selects the Kafka message decoder: "debezium" (default) parses
	// the envelope into typed rows; "raw" lands the payload verbatim
	// without interpreting it (bronze landing); "avro" decodes
	// Confluent-Avro records resolved by schema id from the registry. Raw
	// and avro require append-only tables.
	Format string `json:"format,omitempty"`
	// SchemaRegistry is the Confluent-compatible schema registry base URL
	// (e.g. http://registry:8081), required when format is avro.
	SchemaRegistry string `json:"schemaRegistry,omitempty"`
}

type Sink struct {
	// Type selects the sink implementation ("iceberg+rest"). Empty defaults
	// to "iceberg+rest"; a future sink declares its own type and the spec
	// names it explicitly.
	Type         string   `json:"type,omitempty"`
	URI          string   `json:"uri"`
	Namespace    string   `json:"namespace"`
	Warehouse    string   `json:"warehouse,omitempty"`
	ClientID     string   `json:"clientId,omitempty"`
	ClientSecret string   `json:"clientSecret,omitempty"`
	Scope        string   `json:"scope,omitempty"`
	Defaults     Defaults `json:"defaults"`
	// CommitMode selects how a sink that cannot commit data and position in
	// one atomic write sequences the two (Couchbase today). Empty means the
	// sink's own default ("fast": data first, control document last —
	// recovery replays the batch idempotently). "atomic" wraps data and
	// control document in a distributed transaction, closing the recovery
	// window at the cost of transaction overhead per batch.
	CommitMode CommitMode `json:"commitMode,omitempty"`
}

// CommitMode is the data-vs-position commit sequencing selector.
type CommitMode string

const (
	// CommitModeFast writes data first, the position-carrying control
	// document last. A crash in between leaves the position un-advanced and
	// the restart replays the batch — idempotent for key-addressed sinks.
	CommitModeFast CommitMode = "fast"
	// CommitModeAtomic commits data and the control document inside one
	// distributed transaction.
	CommitModeAtomic CommitMode = "atomic"
)

type Defaults struct {
	WriteMode      WriteMode `json:"writeMode,omitempty"`
	TargetFileSize string    `json:"targetFileSize,omitempty"`
}

type Table struct {
	Source      string    `json:"source"`
	Target      string    `json:"target"`
	PrimaryKey  []string  `json:"primaryKey,omitempty"`
	PartitionBy []string  `json:"partitionBy,omitempty"`
	Filter      *Filter   `json:"filter,omitempty"`
	WriteMode   WriteMode `json:"writeMode,omitempty"`
	OnDelete    OnDelete  `json:"onDelete,omitempty"`
	// Identity declares the transport-metadata columns that make an
	// append-idempotent table logically idempotent. Each entry is the
	// destination column (as) of a transport metadata column declared in
	// Metadata. Empty outside append-idempotent.
	Identity          []string `json:"identity,omitempty"`
	Worker            string   `json:"worker,omitempty"`
	CreateIfNotExists bool     `json:"createIfNotExists,omitempty"`
	FilterImmutable   bool     `json:"filterImmutable,omitempty"`
	// Metadata lands pipeline metadata columns (op, commit_ts, position, ...)
	// in the target table. The destination name is explicit via As.
	Metadata []core.MetadataColumn `json:"metadata,omitempty"`
	// Cast overrides one source column's canonical type. Key is the source
	// column name; value is the textual canonical target (e.g. "string",
	// "decimal(20,4)", "timestamptz(assume_utc)").
	Cast map[string]string `json:"cast,omitempty"`
	// Columns defines the source schema explicitly for sources that cannot
	// be introspected (e.g. Kafka). Each entry is either a scalar type
	// string ("int64", "decimal(20,4)") or a composite declaration
	// (struct/list/map, nested to any depth) — see ColumnDecl. Ignored for
	// SQL sources which introspect automatically.
	Columns map[string]ColumnDecl `json:"columns,omitempty"`
	// Bootstrap configures how the initial snapshot is handled.
	Bootstrap *Bootstrap `json:"bootstrap,omitempty"`
	// Enrich joins each event against reference tables loaded in memory
	// (broadcast hash join): the reference is read whole at boot and fully
	// re-read on a refresh interval — never CDC'd, never queried per event.
	// Applied in declaration order; an inner-join miss at any reference
	// drops the event.
	Enrich []Enrich `json:"enrich,omitempty"`
}

// Enrich declares one broadcast reference join. The reference table is
// small by contract: it must fit the worker's RAM, because it is held
// whole as a map keyed by the join column. MaxRows enforces the contract
// with a hard, configurable cap instead of an unannounced OOM.
type Enrich struct {
	// Table names the reference (diagnostics and duplicate detection).
	Table string `json:"table"`
	// Source is the reference read: a plain SQL connection and a query
	// returning the full reference image. Types matter at the join: cast
	// in SQL (CAST(id AS CHAR)) when the event column's type differs.
	Source EnrichSource `json:"source"`
	// On maps event column → reference column (the join key pair). One
	// pair today; a composite key is a future need, not a current one.
	On map[string]string `json:"on"`
	// Select limits the reference columns taken; ["*"] takes all (the
	// wildcard sugar). Empty is rejected — declare the columns explicitly
	// or use ["*"].
	Select []string `json:"select,omitempty"`
	// As renames reference columns on the way into the event (ref column
	// → destination name). Keys must appear in Select when Select is set.
	As map[string]string `json:"as,omitempty"`
	// JoinType is required — there is no universal miss policy: left
	// passes the event with NULL reference columns (and enrich_miss when
	// declared), inner drops the event.
	JoinType string `json:"joinType"`
	// Refresh is the full re-read interval (e.g. 5m). Empty means the
	// default (5m). A refresh swaps the map atomically: in-flight events
	// finish on the old image.
	Refresh string `json:"refresh,omitempty"`
	// OnColdStart governs events that arrive before the first reference
	// load completes: buffer (default) holds them up to BufferLimits and
	// drains in order once the reference is hot; pass processes them
	// immediately against the cold map (a miss follows JoinType); drop
	// discards them.
	OnColdStart string `json:"onColdStart,omitempty"`
	// BufferLimits bounds the cold-start queue: MaxEvents caps memory
	// (the oldest event is evacuated, and follows JoinType), MaxWait caps
	// latency (an event queued longer follows JoinType at drain time).
	BufferLimits EnrichBufferLimits `json:"bufferLimits,omitempty"`
	// MaxRows caps the reference image's row count: the broadcast join
	// holds the whole reference in RAM (refTable + a keyIndex entry per
	// row), so an unbounded reference is an unbounded, unannounced OOM.
	// Zero means the package default (enrich.DefaultMaxRows). A load that
	// returns more rows than this is rejected loudly, not truncated
	// silently — the reference is small by contract; a reference that
	// outgrows this needs a raised limit or a different tool, not a
	// silent truncation.
	MaxRows int `json:"maxRows,omitempty"`
}

// EnrichSource is where a reference table is read from.
type EnrichSource struct {
	URI   string `json:"uri"`
	Query string `json:"query"`
}

// EnrichBufferLimits bounds the cold-start buffer. Both are optional; a
// zero disables that bound.
type EnrichBufferLimits struct {
	MaxEvents int    `json:"maxEvents,omitempty"`
	MaxWait   string `json:"maxWait,omitempty"`
}

// OnDelete declares how a DELETE is represented in append-only tables
// (writeMode: append). Upsert tables never see this — deletes remove rows.
type OnDelete string

const (
	// OnDeleteRecord appends the deleted row from its before-image. Only
	// valid for sources that carry a before image on deletes; a delete with
	// no before image is dropped and counted, never written as an all-null
	// row.
	OnDeleteRecord OnDelete = "record"
	// OnDeleteSkip drops deletes entirely — the right choice for pure event
	// streams (Kafka tombstones have no image to record) and for sources
	// without a before image.
	OnDeleteSkip OnDelete = "skip"
)

// BootstrapMode controls how the initial data load is performed.
type BootstrapMode string

const (
	// BootstrapSnapshot loads all data from the source (default).
	BootstrapSnapshot BootstrapMode = "snapshot"
	// Adopt trusts existing data in the target table and starts streaming
	// from the given position. No data is read from the source.
	Adopt BootstrapMode = "adopt"
	// AdoptVerify trusts existing data but verifies it against the source
	// by counting rows per chunk. Divergent chunks are reloaded.
	AdoptVerify BootstrapMode = "adopt-verify"
)

// BootstrapStartAt controls where the stream starts after adoption.
type BootstrapStartAt string

const (
	// StartAtCurrent captures the source position at adopt time (default).
	StartAtCurrent BootstrapStartAt = "current"
	// StartAtExplicit uses a specific position string.
	StartAtExplicit BootstrapStartAt = "explicit"
)

// Bootstrap configures how the initial data load is performed.
type Bootstrap struct {
	Mode    BootstrapMode    `json:"mode,omitempty"`
	StartAt BootstrapStartAt `json:"startAt,omitempty"`
	// Position is the explicit position string when StartAt is "explicit".
	Position string `json:"position,omitempty"`
}
