// Package spec defines the resolvedSpec: the contract between the planner,
// the admission webhook, and coordinator boot. Validation is single and
// server-side. JSON tags are the wire contract with the planner and do not
// change.
package spec

import (
	"fmt"
	"time"

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
	// one atomic write sequences the two. Couchbase-only: any other sink
	// type rejects a non-empty commitMode in Validate rather than silently
	// ignoring it. Empty means the sink's own default ("fast": data first,
	// control document last — recovery replays the batch idempotently).
	// "atomic" wraps data and control document in a distributed transaction,
	// closing the recovery window at the cost of transaction overhead per
	// batch.
	CommitMode CommitMode `json:"commitMode,omitempty"`
	// Maintenance configures background Iceberg table maintenance
	// (compaction, snapshot expiry, orphan cleanup). Iceberg-only: any
	// other sink type rejects a maintenance block in Validate rather than
	// silently ignoring it. Nil disables it entirely.
	Maintenance *Maintenance `json:"maintenance,omitempty"`
}

// MaintenanceEnabled reports whether the operator turned maintenance on.
// Disabled by default in both ways a spec can express that: no maintenance
// block at all (Maintenance is nil — the ordinary case, since no existing
// spec fixture mentions the key), or a block present but Enabled left at
// its bool zero value (false). The runner and coordinator both gate their
// Maintainer goroutine on this exact call, so a change here changes both.
func (s Sink) MaintenanceEnabled() bool {
	return s.Maintenance != nil && s.Maintenance.Enabled
}

// Maintenance enables and configures the three Iceberg table-maintenance
// operations. Each sub-config is independently optional: a nil one disables
// that operation while leaving the others active.
type Maintenance struct {
	Enabled        bool                  `json:"enabled,omitempty"`
	Compaction     *CompactionConfig     `json:"compaction,omitempty"`
	SnapshotExpiry *SnapshotExpiryConfig `json:"snapshotExpiry,omitempty"`
	OrphanCleanup  *OrphanCleanupConfig  `json:"orphanCleanup,omitempty"`
}

// CompactionConfig tunes small-file compaction (iceberg-go
// table/compaction.Config.PlanCompaction + Transaction.RewriteDataFiles).
// There is no safety-window field here: a concurrent CDC commit that
// deletes a row in a file being rewritten is caught by iceberg-go's own
// rewrite conflict validator, which the retry loop already handles the same
// way it handles any other commit conflict — no time-based avoidance is
// needed, in either write mode. See issue #96 for the analysis (append-only
// tables never produce delete files, so there is no conflict surface to
// protect there either).
type CompactionConfig struct {
	// Interval between compaction runs. Go duration syntax. Default "5m".
	Interval string `json:"interval,omitempty"`
	// TargetFileSize is the desired output file size after compaction, e.g.
	// "512Mi". Default "512Mi" (iceberg-go's own default).
	TargetFileSize string `json:"targetFileSize,omitempty"`
	// MinInputFiles is the minimum number of small files in a partition
	// group before compaction rewrites it. Default 5. 0 also means
	// "unset, use the default" — omitempty means the wire form cannot
	// distinguish an explicit 0 from an absent field, so "compact every
	// group down to a single file" is not expressible here. Use 1 for
	// "almost always compact" if that is the intent.
	MinInputFiles uint `json:"minInputFiles,omitempty"`
}

// SnapshotExpiryConfig tunes snapshot history pruning
// (Transaction.ExpireSnapshots). This is the ONE operation in Maintenance
// that needs an operator-chosen safety window. The committed position has
// two homes: the cdc.position TABLE property (the fast resume path, read
// first by CommittedPosition) and each snapshot's summary (the walk-back
// fallback, scanned only when the property is absent). Snapshot expiry
// cannot touch the table property — it lives in the table metadata, not in a
// snapshot — but it does prune the snapshot summaries the fallback needs, so
// expiring them before a crashed pipeline has recovered can strand position
// recovery or resume from a point further ahead than what was actually
// committed. This applies identically in append-only and upsert mode: the
// position is written the same way in both.
//
// MaxAge (and RetainLast) together ARE that window: set it to cover the
// worst-case time your pipeline could be down before you give up on
// resuming it from where it left off, not just "how much snapshot history
// to keep for time-travel queries."
type SnapshotExpiryConfig struct {
	// Interval between expiry runs. Go duration syntax. Default "10m".
	Interval string `json:"interval,omitempty"`
	// RetainLast is the minimum number of snapshots kept regardless of age.
	// Default 1.
	RetainLast int `json:"retainLast,omitempty"`
	// MaxAge is the safety window described above: no snapshot younger than
	// this is ever expired. Go duration syntax. Default "168h" (7 days).
	MaxAge string `json:"maxAge,omitempty"`
}

// OrphanCleanupConfig tunes unreferenced-file removal (Table.DeleteOrphanFiles).
// OlderThan is iceberg-go's own native safety window: it protects against
// deleting a file a concurrent read or in-flight commit might still
// reference, which is a different hazard than SnapshotExpiryConfig.MaxAge
// (that one protects position recovery, not file references).
type OrphanCleanupConfig struct {
	// Interval between cleanup runs. Go duration syntax. Default "1h".
	Interval string `json:"interval,omitempty"`
	// OlderThan: only files older than this are eligible for deletion. Go
	// duration syntax. Default "72h" (3 days) — iceberg-go's own default.
	OlderThan string `json:"olderThan,omitempty"`
}

// Default maintenance intervals, applied when the operator leaves the
// corresponding interval unset. These are the documented defaults for the
// three Maintenance sub-configs above. The SCHEDULER reads them — the
// collapsed runner, or the coordinator that provisions an ephemeral
// maintenance worker per table — to decide when each operation is
// due; the operation itself never sees the interval. They live here, not in
// the Iceberg sink, because the orchestration that schedules maintenance
// consumes only the spec/sink contracts and cannot import a concrete sink
// package.
const (
	DefaultCompactionInterval     = 5 * time.Minute
	DefaultSnapshotExpiryInterval = 10 * time.Minute
	DefaultOrphanCleanupInterval  = time.Hour
)

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
	Identity []string `json:"identity,omitempty"`
	// Workers partitions this table's work across N worker groups by a
	// contiguous primary-key range — like Spark/Flink partition an
	// executor pool over a hot table, instead of capping it at one
	// worker's throughput. Nil or Number<=1 means today's single-worker
	// behavior: no partitioning, one group. A worker group's name is
	// always derived (see Table.WorkerGroupNames) — there is no
	// operator-chosen worker name. CPU/Memory are Kubernetes resource
	// quantities (e.g. "2", "4Gi") applied to every worker Deployment
	// this table provisions; ignored outside Kubernetes provisioning.
	Workers           *WorkerSpec `json:"workers,omitempty"`
	CreateIfNotExists bool        `json:"createIfNotExists,omitempty"`
	FilterImmutable   bool        `json:"filterImmutable,omitempty"`
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

// WorkerSpec declares a table's partition count and the Kubernetes
// resources given to each partition's worker — the per-table equivalent
// of Spark's spark.executor.cores/memory, scoped to one hot table instead
// of the whole job.
type WorkerSpec struct {
	Number int    `json:"number,omitempty"`
	CPU    string `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
}

// WorkerCount returns this table's partition count — Number, or 1 when
// Workers is nil or Number<=1 (today's single-worker behavior).
func (t Table) WorkerCount() int {
	if t.Workers == nil || t.Workers.Number < 1 {
		return 1
	}
	return t.Workers.Number
}

// WorkerGroupNames returns this table's derived worker group names — one
// per partition, "<pipeline>-<target>-<index>" for index in
// [0, WorkerCount()). This is the ONLY way a worker group is named: there
// is no operator-chosen name (Workers.Number is a count, not a list of
// names), so the coordinator's routing and its Kubernetes worker
// provisioning always derive the same names from the same inputs.
func (t Table) WorkerGroupNames(pipeline string) []string {
	n := t.WorkerCount()
	names := make([]string, n)
	for i := 0; i < n; i++ {
		names[i] = fmt.Sprintf("%s-%s-%d", pipeline, t.Target, i)
	}
	return names
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
