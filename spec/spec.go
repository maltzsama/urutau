// Package spec defines the resolvedSpec: the contract between the planner,
// the admission webhook, and coordinator boot. Validation is single and
// server-side. JSON tags are the wire contract with the planner and do not
// change.
package spec

import (
	"github.com/maltzsama/urutau/core"
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

type Spec struct {
	Pipeline string  `json:"pipeline"`
	Source   Source  `json:"source"`
	Sink     Sink    `json:"sink"`
	Tables   []Table `json:"tables"`
}

type Source struct {
	Kind          string `json:"kind"`
	URI           string `json:"uri"`
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
	URI          string   `json:"uri"`
	Namespace    string   `json:"namespace"`
	Warehouse    string   `json:"warehouse,omitempty"`
	ClientID     string   `json:"clientId,omitempty"`
	ClientSecret string   `json:"clientSecret,omitempty"`
	Scope        string   `json:"scope,omitempty"`
	Defaults     Defaults `json:"defaults"`
}

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
	// be introspected (e.g. Kafka). Each entry maps a column name to its
	// canonical type string. Ignored for SQL sources which introspect
	// automatically.
	Columns map[string]string `json:"columns,omitempty"`
	// Bootstrap configures how the initial snapshot is handled.
	Bootstrap *Bootstrap `json:"bootstrap,omitempty"`
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
