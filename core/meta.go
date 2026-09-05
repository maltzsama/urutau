// Package core defines the canonical, sink-agnostic type system. This file
// adds the closed catalog of pipeline metadata columns that a target table
// may carry: values the engine already holds on every change (operation,
// commit time, position, source table, phase) and the moment the pipeline
// itself processed the event. Nothing here derives data — every key exposes
// a value that already exists.
package core

// MetadataKey identifies one pipeline-provided value that can be landed as a
// column in the target table. The catalog is closed: every key has a fixed
// canonical type and a fixed semantic, and there is no free-form field.
type MetadataKey string

const (
	// MetaOp is the row operation: "insert", "update" or "delete". In upsert
	// mode a delete removes the row entirely, so op never lands as "delete"
	// there — the row stops existing. In append mode every event becomes a
	// row and delete survives in op.
	MetaOp MetadataKey = "op"
	// MetaCommitTS is when the source committed the transaction that
	// produced the row. Snapshot rows carry NULL: the chunk SELECT does not
	// return the creating transaction's commit time.
	MetaCommitTS MetadataKey = "commit_ts"
	// MetaIngestTS is when the pipeline processed the event. The difference
	// between commit_ts and ingest_ts is the replication lag, measured per
	// row at the destination.
	MetaIngestTS MetadataKey = "ingest_ts"
	// MetaPosition is the row's source coordinate (GTID set, LSN, or Kafka
	// offset map). Snapshot rows carry the chunk's low watermark.
	MetaPosition MetadataKey = "position"
	// MetaSourceTable is the source table the row came from, e.g.
	// "shop.orders".
	MetaSourceTable MetadataKey = "source_table"
	// MetaPhase distinguishes backfill from live traffic: "snapshot" for
	// rows read by the DBLog chunk SELECT, "stream" for live events. It is
	// an axis orthogonal to op — a snapshot row is semantically an insert.
	MetaPhase MetadataKey = "phase"

	// Transport metadata — the message-queue envelope (Kafka today, Kinesis
	// next). Names are transport-neutral so they do not need renaming as
	// sources are added. For CDC sources only MetaStream and MetaSequence
	// carry values (the source table and the GTID/LSN of the event); the
	// rest are NULL.
	MetaStream  MetadataKey = "stream"   // topic / stream name (CDC: source table)
	MetaShard   MetadataKey = "shard"    // partition / shard id (CDC: NULL)
	MetaSeq     MetadataKey = "sequence" // offset / sequence number (CDC: GTID/LSN of the event)
	MetaMsgTS   MetadataKey = "msg_ts"   // message arrival time (CDC: NULL)
	MetaMsgKey  MetadataKey = "msg_key"  // message key (CDC: NULL)
	MetaHeaders MetadataKey = "headers"  // JSON-serialized headers (CDC: NULL)
)

// String renders the key name.
func (k MetadataKey) String() string { return string(k) }

// ColumnType returns the canonical type the key lands as.
func (k MetadataKey) ColumnType() ColumnType {
	switch k {
	case MetaCommitTS, MetaIngestTS, MetaMsgTS:
		return ColumnType{Kind: KindTimestampTZ}
	default:
		return ColumnType{Kind: KindString}
	}
}

// MetadataColumn maps one metadata key to a destination column name. The
// destination name is explicit — there is no default, so an internal wire
// name never leaks into the user's schema.
type MetadataColumn struct {
	From MetadataKey `json:"from"`
	As   string      `json:"as"`
}
