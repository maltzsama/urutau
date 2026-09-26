// Package rowmeta resolves a row's metadata columns (op, commit_ts,
// position, …) for the row-based sinks, ClickHouse and Couchbase, so both
// write the same value for the same key (issue #403). The Iceberg sink
// projects the same keys column-wise (project_record.go) with the same nil
// semantics.
package rowmeta

import (
	"fmt"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

// Row is the per-row metadata view Value needs, read straight from the wire
// record. Transport-envelope fields (stream/shard/headers) are nil on the
// wire path.
type Row struct {
	Op       rowchange.Op
	Position string
	CommitTS time.Time
	IngestTS time.Time
	Snapshot bool
	Phase    string
}

// Of reads row i's metadata from a wire batch.
func Of(r *transport.BatchReader, i int) Row {
	commitTS, _ := r.CommitTS(i)
	ingestTS, _ := r.IngestTS(i)
	return Row{
		Op:       r.Op(i),
		Position: r.Position(i),
		CommitTS: commitTS,
		IngestTS: ingestTS,
		Snapshot: r.Snapshot(i),
		Phase:    r.Phase(i),
	}
}

// Value resolves one metadata key to its concrete value for a row. Every
// metadata column is nullable (core.MetadataKey.ColumnType): an absent value
// is nil, never a zero. Time values stay time.Time.
func Value(key core.MetadataKey, c Row, sourceTable string) (any, error) {
	switch key {
	case core.MetaOp:
		return c.Op.String(), nil
	case core.MetaCommitTS:
		if c.CommitTS.IsZero() {
			return nil, nil
		}
		return c.CommitTS, nil
	case core.MetaIngestTS:
		if c.IngestTS.IsZero() {
			return nil, nil
		}
		return c.IngestTS, nil
	case core.MetaPosition:
		if c.Position == "" {
			return nil, nil
		}
		return c.Position, nil
	case core.MetaSourceTable:
		return sourceTable, nil
	case core.MetaPhase:
		if c.Phase != "" {
			return c.Phase, nil
		}
		if c.Snapshot {
			return core.PhaseSnapshot, nil
		}
		return core.PhaseStream, nil
	case core.MetaStream:
		// Wire path: no transport envelope; the source table IS the stream.
		return sourceTable, nil
	case core.MetaShard:
		return nil, nil
	case core.MetaSeq:
		if c.Position == "" {
			return nil, nil
		}
		return c.Position, nil // CDC: the event coordinate (GTID/LSN)
	case core.MetaMsgTS:
		return nil, nil
	case core.MetaMsgKey:
		return nil, nil
	case core.MetaHeaders:
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown metadata key %q", key)
	}
}
