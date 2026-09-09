package clickhouse

import (
	"context"
	"fmt"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

// column is one target column as read from system.columns — the table is
// the schema's source of truth, whatever process created it.
type column struct {
	name     string
	base     string
	nullable bool
	pk       bool
}

// tableWriter commits one table's batches as a single columnar INSERT.
// Upserts land as rows; deletes land as tombstone rows (is_deleted=1, key
// filled, everything else zero) that ReplacingMergeTree resolves — FINAL
// reads hide them, and physical cleanup is operator maintenance. There is
// no second commit to order wrong: the batch's position travels on every
// row of its one insert.
type tableWriter struct {
	conn        ch.Conn
	ident       tableIdent
	quoted      string
	pk          []string
	cast        core.CastPolicy
	metaByName  map[string]core.MetadataColumn
	sourceTable string
	cols        []column

	// now is injectable so the seq guard is testable deterministically.
	now     func() time.Time
	lastSeq uint64
}

// openTableWriter loads the target column list and seeds the seq floor from
// the highest seq ever committed: nextSeq must stay strictly increasing
// across restarts, and an in-memory counter alone would reset on boot and
// make ReplacingMergeTree prefer stale rows.
func openTableWriter(ctx context.Context, conn ch.Conn, ident tableIdent, ref core.TableRef, cast core.CastPolicy, meta []core.MetadataColumn, sourceTable string, now func() time.Time) (*tableWriter, error) {
	rows, err := conn.Query(ctx,
		"SELECT name, type FROM system.columns WHERE database = ? AND table = ? ORDER BY position",
		ident.db, ident.table)
	if err != nil {
		return nil, fmt.Errorf("columns %s: %w", ident.quoted(), err)
	}
	defer func() { _ = rows.Close() }()
	var cols []column
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, err
		}
		base, nullable := parseCHType(typ)
		cols = append(cols, column{name: name, base: base, nullable: nullable})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("table %s has no columns — ensure it first", ident.quoted())
	}

	var seed uint64
	if err := conn.QueryRow(ctx, "SELECT max(seq) FROM "+ident.quoted()).Scan(&seed); err != nil {
		return nil, fmt.Errorf("seed seq %s: %w", ident.quoted(), err)
	}

	metaByName := make(map[string]core.MetadataColumn, len(meta))
	for _, m := range meta {
		metaByName[m.As] = m
	}
	pkSet := make(map[string]bool, len(ref.PrimaryKey))
	for _, k := range ref.PrimaryKey {
		pkSet[k] = true
	}
	for i := range cols {
		cols[i].pk = pkSet[cols[i].name]
	}
	if now == nil {
		now = time.Now
	}
	return &tableWriter{
		conn:        conn,
		ident:       ident,
		quoted:      ident.quoted(),
		pk:          ref.PrimaryKey,
		cast:        cast,
		metaByName:  metaByName,
		sourceTable: sourceTable,
		cols:        cols,
		now:         now,
		lastSeq:     seed,
	}, nil
}

// nextSeq returns the next commit's version coordinate: strictly increasing
// per table, within the process and across boots. The wall clock is the
// floor; the seed (max(seq) at open) and the guard (lastSeq+1) keep a
// nanosecond tie or an NTP step-back from producing a repeated or regressive
// seq. In upsert mode ReplacingMergeTree uses it to pick the surviving row;
// in append it only orders argMax for the resume read — either way a wrong
// seq is a silent position error, so it is guarded, not assumed.
func (w *tableWriter) nextSeq() uint64 {
	now := uint64(w.now().UnixNano())
	if now <= w.lastSeq {
		now = w.lastSeq + 1
	}
	w.lastSeq = now
	return now
}

// Commit writes the collapsed batch as ONE insert: upserts as rows, deletes
// as tombstones. Atomic if — and only if — every row lands in the same
// partition, which is why the default table has no PARTITION BY.
//
// Column-oriented: the record is consumed through the BatchReader with
// per-column resolvers bound once per commit — no rowchange intermediate,
// no per-row projection maps.
func (w *tableWriter) Commit(ctx context.Context, b *dataplane.Batch) error {
	reader, err := transport.NewBatchReader(b.Record, w.pk)
	if err != nil {
		return fmt.Errorf("clickhouse: %w", err)
	}
	resolvers, err := w.bindResolvers(reader)
	if err != nil {
		return err
	}

	seq := w.nextSeq()
	batchPos := string(b.Watermark)
	batch, err := w.conn.PrepareBatch(ctx, "INSERT INTO "+w.quoted)
	if err != nil {
		return fmt.Errorf("prepare %s: %w", w.quoted, err)
	}
	for i := range reader.NumRows() {
		isDeleted := reader.Op(i) == rowchange.OpDelete
		row := clickhouseRowMetaOf(reader, i)
		for si, col := range w.cols {
			v, err := resolvers[si](reader, i, isDeleted, batchPos, seq, row)
			if err != nil {
				_ = batch.Abort()
				return fmt.Errorf("row %d column %q: %w", i, col.name, err)
			}
			v, err = w.valueOf(col, v)
			if err != nil {
				_ = batch.Abort()
				return fmt.Errorf("row %d column %q: %w", i, col.name, err)
			}
			// AppendRow (not Append): the per-value tolerant converter —
			// Append expects a whole typed slice, AppendRow accepts the
			// canonical Go values coerce() produces, nil included.
			if err := batch.Column(si).AppendRow(v); err != nil {
				_ = batch.Abort()
				return fmt.Errorf("append %s column %q: %w", w.quoted, col.name, err)
			}
		}
	}
	if err := batch.Send(); err != nil {
		_ = batch.Abort()
		return fmt.Errorf("insert %s: %w", w.quoted, err)
	}
	return nil
}

// Close is a no-op: the writer holds no connection of its own — the Sink
// owns the pool and its lifecycle.
func (w *tableWriter) Close() error { return nil }

// colResolver is the per-target-column value source, bound once per commit
// against the record schema. The old project()/valueFor() pair built a
// per-row map first and re-keyed it per column; the resolver reads the
// value straight from the record.
type colResolver func(r *transport.BatchReader, i int, isDeleted bool, batchPos string, seq uint64, row chRowMeta) (any, error)

// bindResolvers maps every target column to its value source. Binding once
// per commit keeps the row loop allocation-free.
func (w *tableWriter) bindResolvers(r *transport.BatchReader) ([]colResolver, error) {
	resolvers := make([]colResolver, len(w.cols))
	for i, col := range w.cols {
		name := col.name
		switch name {
		case "position":
			resolvers[i] = func(_ *transport.BatchReader, _ int, _ bool, batchPos string, _ uint64, _ chRowMeta) (any, error) {
				return batchPos, nil
			}
			continue
		case "seq":
			resolvers[i] = func(_ *transport.BatchReader, _ int, _ bool, _ string, seq uint64, _ chRowMeta) (any, error) {
				return seq, nil
			}
			continue
		case "is_deleted":
			resolvers[i] = func(_ *transport.BatchReader, _ int, isDeleted bool, _ string, _ uint64, _ chRowMeta) (any, error) {
				if isDeleted {
					return uint8(1), nil
				}
				return uint8(0), nil
			}
			continue
		}
		if m, ok := w.metaByName[name]; ok {
			key := m.From
			resolvers[i] = func(_ *transport.BatchReader, _ int, _ bool, _ string, _ uint64, row chRowMeta) (any, error) {
				v, err := metaValue(key, row, w.sourceTable)
				if err != nil {
					return nil, fmt.Errorf("metadata %q: %w", name, err)
				}
				return v, nil
			}
			continue
		}
		if !r.HasColumn(name) {
			// A target column the stream does not carry: nil per the
			// nullability rules, identical to the old absent-key path.
			resolvers[i] = func(_ *transport.BatchReader, _ int, _ bool, _ string, _ uint64, _ chRowMeta) (any, error) {
				return nil, nil
			}
			continue
		}
		var target *core.CastTarget
		if ct, ok := w.cast.Target(name); ok {
			t := ct
			target = &t
		}
		colName := name
		resolvers[i] = func(r *transport.BatchReader, i int, _ bool, _ string, _ uint64, _ chRowMeta) (any, error) {
			v, ok := r.Value(colName, i)
			if !ok {
				return nil, nil
			}
			if target != nil {
				cv, err := target.Convert(v)
				if err != nil {
					return nil, fmt.Errorf("column %q: %w", colName, err)
				}
				v = cv
			}
			return v, nil
		}
	}
	return resolvers, nil
}

// valueOf applies the nullability/zero rules for one resolved value. A nil
// on a non-nullable key is malformed input, not a fillable hole: zeroing it
// would silently rewrite history onto key zero. Non-key columns may still
// zero — tombstones carry only their key by design.
func (w *tableWriter) valueOf(col column, v any) (any, error) {
	v, err := coerce(col.base, v)
	if err != nil {
		return nil, err
	}
	if v == nil {
		if col.nullable {
			return nil, nil
		}
		if col.pk {
			return nil, fmt.Errorf("null value in primary key column")
		}
		return zeroOf(col.base), nil
	}
	return v, nil
}

// chRowMeta is the per-row metadata view the metadata resolvers need.
type chRowMeta struct {
	Op         rowchange.Op
	Position   string
	CommitTS   time.Time
	IngestTS   time.Time
	Snapshot   bool
	EnrichMiss bool
}

func clickhouseRowMetaOf(r *transport.BatchReader, i int) chRowMeta {
	commitTS, _ := r.CommitTS(i)
	ingestTS, _ := r.IngestTS(i)
	return chRowMeta{
		Op:       r.Op(i),
		Position: r.Position(i),
		CommitTS: commitTS,
		IngestTS: ingestTS,
		Snapshot: r.Snapshot(i),
	}
}

// Keys project() uses to hand the technical values to valueFor without
// colliding with user column names.
const (
	positionKey = "\x00position"
	seqKey      = "\x00seq"
	deletedKey  = "\x00is_deleted"
)

// metaValue resolves one metadata key to its concrete value for a rowchange.
// Mirrors the Iceberg sink's projection — same keys, same nil semantics.
func metaValue(key core.MetadataKey, c chRowMeta, sourceTable string) (any, error) {
	switch key {
	case core.MetaOp:
		return c.Op.String(), nil
	case core.MetaCommitTS:
		if c.CommitTS.IsZero() {
			return nil, nil
		}
		return c.CommitTS, nil
	case core.MetaIngestTS:
		return c.IngestTS, nil
	case core.MetaPosition:
		if c.Position == "" {
			return nil, nil
		}
		return c.Position, nil
	case core.MetaSourceTable:
		return sourceTable, nil
	case core.MetaPhase:
		if c.Snapshot {
			return "snapshot", nil
		}
		return "stream", nil
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
	case core.MetaEnrichMiss:
		if c.EnrichMiss {
			return true, nil
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown metadata key %q", key)
	}
}


