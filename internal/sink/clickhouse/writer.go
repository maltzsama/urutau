package clickhouse

import (
	"context"
	"fmt"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
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
func (w *tableWriter) Commit(ctx context.Context, b change.Batch) error {
	seq := w.nextSeq()
	n := len(b.Upserts) + len(b.Deletes)
	cols := make([][]any, len(w.cols))
	for i := range cols {
		cols[i] = make([]any, 0, n)
	}

	emit := func(c change.Change, isDeleted bool) error {
		proj, err := w.project(c, isDeleted, b.Position, seq)
		if err != nil {
			return err
		}
		for i, col := range w.cols {
			v, err := w.valueFor(col, proj)
			if err != nil {
				return fmt.Errorf("row key %v column %q: %w", c.Key, col.name, err)
			}
			cols[i] = append(cols[i], v)
		}
		return nil
	}
	for _, u := range b.Upserts {
		if err := emit(u, false); err != nil {
			return err
		}
	}
	for _, d := range b.Deletes {
		if err := emit(d, true); err != nil {
			return err
		}
	}

	batch, err := w.conn.PrepareBatch(ctx, "INSERT INTO "+w.quoted)
	if err != nil {
		return fmt.Errorf("prepare %s: %w", w.quoted, err)
	}
	for i := range cols {
		col := batch.Column(i)
		for _, v := range cols[i] {
			// AppendRow (not Append): the per-value tolerant converter —
			// Append expects a whole typed slice, AppendRow accepts the
			// canonical Go values coerce() produces, nil included.
			if err := col.AppendRow(v); err != nil {
				_ = batch.Abort()
				return fmt.Errorf("append %s column %q: %w", w.quoted, w.cols[i].name, err)
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

// project resolves one change into column-name → encoded-ready value. Data
// columns come from After through the cast plan; metadata columns come from
// the change header; a tombstone carries only its key. The technical values
// (position/seq/is_deleted) travel under reserved keys — the commit
// coordinate is the BATCH's position, the one thing resume may trust.
func (w *tableWriter) project(c change.Change, isDeleted bool, batchPos string, seq uint64) (map[string]any, error) {
	out := make(map[string]any, len(w.cols))
	out[positionKey] = batchPos
	out[seqKey] = seq
	out[deletedKey] = isDeleted
	if isDeleted {
		for i, k := range w.pk {
			if i < len(c.Key) {
				out[k] = c.Key[i]
			}
		}
		return out, nil
	}
	for _, col := range w.cols {
		if reserved[col.name] {
			continue
		}
		if m, ok := w.metaByName[col.name]; ok {
			v, err := metaValue(m.From, c, w.sourceTable)
			if err != nil {
				return nil, fmt.Errorf("metadata %q: %w", col.name, err)
			}
			out[col.name] = v
			continue
		}
		v, ok := c.After[col.name]
		if !ok {
			continue
		}
		if ct, ok := w.cast.Target(col.name); ok {
			cv, err := ct.Convert(v)
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", col.name, err)
			}
			v = cv
		}
		out[col.name] = v
	}
	return out, nil
}

// valueFor picks one column's encoded value for a projected row: the
// technical columns are the mechanism (position = the batch's commit
// coordinate, seq the version ordering, is_deleted the tombstone mark).
func (w *tableWriter) valueFor(col column, proj map[string]any) (any, error) {
	switch col.name {
	case "position":
		return proj[positionKey].(string), nil
	case "seq":
		return proj[seqKey].(uint64), nil
	case "is_deleted":
		if proj[deletedKey] == true {
			return uint8(1), nil
		}
		return uint8(0), nil
	}
	v, err := coerce(col.base, proj[col.name])
	if err != nil {
		return nil, err
	}
	if v == nil {
		if col.nullable {
			return nil, nil
		}
		// A non-nullable key receiving nil is malformed input, not a
		// fillable hole: zeroing it would silently rewrite history onto
		// key zero. Non-key columns may still zero — tombstones carry
		// only their key by design.
		if col.pk {
			return nil, fmt.Errorf("null value in primary key column")
		}
		return zeroOf(col.base), nil
	}
	return v, nil
}

// Keys project() uses to hand the technical values to valueFor without
// colliding with user column names.
const (
	positionKey = "\x00position"
	seqKey      = "\x00seq"
	deletedKey  = "\x00is_deleted"
)

// metaValue resolves one metadata key to its concrete value for a change.
// Mirrors the Iceberg sink's projection — same keys, same nil semantics.
func metaValue(key core.MetadataKey, c change.Change, sourceTable string) (any, error) {
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
		if c.Transport != nil && c.Transport.Stream != "" {
			return c.Transport.Stream, nil
		}
		return sourceTable, nil // CDC: the source table IS the stream
	case core.MetaShard:
		if c.Transport == nil || c.Transport.Shard == "" {
			return nil, nil
		}
		return c.Transport.Shard, nil
	case core.MetaSeq:
		if c.Transport != nil && c.Transport.Seq != "" {
			return c.Transport.Seq, nil
		}
		if c.Position == "" {
			return nil, nil
		}
		return c.Position, nil // CDC: the event coordinate (GTID/LSN)
	case core.MetaMsgTS:
		if c.Transport == nil || c.Transport.MsgTS.IsZero() {
			return nil, nil
		}
		return c.Transport.MsgTS, nil
	case core.MetaMsgKey:
		if c.Transport == nil {
			return nil, nil
		}
		return c.Transport.MsgKey, nil
	case core.MetaHeaders:
		if c.Transport == nil || c.Transport.Headers == "" {
			return nil, nil
		}
		return c.Transport.Headers, nil
	default:
		return nil, fmt.Errorf("unknown metadata key %q", key)
	}
}
