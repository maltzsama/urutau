package postgres

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	pglogrepl "github.com/jackc/pglogrepl"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/source"
)

// heldRec is one materialized record of the committing transaction, before
// its position is finalized.
type heldRec struct {
	target string
	rec    arrow.RecordBatch
}

// Direct columnar size ceilings, mirroring the MySQL path: a live transaction
// is held as Arrow (not map[string]any) until it commits, and a record is
// materialized as soon as it crosses either ceiling.
const (
	maxDirectRows  = 2000
	maxDirectBytes = 4 << 20
)

// pgTable is one target table's incremental Arrow builder for the live path.
// cs is the resolved canonical (projected) schema; cols maps each encoder
// column to its position in the introspected table, so a decoded cell goes
// straight into its column's Arrow buffer with no map in between (#455).
type pgTable struct {
	target string
	proj   Projection
	enc    *transport.RowEncoder
	cols   []int
	// imgCols is the table's column count when cols was built; a different
	// count is source drift (ADD/DROP COLUMN) — fail loud.
	imgCols int
	bytes   int
	rows    int
}

// newPgTable builds an encoder for one target table. The column mapping
// resolves each projected column against the introspected table now.
func newPgTable(target string, cs core.Schema, st *TableState, proj Projection) (*pgTable, error) {
	enc, err := transport.NewRowEncoder(cs, nil)
	if err != nil {
		return nil, err
	}
	cols := make([]int, len(cs.Columns))
	for i, c := range cs.Columns {
		idx := st.FindColumn(c.Name)
		if idx < 0 {
			enc.Release()
			return nil, fmt.Errorf("%w: %s", transport.ErrColumnNotInSchema, c.Name)
		}
		cols[i] = idx
	}
	return &pgTable{target: target, proj: proj, enc: enc, cols: cols, imgCols: len(st.Columns)}, nil
}

// appendRow writes one projected image (positional, in table column order)
// into the builders and ends the row with meta's operation/position.
func (te *pgTable) appendRow(row []any, st *TableState, meta transport.RowMeta) error {
	if len(st.Columns) != te.imgCols {
		return fmt.Errorf("postgres: schema drift: table now carries %d columns, the schema was built for %d — declare the change and resume", len(st.Columns), te.imgCols)
	}
	for i, si := range te.cols {
		if si < 0 || si >= len(row) || row[si] == nil {
			te.enc.AppendNull(i)
			continue
		}
		te.bytes += pgCellBytes(row[si])
		if err := te.enc.AppendValue(i, row[si]); err != nil {
			return err
		}
	}
	te.enc.EndRow(meta)
	te.rows++
	return nil
}

func (te *pgTable) materialize() arrow.RecordBatch {
	if te.rows == 0 {
		return nil
	}
	rec := te.enc.NewRecord()
	te.rows, te.bytes = 0, 0
	return rec
}

// pgCellBytes estimates one cell's Arrow payload for the byte ceiling.
func pgCellBytes(v any) int {
	switch t := v.(type) {
	case string:
		return len(t)
	case []byte:
		return len(t)
	case time.Time:
		return 16
	default:
		return 8
	}
}

// tupleRow decodes a pgoutput tuple into a positional row in table column
// order (nil for NULL and for columns the tuple omits). Unchanged-TOAST
// columns are recovered from the old tuple. This replaces the row map that
// tupleToMap built, so no map[string]any is materialized on the hot path.
func tupleRow(st *TableState, tuple *pglogrepl.TupleData, old *pglogrepl.TupleData) ([]any, error) {
	row := make([]any, len(st.Columns))
	for i, col := range tuple.Columns {
		if i >= len(st.Columns) {
			return nil, fmt.Errorf("postgres: decode %s.%s: tuple has more columns than introspection", st.Schema, st.Name)
		}
		name := st.Columns[i].Name
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			row[i] = nil
		case pglogrepl.TupleDataTypeText:
			v, err := decodeScalar(st.Columns[i].DataType, col.Data)
			if err != nil {
				return nil, fmt.Errorf("postgres: decode %s.%s.%s: %w", st.Schema, st.Name, name, err)
			}
			row[i] = v
		case pglogrepl.TupleDataTypeToast:
			if v, ok := toastFromOld(st, old, i); ok {
				row[i] = v
				continue
			}
			return nil, fmt.Errorf("postgres: decode %s.%s.%s: unchanged TOAST with no old image", st.Schema, st.Name, name)
		default:
			return nil, fmt.Errorf("postgres: decode %s.%s.%s: unsupported tuple kind %q", st.Schema, st.Name, name, col.DataType)
		}
	}
	return row, nil
}

// keyTupleRow decodes a key-only old tuple ('K') by the primary key's column
// order, returning a positional row (nil for every non-key column). A shorter
// or longer tuple is a malformed identity tuple and fails loud.
func keyTupleRow(st *TableState, tuple *pglogrepl.TupleData, names []string) ([]any, error) {
	if len(tuple.Columns) != len(names) {
		return nil, fmt.Errorf("postgres: decode %s.%s: key tuple has %d columns, want the primary key's %d",
			st.Schema, st.Name, len(tuple.Columns), len(names))
	}
	row := make([]any, len(st.Columns))
	for i, col := range tuple.Columns {
		name := names[i]
		j := st.FindColumn(name)
		if j < 0 {
			return nil, fmt.Errorf("postgres: decode %s.%s: key column %q not found", st.Schema, st.Name, name)
		}
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			row[j] = nil
		case pglogrepl.TupleDataTypeText:
			v, err := decodeScalar(st.Columns[j].DataType, col.Data)
			if err != nil {
				return nil, fmt.Errorf("postgres: decode %s.%s.%s: %w", st.Schema, st.Name, name, err)
			}
			row[j] = v
		default:
			return nil, fmt.Errorf("postgres: decode %s.%s.%s: unsupported key tuple kind %q", st.Schema, st.Name, name, col.DataType)
		}
	}
	return row, nil
}

// oldTupleRow decodes an old tuple: key-only by the primary key's column
// order, a full one positionally (issue #500).
func oldTupleRow(st *TableState, t *pglogrepl.TupleData, keyOnly bool, key []string) ([]any, error) {
	if keyOnly {
		return keyTupleRow(st, t, key)
	}
	return tupleRow(st, t, nil)
}

// wal2jsonRowPos keys a wal2json change's columns to a positional table row.
func wal2jsonRowPos(st *TableState, names []string, values []any) ([]any, error) {
	row := make([]any, len(st.Columns))
	for i, name := range names {
		col := st.FindColumn(name)
		if col < 0 {
			continue
		}
		var v any
		if i < len(values) {
			v = values[i]
		}
		cv, err := coerceWal2json(v, st.Columns[col].DataType)
		if err != nil {
			return nil, fmt.Errorf("postgres: wal2json: column %s: %w", name, err)
		}
		row[col] = cv
	}
	return row, nil
}

// keyTuple builds a row's primary-key tuple from the positional row, in spec
// order.
func keyTuple(st *TableState, ref source.TableRef, row []any) []any {
	key := make([]any, 0, len(ref.PrimaryKey))
	for _, pk := range ref.PrimaryKey {
		if j := st.FindColumn(pk); j >= 0 && j < len(row) {
			key = append(key, row[j])
		} else {
			key = append(key, nil)
		}
	}
	return key
}

// encoder returns (building once) the target table's Arrow builder. A table
// image whose column count no longer matches the one the mapping was built
// from is source drift: fail loud.
func (r *Reader) encoder(entry relEntry) (*pgTable, error) {
	if te, ok := r.encoders[entry.ref.Target]; ok {
		return te, nil
	}
	cs, ok := r.cfg.Schemas[entry.ref.Target]
	if !ok || len(cs.Columns) == 0 {
		return nil, fmt.Errorf("postgres: table %s has no canonical schema", entry.ref.Target)
	}
	te, err := newPgTable(entry.ref.Target, cs, entry.state, entry.proj)
	if err != nil {
		return nil, fmt.Errorf("postgres: %s: %w", entry.ref.Source, err)
	}
	r.encoders[entry.ref.Target] = te
	r.encoderOrder = append(r.encoderOrder, entry.ref.Target)
	return te, nil
}

// appendChange appends one row image to the target's builder as one Arrow
// row, with no map[string]any in between. A delete writes the before image;
// an insert/update the after image.
func (r *Reader) appendChange(entry relEntry, op rowchange.Op, after, before []any) error {
	te, err := r.encoder(entry)
	if err != nil {
		return err
	}
	image := after
	if op == rowchange.OpDelete {
		image = before
	}
	if op == rowchange.OpDelete && len(te.enc.Schema().PrimaryKey) == 0 {
		return fmt.Errorf("sourcepull: batch %q carries a delete but the schema has no primary key — declare it and resume", te.target)
	}
	meta := transport.RowMeta{Op: op, Position: r.safePos, CommitTS: r.curCommitTS, IngestTS: time.Now()}
	if err := te.appendRow(image, entry.state, meta); err != nil {
		return err
	}
	if te.rows >= maxDirectRows || te.bytes >= maxDirectBytes {
		r.pending = append(r.pending, heldRec{target: te.target, rec: te.materialize()})
	}
	return nil
}

// buffering reports whether a transaction is mid-decode (rows buffered or a
// materialized piece pending).
func (r *Reader) buffering() bool {
	if len(r.pending) > 0 {
		return true
	}
	for _, te := range r.encoders {
		if te.rows > 0 {
			return true
		}
	}
	return false
}

// releasePending drops the buffered records of the transaction being decoded.
func (r *Reader) releasePending() {
	for _, hb := range r.pending {
		if hb.rec != nil {
			hb.rec.Release()
		}
	}
	r.pending = nil
}

// resetTxn discards any buffered state of the transaction being decoded. It
// runs on Begin and on reconnect: the previous transaction was either
// committed (encoders already materialized) or lost.
func (r *Reader) resetTxn() {
	r.releasePending()
	for _, te := range r.encoders {
		te.rows, te.bytes = 0, 0
	}
}
