package postgres

import (
	"context"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// rowPos builds a positional table row (table column order) from a sparse
// named image; columns not named are nil. Test shim for the direct path.
func rowPos(st *TableState, vals map[string]any) []any {
	row := make([]any, len(st.Columns))
	for name, v := range vals {
		if i := st.FindColumn(name); i >= 0 {
			row[i] = v
		}
	}
	return row
}

// testSchema maps an introspected table to a canonical schema without the
// decimal precision the test fixtures omit; numeric/text both encode as string,
// matching decodeScalar.
func testSchema(st *TableState, pk []string) core.Schema {
	cols := make([]core.Column, len(st.Columns))
	for i, c := range st.Columns {
		cols[i] = core.Column{Name: c.Name, Type: core.ColumnType{Kind: testKind(c.DataType)}}
	}
	return core.Schema{Columns: cols, PrimaryKey: pk}
}

func testKind(dataType string) core.Kind {
	switch dataType {
	case "bigint", "int8", "integer", "smallint":
		return core.KindInt64
	case "boolean", "bool":
		return core.KindBool
	case "real", "double precision", "float8":
		return core.KindFloat64
	default:
		return core.KindString
	}
}

// directReader builds a Reader wired to a batch channel for the direct path:
// the canonical schema, projection and relation entry the encoder needs.
func directReader(out chan *dataplane.Batch, st *TableState, ref source.TableRef, upsert bool) *Reader {
	cs := testSchema(st, ref.PrimaryKey)
	proj, _ := newProjection(nil, st)
	entry := relEntry{state: st, ref: ref, proj: proj}
	return &Reader{
		batchOut: out,
		bySrc:    map[string]source.TableRef{ref.Source: ref},
		states:   map[string]*TableState{ref.Source: st},
		relByID:  map[uint32]relEntry{1: entry},
		encoders: map[string]*pgTable{},
		cfg: Config{
			Schemas:       map[string]core.Schema{ref.Target: cs},
			UpsertTargets: map[string]bool{ref.Target: upsert},
		},
		projections: map[string]Projection{ref.Source: proj},
	}
}

// flushAndDecode closes the transaction at pos and returns every emitted
// change, decoded from the Arrow batches.
func flushAndDecode(t *testing.T, r *Reader, out chan *dataplane.Batch, ref source.TableRef, pos position.LSN) []rowchange.Change {
	t.Helper()
	if err := r.closeTxn(context.Background(), pos); err != nil {
		t.Fatalf("closeTxn: %v", err)
	}
	var all []rowchange.Change
	for len(out) > 0 {
		b := <-out
		ch, err := transport.DecodeBatch(b.Record, b.Table, ref.PrimaryKey)
		b.Record.Release()
		if err != nil {
			t.Fatalf("decode batch: %v", err)
		}
		all = append(all, ch...)
	}
	return all
}
