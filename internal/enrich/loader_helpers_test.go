package enrich

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
)

// evSchema builds a nullable-Int64 event schema for the given columns —
// the default a join test needs (join keys are ints). A test that joins on
// a string column overrides via evSchemaTyped.
func evSchema(cols ...string) core.Schema {
	out := core.Schema{Columns: make([]core.Column, len(cols))}
	for i, c := range cols {
		out.Columns[i] = core.Column{Name: c, Type: core.ColumnType{Kind: core.KindInt64, Nullable: true}}
	}
	return out
}

// evSchemaTyped builds an event schema with per-column kinds.
func evSchemaTyped(kinds map[string]core.Kind, cols ...string) core.Schema {
	out := core.Schema{Columns: make([]core.Column, len(cols))}
	for i, c := range cols {
		k := core.KindInt64
		if kk, ok := kinds[c]; ok {
			k = kk
		}
		out.Columns[i] = core.Column{Name: c, Type: core.ColumnType{Kind: k, Nullable: true}}
	}
	return out
}

// fakeLoader is the test seam. It holds an Arrow record (not a Go map) and
// counts Load calls — the O(N) proof is "the loader ran once per refresh,
// never per event". Thread-safe: the concurrency test swaps the record
// while the refresher goroutine reads.
type fakeLoader struct {
	mu    sync.Mutex
	rec   arrow.RecordBatch
	err   error
	loads int
}

func (f *fakeLoader) SetRec(rec arrow.RecordBatch) {
	f.mu.Lock()
	if f.rec != nil {
		f.rec.Release()
	}
	f.rec = rec
	f.mu.Unlock()
}

func (f *fakeLoader) SetErr(err error) {
	f.mu.Lock()
	f.err = err
	f.mu.Unlock()
}

// SetRows is the readable path: swap the held record from map fixtures.
func (f *fakeLoader) SetRows(t *testing.T, rows []map[string]any) {
	t.Helper()
	f.SetRec(rowsToRec(t, rows))
}

func (f *fakeLoader) Load(context.Context) (arrow.RecordBatch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loads++
	if f.err != nil {
		return nil, f.err
	}
	if f.rec == nil {
		return array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, 0), nil
	}
	f.rec.Retain()
	return f.rec, nil
}

// loadsCount reads the call counter under the lock — races with a
// concurrent Load from a refresh goroutine are otherwise real (-race
// catches a bare field read here, unlike the single-threaded-by-then
// reads elsewhere in this package's older tests).
func (f *fakeLoader) loadsCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loads
}

func (f *fakeLoader) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rec != nil {
		f.rec.Release()
		f.rec = nil
	}
	return nil
}

// refRec builds an Arrow record for a reference image from column-oriented
// Go literals. Column order is `cols`; each entry in `data` is that
// column's values (nil for a NULL cell). The value type of the first
// non-nil cell in a column decides the Arrow type: string→String,
// int64→Int64, uint64→Uint64, float64→Float64, bool→Boolean,
// []byte→Binary, time.Time→Timestamp(µs, UTC). An all-nil column is String.
func refRec(t *testing.T, cols []string, data map[string][]any) arrow.RecordBatch {
	t.Helper()
	if len(cols) == 0 {
		return array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, 0)
	}
	n := len(data[cols[0]])
	fields := make([]arrow.Field, len(cols))
	arrs := make([]arrow.Array, len(cols))
	for ci, name := range cols {
		vals := data[name]
		if len(vals) != n {
			t.Fatalf("refRec: column %q has %d values, want %d", name, len(vals), n)
		}
		dt := refColType(vals)
		fields[ci] = arrow.Field{Name: name, Type: dt, Nullable: true}
		b := array.NewBuilder(memory.DefaultAllocator, dt)
		for _, v := range vals {
			if err := appendTyped(b, v); err != nil {
				b.Release()
				t.Fatalf("refRec: column %q: %v", name, err)
			}
		}
		arrs[ci] = b.NewArray()
		b.Release()
	}
	rec := array.NewRecordBatch(arrow.NewSchema(fields, nil), arrs, int64(n))
	for _, a := range arrs {
		a.Release()
	}
	return rec
}

func refColType(vals []any) arrow.DataType {
	for _, v := range vals {
		switch v.(type) {
		case string:
			return arrow.BinaryTypes.String
		case int64, int, int32:
			return arrow.PrimitiveTypes.Int64
		case uint64:
			return arrow.PrimitiveTypes.Uint64
		case float64, float32:
			return arrow.PrimitiveTypes.Float64
		case bool:
			return arrow.FixedWidthTypes.Boolean
		case []byte:
			return arrow.BinaryTypes.Binary
		case time.Time:
			return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
		}
	}
	return arrow.BinaryTypes.String
}

// fakeRows builds a fakeLoader from map fixtures (the readable path).
func fakeRows(t *testing.T, rows []map[string]any) *fakeLoader {
	t.Helper()
	fl := &fakeLoader{}
	fl.SetRec(rowsToRec(t, rows))
	return fl
}

// fakeErr builds a fakeLoader whose Load fails until SetRows/SetRec.
func fakeErr(err error) *fakeLoader {
	fl := &fakeLoader{}
	fl.SetErr(err)
	return fl
}

// rowsToRec converts the readable []map[string]any fixtures into the Arrow
// record the loader contract returns. Column set is the union of keys
// across all rows, ordered: the join-ish columns first (id, then anything
// ending in _id), then the rest sorted. Column type is the first non-nil
// value's type. An all-nil column is String.
func rowsToRec(t *testing.T, rows []map[string]any) arrow.RecordBatch {
	t.Helper()
	if len(rows) == 0 {
		return array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, 0)
	}
	seen := map[string]bool{}
	var names []string
	for _, r := range rows {
		for k := range r {
			if !seen[k] {
				seen[k] = true
				names = append(names, k)
			}
		}
	}
	sort.Slice(names, func(i, j int) bool {
		// stable, deterministic; "id" first so a wildcard test sees it at 0
		if names[i] == "id" {
			return true
		}
		if names[j] == "id" {
			return false
		}
		return names[i] < names[j]
	})
	data := make(map[string][]any, len(names))
	for _, name := range names {
		col := make([]any, len(rows))
		for i, r := range rows {
			col[i] = r[name] // missing key -> nil -> NULL cell
		}
		data[name] = col
	}
	return refRec(t, names, data)
}
