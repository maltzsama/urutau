package enrich

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Loader reads one reference image as a typed Arrow record. Every column
// the query returns is a column of the record, in query order; the join
// column and the projected data columns are separated by name in
// buildImage. Load re-runs on every refresh; the caller owns and releases
// the returned record.
//
// The SQL implementation lives in loader_sql.go and carries THE TWO ROWS —
// the only per-row loop and the only value→builder switch in the system,
// both imposed by database/sql and both scheduled to die with ADBC.
type Loader interface {
	Load(ctx context.Context) (arrow.RecordBatch, error)
	Close() error
}

// recordToRows is a P2→P3 bridge: buildImage still consumes
// []map[string]any until P3 rewrites it to consume the Arrow record
// directly. Deleted in P3.
//
//allow:rowloop P2→P3 bridge; deleted when buildImage goes Arrow-native.
func recordToRows(rec arrow.RecordBatch) []map[string]any {
	if rec == nil || rec.NumRows() == 0 {
		return nil
	}
	schema := rec.Schema()
	n := int(rec.NumRows())
	out := make([]map[string]any, n)
	for i := 0; i < n; i++ {
		row := make(map[string]any, schema.NumFields())
		for c := 0; c < schema.NumFields(); c++ {
			col := rec.Column(c)
			name := schema.Field(c).Name
			if col.IsNull(i) {
				continue
			}
			switch a := col.(type) {
			case *array.String:
				row[name] = a.Value(i)
			case *array.Binary:
				row[name] = a.Value(i)
			case *array.Boolean:
				row[name] = a.Value(i)
			case *array.Int64:
				row[name] = a.Value(i)
			case *array.Uint64:
				row[name] = a.Value(i)
			case *array.Float64:
				row[name] = a.Value(i)
			case *array.Timestamp:
				row[name] = a.Value(i).ToTime(arrow.Microsecond)
			}
		}
		out[i] = row
	}
	return out
}
