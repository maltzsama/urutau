package enrich

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
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
