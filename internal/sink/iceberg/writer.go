package iceberg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/internal/change"
)

// ErrCommitExhausted marks a terminal commit failure: retries against the
// catalog are spent and the caller (the worker) must die rather than skip
// the batch — skipping would advance the position past uncommitted data.
var ErrCommitExhausted = errors.New("iceberg: commit retries exhausted")

// TableWriter owns the write path of a single target table.
//
// Commit applies a collapsed batch as delete-then-append — two snapshots —
// per the spike finding: in iceberg-go an append and an equality
// delete staged together produce two snapshots anyway, and the delete wins,
// so the delete must land BEFORE the fresh rows. Both snapshots carry
// cdc.position; the table property ends at the batch position. A crash
// between the two snapshots is safe: the resume reprocesses the batch, and
// under upsert that is idempotent.
//
// A writer is NOT safe for concurrent use: commits must be serialized by the
// caller (the worker dedicates one committer goroutine per table, which is
// the design's serialization invariant).
type TableWriter struct {
	cat        *rest.Catalog
	ident      table.Identifier
	eqIDs      []int
	dataSchema *arrow.Schema
	delSchema  *arrow.Schema
	delCols    []string
	maxTries   int
	backoff    time.Duration
}

// NewTableWriter loads the table, resolves the equality-delete key (the
// primary key) and precomputes the arrow schemas for data and delete rows.
func NewTableWriter(ctx context.Context, cat *rest.Catalog, ident table.Identifier, primaryKey []string) (*TableWriter, error) {
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return nil, fmt.Errorf("iceberg: load %v: %w", ident, err)
	}
	ischema := tbl.Schema()

	eqIDs := make([]int, 0, len(primaryKey))
	for _, name := range primaryKey {
		f, ok := ischema.FindFieldByName(name)
		if !ok {
			return nil, fmt.Errorf("iceberg: %v: primary key column %q not found", ident, name)
		}
		eqIDs = append(eqIDs, f.ID)
	}

	dataSchema, err := table.SchemaToArrowSchema(ischema, nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("iceberg: data schema: %w", err)
	}
	delISchema, err := ischema.Select(true, primaryKey...)
	if err != nil {
		return nil, fmt.Errorf("iceberg: delete schema: %w", err)
	}
	delSchema, err := table.SchemaToArrowSchema(delISchema, nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("iceberg: delete schema: %w", err)
	}

	return &TableWriter{
		cat:        cat,
		ident:      ident,
		eqIDs:      eqIDs,
		dataSchema: dataSchema,
		delSchema:  delSchema,
		delCols:    primaryKey,
		maxTries:   5,
		backoff:    200 * time.Millisecond,
	}, nil
}

// Commit applies the batch: snapshot one equality-deletes every key in it
// (upserts delete their older versions, deletes are the last word), snapshot
// two appends the surviving rows. Only then is the position considered
// advanced.
func (w *TableWriter) Commit(ctx context.Context, b change.Batch) error {
	keys := append(collectKeys(b.Upserts), collectKeys(b.Deletes)...)
	if len(keys) > 0 {
		if err := w.commitDeletes(ctx, keys, b.Position); err != nil {
			return err
		}
	}
	if len(b.Upserts) > 0 {
		if err := w.commitAppend(ctx, b.Upserts, b.Position); err != nil {
			return err
		}
	}
	return nil
}

func (w *TableWriter) commitDeletes(ctx context.Context, keys [][]any, pos string) error {
	rec, err := w.deleteRecord(keys)
	if err != nil {
		return err
	}
	defer rec.Release()

	var lastErr error
	for attempt := 0; attempt < w.maxTries; attempt++ {
		if attempt > 0 {
			if err := sleepCtx(ctx, w.backoff*time.Duration(attempt)); err != nil {
				return err
			}
		}
		tbl, err := w.cat.LoadTable(ctx, w.ident) // fresh metadata every attempt
		if err != nil {
			return err
		}
		txn := tbl.NewTransaction()
		files, err := txn.WriteEqualityDeletes(ctx, w.eqIDs, oneBatch(rec))
		if err != nil {
			return err // non-conflict errors are terminal
		}
		if len(files) == 0 {
			return nil
		}
		if err := txn.NewRowDelta(props(pos)).AddDeletes(files...).Commit(ctx); err != nil {
			if errors.Is(err, table.ErrCommitFailed) {
				lastErr = err
				continue
			}
			return err
		}
		if err := txn.SetProperties(props(pos)); err != nil {
			return err
		}
		if _, err := txn.Commit(ctx); err != nil {
			if errors.Is(err, table.ErrCommitFailed) {
				lastErr = err
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("%w: delete commit on %v: %v", ErrCommitExhausted, w.ident, lastErr)
}

func (w *TableWriter) commitAppend(ctx context.Context, upserts []change.Change, pos string) error {
	rec, err := w.dataRecord(upserts)
	if err != nil {
		return err
	}
	defer rec.Release()
	at := array.NewTableFromRecords(rec.Schema(), []arrow.RecordBatch{rec})
	defer at.Release()

	var lastErr error
	for attempt := 0; attempt < w.maxTries; attempt++ {
		if attempt > 0 {
			if err := sleepCtx(ctx, w.backoff*time.Duration(attempt)); err != nil {
				return err
			}
		}
		tbl, err := w.cat.LoadTable(ctx, w.ident)
		if err != nil {
			return err
		}
		txn := tbl.NewTransaction()
		if err := txn.AppendTable(ctx, at, -1, props(pos)); err != nil {
			return err
		}
		if err := txn.SetProperties(props(pos)); err != nil {
			return err
		}
		if _, err := txn.Commit(ctx); err != nil {
			if errors.Is(err, table.ErrCommitFailed) {
				lastErr = err
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("%w: append commit on %v: %v", ErrCommitExhausted, w.ident, lastErr)
}

// EnsureTable creates the table when it does not exist. Schema derivation
// from source types arrives with the source plugin; callers pass the schema
// explicitly for now.
func EnsureTable(ctx context.Context, cat *rest.Catalog, ident table.Identifier, schema *iceberg.Schema) error {
	if _, err := cat.LoadTable(ctx, ident); err == nil {
		return nil
	}
	_, err := cat.CreateTable(ctx, ident, schema,
		catalog.WithProperties(iceberg.Properties{"format-version": "2"}))
	if err != nil {
		return fmt.Errorf("iceberg: create %v: %w", ident, err)
	}
	return nil
}

func props(pos string) iceberg.Properties {
	return iceberg.Properties{"cdc.position": pos}
}

func collectKeys(cs []change.Change) [][]any {
	keys := make([][]any, 0, len(cs))
	for _, c := range cs {
		keys = append(keys, c.Key)
	}
	return keys
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// deleteRecord builds one arrow record with the primary key tuples.
func (w *TableWriter) deleteRecord(keys [][]any) (arrow.RecordBatch, error) {
	b := array.NewRecordBuilder(memory.DefaultAllocator, w.delSchema)
	defer b.Release()
	for i, col := range w.delCols {
		values := make([]any, len(keys))
		for j, k := range keys {
			if len(k) != len(w.delCols) {
				return nil, fmt.Errorf("iceberg: key arity %d != primary key arity %d", len(k), len(w.delCols))
			}
			values[j] = k[i]
		}
		if err := appendColumn(b.Field(i), col, values); err != nil {
			return nil, err
		}
	}
	return b.NewRecordBatch(), nil
}

// dataRecord builds one arrow record from the surviving rows.
func (w *TableWriter) dataRecord(upserts []change.Change) (arrow.RecordBatch, error) {
	b := array.NewRecordBuilder(memory.DefaultAllocator, w.dataSchema)
	defer b.Release()
	for i, field := range w.dataSchema.Fields() {
		values := make([]any, len(upserts))
		for j, c := range upserts {
			v, ok := c.After[field.Name]
			if !ok {
				values[j] = nil
				continue
			}
			values[j] = v
		}
		if err := appendColumn(b.Field(i), field.Name, values); err != nil {
			return nil, err
		}
	}
	return b.NewRecordBatch(), nil
}

// appendColumn appends values into a builder, tolerating a small, explicit
// set of scalar types (int64, string, float64, bool). Anything else is an
// error — silent coercion is how type bugs are born.
func appendColumn(builder array.Builder, name string, values []any) error {
	switch b := builder.(type) {
	case *array.Int64Builder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case int64:
				b.Append(t)
			case int:
				b.Append(int64(t))
			case int32:
				b.Append(int64(t))
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as int64", name, v)
			}
		}
	case *array.StringBuilder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case string:
				b.Append(t)
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as string", name, v)
			}
		}
	case *array.Float64Builder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case float64:
				b.Append(t)
			case float32:
				b.Append(float64(t))
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as double", name, v)
			}
		}
	case *array.BooleanBuilder:
		for _, v := range values {
			switch t := v.(type) {
			case nil:
				b.AppendNull()
			case bool:
				b.Append(t)
			default:
				return fmt.Errorf("iceberg: column %q: cannot append %T as boolean", name, v)
			}
		}
	default:
		return fmt.Errorf("iceberg: column %q: unsupported builder %T", name, builder)
	}
	return nil
}