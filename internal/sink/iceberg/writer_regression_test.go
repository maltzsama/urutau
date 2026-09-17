package iceberg

// Regression tests for two defects fixed in writer.go:
//
//	1. append mode dropped a delete-with-image row and never advanced position
//	4. appendColumn narrowed int64 to int32 with no range check

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// loadErrCatalog is a catalog.Catalog whose LoadTable always fails, so a
// Commit that reaches the commit path surfaces the error instead of silently
// returning nil.
type loadErrCatalog struct {
	catalog.Catalog
	err error
}

// LoadTable always fails with the stub's error, proving a commit path was
// entered without needing a live catalog.
func (c loadErrCatalog) LoadTable(context.Context, table.Identifier) (*table.Table, error) {
	return nil, c.err
}

// In append mode the worker keeps a delete that carries a before image
// (appendRowsToKeep) and expects the sink to write it. Commit must reach the
// append path for that batch — including when it is the only row, so
// cdc.position advances. A catalog error proves the commit path was entered;
// before the fix Commit returned nil having done nothing.
func TestAppendModeCommitsDeleteWithImage(t *testing.T) {
	b := wireBatch(t, [3]any{int64(1), "before-image", rowchange.OpDelete})
	defer b.Release()
	b.Mode = dataplane.AppendMode

	boom := errors.New("catalog down")
	w := &TableWriter{
		cat:        loadErrCatalog{err: boom},
		ident:      table.Identifier{"raw", "t"},
		dataSchema: arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil),
		maxTries:   1,
	}
	if err := w.Commit(context.Background(), b); !errors.Is(err, boom) {
		t.Fatalf("append-mode Commit = %v, want the catalog error (the commit path must be entered)", err)
	}
}

// appendColumn's Int32Builder branch used to append int/int64 with a bare
// int32(t): an out-of-range key wrapped negative and the equality delete was
// issued for the wrong key. It must reject the overflow instead.
func TestAppendColumnRejectsInt32Overflow(t *testing.T) {
	field := arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32}

	b := array.NewInt32Builder(memory.DefaultAllocator)
	defer b.Release()
	if err := appendColumn(b, field, []any{int64(3_000_000_000)}); err == nil {
		t.Fatal("appendColumn(int64 3_000_000_000) = nil, want an overflow error")
	}

	// In-range values still append.
	if err := appendColumn(b, field, []any{int64(42), int(7)}); err != nil {
		t.Fatalf("appendColumn(in range): %v", err)
	}
	arr := b.NewInt32Array()
	defer arr.Release()
	if arr.Value(0) != 42 || arr.Value(1) != 7 {
		t.Fatalf("values = %d, %d; want 42, 7", arr.Value(0), arr.Value(1))
	}
}

// createErrCatalog is a catalog.Catalog whose CreateTable always fails with
// err, so createTable's already-exists tolerance is testable in isolation.
type createErrCatalog struct {
	catalog.Catalog
	err error
}

// CreateTable always fails with the stub's error, so createTable's
// already-exists tolerance is testable in isolation.
func (c createErrCatalog) CreateTable(context.Context, table.Identifier, *iceberg.Schema, ...catalog.CreateTableOpt) (*table.Table, error) {
	return nil, c.err
}

// createTable must report a lost create race (ErrTableAlreadyExists) as
// "not created, no error" so EnsureTable reloads and validates the winner,
// and must propagate any other error.
func TestCreateTableToleratesAlreadyExists(t *testing.T) {
	ident := table.Identifier{"raw", "t"}
	schema := testSchema(t)

	created, err := createTable(context.Background(), createErrCatalog{err: catalog.ErrTableAlreadyExists}, ident, schema, nil, nil)
	if created || err != nil {
		t.Fatalf("createTable(already exists) = %v, %v; want false, nil", created, err)
	}

	boom := errors.New("boom")
	if _, err := createTable(context.Background(), createErrCatalog{err: boom}, ident, schema, nil, nil); !errors.Is(err, boom) {
		t.Fatalf("createTable(boom) = %v, want boom", err)
	}
}
