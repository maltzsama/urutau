package iceberg

// Batch 4c: the table-maintenance operations (compaction, snapshot expiry,
// orphan cleanup) run against a real in-process Iceberg catalog. A fresh
// table exercises the no-work arms; appending several small files exercises
// the actual compaction rewrite.

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

type recordingMetrics struct {
	compactions int
	expiries    int
	orphans     int
}

func (m *recordingMetrics) CompactionRun(string, int, int, int64, int64, error) { m.compactions++ }
func (m *recordingMetrics) SnapshotExpiryRun(string, int, error)                { m.expiries++ }
func (m *recordingMetrics) OrphanCleanupRun(string, int, int64, error)          { m.orphans++ }

func fullMaintenance() spec.Maintenance {
	return spec.Maintenance{
		Enabled:        true,
		Compaction:     &spec.CompactionConfig{},
		SnapshotExpiry: &spec.SnapshotExpiryConfig{},
		OrphanCleanup:  &spec.OrphanCleanupConfig{},
	}
}

func TestMaintainerRunOnceOnFreshTable(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	createOrders(t, s)

	metrics := &recordingMetrics{}
	m := NewMaintainer(s.cat, s.ident("orders"), fullMaintenance(), nil, nil, metrics)
	err := m.RunOnce(ctx, []sink.MaintenanceOp{
		sink.MaintenanceCompaction,
		sink.MaintenanceSnapshotExpiry,
		sink.MaintenanceOrphanCleanup,
	})
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	// Nothing to compact is not a run; expiry and orphan cleanup always run.
	if metrics.compactions != 0 {
		t.Fatalf("compactions = %d, want 0 on an empty table", metrics.compactions)
	}
	if metrics.expiries == 0 || metrics.orphans == 0 {
		t.Fatalf("expiry/orphan metrics = %d/%d, want both recorded", metrics.expiries, metrics.orphans)
	}
}

func TestMaintainerCompactionRewritesSmallFiles(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	createOrders(t, s)
	ident := s.ident("orders")

	// Three separate appends create three small data files.
	tbl, err := s.cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	dataSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	if err != nil {
		t.Fatalf("arrow schema: %v", err)
	}
	for i := range 3 {
		tbl, err = s.cat.LoadTable(ctx, ident)
		if err != nil {
			t.Fatalf("reload: %v", err)
		}
		rec := arrowRecord(t, dataSchema, func(rb *array.RecordBuilder) {
			rb.Field(0).(*array.Int64Builder).Append(int64(i))
			rb.Field(1).(*array.StringBuilder).Append("x")
		})
		if err := Append(ctx, tbl, rec, iceberg.Properties{"cdc.position": "p"}); err != nil {
			rec.Release()
			t.Fatalf("Append: %v", err)
		}
		rec.Release()
	}

	cfg := fullMaintenance()
	cfg.Compaction = &spec.CompactionConfig{MinInputFiles: 2}
	metrics := &recordingMetrics{}
	m := NewMaintainer(s.cat, ident, cfg, nil, func() string { return "p" }, metrics)
	if err := m.RunOnce(ctx, []sink.MaintenanceOp{sink.MaintenanceCompaction}); err != nil {
		t.Fatalf("RunOnce(compaction): %v", err)
	}
	if metrics.compactions != 1 {
		t.Fatalf("compactions = %d, want 1 rewrite", metrics.compactions)
	}
}

func TestMaintainerExpireAndCleanOnPopulatedTable(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	createOrders(t, s)
	ident := s.ident("orders")

	tbl, err := s.cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	dataSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	if err != nil {
		t.Fatalf("arrow schema: %v", err)
	}
	for i := range 2 {
		tbl, err = s.cat.LoadTable(ctx, ident)
		if err != nil {
			t.Fatalf("reload: %v", err)
		}
		rec := arrowRecord(t, dataSchema, func(rb *array.RecordBuilder) {
			rb.Field(0).(*array.Int64Builder).Append(int64(i))
			rb.Field(1).(*array.StringBuilder).Append("x")
		})
		if err := Append(ctx, tbl, rec, iceberg.Properties{"cdc.position": "p"}); err != nil {
			rec.Release()
			t.Fatalf("Append: %v", err)
		}
		rec.Release()
	}

	metrics := &recordingMetrics{}
	m := NewMaintainer(s.cat, ident, fullMaintenance(), nil, nil, metrics)
	if err := m.RunOnce(ctx, []sink.MaintenanceOp{
		sink.MaintenanceSnapshotExpiry,
		sink.MaintenanceOrphanCleanup,
	}); err != nil {
		t.Fatalf("RunOnce(expiry+cleanup): %v", err)
	}
	if metrics.expiries == 0 || metrics.orphans == 0 {
		t.Fatalf("expiry/orphan metrics = %d/%d", metrics.expiries, metrics.orphans)
	}
}

// A missing table fails each operation with a wrapped load error, and the
// metrics hook still observes the failure.
func TestMaintainerOperationsOnMissingTable(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ident := table.Identifier{"raw", "missing"}

	metrics := &recordingMetrics{}
	m := NewMaintainer(s.cat, ident, fullMaintenance(), nil, nil, metrics)

	if err := m.RunOnce(ctx, []sink.MaintenanceOp{sink.MaintenanceOrphanCleanup}); err == nil {
		t.Fatal("orphan cleanup on a missing table must fail")
	}
	if metrics.orphans == 0 {
		t.Fatal("a failed orphan cleanup must still be recorded")
	}
}
