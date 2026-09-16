package coordinator

import (
	"testing"
	"time"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/spec"
)

// The dashboard's TableStatus is folded from acks (rows/deletes/commits) and
// maintenance results — no worker scrape.
func TestDashStateTablesAggregates(t *testing.T) {
	c := &Coordinator{
		cfg: Config{Spec: &spec.Spec{Tables: []spec.Table{
			{Source: "shop.orders", Target: "raw.orders", WriteMode: spec.WriteModeUpsert},
		}}},
	}
	now := time.Now()
	c.recordTableStats("w-0", "raw.orders", 10, 2, now)
	c.recordTableStats("w-0", "raw.orders", 5, 0, now)
	c.recordMaintStats("raw.orders", "compaction", now, func(s *maintStats) {
		s.filesRemoved += 3
		s.bytesBefore += 100
		s.bytesAfter += 40
	})
	c.recordMaintStats("raw.orders", "orphan_cleanup", now, func(s *maintStats) {
		s.filesDeleted += 2
		s.bytesFreed += 512
	})

	got := dashState{c}.Tables()
	if len(got) != 1 {
		t.Fatalf("Tables = %d, want 1", len(got))
	}
	st := got[0]
	if st.Commits != 2 || st.RowsTotal != 15 || st.EqualityDeletes != 2 {
		t.Errorf("ack aggregate = %+v", st)
	}
	if st.Maintenance == nil || st.Maintenance.Compaction == nil || st.Maintenance.Compaction.FilesRemoved != 3 {
		t.Errorf("compaction aggregate = %+v", st.Maintenance)
	}
	if st.Maintenance.Orphan == nil || st.Maintenance.Orphan.BytesFreed != 512 {
		t.Errorf("orphan aggregate = %+v", st.Maintenance)
	}
	if st.Maintenance.Expiry != nil {
		t.Errorf("expiry must be absent when never reported: %+v", st.Maintenance.Expiry)
	}
}

// A zero Coordinator (no maps) must not panic on the recording paths.
func TestRecordStatsLazyInit(t *testing.T) {
	c := &Coordinator{}
	c.recordTableStats("w", "t", 1, 0, time.Now())
	c.recordMaintStats("t", "compaction", time.Now(), func(*maintStats) {})
	if len(dashState{c}.Tables()) != 0 {
		t.Error("no spec tables, so no rows")
	}
}

// The worker metrics report folds the series the coordinator cannot derive
// from acks into the per-table aggregate.
func TestOnWorkerMetricsFoldsTotals(t *testing.T) {
	c := &Coordinator{
		cfg: Config{Spec: &spec.Spec{Tables: []spec.Table{
			{Source: "shop.orders", Target: "raw.orders"},
		}}},
	}
	c.onWorkerMetrics(&pb.WorkerMetricsReport{Tables: []*pb.TableMetrics{{
		Table:            "raw.orders",
		CommitFailures:   4,
		DeletesDropped:   2,
		SnapshotProgress: 0.3,
	}}})

	got := dashState{c}.Tables()
	if len(got) != 1 {
		t.Fatalf("Tables = %d, want 1", len(got))
	}
	if got[0].CommitFailures != 4 || got[0].DeletesDropped != 2 || got[0].SnapshotProgress != 0.3 {
		t.Errorf("worker metrics not folded: %+v", got[0])
	}
}
