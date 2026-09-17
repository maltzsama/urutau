package coordinator

import (
	"testing"
	"time"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
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
	c.recordTableStats("w-0", "raw.orders", 10, 2, 12, now)
	c.recordTableStats("w-0", "raw.orders", 5, 0, 8, now)
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
	if st.CommitLatencyMs != 8 {
		t.Errorf("commit latency = %v, want the last ack's (8)", st.CommitLatencyMs)
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
	c.recordTableStats("w", "t", 1, 0, 0, time.Now())
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
		CommitLatencyMs:  150,
	}}})

	got := dashState{c}.Tables()
	if len(got) != 1 {
		t.Fatalf("Tables = %d, want 1", len(got))
	}
	if got[0].CommitFailures != 4 || got[0].DeletesDropped != 2 || got[0].SnapshotProgress != 0.3 {
		t.Errorf("worker metrics not folded: %+v", got[0])
	}
	if got[0].CommitLatencyMs != 150 {
		t.Errorf("commit latency not folded: %+v", got[0])
	}
}

// The per-table rows/s rate is a sliding-window delta; the first ack seeds the
// window baseline, so its rows do not count toward the first rate.
func TestDashStateTablesRowsRate(t *testing.T) {
	c := &Coordinator{
		cfg: Config{Spec: &spec.Spec{Tables: []spec.Table{
			{Source: "shop.orders", Target: "raw.orders"},
		}}},
	}
	base := time.Now()
	c.recordTableStats("w-0", "raw.orders", 10, 0, 0, base)                     // baseline
	c.recordTableStats("w-0", "raw.orders", 40, 0, 0, base.Add(10*time.Second)) // +40 rows over 10s

	got := dashState{c}.Tables()
	if len(got) != 1 {
		t.Fatalf("Tables = %d, want 1", len(got))
	}
	if rate := got[0].RowsRate; rate < 3.5 || rate > 4.5 {
		t.Errorf("RowsRate = %v, want ~4 rows/s (40 rows over 10s)", rate)
	}
}

// The drawer's "current position" comes from the serving worker's last
// durably-committed position (c.confirmed), keyed by worker and mapped to its
// table.
func TestDashStateTablesPosition(t *testing.T) {
	pos, err := position.ParseGTID("00000000-0000-0000-0000-000000000001:1-42")
	if err != nil {
		t.Fatalf("ParseGTID: %v", err)
	}
	c := &Coordinator{
		cfg: Config{Spec: &spec.Spec{Tables: []spec.Table{
			{Source: "shop.orders", Target: "raw.orders"},
		}}},
		workers: map[string]*workerState{
			"w-0": {refs: []source.TableRef{{Source: "shop.orders", Target: "raw.orders"}}},
		},
		confirmed: map[string]position.Position{"w-0": pos},
	}

	got := dashState{c}.Tables()
	if len(got) != 1 {
		t.Fatalf("Tables = %d, want 1", len(got))
	}
	if got[0].Position != pos.String() {
		t.Errorf("Position = %q, want %q", got[0].Position, pos.String())
	}
}
