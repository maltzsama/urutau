package remote

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/driver"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

// reportSink is a Maintainable sink whose maintainer records, when each
// operation starts, how many results the pass had reported by then.
type reportSink struct {
	sink.Sink
	m *reportMaintainer
}

func (s *reportSink) Close() error { return nil }
func (s *reportSink) Maintain(_ core.TableRef, _ spec.Maintenance, _ *slog.Logger, _ func() string, metrics sink.MaintainerMetrics) sink.Maintainer {
	s.m.metrics = metrics
	return s.m
}

type reportMaintainer struct {
	mu          sync.Mutex
	metrics     sink.MaintainerMetrics
	reported    func() int
	atCompact   int
	compactSeen bool
}

func (m *reportMaintainer) RunOnce(_ context.Context, ops []sink.MaintenanceOp) error {
	for _, op := range ops {
		switch op {
		case sink.MaintenanceSnapshotExpiry:
			m.metrics.SnapshotExpiryRun("raw.orders", 3, nil)
		case sink.MaintenanceOrphanCleanup:
			m.metrics.OrphanCleanupRun("raw.orders", 0, 0, nil)
		case sink.MaintenanceCompaction:
			m.mu.Lock()
			m.atCompact, m.compactSeen = m.reported(), true
			m.mu.Unlock()
			m.metrics.CompactionRun("raw.orders", 2, 1, 10, 5, nil)
		}
	}
	return nil
}

var reportM = &reportMaintainer{}

func init() {
	_ = driver.RegisterSink("maintenance-report-test", func(context.Context, sink.Config) (sink.Sink, error) {
		return &reportSink{m: reportM}, nil
	})
}

// Issue #457: the maintenance Pod ran compaction, expiry and cleanup, and
// reported them in one message at the end. A compaction that dies in the Pod
// (OOM-killed at 5Gi in chaos-1M-5a01915) took the report of the operations
// that had already run with it: the coordinator never marked them run nor
// recorded them. Each operation is reported as it ends.
func TestEachMaintenanceOperationIsReportedAsItEnds(t *testing.T) {
	var mu sync.Mutex
	var reported []*pb.MaintenanceOpResult
	reportM.reported = func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(reported)
	}
	cfg := RemoteConfig{Sink: sink.Config{Type: "maintenance-report-test"}, Logger: slog.New(slog.DiscardHandler)}
	assign := &pb.MaintenanceAssignment{
		TargetTable: "raw.orders",
		Ops:         []string{string(sink.MaintenanceSnapshotExpiry), string(sink.MaintenanceOrphanCleanup), string(sink.MaintenanceCompaction)},
	}
	_, err := runMaintenancePass(context.Background(), cfg, assign, func(ops []*pb.MaintenanceOpResult) error {
		mu.Lock()
		defer mu.Unlock()
		reported = append(reported, ops...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reportM.compactSeen || reportM.atCompact != 2 {
		t.Fatalf("results reported when compaction started = %d, want 2: expiry and cleanup reported before it runs", reportM.atCompact)
	}
	if len(reported) != 3 {
		t.Fatalf("%d results reported, want 3", len(reported))
	}
}
