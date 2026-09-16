package maintenance

import (
	"slices"
	"testing"
	"time"

	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

func enabledCfg() *spec.Maintenance {
	return &spec.Maintenance{
		Enabled:        true,
		Compaction:     &spec.CompactionConfig{Interval: "5m"},
		SnapshotExpiry: &spec.SnapshotExpiryConfig{Interval: "10m"},
		OrphanCleanup:  &spec.OrphanCleanupConfig{Interval: "1h"},
	}
}

// Never-run operations are all due, in execution order.
func TestDueAllEnabledWhenNeverRun(t *testing.T) {
	s := NewSchedule()
	got := s.Due("raw.orders", enabledCfg(), time.Now())
	want := []sink.MaintenanceOp{
		sink.MaintenanceCompaction,
		sink.MaintenanceSnapshotExpiry,
		sink.MaintenanceOrphanCleanup,
	}
	if !slices.Equal(got, want) {
		t.Fatalf("Due = %v, want %v (execution order)", got, want)
	}
}

func TestDueDisabledIsNil(t *testing.T) {
	s := NewSchedule()
	cfg := enabledCfg()
	cfg.Enabled = false
	if got := s.Due("raw.orders", cfg, time.Now()); got != nil {
		t.Fatalf("Due = %v, want nil for disabled maintenance", got)
	}
	if got := s.Due("raw.orders", nil, time.Now()); got != nil {
		t.Fatalf("Due = %v, want nil for nil config", got)
	}
}

// A sub-config the operator never declared is not due, even though the
// operation name exists.
func TestDueSkipsNilSubConfigs(t *testing.T) {
	s := NewSchedule()
	cfg := &spec.Maintenance{Enabled: true, Compaction: &spec.CompactionConfig{}}
	got := s.Due("t", cfg, time.Now())
	if !slices.Equal(got, []sink.MaintenanceOp{sink.MaintenanceCompaction}) {
		t.Fatalf("Due = %v, want only compaction", got)
	}
}

// MarkRun suppresses an operation until its own interval elapses, leaving the
// other operations due — the whole point of per-operation scheduling.
func TestMarkRunSuppressesUntilInterval(t *testing.T) {
	s := NewSchedule()
	cfg := enabledCfg()
	now := time.Now()
	s.MarkRun("t", []sink.MaintenanceOp{sink.MaintenanceCompaction}, now)

	got := s.Due("t", cfg, now.Add(4*time.Minute))
	if slices.Contains(got, sink.MaintenanceCompaction) {
		t.Fatalf("compaction due at 4m, want not due until 5m: %v", got)
	}
	if !slices.Contains(got, sink.MaintenanceSnapshotExpiry) || !slices.Contains(got, sink.MaintenanceOrphanCleanup) {
		t.Fatalf("expiry/orphan must still be due: %v", got)
	}

	got = s.Due("t", cfg, now.Add(6*time.Minute))
	if !slices.Contains(got, sink.MaintenanceCompaction) {
		t.Fatalf("compaction not due at 6m, want due: %v", got)
	}
}

// Tables are tracked independently: marking one run must not suppress
// another table's operation.
func TestDueIsPerTable(t *testing.T) {
	s := NewSchedule()
	cfg := enabledCfg()
	now := time.Now()
	s.MarkRun("a", []sink.MaintenanceOp{sink.MaintenanceCompaction}, now)

	if got := s.Due("a", cfg, now.Add(time.Minute)); slices.Contains(got, sink.MaintenanceCompaction) {
		t.Fatalf("table a compaction must be suppressed, got %v", got)
	}
	if got := s.Due("b", cfg, now.Add(time.Minute)); !slices.Contains(got, sink.MaintenanceCompaction) {
		t.Fatalf("table b must be independent of a, got %v", got)
	}
}

// An unset interval falls back to the spec default, so a config with only
// "enabled: true" still schedules on the documented cadence.
func TestDueAppliesDefaultInterval(t *testing.T) {
	s := NewSchedule()
	cfg := &spec.Maintenance{Enabled: true, Compaction: &spec.CompactionConfig{}}
	now := time.Now()
	s.MarkRun("t", []sink.MaintenanceOp{sink.MaintenanceCompaction}, now)

	if got := s.Due("t", cfg, now.Add(spec.DefaultCompactionInterval-time.Second)); slices.Contains(got, sink.MaintenanceCompaction) {
		t.Fatalf("compaction due one second before the default interval: %v", got)
	}
	if got := s.Due("t", cfg, now.Add(spec.DefaultCompactionInterval+time.Second)); !slices.Contains(got, sink.MaintenanceCompaction) {
		t.Fatalf("compaction not due one second after the default interval: %v", got)
	}
}

func TestCheckInterval(t *testing.T) {
	if got := CheckInterval(&spec.Maintenance{Enabled: true, Compaction: &spec.CompactionConfig{Interval: "5m"}}); got != 150*time.Second {
		t.Errorf("CheckInterval = %v, want 2m30s (half the shortest interval)", got)
	}
	if got := CheckInterval(&spec.Maintenance{Enabled: true, Compaction: &spec.CompactionConfig{Interval: "1s"}}); got != time.Second {
		t.Errorf("CheckInterval = %v, want the 1s floor", got)
	}
	if got := CheckInterval(nil); got != time.Second {
		t.Errorf("CheckInterval(nil) = %v, want 1s", got)
	}
}
