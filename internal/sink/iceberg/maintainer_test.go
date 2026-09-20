package iceberg

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"

	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

// The compile-time assertions in sink.go already prove *Sink implements
// sink.Maintainable and *Maintainer implements sink.Maintainer. This proves
// it at the value the orchestration actually type-asserts against — a
// sink.Sink interface value, the exact shape runner.go/coordinator.go hold.
func TestSinkSatisfiesMaintainableThroughTheInterface(t *testing.T) {
	var s sink.Sink = &Sink{}
	if _, ok := s.(sink.Maintainable); !ok {
		t.Error("iceberg.Sink held as a sink.Sink must still assert to sink.Maintainable — the orchestration's exact check")
	}
}

// discardLogger is a *slog.Logger that writes nowhere — the tests assert on
// behavior (call counts, skipped operations), not log output.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, nil))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// A nil CompactionConfig must fall through to iceberg-go's own
// compaction.DefaultConfig() untouched.
func TestCompactionConfigFromDefaults(t *testing.T) {
	cfg := compactionConfigFrom(nil, discardLogger())
	def := compaction.DefaultConfig()
	if cfg.TargetFileSizeBytes != def.TargetFileSizeBytes || cfg.MinInputFiles != def.MinInputFiles {
		t.Errorf("nil config = %+v, want iceberg-go's own default %+v", cfg, def)
	}
}

func TestCompactionConfigFromTargetFileSize(t *testing.T) {
	cfg := compactionConfigFrom(&spec.CompactionConfig{TargetFileSize: "128Mi"}, discardLogger())
	want := int64(128 * 1024 * 1024)
	if cfg.TargetFileSizeBytes != want {
		t.Fatalf("TargetFileSizeBytes = %d, want %d", cfg.TargetFileSizeBytes, want)
	}
	// The 75%/180% ratios mirror cmd/iceberg/compact.go's own derivation
	// from a custom target size, so a spec-driven target size gets the same
	// min/max bounds a CLI-driven one would.
	if wantMin := want * 3 / 4; cfg.MinFileSizeBytes != wantMin {
		t.Errorf("MinFileSizeBytes = %d, want %d (75%% of target)", cfg.MinFileSizeBytes, wantMin)
	}
	if wantMax := want * 9 / 5; cfg.MaxFileSizeBytes != wantMax {
		t.Errorf("MaxFileSizeBytes = %d, want %d (180%% of target)", cfg.MaxFileSizeBytes, wantMax)
	}
}

// A malformed TargetFileSize must not reach here in practice (Validate
// rejects it first), but compactionConfigFrom must not panic or produce a
// zero/negative size if it somehow does — it silently keeps the default.
func TestCompactionConfigFromBadTargetFileSizeKeepsDefault(t *testing.T) {
	def := compactionConfigFrom(nil, discardLogger())
	cfg := compactionConfigFrom(&spec.CompactionConfig{TargetFileSize: "not-a-size"}, discardLogger())
	if cfg.TargetFileSizeBytes != def.TargetFileSizeBytes {
		t.Errorf("bad TargetFileSize changed the default: got %d, want %d", cfg.TargetFileSizeBytes, def.TargetFileSizeBytes)
	}
}

func TestCompactionConfigFromMinInputFiles(t *testing.T) {
	cfg := compactionConfigFrom(&spec.CompactionConfig{MinInputFiles: 10}, discardLogger())
	if cfg.MinInputFiles != 10 {
		t.Fatalf("MinInputFiles = %d, want 10", cfg.MinInputFiles)
	}
}

func TestDurationOr(t *testing.T) {
	if got := durationOr("", 5*time.Minute, discardLogger(), "x"); got != 5*time.Minute {
		t.Errorf("empty string: got %v, want default", got)
	}
	if got := durationOr("not-a-duration", 5*time.Minute, discardLogger(), "x"); got != 5*time.Minute {
		t.Errorf("malformed: got %v, want default", got)
	}
	if got := durationOr("0s", 5*time.Minute, discardLogger(), "x"); got != 5*time.Minute {
		t.Errorf("zero duration: got %v, want default (a zero interval would busy-loop)", got)
	}
	if got := durationOr("10m", 5*time.Minute, discardLogger(), "x"); got != 10*time.Minute {
		t.Errorf("valid: got %v, want 10m", got)
	}
}

func TestIntOr(t *testing.T) {
	if got := intOr(0, 1, discardLogger(), "x"); got != 1 {
		t.Errorf("zero: got %d, want default", got)
	}
	if got := intOr(-1, 1, discardLogger(), "x"); got != 1 {
		t.Errorf("negative: got %d, want default", got)
	}
	if got := intOr(3, 1, discardLogger(), "x"); got != 3 {
		t.Errorf("positive: got %d, want 3", got)
	}
}

func TestIdentString(t *testing.T) {
	if got := identString(table.Identifier{"raw", "orders"}); got != "raw.orders" {
		t.Errorf("got %q, want %q", got, "raw.orders")
	}
	if got := identString(table.Identifier{"solo"}); got != "solo" {
		t.Errorf("got %q, want %q", got, "solo")
	}
}

// RunOnce is the one-shot pass the ephemeral worker (and the collapsed
// runner's in-process scheduler) drives. It must be a no-op when maintenance
// is disabled or the caller passes no operations.
func TestRunOnceDisabledIsNoop(t *testing.T) {
	m := &Maintainer{cfg: spec.Maintenance{Enabled: false}, log: discardLogger()}
	if err := m.RunOnce(context.Background(), []sink.MaintenanceOp{sink.MaintenanceCompaction}); err != nil {
		t.Fatalf("disabled maintenance must be a no-op: %v", err)
	}
}

func TestRunOnceEmptyOpsIsNoop(t *testing.T) {
	m := &Maintainer{cfg: spec.Maintenance{Enabled: true}, log: discardLogger()}
	if err := m.RunOnce(context.Background(), nil); err != nil {
		t.Fatalf("empty ops must be a no-op: %v", err)
	}
}

func TestRunOnceUnknownOpErrors(t *testing.T) {
	m := &Maintainer{cfg: spec.Maintenance{Enabled: true}, log: discardLogger()}
	if err := m.RunOnce(context.Background(), []sink.MaintenanceOp{"nope"}); err == nil {
		t.Fatal("an unknown operation must error")
	}
}

// A due operation whose sub-config is nil must be skipped WITHOUT touching
// the catalog: runOne returns before m.compact/m.expireSnapshots/
// m.cleanOrphans, so this passes with a nil catalog. A table with only
// SnapshotExpiry configured must never compact or clean orphans.
func TestRunOnceSkipsDisabledSubConfigs(t *testing.T) {
	m := &Maintainer{cfg: spec.Maintenance{Enabled: true}, log: discardLogger()}
	ops := []sink.MaintenanceOp{
		sink.MaintenanceCompaction,
		sink.MaintenanceSnapshotExpiry,
		sink.MaintenanceOrphanCleanup,
	}
	if err := m.RunOnce(context.Background(), ops); err != nil {
		t.Fatalf("all sub-configs nil: every operation must be skipped, got %v", err)
	}
}
