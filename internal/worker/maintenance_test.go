package worker

import (
	"errors"
	"testing"
)

// resultCollector is the seam between the maintainer's per-op metrics
// callbacks and the wire: it must capture every operation, in order, with its
// counts and error, so the coordinator can record them after this ephemeral
// process is gone.
func TestResultCollectorCapturesOps(t *testing.T) {
	c := &resultCollector{}
	c.CompactionRun("raw.orders", 3, 1, 100, 40, nil)
	c.SnapshotExpiryRun("raw.orders", 2, nil)
	c.OrphanCleanupRun("raw.orders", 5, 500, errors.New("boom"))

	if len(c.ops) != 3 {
		t.Fatalf("captured %d ops, want 3", len(c.ops))
	}
	if comp := c.ops[0].GetCompaction(); comp == nil ||
		comp.FilesRemoved != 3 || comp.FilesAdded != 1 ||
		comp.BytesBefore != 100 || comp.BytesAfter != 40 || comp.Error != "" {
		t.Errorf("compaction result = %+v", comp)
	}
	if exp := c.ops[1].GetExpiry(); exp == nil || exp.SnapshotsRemoved != 2 || exp.Error != "" {
		t.Errorf("expiry result = %+v", exp)
	}
	if orph := c.ops[2].GetOrphan(); orph == nil ||
		orph.FilesDeleted != 5 || orph.BytesFreed != 500 || orph.Error != "boom" {
		t.Errorf("orphan result = %+v", orph)
	}
	// The oneof getters must be exclusive: only the populated arm returns.
	if c.ops[0].GetExpiry() != nil || c.ops[0].GetOrphan() != nil {
		t.Errorf("compaction op leaked into another arm: %+v", c.ops[0])
	}
}
