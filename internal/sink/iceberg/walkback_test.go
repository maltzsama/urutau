package iceberg

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// TestWalkBackPosition proves the compaction-immunity fallback: when the
// current table property was dropped by a third-party replace snapshot, the
// position still comes back from the newest snapshot summary (design §2.2).
func TestWalkBackPosition(t *testing.T) {
	// Snapshots arrive chronologically (oldest first) — the order iceberg-go
	// keeps them in. The NEWEST snapshot has no property (a compaction
	// replace); the one behind it carries cdc.position.
	snaps := []table.Snapshot{
		{SnapshotID: 10, Summary: &table.Summary{
			Operation:  table.OpAppend,
			Properties: iceberg.Properties{"cdc.position": "gtid:1-10"},
		}},
		{SnapshotID: 20, Summary: &table.Summary{
			Operation:  table.OpAppend,
			Properties: iceberg.Properties{"cdc.position": "gtid:1-20"},
		}},
		{SnapshotID: 30, Summary: &table.Summary{Operation: table.OpReplace}},
	}

	// Property present: fast path, no walk.
	if pos := walkBackPosition(iceberg.Properties{"cdc.position": "gtid:1-30"}, snaps); pos != "gtid:1-30" {
		t.Fatalf("property path = %q, want gtid:1-30", pos)
	}

	// Property gone: newest snapshot with the property wins.
	if pos := walkBackPosition(nil, snaps); pos != "gtid:1-20" {
		t.Fatalf("walk-back = %q, want gtid:1-20", pos)
	}

	// Never committed.
	if pos := walkBackPosition(nil, []table.Snapshot{{SnapshotID: 5, Summary: &table.Summary{Operation: table.OpReplace}}}); pos != "" {
		t.Fatalf("uncommitted = %q, want empty", pos)
	}
}
