package iceberg

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

func parentPtr(id int64) *int64 { return &id }

// lookupFrom builds a SnapshotLookup over a snapshot set.
func lookupFrom(snaps ...table.Snapshot) table.SnapshotLookup {
	byID := make(map[int64]*table.Snapshot, len(snaps))
	for i := range snaps {
		byID[snaps[i].SnapshotID] = &snaps[i]
	}
	return func(id int64) *table.Snapshot { return byID[id] }
}

// TestWalkBackPosition proves the compaction-immunity fallback: when the
// current table property was dropped by a third-party replace snapshot, the
// position still comes back from the newest ancestor summary (design §2.2).
func TestWalkBackPosition(t *testing.T) {
	// Linear history 10 -> 20 -> 30. 30 is a compaction replace without the
	// property; 20 carries it.
	s10 := table.Snapshot{SnapshotID: 10, Summary: &table.Summary{
		Operation:  table.OpAppend,
		Properties: iceberg.Properties{"cdc.position": "gtid:1-10"},
	}}
	s20 := table.Snapshot{SnapshotID: 20, ParentSnapshotID: parentPtr(10), Summary: &table.Summary{
		Operation:  table.OpAppend,
		Properties: iceberg.Properties{"cdc.position": "gtid:1-20"},
	}}
	s30 := table.Snapshot{SnapshotID: 30, ParentSnapshotID: parentPtr(20), Summary: &table.Summary{Operation: table.OpReplace}}
	lookup := lookupFrom(s10, s20, s30)

	// Property present: fast path, no walk.
	if pos := walkBackPosition(iceberg.Properties{"cdc.position": "gtid:1-30"}, &s30, lookup); pos != "gtid:1-30" {
		t.Fatalf("property path = %q, want gtid:1-30", pos)
	}

	// Property gone: the newest ANCESTOR with the property wins.
	if pos := walkBackPosition(nil, &s30, lookup); pos != "gtid:1-20" {
		t.Fatalf("walk-back = %q, want gtid:1-20", pos)
	}

	// Never committed.
	only := table.Snapshot{SnapshotID: 5, Summary: &table.Summary{Operation: table.OpReplace}}
	if pos := walkBackPosition(nil, &only, lookupFrom(only)); pos != "" {
		t.Fatalf("uncommitted = %q, want empty", pos)
	}
}

// TestWalkBackPositionFollowsAncestryAfterRollback pins issue #125: after a
// rollback the head moves back while the newer snapshots stay in the metadata
// list, so the walk must follow the head's ancestry — a flat newest-first scan
// would return a position ahead of the visible state and skip data on resume.
func TestWalkBackPositionFollowsAncestryAfterRollback(t *testing.T) {
	s10 := table.Snapshot{SnapshotID: 10, Summary: &table.Summary{
		Operation:  table.OpAppend,
		Properties: iceberg.Properties{"cdc.position": "gtid:1-10"},
	}}
	s20 := table.Snapshot{SnapshotID: 20, ParentSnapshotID: parentPtr(10), Summary: &table.Summary{
		Operation:  table.OpAppend,
		Properties: iceberg.Properties{"cdc.position": "gtid:1-20"},
	}}
	s30 := table.Snapshot{SnapshotID: 30, ParentSnapshotID: parentPtr(20), Summary: &table.Summary{
		Operation:  table.OpAppend,
		Properties: iceberg.Properties{"cdc.position": "gtid:1-30"},
	}}
	// 20 and 30 still exist in the metadata, but the head rolled back to 10.
	lookup := lookupFrom(s10, s20, s30)
	if pos := walkBackPosition(nil, &s10, lookup); pos != "gtid:1-10" {
		t.Fatalf("after rollback = %q, want gtid:1-10 (the head's ancestry, not the newer snapshots)", pos)
	}
}
