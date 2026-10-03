package iceberg

import (
	"context"
	"fmt"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

// CommittedPosition reads the committed cdc.position of a table. The fast
// path is the table property written atomically with every commit; when a
// third-party maintenance job replaced the current snapshot (compaction
// produces a replace without the property), the walk-back over snapshot
// summaries is the fallback — defense, not coupling (design §2.2). Empty
// string means the table has never committed (snapshot needed).
func CommittedPosition(ctx context.Context, cat catalog.Catalog, ident table.Identifier) (string, error) {
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return "", fmt.Errorf("iceberg: load %v: %w", ident, err)
	}
	return committedPosition(tbl), nil
}

// committedPosition implements the property-first, walk-back fallback over
// a loaded table's metadata. Split out for unit testing without a live
// catalog.
func committedPosition(tbl *table.Table) string {
	return walkBackPosition(tbl.Properties(), tbl.CurrentSnapshot(), tbl.Metadata().SnapshotByID)
}

// walkBackPosition finds cdc.position in the table properties, else walks the
// BRANCH ancestry from the current head, newest-first, returning the first
// summary that carries it. Walking the ancestry rather than the flat snapshot
// list matters after a third-party rollback-to-snapshot: the newer snapshots
// stay in the metadata list while the branch head has moved back, so a flat
// newest-first scan could return a position AHEAD of the table's visible
// state and a resume from it would silently skip data. Pure, for tests.
func walkBackPosition(props iceberg.Properties, head *table.Snapshot, lookup table.SnapshotLookup) string {
	if pos := props[propPosition]; pos != "" {
		return pos
	}
	if head == nil {
		return ""
	}
	for _, snap := range table.AncestorsOf(head.SnapshotID, lookup) {
		if snap.Summary == nil {
			continue
		}
		if pos := snap.Summary.Properties[propPosition]; pos != "" {
			return pos
		}
	}
	return ""
}
