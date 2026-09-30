package iceberg

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

// conflictingCatalog answers the next conflicts commits the way the REST
// catalog answers a table another writer just changed.
type conflictingCatalog struct {
	catalog.Catalog
	conflicts int
}

func (c *conflictingCatalog) LoadTable(ctx context.Context, id table.Identifier) (*table.Table, error) {
	tbl, err := c.Catalog.LoadTable(ctx, id)
	if err != nil {
		return nil, err
	}
	return table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), tbl.FS, c), nil
}

func (c *conflictingCatalog) CommitTable(ctx context.Context, id table.Identifier, reqs []table.Requirement, ups []table.Update) (table.Metadata, string, error) {
	if c.conflicts > 0 {
		c.conflicts--
		return nil, "", fmt.Errorf("%w: CommitConflictException: concurrently modified", table.ErrCommitFailed)
	}
	return c.Catalog.CommitTable(ctx, id, reqs, ups)
}

// Recording a snapshot's progress races the live CDC commits on the same
// table, and one conflict ended the run (chaos-1M-406f460, pr_items, twice in
// a minute): the property commit is retried like every other sink commit.
func TestSetTablePropertiesRetriesACommitConflict(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)
	s.cat = &conflictingCatalog{Catalog: s.cat, conflicts: 2}
	if err := s.SetProperties(ctx, ref, map[string]string{"cdc.snapshot.state": "in_progress"}); err != nil {
		t.Fatalf("SetProperties after 2 conflicts: %v", err)
	}
	props, err := s.Properties(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if props["cdc.snapshot.state"] != "in_progress" {
		t.Fatalf("property not set: %v", props)
	}
}
