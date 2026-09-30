package iceberg

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
)

// restLikeCatalog answers a failed requirement as the REST catalog does, with
// a CommitFailedException, which iceberg-go retries by refreshing the table
// and replaying the commit on it. The hadoop catalog's own error is final.
type restLikeCatalog struct{ catalog.Catalog }

func (c *restLikeCatalog) LoadTable(ctx context.Context, id table.Identifier) (*table.Table, error) {
	tbl, err := c.Catalog.LoadTable(ctx, id)
	if err != nil {
		return nil, err
	}
	return table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), tbl.FS, c), nil
}

func (c *restLikeCatalog) CommitTable(ctx context.Context, id table.Identifier, reqs []table.Requirement, ups []table.Update) (table.Metadata, string, error) {
	m, loc, err := c.Catalog.CommitTable(ctx, id, reqs, ups)
	if err != nil && strings.Contains(err.Error(), "requirement failed") && !errors.Is(err, table.ErrCommitFailed) {
		return nil, "", fmt.Errorf("%w: %v", table.ErrCommitFailed, err)
	}
	return m, loc, err
}

// tableIDs scans raw.orders and returns its ids.
func tableIDs(t *testing.T, s *Sink) []int64 {
	t.Helper()
	ctx := context.Background()
	tbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatal(err)
	}
	at, err := tbl.Scan().ToArrowTable(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer at.Release()
	var ids []int64
	col := at.Column(0)
	for _, ch := range col.Data().Chunks() {
		a := ch.(*array.Int64)
		for i := 0; i < a.Len(); i++ {
			ids = append(ids, a.Value(i))
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// chaos-1M-3daffab lost pr_items chunks 19 and 20: their cycles committed
// while the table's compaction, planned on an earlier snapshot, was still
// rewriting, and their data files are gone from the table. A compaction must
// never drop data files committed after the snapshot it planned on.
func TestACompactionKeepsFilesCommittedWhileItRewrites(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	s.cat = &restLikeCatalog{Catalog: s.cat}
	createOrders(t, s)
	for id := int64(1); id <= 4; id++ {
		appendOrder(t, s, id)
	}
	tbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := compaction.DefaultConfig()
	cfg.MinInputFiles = 2
	plan, err := compaction.Analyze(ctx, tbl, cfg)
	if err != nil || len(plan.Groups) == 0 {
		t.Fatalf("plan: %v groups=%d", err, len(plan.Groups))
	}
	groups := make([]table.CompactionTaskGroup, len(plan.Groups))
	for i, g := range plan.Groups {
		groups[i] = table.CompactionTaskGroup{PartitionKey: g.PartitionKey, Tasks: g.Tasks, TotalSizeBytes: g.TotalSizeBytes}
	}
	txn := tbl.NewTransaction()
	if _, err := txn.RewriteDataFiles(ctx, groups, table.RewriteDataFilesOptions{}); err != nil {
		t.Fatal(err)
	}

	// Two cycles commit while the compaction is still to commit.
	appendOrder(t, s, 100)
	appendOrder(t, s, 101)

	_, cerr := txn.Commit(ctx)
	t.Logf("compaction commit: %v", cerr)

	got := tableIDs(t, s)
	want := map[int64]bool{1: true, 2: true, 3: true, 4: true, 100: true, 101: true}
	for _, id := range got {
		delete(want, id)
	}
	if len(want) > 0 {
		t.Fatalf("ids %v after the compaction; missing %v", got, want)
	}
}
