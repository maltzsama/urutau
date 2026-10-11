package iceberg

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// unknownOutcome is what the REST client returns for a commit answered with
// 500/502/503/504: it wraps rest.ErrCommitStateUnknown and its text is the
// server's "Type: Message", which carries no status code.
type unknownOutcome struct{}

func (unknownOutcome) Error() string {
	return "CommitStateUnknownException: the commit may or may not have been applied"
}
func (unknownOutcome) Unwrap() error { return rest.ErrCommitStateUnknown }

// lostResponseCatalog applies every commit, then answers the first lose of
// them as a catalog does when it cannot tell whether the commit landed.
type lostResponseCatalog struct {
	catalog.Catalog
	lose    int
	applied int
}

func (c *lostResponseCatalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	tbl, err := c.Catalog.LoadTable(ctx, ident)
	if err != nil {
		return nil, err
	}
	// Rebind the table to this catalog so its commits come through CommitTable below.
	return table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), tbl.FS, c), nil
}

func (c *lostResponseCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	meta, loc, err := c.Catalog.CommitTable(ctx, ident, reqs, updates)
	if err != nil {
		return meta, loc, err
	}
	c.applied++
	if c.applied <= c.lose {
		return nil, "", unknownOutcome{}
	}
	return meta, loc, nil
}

// A commit that the catalog applied but answered with an unknown outcome must
// end as one commit: the writer reloads the table, sees its own commit and
// stops, instead of failing the batch or applying it again.
func TestCommitWithUnknownOutcomeIsAppliedOnce(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode dataplane.WriteMode
		pk   []string
	}{
		{"upsert", dataplane.UpsertMode, []string{"id"}},
		{"append", dataplane.AppendMode, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s := hadoopSink(t)
			lossy := &lostResponseCatalog{Catalog: s.cat, lose: 1}
			s.cat = lossy

			ref := core.TableRef{Target: "orders", PrimaryKey: tc.pk}
			if err := s.EnsureTable(ctx, ref, canonicalSchema(), nil, core.CastPolicy{}, tc.mode); err != nil {
				t.Fatalf("EnsureTable: %v", err)
			}
			w, err := s.Writer(ctx, ref, core.CastPolicy{}, nil)
			if err != nil {
				t.Fatalf("Writer: %v", err)
			}
			w.(*TableWriter).backoff = 0

			b := wireBatch(t, [3]any{int64(1), "a", rowchange.OpInsert}, [3]any{int64(2), "b", rowchange.OpInsert})
			b.Mode = tc.mode
			defer b.Release()
			before := lossy.applied
			if err := w.Commit(ctx, b); err != nil {
				t.Fatalf("Commit = %v, want nil: the commit landed and the writer must find it on reload", err)
			}
			if got := lossy.applied - before; got != 1 {
				t.Errorf("catalog applied %d commits for one batch, want 1", got)
			}

			tbl, err := s.cat.LoadTable(ctx, s.ident(ref.Target))
			if err != nil {
				t.Fatalf("LoadTable: %v", err)
			}
			if ids := scanIDs(t, tbl); len(ids) != 2 {
				t.Errorf("table holds ids %v, want exactly [1 2]", ids)
			}
		})
	}
}

// The classification must not depend on the server's wording: an unknown
// commit outcome is retryable because every retry reloads the table and
// checks whether the commit already landed.
func TestUnknownCommitOutcomeIsRetryable(t *testing.T) {
	if !isRetryableError(unknownOutcome{}) {
		t.Fatal("an unknown commit outcome is classified terminal; the batch would fail although the commit may have landed")
	}
	if isRetryableError(errors.New("CommitStateUnknownException: looks alike but is not the catalog's error")) {
		t.Fatal("a look-alike message is classified retryable; only the catalog's own error may be")
	}
}
