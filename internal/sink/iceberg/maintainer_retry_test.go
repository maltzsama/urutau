package iceberg

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

// countingLoadCatalog counts LoadTable calls and always fails with err.
type countingLoadCatalog struct {
	catalog.Catalog
	err   error
	calls int
}

func (c *countingLoadCatalog) LoadTable(context.Context, table.Identifier) (*table.Table, error) {
	c.calls++
	return nil, c.err
}

// cleanOrphans must retry LoadTable on a transient error, like compact and
// expireSnapshots do (issue #198).
func TestCleanOrphansRetriesLoadTable(t *testing.T) {
	old := maintainerBackoff
	maintainerBackoff = 0
	t.Cleanup(func() { maintainerBackoff = old })

	cat := &countingLoadCatalog{err: errors.New("connection refused")}
	m := &Maintainer{cat: cat, ident: table.Identifier{"raw", "t"}, log: discardLogger()}
	if err := m.cleanOrphans(context.Background()); !errors.Is(err, ErrCommitExhausted) {
		t.Fatalf("cleanOrphans = %v, want ErrCommitExhausted", err)
	}
	if cat.calls != maintainerMaxTries {
		t.Fatalf("LoadTable calls = %d, want %d (a transient error must be retried)", cat.calls, maintainerMaxTries)
	}
}

// A terminal LoadTable error is not retried: the run fails fast with the
// underlying error rather than reporting retries exhausted.
func TestCleanOrphansTerminalLoadTableNotRetried(t *testing.T) {
	cat := &countingLoadCatalog{err: errors.New("no such table")}
	m := &Maintainer{cat: cat, ident: table.Identifier{"raw", "t"}, log: discardLogger()}
	err := m.cleanOrphans(context.Background())
	if err == nil || errors.Is(err, ErrCommitExhausted) {
		t.Fatalf("cleanOrphans = %v, want the terminal error, not exhausted", err)
	}
	if cat.calls != 1 {
		t.Fatalf("LoadTable calls = %d, want 1 (a terminal error must not be retried)", cat.calls)
	}
}
