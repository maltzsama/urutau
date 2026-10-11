package iceberg

import (
	"context"
	"fmt"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

// formatVersion is the Iceberg format every sink table uses. Deletion vectors
// and row lineage exist only from version 3 on.
const formatVersion = 3

// upgradeFormat brings a table created by an older release (format-version 2)
// to formatVersion and returns the reloaded table. The upgrade is a metadata
// change: no data or delete file is rewritten, and the equality deletes the
// table already carries stay valid.
func upgradeFormat(ctx context.Context, cat catalog.Catalog, ident table.Identifier, tbl *table.Table) (*table.Table, error) {
	if tbl.Metadata().Version() >= formatVersion {
		return tbl, nil
	}
	txn := tbl.NewTransaction()
	if err := txn.UpgradeFormatVersion(formatVersion); err != nil {
		return nil, fmt.Errorf("iceberg: upgrade %v to format-version %d: %w", ident, formatVersion, err)
	}
	if _, err := txn.Commit(ctx); err != nil {
		return nil, fmt.Errorf("iceberg: upgrade %v to format-version %d: %w", ident, formatVersion, err)
	}
	upgraded, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return nil, fmt.Errorf("iceberg: load %v after the format upgrade: %w", ident, err)
	}
	return upgraded, nil
}
