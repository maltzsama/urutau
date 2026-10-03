package iceberg

import (
	"context"
	"fmt"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

// housekeepingProperties keep Iceberg metadata bounded under CDC's commit
// rate (#463). delete-after-commit removes the superseded metadata.json files
// at commit; manifest-merge folds the data manifests a commit would otherwise
// add one at a time. Both are applied to every table urutau creates or opens,
// so a table created before the setting existed converges on its next boot.
var housekeepingProperties = iceberg.Properties{
	table.MetadataDeleteAfterCommitEnabledKey: "true",
	table.ManifestMergeEnabledKey:             "true",
}

// EnsureHousekeeping sets housekeepingProperties on an existing table when
// they are not already "true", committing only the missing keys so a
// steady-state boot writes no metadata. createTable passes the same
// properties on create; this covers tables created before it did.
func EnsureHousekeeping(ctx context.Context, cat catalog.Catalog, ident table.Identifier, tbl *table.Table) error {
	missing := iceberg.Properties{}
	for k, v := range housekeepingProperties {
		if tbl.Properties().Get(k, "") != v {
			missing[k] = v
		}
	}
	return SetTableProperties(ctx, cat, ident, missing)
}

// SetTableProperties writes arbitrary properties to an Iceberg table.
// Used by adoption to mark snapshot complete without committing data.
func SetTableProperties(ctx context.Context, cat catalog.Catalog, ident table.Identifier, props iceberg.Properties) error {
	if len(props) == 0 {
		return nil
	}
	// The table's live CDC commits race this one, so a conflict is expected:
	// retried like every other sink commit, on a freshly loaded table, instead
	// of ending the run (a snapshot's progress record did, #461).
	var lastErr error
	for attempt := 0; attempt < maxCommitTries; attempt++ {
		if attempt > 0 {
			if err := sleepCtx(ctx, backoffDuration(stagedBackoff, attempt)); err != nil {
				return err
			}
		}
		tbl, err := cat.LoadTable(ctx, ident)
		if err != nil {
			if !isRetryableError(err) {
				return fmt.Errorf("iceberg: load %v: %w", ident, err)
			}
			lastErr = err
			continue
		}
		txn := tbl.NewTransaction()
		if err := txn.SetProperties(props); err != nil {
			return err
		}
		if _, err := txn.Commit(ctx); err != nil {
			if !isRetryableError(err) {
				return err
			}
			lastErr = err
			continue
		}
		return nil
	}
	return fmt.Errorf("%w: property commit on %v: %w", ErrCommitExhausted, ident, lastErr)
}
