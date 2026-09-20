package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// serverPreflightSQL reads every variable the replication reader depends on in
// one round trip, so the whole preflight passes or fails atomically.
const serverPreflightSQL = `SELECT @@GLOBAL.log_bin, @@GLOBAL.binlog_format, ` +
	`@@GLOBAL.gtid_mode, @@GLOBAL.enforce_gtid_consistency, @@GLOBAL.binlog_row_image`

// ValidateServer checks the server variables the MySQL replication reader
// depends on. The four hard requirements fail loud; binlog_row_image returns a
// warning instead (a partial after-image is a correctness risk the operator
// must know about, but not a boot blocker). It mirrors the Postgres
// EnsureSetup preflight.
func ValidateServer(ctx context.Context, db *sql.DB) (warning string, err error) {
	// All five scan as text: log_bin arrives as "1", the rest as their
	// keyword. A text scan is driver-agnostic and needs no per-type handling.
	var logBin, binlogFormat, gtidMode, enforceGTID, rowImage string
	if err := db.QueryRowContext(ctx, serverPreflightSQL).Scan(
		&logBin, &binlogFormat, &gtidMode, &enforceGTID, &rowImage,
	); err != nil {
		return "", fmt.Errorf("mysql: server preflight: %w", err)
	}

	if logBin != "1" {
		return "", fmt.Errorf("mysql: log_bin must be ON (binlog disabled)")
	}
	if !strings.EqualFold(binlogFormat, "ROW") {
		return "", fmt.Errorf("mysql: binlog_format must be ROW, got %q", binlogFormat)
	}
	if !strings.EqualFold(gtidMode, "ON") {
		return "", fmt.Errorf("mysql: gtid_mode must be ON, got %q", gtidMode)
	}
	if !strings.EqualFold(enforceGTID, "ON") {
		return "", fmt.Errorf("mysql: enforce_gtid_consistency must be ON, got %q", enforceGTID)
	}
	if !strings.EqualFold(rowImage, "FULL") {
		return fmt.Sprintf("mysql: binlog_row_image is %q, not FULL — updates/deletes may carry partial after-images and silently drop columns", rowImage), nil
	}
	return "", nil
}
