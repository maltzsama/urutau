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
	`@@GLOBAL.gtid_mode, @@GLOBAL.enforce_gtid_consistency, @@GLOBAL.binlog_row_image, ` +
	`@@GLOBAL.binlog_row_value_options`

// serverPreflightSQLNoPartialJSON is the same preflight without
// binlog_row_value_options, for servers that do not expose it (MySQL < 8.0.3,
// most MariaDB): on those the option cannot be enabled, so the whole partial
// JSON question is moot.
const serverPreflightSQLNoPartialJSON = `SELECT @@GLOBAL.log_bin, @@GLOBAL.binlog_format, ` +
	`@@GLOBAL.gtid_mode, @@GLOBAL.enforce_gtid_consistency, @@GLOBAL.binlog_row_image`

// ValidateServer checks the server variables the MySQL replication reader
// depends on. Every requirement is hard: a server that cannot produce a
// complete row image must fail loud at boot, not decode a partial image
// silently. It mirrors the Postgres EnsureSetup preflight.
//
// binlog_row_image=FULL is required because the snapshot and the upsert path
// carry a row's after-image as the row's content: a partial after-image (an
// UPDATE that changed only some columns, or a DELETE with the key alone)
// would drop every unchanged column. binlog_row_value_options must not
// contain PARTIAL_JSON because a JSON column updated in place would arrive as
// a partial document that cannot be decoded safely. That option is read
// separately-tolerant: a server without it simply cannot use PARTIAL_JSON.
//
// The PARTIAL_JSON check reads the GLOBAL value. MySQL also allows the option
// per session, which a boot-time preflight cannot observe: a session that
// enables it after boot can still write a partial JSON after-image. That event
// (PARTIAL_UPDATE_ROWS_EVENT) is out of scope for the global check; the reader
// must reject it at decode time rather than trust the preflight alone.
func ValidateServer(ctx context.Context, db *sql.DB) (warning string, err error) {
	// All values scan as text: log_bin arrives as "1", the rest as their
	// keyword. A text scan is driver-agnostic and needs no per-type handling.
	var logBin, binlogFormat, gtidMode, enforceGTID, rowImage, rowValueOptions string
	err = db.QueryRowContext(ctx, serverPreflightSQL).Scan(
		&logBin, &binlogFormat, &gtidMode, &enforceGTID, &rowImage, &rowValueOptions,
	)
	if err != nil {
		// binlog_row_value_options is absent on older servers; retry without
		// it rather than rejecting an otherwise usable server.
		if qerr := db.QueryRowContext(ctx, serverPreflightSQLNoPartialJSON).Scan(
			&logBin, &binlogFormat, &gtidMode, &enforceGTID, &rowImage,
		); qerr != nil {
			return "", fmt.Errorf("mysql: server preflight: %w", qerr)
		}
		rowValueOptions = ""
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
		return "", fmt.Errorf("mysql: binlog_row_image is %q, not FULL — a row is carried as its after-image, so a partial image drops columns; set binlog_row_image=FULL", rowImage)
	}
	if strings.Contains(strings.ToUpper(rowValueOptions), "PARTIAL_JSON") {
		return "", fmt.Errorf("mysql: binlog_row_value_options is %q and contains PARTIAL_JSON — a partial JSON after-image cannot be decoded safely; remove PARTIAL_JSON", rowValueOptions)
	}
	return "", nil
}
