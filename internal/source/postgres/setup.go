package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// slotNameRe matches the server's slot-name rules: letters (any case),
// digits, underscore, up to 63 characters.
var slotNameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// publicationFor derives the publication name from the slot name: one
// publication per pipeline slot.
func publicationFor(slotName string) string { return slotName + "_pub" }

// EnsureSetup makes the server side ready, idempotently:
//
//   - REPLICA IDENTITY FULL on every replicated table, so updates and
//     deletes carry the full old row (parity with MySQL's row_image=FULL);
//   - the logical publication listing exactly the pipeline's tables;
//   - the logical decoding slot (pgoutput).
//
// The slot is the consistency anchor: created before the snapshot starts,
// it guarantees no transaction between slot creation and the stream start
// is ever lost.
func EnsureSetup(ctx context.Context, db *sql.DB, slotName string, tables []source.TableRef) error {
	if !slotNameRe.MatchString(slotName) {
		return fmt.Errorf("postgres: slot name %q must match %s", slotName, slotNameRe)
	}
	if len(tables) == 0 {
		return fmt.Errorf("postgres: ensure: no tables")
	}

	for _, ref := range tables {
		schema, table, ok := strings.Cut(ref.Source, ".")
		if !ok {
			return fmt.Errorf("postgres: ensure: source %q must be schema.table", ref.Source)
		}
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf(`ALTER TABLE %s.%s REPLICA IDENTITY FULL`,
				quoteIdent(schema), quoteIdent(table))); err != nil {
			return fmt.Errorf("postgres: replica identity %s: %w", ref.Source, err)
		}
	}

	pub := publicationFor(slotName)
	var exists bool
	if err := db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = $1)`, pub,
	).Scan(&exists); err != nil {
		return fmt.Errorf("postgres: publication lookup: %w", err)
	}
	if !exists {
		defs := make([]string, 0, len(tables))
		for _, ref := range tables {
			schema, table, _ := strings.Cut(ref.Source, ".")
			defs = append(defs, quoteIdent(schema)+"."+quoteIdent(table))
		}
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf(`CREATE PUBLICATION %s FOR TABLE %s`, quoteIdent(pub), strings.Join(defs, ", "))); err != nil {
			return fmt.Errorf("postgres: create publication: %w", err)
		}
	} else if err := syncPublication(ctx, db, pub, tables); err != nil {
		return err
	}

	var slotExists bool
	if err := db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1)`, slotName,
	).Scan(&slotExists); err != nil {
		return fmt.Errorf("postgres: slot lookup: %w", err)
	}
	if !slotExists {
		if _, err := db.ExecContext(ctx,
			`SELECT pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')`, slotName); err != nil {
			return fmt.Errorf("postgres: create slot: %w", err)
		}
	}
	return nil
}

// syncPublication aligns an existing publication's table set with the
// pipeline: adds missing members, drops extra ones. All ALTER PUBLICATION
// statements run inside a single transaction so a failure leaves the
// publication in its previous state.
//
// The members come from pg_publication_rel (the tables added DIRECTLY), not
// pg_publication_tables: that view expands a partitioned parent into its
// partitions, so a publication of a partitioned table would look like it held
// the leaf tables and syncPublication would try to DROP them (which fails —
// they were never added directly).
func syncPublication(ctx context.Context, db *sql.DB, pub string, tables []source.TableRef) error {
	rows, err := db.QueryContext(ctx, `
		SELECT n.nspname, c.relname
		FROM pg_catalog.pg_publication_rel pr
		JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
		WHERE p.pubname = $1`, pub)
	if err != nil {
		return fmt.Errorf("postgres: publication members: %w", err)
	}
	members := map[string]bool{}
	for rows.Next() {
		var s, t string
		if err := rows.Scan(&s, &t); err != nil {
			_ = rows.Close()
			return err
		}
		members[s+"."+t] = true
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()

	want := map[string]bool{}
	for _, ref := range tables {
		want[ref.Source] = true
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("postgres: publication tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for src := range members {
		if !want[src] {
			schema, table, _ := strings.Cut(src, ".")
			if _, err := tx.ExecContext(ctx, fmt.Sprintf(
				`ALTER PUBLICATION %s DROP TABLE %s.%s`,
				quoteIdent(pub), quoteIdent(schema), quoteIdent(table))); err != nil {
				return fmt.Errorf("postgres: publication drop %s: %w", src, err)
			}
		}
	}
	for src := range want {
		if !members[src] {
			schema, table, _ := strings.Cut(src, ".")
			if _, err := tx.ExecContext(ctx, fmt.Sprintf(
				`ALTER PUBLICATION %s ADD TABLE %s.%s`,
				quoteIdent(pub), quoteIdent(schema), quoteIdent(table))); err != nil {
				return fmt.Errorf("postgres: publication add %s: %w", src, err)
			}
		}
	}
	return tx.Commit()
}

// ConfirmedLSN reads the slot's confirmed_flush_lsn: the stream start for
// a first boot (no committed resume position).
func ConfirmedLSN(ctx context.Context, db *sql.DB, slotName string) (*position.LSN, error) {
	var raw string
	err := db.QueryRowContext(ctx, `
		SELECT confirmed_flush_lsn::text FROM pg_catalog.pg_replication_slots WHERE slot_name = $1`,
		slotName).Scan(&raw)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("postgres: slot %q does not exist", slotName)
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: confirmed lsn: %w", err)
	}
	return position.ParseLSN(raw)
}

// ValidateSlotState reconciles a stored resume LSN with the slot's
// confirmed_flush_lsn and returns the LSN the stream should start from (#156):
//
//   - resume == nil (first boot): start from the slot's confirmed point.
//   - resume == confirmed: no drift.
//   - resume < confirmed: the slot is ahead of the sink. The sink's stored
//     point is behind what the server already confirmed (and may have
//     recycled), so the slot's point is authoritative — warn and use it.
//   - resume > confirmed: the sink durably committed past the slot's last
//     confirmed_flush_lsn (the server learns the applied position from a later
//     standby-status update, so a crash can leave the slot behind). This is
//     recoverable: the WAL from resume onward is still retained because the
//     slot never confirmed past it, so advance the slot to the sink's point
//     rather than forcing a re-snapshot.
func ValidateSlotState(ctx context.Context, db *sql.DB, slotName string, resume *position.LSN, logger *slog.Logger) (*position.LSN, error) {
	confirmed, err := ConfirmedLSN(ctx, db, slotName)
	if err != nil {
		return nil, err
	}
	if resume == nil {
		return confirmed, nil
	}
	switch resume.Compare(confirmed) {
	case 0:
		return confirmed, nil
	case -1:
		if logger != nil {
			logger.Warn("postgres: stored position is behind the slot's confirmed_flush_lsn; resuming from the slot",
				"slot", slotName, "stored", resume.String(), "confirmed", confirmed.String())
		}
		return confirmed, nil
	default:
		if logger != nil {
			logger.Warn("postgres: stored position is ahead of the slot's confirmed_flush_lsn; advancing the slot to the committed sink position",
				"slot", slotName, "stored", resume.String(), "confirmed", confirmed.String())
		}
		return resume, nil
	}
}

// AdvanceSlot advances the logical slot's confirmed_flush_lsn to target,
// letting the server recycle WAL the sink has already committed (#156). It is
// idempotent and must run while the slot is inactive — before START_REPLICATION
// attaches the walsender.
func AdvanceSlot(ctx context.Context, db *sql.DB, slotName string, target position.LSN) error {
	if _, err := db.ExecContext(ctx,
		`SELECT pg_catalog.pg_replication_slot_advance($1, $2::pg_lsn)`, slotName, target.String()); err != nil {
		return fmt.Errorf("postgres: advance slot %q to %s: %w", slotName, target.String(), err)
	}
	return nil
}
