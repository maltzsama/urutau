package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/maltzsama/urutau/internal/position"
	"github.com/maltzsama/urutau/internal/source/dblog"
)

// slotNameRe matches the server's slot-name rules: lowercase letters,
// digits, underscore.
var slotNameRe = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

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
func EnsureSetup(ctx context.Context, db *sql.DB, slotName string, tables []dblog.TableRef) error {
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
// pipeline: adds missing members, drops extra ones.
func syncPublication(ctx context.Context, db *sql.DB, pub string, tables []dblog.TableRef) error {
	rows, err := db.QueryContext(ctx, `
		SELECT schemaname, tablename FROM pg_catalog.pg_publication_tables WHERE pubname = $1`, pub)
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
	for src := range members {
		if !want[src] {
			schema, table, _ := strings.Cut(src, ".")
			if _, err := db.ExecContext(ctx, fmt.Sprintf(
				`ALTER PUBLICATION %s DROP TABLE %s.%s`,
				quoteIdent(pub), quoteIdent(schema), quoteIdent(table))); err != nil {
				return fmt.Errorf("postgres: publication drop %s: %w", src, err)
			}
		}
	}
	for src := range want {
		if !members[src] {
			schema, table, _ := strings.Cut(src, ".")
			if _, err := db.ExecContext(ctx, fmt.Sprintf(
				`ALTER PUBLICATION %s ADD TABLE %s.%s`,
				quoteIdent(pub), quoteIdent(schema), quoteIdent(table))); err != nil {
				return fmt.Errorf("postgres: publication add %s: %w", src, err)
			}
		}
	}
	return nil
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
