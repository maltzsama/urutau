package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/sink"
)

// Sink implements the sink contract on ClickHouse. It owns the native
// connection; the orchestration never touches clickhouse-go types.
type Sink struct {
	conn ch.Conn
	ns   string // default database for bare targets
	// sourceKind decodes the opaque position strings stored in the
	// per-partition control table (WK-001 C7). A hint from the spec, not a
	// coupling — see sink.Config.SourceKind.
	sourceKind string
}

// Open dials the native port. The DSN is the connection truth
// (clickhouse://host:port?user=…&password=…); the sink namespace is the
// fallback database, created if absent.
func Open(ctx context.Context, cfg sink.Config) (*Sink, error) {
	if cfg.URI == "" {
		return nil, errors.New("clickhouse: sink.uri is required")
	}
	opt, err := ch.ParseDSN(cfg.URI)
	if err != nil {
		if strings.HasPrefix(cfg.URI, "clickhouse://") {
			return nil, fmt.Errorf("clickhouse: parse dsn %q: %w", cfg.URI, err)
		}
		// A bare host:port is accepted as the native address.
		opt = &ch.Options{Addr: []string{cfg.URI}}
	}
	if opt.DialTimeout == 0 {
		opt.DialTimeout = 10 * time.Second
	}
	if opt.Auth.Username == "" {
		opt.Auth.Username = "default"
	}
	if opt.Auth.Database == "" {
		opt.Auth.Database = cfg.Namespace
	}
	conn, err := ch.Open(opt)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: open: %w", err)
	}
	s := &Sink{conn: conn, ns: opt.Auth.Database, sourceKind: cfg.SourceKind}
	if s.ns != "" {
		if err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(s.ns)); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("clickhouse: ensure database %q: %w", s.ns, err)
		}
	}
	if err := s.ensureProgressTable(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return s, nil
}

// ident resolves a target table name into database + name, falling back to
// the namespace for bare names.
func (s *Sink) ident(target string) (tableIdent, error) {
	if db, name, ok := strings.Cut(target, "."); ok {
		return tableIdent{db: db, table: name}, nil
	}
	if s.ns == "" {
		return tableIdent{}, fmt.Errorf("target %q: %w", target, errUnqualified)
	}
	return tableIdent{db: s.ns, table: target}, nil
}

// EnsureTable creates the target table if absent. The DDL is chosen by the
// write mode (see buildDDL); an existing table is left untouched — schema
// drift surfaces as a write error and pauses the pipeline, per the
// pause-and-alert policy. The cast plan is already baked into the resolved
// schema's types, so there is nothing to re-derive here.
func (s *Sink) EnsureTable(ctx context.Context, ref core.TableRef, schema core.Schema, partitionBy []string, _ core.CastPolicy, mode dataplane.WriteMode) error {
	ident, err := s.ident(ref.Target)
	if err != nil {
		return err
	}
	ddl, err := buildDDL(ident, ref, schema, partitionBy, mode)
	if err != nil {
		return err
	}
	if err := s.conn.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("ensure %s: %w", ident.quoted(), err)
	}
	// The per-partition position table (WK-001 C7). One row per worker
	// group; created alongside the data table so Position() can read the
	// MinSafe across partitions. Idempotent, like the data DDL above.
	posDDL := "CREATE TABLE IF NOT EXISTS " + ident.posQuoted() +
		" (owner String, position String, seq UInt64) " +
		"ENGINE = ReplacingMergeTree(seq) ORDER BY owner"
	if err := s.conn.Exec(ctx, posDDL); err != nil {
		return fmt.Errorf("ensure %s: %w", ident.posQuoted(), err)
	}
	return nil
}

// Writer opens the per-table committer.
func (s *Sink) Writer(ctx context.Context, ref core.TableRef, cast core.CastPolicy, meta []core.MetadataColumn) (sink.TableWriter, error) {
	ident, err := s.ident(ref.Target)
	if err != nil {
		return nil, err
	}
	return openTableWriter(ctx, s.conn, ident, ref, cast, meta, ref.Source, nil)
}

// Position reads the committed CDC position. When the per-partition control
// table exists (WK-001 C7) it is the MinSafe across partitions — a lagging
// partition must never be resumed past. Otherwise (a table written before
// C7, or the collapsed runner) it falls back to the legacy argMax over the
// data rows. An absent or empty table means never written.
func (s *Sink) Position(ctx context.Context, ref core.TableRef) (string, error) {
	ident, err := s.ident(ref.Target)
	if err != nil {
		return "", err
	}
	var exists uint8
	if err := s.conn.QueryRow(ctx, "EXISTS TABLE "+ident.quoted()).Scan(&exists); err != nil {
		return "", fmt.Errorf("exists %s: %w", ident.quoted(), err)
	}
	if exists == 0 {
		return "", nil
	}
	if pos, ok, err := s.partitionPosition(ctx, ident); err != nil {
		return "", err
	} else if ok {
		return pos, nil
	}
	var pos sql.NullString
	if err := s.conn.QueryRow(ctx, "SELECT argMax(position, seq) FROM "+ident.quoted()).Scan(&pos); err != nil {
		return "", fmt.Errorf("position %s: %w", ident.quoted(), err)
	}
	return pos.String, nil
}

// partitionPosition reads the latest position per owner from the control
// table and returns their MinSafe. ok is false when the table is absent or
// has no rows (a pre-C7 table), so the caller falls back to the legacy read.
func (s *Sink) partitionPosition(ctx context.Context, ident tableIdent) (pos string, ok bool, err error) {
	var exists uint8
	if err := s.conn.QueryRow(ctx, "EXISTS TABLE "+ident.posQuoted()).Scan(&exists); err != nil {
		return "", false, fmt.Errorf("exists %s: %w", ident.posQuoted(), err)
	}
	if exists == 0 {
		return "", false, nil
	}
	rows, err := s.conn.Query(ctx,
		"SELECT owner, argMax(position, seq) FROM "+ident.posQuoted()+" GROUP BY owner")
	if err != nil {
		return "", false, fmt.Errorf("positions %s: %w", ident.posQuoted(), err)
	}
	defer func() { _ = rows.Close() }()
	var positions []string
	for rows.Next() {
		var owner string
		var p sql.NullString
		if err := rows.Scan(&owner, &p); err != nil {
			return "", false, err
		}
		if !p.Valid {
			continue
		}
		positions = append(positions, p.String)
	}
	if err := rows.Err(); err != nil {
		return "", false, err
	}
	if len(positions) == 0 {
		return "", false, nil
	}
	pos, err = minSafePosition(s.sourceKind, positions)
	if err != nil {
		return "", false, fmt.Errorf("positions %s: %w", ident.posQuoted(), err)
	}
	return pos, true, nil
}

// minSafePosition parses one committed position string per owner and returns
// their MinSafe (WK-001 C7). It is the pure core of partitionPosition: the
// per-partition minimum is what a resume must use, never one partition's
// (argMax) or the last writer's.
func minSafePosition(sourceKind string, positions []string) (string, error) {
	parsed := make([]position.Position, 0, len(positions))
	for _, p := range positions {
		pp, err := position.Parse(sourceKind, p)
		if err != nil {
			return "", err
		}
		parsed = append(parsed, pp)
	}
	best, err := position.MinSafe(parsed)
	if err != nil {
		return "", err
	}
	if best == nil {
		return "", nil
	}
	return best.String(), nil
}

// Close releases the connection pool.
func (s *Sink) Close() error { return s.conn.Close() }

// SupportsConcurrentWriters reports whether N workers may commit to one
// table. True since WK-001 C7: the ReplacingMergeTree version column is the
// coordinator's shared sequence (C3), and the durable position is kept per
// partition and read as a MinSafe (Position), so N workers neither fight
// over the version nor resume past a lagging partition.
func (s *Sink) SupportsConcurrentWriters() bool { return true }

func init() {
	factory := func(ctx context.Context, cfg sink.Config) (sink.Sink, error) {
		return Open(ctx, cfg)
	}
	if err := driver.RegisterSink("clickhouse", factory); err != nil {
		panic(err)
	}
}
