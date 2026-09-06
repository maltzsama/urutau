package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/sink"
)

// Sink implements the sink contract on ClickHouse. It owns the native
// connection; the orchestration never touches clickhouse-go types.
type Sink struct {
	conn ch.Conn
	ns   string // default database for bare targets
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
	s := &Sink{conn: conn, ns: opt.Auth.Database}
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
func (s *Sink) EnsureTable(ctx context.Context, ref core.TableRef, schema core.Schema, partitionBy []string, _ core.CastPolicy, mode change.WriteMode) error {
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

// Position reads the committed CDC position: the commit coordinate on the
// row with the highest seq. An absent or empty table means never written.
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
	var pos sql.NullString
	if err := s.conn.QueryRow(ctx, "SELECT argMax(position, seq) FROM "+ident.quoted()).Scan(&pos); err != nil {
		return "", fmt.Errorf("position %s: %w", ident.quoted(), err)
	}
	return pos.String, nil
}

// Close releases the connection pool.
func (s *Sink) Close() error { return s.conn.Close() }

func init() {
	driver.RegisterSink("clickhouse", func(ctx context.Context, cfg sink.Config) (sink.Sink, error) {
		return Open(ctx, cfg)
	})
}
