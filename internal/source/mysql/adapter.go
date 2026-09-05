package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// Source adapts the MySQL canal reader to the source contract. It is the
// self-contained driver entry point: the adapter, its capabilities, and its
// query connection all live here, and init() registers the kind.
type Source struct {
	spec *spec.Spec
	rt   source.Runtime
	db   *sql.DB // the source's query connection (chunk SELECTs, introspection)
}

func capabilities() source.Capabilities {
	return source.Capabilities{
		Snapshot:          true,
		ChunkQuery:        true,
		Stream:            true,
		MaxConnections:    10,
		Modes:             []source.Mode{source.ModeCDC},
		BeforeImage:       true,  // binlog row format always carries the deleted row
		MonotonicSequence: false, // GTID sets grow but are not per-message coordinates
	}
}

func init() {
	driver.RegisterSource("mysql", capabilities(), func(s *spec.Spec, rt source.Runtime) (source.Source, error) {
		conn, err := ParseURI(s.Source.URI)
		if err != nil {
			return nil, err
		}
		db, err := sql.Open("mysql", conn.QueryDSN())
		if err != nil {
			return nil, err
		}
		return Source{spec: s, rt: rt, db: db}, nil
	})
}

var _ source.Source = Source{}
var _ source.QuerySource = Source{}

// Introspect resolves one spec table into its ref and canonical schema.
func (a Source) Introspect(ctx context.Context, t spec.Table) (core.TableRef, core.Schema, []core.Warning, error) {
	schemaName, tableName, ok := strings.Cut(t.Source, ".")
	if !ok {
		return source.TableRef{}, core.Schema{}, nil, fmt.Errorf("mysql: source %q must be db.table", t.Source)
	}
	st, err := QueryTable(ctx, a.db, schemaName, tableName)
	if err != nil {
		return source.TableRef{}, core.Schema{}, nil, fmt.Errorf("mysql: introspect %s: %w", t.Source, err)
	}
	pk := t.PrimaryKey
	if len(pk) == 0 && len(st.PKColumns) > 0 {
		for _, idx := range st.PKColumns {
			pk = append(pk, st.Columns[idx].Name)
		}
	}
	cs, err := CanonicalSchema(st)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, fmt.Errorf("mysql: schema %s: %w", t.Source, err)
	}
	cast, err := core.ParseCastPolicy(t.Cast)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, fmt.Errorf("mysql: schema %s: %w", t.Source, err)
	}
	resolved, warns, err := core.ResolveSchema(cs, cast, t.Metadata)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, fmt.Errorf("mysql: schema %s: %w", t.Source, err)
	}
	return core.TableRef{Source: t.Source, Target: t.Target, PrimaryKey: pk}, resolved, warns, nil
}

// NewChunker builds the chunk SELECT source for one table.
func (a Source) NewChunker(source, pk string, chunkSize int) (source.ChunkSource, error) {
	return NewChunker(a.db, source, pk, chunkSize)
}

// CloseQuery releases the query connection.
func (a Source) CloseQuery() error {
	if a.db != nil {
		return a.db.Close()
	}
	return nil
}

// Open builds the replication reader over the pipeline's tables.
func (a Source) Open(ctx context.Context, refs []source.TableRef) (source.Reader, error) {
	conn, err := ParseURI(a.spec.Source.URI)
	if err != nil {
		return nil, err
	}
	out := make(chan change.Change, 1024)
	rdr, err := New(ctx, Config{
		Addr:      conn.Addr(),
		User:      conn.User,
		Password:  conn.Password,
		ServerID:  a.rt.ServerID,
		Heartbeat: a.rt.Heartbeat,
		Tables:    refs,
		Logger:    a.rt.Logger,
	}, out)
	if err != nil {
		return nil, err
	}
	return stream{Reader: rdr, out: out}, nil
}

// InitialPosition returns the master's executed GTID set — the same query
// canal's GetMasterGTIDSet issues, without spinning a replication handle.
func (a Source) InitialPosition(ctx context.Context) (position.Position, error) {
	var gtid string
	if err := a.db.QueryRowContext(ctx, `SELECT @@GLOBAL.gtid_executed`).Scan(&gtid); err != nil {
		return nil, fmt.Errorf("mysql: gtid_executed: %w", err)
	}
	return position.ParseGTID(gtid)
}

// ParsePosition decodes a stored cdc.position as a GTID set.
func (a Source) ParsePosition(s string) (position.Position, error) {
	return position.ParseGTID(s)
}

// stream adapts the concrete canal reader to the Reader contract, carrying
// the change channel the concrete reader writes into.
type stream struct {
	*Reader
	out chan change.Change
}

// Stream begins the stream at the given GTID set.
func (s stream) Stream(ctx context.Context, from position.Position) (<-chan change.Change, <-chan error) {
	errCh := make(chan error, 1)
	g, ok := from.(*position.GTID)
	if !ok {
		errCh <- fmt.Errorf("mysql: start position must be a GTID set, got %T", from)
		return s.out, errCh
	}
	go func() { errCh <- s.StartFromGTID(ctx, g) }()
	return s.out, errCh
}
