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
// self-contained driver entry point: the adapter and its capabilities live
// here, and init() registers the kind with the driver registry.
type Source struct {
	spec *spec.Spec
	rt   source.Runtime
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
		return Source{spec: s, rt: rt}, nil
	})
}

var _ source.Source = Source{}
var _ source.QuerySource = Source{}

// OpenQuery opens the SQL connection for chunk SELECTs and introspection.
func (a Source) OpenQuery(ctx context.Context) (*sql.DB, error) {
	conn, err := ParseURI(a.spec.Source.URI)
	if err != nil {
		return nil, err
	}
	return sql.Open("mysql", conn.QueryDSN())
}

// Introspect resolves one spec table into its ref and canonical schema.
func (a Source) Introspect(ctx context.Context, db *sql.DB, t spec.Table) (core.TableRef, core.Schema, []core.Warning, error) {
	schemaName, tableName, ok := strings.Cut(t.Source, ".")
	if !ok {
		return source.TableRef{}, core.Schema{}, nil, fmt.Errorf("mysql: source %q must be db.table", t.Source)
	}
	st, err := QueryTable(ctx, db, schemaName, tableName)
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
func (a Source) NewChunker(db *sql.DB, source, pk string, chunkSize int) (source.ChunkSource, error) {
	return NewChunker(db, source, pk, chunkSize)
}

// NewReader builds the replication reader over the pipeline's tables.
func (a Source) NewReader(ctx context.Context, db *sql.DB, refs []source.TableRef, out chan<- change.Change) (source.StreamSource, error) {
	conn, err := ParseURI(a.spec.Source.URI)
	if err != nil {
		return nil, err
	}
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
	return stream{Reader: rdr}, nil
}

// InitialPosition returns the master's executed GTID set — the same query
// canal's GetMasterGTIDSet issues, without spinning a replication handle.
func (a Source) InitialPosition(ctx context.Context, db *sql.DB) (position.Position, error) {
	var gtid string
	if err := db.QueryRowContext(ctx, `SELECT @@GLOBAL.gtid_executed`).Scan(&gtid); err != nil {
		return nil, fmt.Errorf("mysql: gtid_executed: %w", err)
	}
	return position.ParseGTID(gtid)
}

// ParsePosition decodes a stored cdc.position as a GTID set.
func (a Source) ParsePosition(s string) (position.Position, error) {
	return position.ParseGTID(s)
}

// stream adapts the concrete canal reader to StreamSource.
type stream struct {
	*Reader
}

// Start begins the stream at the given GTID set.
func (s stream) Start(ctx context.Context, at position.Position) error {
	g, ok := at.(*position.GTID)
	if !ok {
		return fmt.Errorf("mysql: start position must be a GTID set, got %T", at)
	}
	return s.StartFromGTID(ctx, g)
}
