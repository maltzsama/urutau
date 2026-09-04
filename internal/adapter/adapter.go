// Package adapter bundles the per-source pieces the pipeline drivers need:
// query access, introspection, chunking, the replication reader, and the
// position codec. The collapsed runner and the coordinator consume the same
// Source; new sources plug in here, everything downstream is shared.
package adapter

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/core"
	"github.com/maltzsama/urutau/internal/position"
	icebergsink "github.com/maltzsama/urutau/internal/sink/iceberg"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/internal/source/kafka"
	"github.com/maltzsama/urutau/internal/source/mysql"
	"github.com/maltzsama/urutau/internal/source/postgres"
	srctypes "github.com/maltzsama/urutau/internal/source/types"
	"github.com/maltzsama/urutau/internal/spec"
)

// Runtime carries the replication knobs a driver passes through to the
// source reader.
type Runtime = srctypes.Runtime

// Capabilities declares what a source supports.
type Capabilities = srctypes.Capabilities

// StreamSource is the replication reader surface a driver drives.
type StreamSource = srctypes.StreamSource

// Source is the per-source surface of the pipeline: everything that differs
// between MySQL and Postgres lives behind it.
type Source interface {
	// Caps returns the source's capabilities. The runner uses this to
	// decide which pipeline phases to run.
	Caps() Capabilities
	// OpenQuery opens the SQL connection for chunk SELECTs, schema
	// introspection, and position queries.
	OpenQuery(ctx context.Context) (*sql.DB, error)
	// Introspect resolves one spec table into its ref, the canonical schema
	// with the declared cast and metadata columns applied, and the final
	// Iceberg schema built from it. The warnings carry the advisory cast
	// outcomes.
	Introspect(ctx context.Context, db *sql.DB, t spec.Table) (core.TableRef, core.Schema, *iceberg.Schema, []core.Warning, error)
	// NewChunker builds the chunk SELECT source for one table.
	NewChunker(db *sql.DB, source, pk string, chunkSize int) (snapshot.ChunkSource, error)
	// NewReader builds the replication reader over the pipeline's tables.
	NewReader(ctx context.Context, db *sql.DB, refs []snapshot.TableRef, out chan<- change.Change) (StreamSource, error)
	// InitialPosition is the stream start for a first boot (no resume).
	InitialPosition(ctx context.Context, db *sql.DB) (position.Position, error)
	// ParsePosition decodes a stored cdc.position string.
	ParsePosition(s string) (position.Position, error)
}

// For resolves the source adapter for a spec's source kind.
func For(s *spec.Spec, rt Runtime) (Source, error) {
	switch s.Source.Kind {
	case "mysql":
		return mysqlSource{spec: s, rt: rt}, nil
	case "postgres":
		return postgresSource{spec: s, rt: rt}, nil
	case "kafka":
		return kafka.Source{Spec: s, Rt: rt}, nil
	default:
		return nil, fmt.Errorf("adapter: unsupported source kind %q", s.Source.Kind)
	}
}

// CapsForKind returns a source kind's capabilities without a full spec.
// Caps never reads the spec, so the zero adapters are safe here. Used by
// admission validation, which must check resource ceilings before boot.
func CapsForKind(kind string) (Capabilities, error) {
	switch kind {
	case "mysql":
		return mysqlSource{}.Caps(), nil
	case "postgres":
		return postgresSource{}.Caps(), nil
	case "kafka":
		return kafka.Source{}.Caps(), nil
	default:
		return Capabilities{}, fmt.Errorf("adapter: unsupported source kind %q", kind)
	}
}

// OpenQueryDB opens a query connection for a source kind and URI — the same
// surface the per-source OpenQuery uses, exposed for remote workers that run
// snapshot chunk SELECTs themselves (design §11.1).
func OpenQueryDB(kind, uri string) (*sql.DB, error) {
	switch kind {
	case "mysql":
		conn, err := mysql.ParseURI(uri)
		if err != nil {
			return nil, err
		}
		return sql.Open("mysql", conn.QueryDSN())
	case "postgres":
		return sql.Open("pgx", uri)
	default:
		return nil, fmt.Errorf("adapter: OpenQueryDB: unknown source kind %q", kind)
	}
}

// ── MySQL ────────────────────────────────────────────────────────────

type mysqlSource struct {
	spec *spec.Spec
	rt   Runtime
}

func (a mysqlSource) Caps() Capabilities {
	return Capabilities{
		Snapshot:          true,
		ChunkQuery:        true,
		Stream:            true,
		MaxConnections:    10,
		Modes:             []srctypes.Mode{srctypes.ModeCDC},
		BeforeImage:       true,  // binlog row format always carries the deleted row
		MonotonicSequence: false, // GTID sets grow but are not per-message coordinates
	}
}

func (a mysqlSource) OpenQuery(ctx context.Context) (*sql.DB, error) {
	conn, err := mysql.ParseURI(a.spec.Source.URI)
	if err != nil {
		return nil, err
	}
	return sql.Open("mysql", conn.QueryDSN())
}

func (a mysqlSource) Introspect(ctx context.Context, db *sql.DB, t spec.Table) (core.TableRef, core.Schema, *iceberg.Schema, []core.Warning, error) {
	schemaName, tableName, ok := strings.Cut(t.Source, ".")
	if !ok {
		return snapshot.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("adapter: source %q must be db.table", t.Source)
	}
	st, err := mysql.QueryTable(ctx, db, schemaName, tableName)
	if err != nil {
		return snapshot.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("adapter: introspect %s: %w", t.Source, err)
	}
	pk := t.PrimaryKey
	if len(pk) == 0 && len(st.PKColumns) > 0 {
		for _, idx := range st.PKColumns {
			pk = append(pk, st.Columns[idx].Name)
		}
	}
	cs, err := mysql.CanonicalSchema(st)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("adapter: schema %s: %w", t.Source, err)
	}
	cast, err := core.ParseCastPolicy(t.Cast)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("adapter: schema %s: %w", t.Source, err)
	}
	resolved, warns, err := core.ResolveSchema(cs, cast, t.Metadata)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("adapter: schema %s: %w", t.Source, err)
	}
	is, err := icebergsink.FromCanonical(resolved)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("adapter: schema %s: %w", t.Source, err)
	}
	return core.TableRef{Source: t.Source, Target: t.Target, PrimaryKey: pk}, resolved, is, warns, nil
}

func (a mysqlSource) NewChunker(db *sql.DB, source, pk string, chunkSize int) (snapshot.ChunkSource, error) {
	return mysql.NewChunker(db, source, pk, chunkSize)
}

func (a mysqlSource) NewReader(ctx context.Context, db *sql.DB, refs []snapshot.TableRef, out chan<- change.Change) (StreamSource, error) {
	conn, err := mysql.ParseURI(a.spec.Source.URI)
	if err != nil {
		return nil, err
	}
	rdr, err := mysql.New(ctx, mysql.Config{
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
	return mysqlStream{Reader: rdr}, nil
}

// InitialPosition returns the master's executed GTID set — the same query
// canal's GetMasterGTIDSet issues, without spinning a replication handle.
func (a mysqlSource) InitialPosition(ctx context.Context, db *sql.DB) (position.Position, error) {
	var gtid string
	if err := db.QueryRowContext(ctx, `SELECT @@GLOBAL.gtid_executed`).Scan(&gtid); err != nil {
		return nil, fmt.Errorf("mysql: gtid_executed: %w", err)
	}
	return position.ParseGTID(gtid)
}

// ParsePosition decodes a stored cdc.position as a GTID set.
func (a mysqlSource) ParsePosition(s string) (position.Position, error) {
	return position.ParseGTID(s)
}

// mysqlStream adapts the concrete canal reader to the StreamSource
// interface (its watermark surface already matches).
type mysqlStream struct {
	*mysql.Reader
}

// Start begins the stream at the given GTID set.
func (s mysqlStream) Start(ctx context.Context, at position.Position) error {
	g, ok := at.(*position.GTID)
	if !ok {
		return fmt.Errorf("mysql: start position must be a GTID set, got %T", at)
	}
	return s.StartFromGTID(ctx, g)
}

// ── PostgreSQL ───────────────────────────────────────────────────────

type postgresSource struct {
	spec *spec.Spec
	rt   Runtime
}

func (a postgresSource) Caps() Capabilities {
	return Capabilities{
		Snapshot:          true,
		ChunkQuery:        true,
		Stream:            true,
		MaxConnections:    10,
		Modes:             []srctypes.Mode{srctypes.ModeCDC},
		BeforeImage:       true,  // old tuple carries the deleted row (PK-only unless REPLICA IDENTITY FULL)
		MonotonicSequence: false, // commit LSNs are monotonic but not per-message coordinates
	}
}

func (a postgresSource) OpenQuery(ctx context.Context) (*sql.DB, error) {
	return sql.Open("pgx", a.spec.Source.URI)
}

func (a postgresSource) Introspect(ctx context.Context, db *sql.DB, t spec.Table) (core.TableRef, core.Schema, *iceberg.Schema, []core.Warning, error) {
	schemaName, tableName, ok := strings.Cut(t.Source, ".")
	if !ok {
		return snapshot.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("adapter: source %q must be db.table", t.Source)
	}
	st, err := postgres.QueryTable(ctx, db, schemaName, tableName)
	if err != nil {
		return snapshot.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("adapter: introspect %s: %w", t.Source, err)
	}
	pk := t.PrimaryKey
	if len(pk) == 0 && len(st.PKColumns) > 0 {
		for _, idx := range st.PKColumns {
			pk = append(pk, st.Columns[idx].Name)
		}
	}
	cs, err := postgres.CanonicalSchema(st)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("adapter: schema %s: %w", t.Source, err)
	}
	cast, err := core.ParseCastPolicy(t.Cast)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("adapter: schema %s: %w", t.Source, err)
	}
	resolved, warns, err := core.ResolveSchema(cs, cast, t.Metadata)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("adapter: schema %s: %w", t.Source, err)
	}
	is, err := icebergsink.FromCanonical(resolved)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("adapter: schema %s: %w", t.Source, err)
	}
	return core.TableRef{Source: t.Source, Target: t.Target, PrimaryKey: pk}, resolved, is, warns, nil
}

func (a postgresSource) NewChunker(db *sql.DB, source, pk string, chunkSize int) (snapshot.ChunkSource, error) {
	return postgres.NewChunker(db, source, pk, chunkSize)
}

func (a postgresSource) NewReader(ctx context.Context, db *sql.DB, refs []snapshot.TableRef, out chan<- change.Change) (StreamSource, error) {
	slot := a.spec.Source.SlotName
	if slot == "" {
		return nil, fmt.Errorf("adapter: postgres source requires slotName")
	}
	rdr, err := postgres.New(ctx, postgres.Config{
		URI:      a.spec.Source.URI,
		DB:       db,
		SlotName: slot,
		Tables:   refs,
		Logger:   a.rt.Logger,
	}, out)
	if err != nil {
		return nil, err
	}
	return pgStream{Reader: rdr}, nil
}

// InitialPosition returns the slot's confirmed LSN: a first boot starts
// streaming from the slot's consistency point — everything after it flows
// through the stream, everything before it belongs to the snapshot.
func (a postgresSource) InitialPosition(ctx context.Context, db *sql.DB) (position.Position, error) {
	return postgres.ConfirmedLSN(ctx, db, a.spec.Source.SlotName)
}

// ParsePosition decodes a stored cdc.position as an LSN.
func (a postgresSource) ParsePosition(s string) (position.Position, error) {
	return position.ParseLSN(s)
}

// pgStream adapts the concrete pgoutput reader to the StreamSource
// interface.
type pgStream struct {
	*postgres.Reader
}

// Start begins the stream at the given LSN.
func (s pgStream) Start(ctx context.Context, at position.Position) error {
	l, ok := at.(*position.LSN)
	if !ok {
		return fmt.Errorf("postgres: start position must be an LSN, got %T", at)
	}
	return s.StartFromLSN(ctx, l)
}
