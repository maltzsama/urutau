package drivers

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/adapter"
	icebergsink "github.com/maltzsama/urutau/internal/sink/iceberg"
	"github.com/maltzsama/urutau/internal/source/kafka"
	"github.com/maltzsama/urutau/internal/source/mysql"
	"github.com/maltzsama/urutau/internal/source/postgres"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// forSource resolves the source adapter for a spec's source kind. The switch
// is the only place concrete sources are named — the assembly point drivers
// owns by design.
func forSource(s *spec.Spec, rt adapter.Runtime) (adapter.Source, error) {
	switch s.Source.Kind {
	case "mysql":
		return mysqlSource{spec: s, rt: rt}, nil
	case "postgres":
		return postgresSource{spec: s, rt: rt}, nil
	case "kafka":
		return kafka.Source{Spec: s, Rt: rt}, nil
	default:
		return nil, fmt.Errorf("drivers: unsupported source kind %q", s.Source.Kind)
	}
}

// capsForKind returns a source kind's capabilities without a full spec.
// Caps never reads the spec, so the zero adapters are safe here. Used by
// admission validation, which must check resource ceilings before boot.
func capsForKind(kind string) (adapter.Capabilities, error) {
	switch kind {
	case "mysql":
		return mysqlSource{}.Caps(), nil
	case "postgres":
		return postgresSource{}.Caps(), nil
	case "kafka":
		return kafka.Source{}.Caps(), nil
	default:
		return adapter.Capabilities{}, fmt.Errorf("drivers: unsupported source kind %q", kind)
	}
}

// openQueryDB opens a query connection for a source kind and URI — the same
// surface the per-source OpenQuery uses, exposed for remote workers that run
// snapshot chunk SELECTs themselves (design §11.1).
func openQueryDB(kind, uri string) (*sql.DB, error) {
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
		return nil, fmt.Errorf("drivers: OpenQueryDB: unknown source kind %q", kind)
	}
}

// ── MySQL ────────────────────────────────────────────────────────────

type mysqlSource struct {
	spec *spec.Spec
	rt   adapter.Runtime
}

func (a mysqlSource) Caps() adapter.Capabilities {
	return adapter.Capabilities{
		Snapshot:          true,
		ChunkQuery:        true,
		Stream:            true,
		MaxConnections:    10,
		Modes:             []source.Mode{source.ModeCDC},
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
		return source.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("drivers: source %q must be db.table", t.Source)
	}
	st, err := mysql.QueryTable(ctx, db, schemaName, tableName)
	if err != nil {
		return source.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("drivers: introspect %s: %w", t.Source, err)
	}
	pk := t.PrimaryKey
	if len(pk) == 0 && len(st.PKColumns) > 0 {
		for _, idx := range st.PKColumns {
			pk = append(pk, st.Columns[idx].Name)
		}
	}
	cs, err := mysql.CanonicalSchema(st)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("drivers: schema %s: %w", t.Source, err)
	}
	cast, err := core.ParseCastPolicy(t.Cast)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("drivers: schema %s: %w", t.Source, err)
	}
	resolved, warns, err := core.ResolveSchema(cs, cast, t.Metadata)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("drivers: schema %s: %w", t.Source, err)
	}
	is, err := icebergsink.FromCanonical(resolved)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("drivers: schema %s: %w", t.Source, err)
	}
	return core.TableRef{Source: t.Source, Target: t.Target, PrimaryKey: pk}, resolved, is, warns, nil
}

func (a mysqlSource) NewChunker(db *sql.DB, source, pk string, chunkSize int) (source.ChunkSource, error) {
	return mysql.NewChunker(db, source, pk, chunkSize)
}

func (a mysqlSource) NewReader(ctx context.Context, db *sql.DB, refs []source.TableRef, out chan<- change.Change) (adapter.StreamSource, error) {
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
	rt   adapter.Runtime
}

func (a postgresSource) Caps() adapter.Capabilities {
	return adapter.Capabilities{
		Snapshot:          true,
		ChunkQuery:        true,
		Stream:            true,
		MaxConnections:    10,
		Modes:             []source.Mode{source.ModeCDC},
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
		return source.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("drivers: source %q must be db.table", t.Source)
	}
	st, err := postgres.QueryTable(ctx, db, schemaName, tableName)
	if err != nil {
		return source.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("drivers: introspect %s: %w", t.Source, err)
	}
	pk := t.PrimaryKey
	if len(pk) == 0 && len(st.PKColumns) > 0 {
		for _, idx := range st.PKColumns {
			pk = append(pk, st.Columns[idx].Name)
		}
	}
	cs, err := postgres.CanonicalSchema(st)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("drivers: schema %s: %w", t.Source, err)
	}
	cast, err := core.ParseCastPolicy(t.Cast)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("drivers: schema %s: %w", t.Source, err)
	}
	resolved, warns, err := core.ResolveSchema(cs, cast, t.Metadata)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("drivers: schema %s: %w", t.Source, err)
	}
	is, err := icebergsink.FromCanonical(resolved)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, nil, fmt.Errorf("drivers: schema %s: %w", t.Source, err)
	}
	return core.TableRef{Source: t.Source, Target: t.Target, PrimaryKey: pk}, resolved, is, warns, nil
}

func (a postgresSource) NewChunker(db *sql.DB, source, pk string, chunkSize int) (source.ChunkSource, error) {
	return postgres.NewChunker(db, source, pk, chunkSize)
}

func (a postgresSource) NewReader(ctx context.Context, db *sql.DB, refs []source.TableRef, out chan<- change.Change) (adapter.StreamSource, error) {
	slot := a.spec.Source.SlotName
	if slot == "" {
		return nil, fmt.Errorf("drivers: postgres source requires slotName")
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
