package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// Source adapts the Postgres pgoutput reader to the source contract. It is
// the self-contained driver entry point; init() registers the kind.
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
		BeforeImage:       true,  // old tuple carries the deleted row (PK-only unless REPLICA IDENTITY FULL)
		MonotonicSequence: false, // commit LSNs are monotonic but not per-message coordinates
	}
}

func init() {
	driver.RegisterSource("postgres", capabilities(), func(s *spec.Spec, rt source.Runtime) (source.Source, error) {
		db, err := sql.Open("pgx", s.Source.URI)
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
		return source.TableRef{}, core.Schema{}, nil, fmt.Errorf("postgres: source %q must be db.table", t.Source)
	}
	st, err := QueryTable(ctx, a.db, schemaName, tableName)
	if err != nil {
		return source.TableRef{}, core.Schema{}, nil, fmt.Errorf("postgres: introspect %s: %w", t.Source, err)
	}
	pk := t.PrimaryKey
	if len(pk) == 0 && len(st.PKColumns) > 0 {
		for _, idx := range st.PKColumns {
			pk = append(pk, st.Columns[idx].Name)
		}
	}
	cs, err := CanonicalSchema(st)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, fmt.Errorf("postgres: schema %s: %w", t.Source, err)
	}
	cast, err := core.ParseCastPolicy(t.Cast)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, fmt.Errorf("postgres: schema %s: %w", t.Source, err)
	}
	resolved, warns, err := core.ResolveSchema(cs, cast, t.Metadata)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, fmt.Errorf("postgres: schema %s: %w", t.Source, err)
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
	slot := a.spec.Source.SlotName
	if slot == "" {
		return nil, fmt.Errorf("postgres: source requires slotName")
	}
	out := make(chan change.Change, 1024)
	rdr, err := New(ctx, Config{
		URI:      a.spec.Source.URI,
		DB:       a.db,
		SlotName: slot,
		Tables:   refs,
		Logger:   a.rt.Logger,
	}, out)
	if err != nil {
		return nil, err
	}
	return stream{Reader: rdr, out: out}, nil
}

// InitialPosition returns the slot's confirmed LSN: a first boot starts
// streaming from the slot's consistency point — everything after it flows
// through the stream, everything before it belongs to the snapshot.
func (a Source) InitialPosition(ctx context.Context) (position.Position, error) {
	return ConfirmedLSN(ctx, a.db, a.spec.Source.SlotName)
}

// ParsePosition decodes a stored cdc.position as an LSN.
func (a Source) ParsePosition(s string) (position.Position, error) {
	return position.ParseLSN(s)
}

// stream adapts the concrete pgoutput reader to the Reader contract.
type stream struct {
	*Reader
	out chan change.Change
}

// Stream begins the stream at the given LSN.
func (s stream) Stream(ctx context.Context, from position.Position) (<-chan change.Change, <-chan error) {
	errCh := make(chan error, 1)
	l, ok := from.(*position.LSN)
	if !ok {
		errCh <- fmt.Errorf("postgres: start position must be an LSN, got %T", from)
		return s.out, errCh
	}
	go func() { errCh <- s.StartFromLSN(ctx, l) }()
	return s.out, errCh
}
