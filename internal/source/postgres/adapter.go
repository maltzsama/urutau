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
		return Source{spec: s, rt: rt}, nil
	})
}

var _ source.Source = Source{}
var _ source.QuerySource = Source{}

// OpenQuery opens the SQL connection for chunk SELECTs and introspection.
func (a Source) OpenQuery(ctx context.Context) (*sql.DB, error) {
	return sql.Open("pgx", a.spec.Source.URI)
}

// Introspect resolves one spec table into its ref and canonical schema.
func (a Source) Introspect(ctx context.Context, db *sql.DB, t spec.Table) (core.TableRef, core.Schema, []core.Warning, error) {
	schemaName, tableName, ok := strings.Cut(t.Source, ".")
	if !ok {
		return source.TableRef{}, core.Schema{}, nil, fmt.Errorf("postgres: source %q must be db.table", t.Source)
	}
	st, err := QueryTable(ctx, db, schemaName, tableName)
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
func (a Source) NewChunker(db *sql.DB, source, pk string, chunkSize int) (source.ChunkSource, error) {
	return NewChunker(db, source, pk, chunkSize)
}

// NewReader builds the replication reader over the pipeline's tables.
func (a Source) NewReader(ctx context.Context, db *sql.DB, refs []source.TableRef, out chan<- change.Change) (source.StreamSource, error) {
	slot := a.spec.Source.SlotName
	if slot == "" {
		return nil, fmt.Errorf("postgres: source requires slotName")
	}
	rdr, err := New(ctx, Config{
		URI:      a.spec.Source.URI,
		DB:       db,
		SlotName: slot,
		Tables:   refs,
		Logger:   a.rt.Logger,
	}, out)
	if err != nil {
		return nil, err
	}
	return stream{Reader: rdr}, nil
}

// InitialPosition returns the slot's confirmed LSN: a first boot starts
// streaming from the slot's consistency point — everything after it flows
// through the stream, everything before it belongs to the snapshot.
func (a Source) InitialPosition(ctx context.Context, db *sql.DB) (position.Position, error) {
	return ConfirmedLSN(ctx, db, a.spec.Source.SlotName)
}

// ParsePosition decodes a stored cdc.position as an LSN.
func (a Source) ParsePosition(s string) (position.Position, error) {
	return position.ParseLSN(s)
}

// stream adapts the concrete pgoutput reader to StreamSource.
type stream struct {
	*Reader
}

// Start begins the stream at the given LSN.
func (s stream) Start(ctx context.Context, at position.Position) error {
	l, ok := at.(*position.LSN)
	if !ok {
		return fmt.Errorf("postgres: start position must be an LSN, got %T", at)
	}
	return s.StartFromLSN(ctx, l)
}
