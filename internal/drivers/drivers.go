// Package drivers wires the concrete source and sink implementations into
// the pipeline. It is the ONLY package (besides cmd/) that knows
// source/mysql, source/postgres, source/kafka and sink/iceberg — runner and
// coordinator consume the Registry through its interface surface and stay
// free of the implementations.
package drivers

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/internal/adapter"
	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/core"
	"github.com/maltzsama/urutau/internal/position"
	icebergsink "github.com/maltzsama/urutau/internal/sink/iceberg"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/internal/spec"
)

// StreamSource is the replication reader surface (snapshot.SourceReader +
// start/stop). Exposed here so runner and coordinator consume it without
// importing the adapter.
type StreamSource = adapter.StreamSource

// Runtime carries the replication knobs passed through to the source.
type Runtime struct {
	ServerID  uint32
	Heartbeat time.Duration
	Logger    *slog.Logger
}

// Registry is the driver assembly for one pipeline.
type Registry struct {
	adapt adapter.Source
	rt    Runtime
}

// New resolves the source adapter for the spec's source kind.
func New(s *spec.Spec, rt Runtime) (*Registry, error) {
	adapt, err := forSource(s, adapter.Runtime(rt))
	if err != nil {
		return nil, err
	}
	return &Registry{adapt: adapt, rt: rt}, nil
}

// Source exposes the source adapter surface.
func (r *Registry) Source() adapter.Source { return r.adapt }

// Caps returns the source's capabilities.
func (r *Registry) Caps() adapter.Capabilities { return r.adapt.Caps() }

// CapsForKind returns a source kind's capabilities without a full spec.
// Used by admission validation to enforce driver-declared resource ceilings.
func CapsForKind(kind string) (adapter.Capabilities, error) {
	return capsForKind(kind)
}

// ValidateParallelism rejects a parallel-chunk setting above the ceiling the
// source driver declares: a shared MySQL with max_connections=100 does not
// tolerate the same snapshot concurrency as a dedicated Postgres. A ceiling
// of 0 means the driver has no opinion.
func ValidateParallelism(kind string, maxParallelChunks int) error {
	if maxParallelChunks <= 0 {
		return nil
	}
	caps, err := capsForKind(kind)
	if err != nil {
		return err
	}
	if caps.MaxConnections > 0 && maxParallelChunks > caps.MaxConnections {
		return fmt.Errorf("drivers: maxParallelChunks (%d) exceeds the %s driver ceiling (%d connections)",
			maxParallelChunks, kind, caps.MaxConnections)
	}
	return nil
}

// SupportsQuery reports whether the source has a SQL query surface.
func (r *Registry) SupportsQuery() bool {
	_, ok := r.adapt.(adapter.QuerySource)
	return ok
}

// OpenQuery opens the source query connection. A source without a SQL
// surface (kafka) returns (nil, nil) — the pipeline mediates this through
// the capability seam instead of the source stubbing the method.
func (r *Registry) OpenQuery(ctx context.Context) (*sql.DB, error) {
	if q, ok := r.adapt.(adapter.QuerySource); ok {
		return q.OpenQuery(ctx)
	}
	return nil, nil
}

// OpenQueryDB opens a query connection for a kind+uri (remote workers).
func OpenQueryDB(kind, uri string) (*sql.DB, error) {
	return openQueryDB(kind, uri)
}

// Introspect resolves one spec table into its ref, the resolved canonical
// schema (cast + metadata applied), the final Iceberg schema, and the
// advisory warnings.
func (r *Registry) Introspect(ctx context.Context, db *sql.DB, t spec.Table) (core.TableRef, core.Schema, *iceberg.Schema, []core.Warning, error) {
	return r.adapt.Introspect(ctx, db, t)
}

// ParsePosition decodes a stored cdc.position string.
func (r *Registry) ParsePosition(s string) (position.Position, error) {
	return r.adapt.ParsePosition(s)
}

// InitialPosition is the stream start for a first boot.
func (r *Registry) InitialPosition(ctx context.Context, db *sql.DB) (position.Position, error) {
	return r.adapt.InitialPosition(ctx, db)
}

// NewReader builds the replication reader.
func (r *Registry) NewReader(ctx context.Context, db *sql.DB, refs []core.TableRef, out chan<- change.Change) (StreamSource, error) {
	return r.adapt.NewReader(ctx, db, refs, out)
}

// NewChunker builds the chunk SELECT source. Only a SQL-capable source has
// one; callers must gate on SupportsQuery (or Caps().Snapshot) first.
func (r *Registry) NewChunker(db *sql.DB, source, pk string, chunkSize int) (snapshot.ChunkSource, error) {
	if q, ok := r.adapt.(adapter.QuerySource); ok {
		return q.NewChunker(db, source, pk, chunkSize)
	}
	return nil, fmt.Errorf("drivers: source has no SQL chunking surface")
}

// ── Sink side ─────────────────────────────────────────────────────────

// TargetIdent resolves a target table name into an iceberg identifier,
// falling back to the spec's sink namespace.
func TargetIdent(s *spec.Spec, target string) table.Identifier {
	return icebergsink.TargetIdent(s, target)
}

// NewCatalog opens the Iceberg REST catalog from the spec's sink config.
func NewCatalog(ctx context.Context, s *spec.Spec) (catalog.Catalog, error) {
	return icebergsink.NewCatalog(ctx, icebergsink.CatalogConfig(s))
}

// NewCatalogFromConfig opens the Iceberg REST catalog from a raw sink config
// (used by remote workers, which hold the config directly).
func NewCatalogFromConfig(ctx context.Context, cfg icebergsink.Config) (catalog.Catalog, error) {
	return icebergsink.NewCatalog(ctx, cfg)
}

// EnsureNamespace creates the sink namespace if absent.
func EnsureNamespace(ctx context.Context, cat catalog.Catalog, ident table.Identifier) error {
	return icebergsink.EnsureNamespace(ctx, cat, ident)
}

// EnsureTable creates the target table if absent; on existing tables it
// verifies cast column types have not diverged. The partition spec is
// applied on create and verified for divergence on existing tables.
func EnsureTable(ctx context.Context, cat catalog.Catalog, ident table.Identifier, schema *iceberg.Schema, partitionBy []string, cast core.CastPolicy) error {
	return icebergsink.EnsureTable(ctx, cat, ident, schema, partitionBy, cast)
}

// NewTableWriter opens the per-table Iceberg writer with its cast and
// metadata plan. Casts are applied to source column values; metadata columns
// are projected from the change header.
func NewTableWriter(ctx context.Context, cat catalog.Catalog, ident table.Identifier, pk []string, cast core.CastPolicy, meta []core.MetadataColumn, sourceTable string) (*icebergsink.TableWriter, error) {
	return icebergsink.NewTableWriter(ctx, cat, ident, pk, cast, meta, sourceTable)
}

// CommittedPosition reads the committed cdc.position (with walk-back).
func CommittedPosition(ctx context.Context, cat catalog.Catalog, ident table.Identifier) (string, error) {
	return icebergsink.CommittedPosition(ctx, cat, ident)
}

// SetTableProperties writes arbitrary properties to an Iceberg table.
func SetTableProperties(ctx context.Context, cat catalog.Catalog, ident table.Identifier, props iceberg.Properties) error {
	return icebergsink.SetTableProperties(ctx, cat, ident, props)
}
