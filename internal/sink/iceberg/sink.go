package iceberg

import (
	"context"
	"strings"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/sink"
)

// Sink adapts the Iceberg REST catalog to the neutral sink.Sink contract. It
// owns the catalog connection and the target→identifier mapping, so the
// orchestration never touches iceberg-go types.
type Sink struct {
	cat catalog.Catalog
	ns  string
}

// Open dials the catalog and ensures the namespace, returning a Sink that
// satisfies the full sink.Sink contract.
func Open(ctx context.Context, cfg sink.Config) (*Sink, error) {
	cat, err := NewCatalog(ctx, Config{
		URI:          cfg.URI,
		Warehouse:    cfg.Options["warehouse"],
		ClientID:     cfg.Options["client_id"],
		ClientSecret: cfg.Options["client_secret"],
		Scope:        cfg.Options["scope"],
	})
	if err != nil {
		return nil, err
	}
	if err := EnsureNamespace(ctx, cat, table.Identifier{cfg.Namespace}); err != nil {
		return nil, err
	}
	return &Sink{cat: cat, ns: cfg.Namespace}, nil
}

// ident resolves a target table name into an iceberg identifier, falling
// back to the namespace for bare names.
func (s *Sink) ident(target string) table.Identifier {
	if ns, name, ok := strings.Cut(target, "."); ok {
		return table.Identifier{ns, name}
	}
	return table.Identifier{s.ns, target}
}

// EnsureTable creates the target table from the canonical schema if absent,
// and validates compatibility if present.
func (s *Sink) EnsureTable(ctx context.Context, ref core.TableRef, schema core.Schema, partitionBy []string, cast core.CastPolicy) error {
	is, err := FromCanonical(schema)
	if err != nil {
		return err
	}
	return EnsureTable(ctx, s.cat, s.ident(ref.Target), is, partitionBy, cast)
}

// Writer opens the per-table committer.
func (s *Sink) Writer(ctx context.Context, ref core.TableRef, cast core.CastPolicy, meta []core.MetadataColumn) (sink.TableWriter, error) {
	return NewTableWriter(ctx, s.cat, s.ident(ref.Target), ref.PrimaryKey, cast, meta, ref.Source)
}

// Position reads the committed CDC position (with walk-back). An empty
// string means the table has never been written.
func (s *Sink) Position(ctx context.Context, ref core.TableRef) (string, error) {
	return CommittedPosition(ctx, s.cat, s.ident(ref.Target))
}

// SetProperties writes arbitrary properties to the target table.
func (s *Sink) SetProperties(ctx context.Context, ref core.TableRef, props map[string]string) error {
	return SetTableProperties(ctx, s.cat, s.ident(ref.Target), props)
}

// Properties reads the target table's properties. A missing table yields an
// empty map with no error (treated as not started).
func (s *Sink) Properties(ctx context.Context, ref core.TableRef) (map[string]string, error) {
	tbl, err := s.cat.LoadTable(ctx, s.ident(ref.Target))
	if err != nil {
		return map[string]string{}, nil
	}
	return tbl.Properties(), nil
}

// Close is a no-op: the REST catalog is stateless and holds no connection.
func (s *Sink) Close() error { return nil }

func init() {
	driver.RegisterSink(driver.DefaultSinkType, func(ctx context.Context, cfg sink.Config) (sink.Sink, error) {
		return Open(ctx, cfg)
	})
}
