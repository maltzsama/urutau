package iceberg

import (
	"context"
	"log/slog"
	"strings"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

// Sink adapts the Iceberg REST catalog to the neutral sink.Sink contract. It
// owns the catalog connection and the target→identifier mapping, so the
// orchestration never touches iceberg-go types.
type Sink struct {
	cat            catalog.Catalog
	ns             string
	targetFileSize int64
}

// Open dials the catalog and ensures the namespace, returning a Sink that
// satisfies the full sink.Sink contract.
func Open(ctx context.Context, cfg sink.Config) (*Sink, error) {
	cat, err := NewCatalog(ctx, Config{
		URI:          cfg.URI,
		Warehouse:    cfg.Options[driver.OptWarehouse],
		ClientID:     cfg.Options[driver.OptClientID],
		ClientSecret: cfg.Options[driver.OptClientSecret],
		Scope:        cfg.Options[driver.OptScope],
	})
	if err != nil {
		return nil, err
	}
	if err := EnsureNamespace(ctx, cat, table.Identifier{cfg.Namespace}); err != nil {
		return nil, err
	}
	return &Sink{cat: cat, ns: cfg.Namespace, targetFileSize: targetFileSizeFrom(cfg.Options[driver.OptTargetFileSize])}, nil
}

// targetFileSizeFrom parses the spec's byte-size string ("128Mi", "512Mi")
// into bytes. Empty or malformed yields 0 — "no override", iceberg-go's own
// default. spec.Validate already rejected malformed strings before a spec
// reaches here, so the fallback is defense, not the primary path. It must go
// through spec.ParseBytes (the "Mi"/"Gi" grammar), not strconv.ParseInt: the
// spec stores the operator's spelling, not a plain byte count.
func targetFileSizeFrom(s string) int64 {
	if s == "" {
		return 0
	}
	n, err := spec.ParseBytes(s)
	if err != nil || n <= 0 {
		return 0
	}
	return n
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
// and validates compatibility if present. Mode is ignored: an Iceberg table
// is write-shape agnostic (the worker's collapse handles upsert vs append).
func (s *Sink) EnsureTable(ctx context.Context, ref core.TableRef, schema core.Schema, partitionBy []string, cast core.CastPolicy, _ dataplane.WriteMode) error {
	is, err := FromCanonical(schema)
	if err != nil {
		return err
	}
	return EnsureTable(ctx, s.cat, s.ident(ref.Target), is, partitionBy, cast)
}

// Writer opens the per-table committer.
func (s *Sink) Writer(ctx context.Context, ref core.TableRef, cast core.CastPolicy, meta []core.MetadataColumn) (sink.TableWriter, error) {
	return NewTableWriter(ctx, s.cat, s.ident(ref.Target), ref.PrimaryKey, cast, meta, ref.Source, s.targetFileSize)
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

// SupportsConcurrentWriters reports whether N workers may commit to one
// table. True since WK-001 C5: partitioned writers stage their data files
// (WriteStaged) and the coordinator commits each binlog batch's cycle as one
// unit (CommitStaged), so no worker writes cdc.position and the last writer
// no longer wins.
func (s *Sink) SupportsConcurrentWriters() bool { return true }

// Sink is the only sink that supports table maintenance today.
var _ sink.Maintainable = (*Sink)(nil)

// Maintainer implements sink.Maintainer (just RunOnce) — the orchestration
// only ever calls that one method through the interface.
var _ sink.Maintainer = (*Maintainer)(nil)

// Maintain implements sink.Maintainable: it builds a Maintainer for one
// target table, so the runner and coordinator can run
// compaction/snapshot-expiry/orphan-cleanup through the neutral sink
// contract, never importing this package directly (the architecture wall
// internal/architecture enforces). cat/ident stay private, per the Sink type
// doc. currentPosition and metrics are passed straight through to
// NewMaintainer — see its doc for what a nil value means for each. Callers
// drive the pass with RunOnce, and should only build a Maintainer when
// Maintenance is non-nil and Enabled (RunOnce itself no-ops on a disabled
// config, but callers should not pay for a scheduler per table when
// maintenance is off entirely).
func (s *Sink) Maintain(ref core.TableRef, cfg spec.Maintenance, log *slog.Logger, currentPosition func() string, metrics sink.MaintainerMetrics) sink.Maintainer {
	return NewMaintainer(s.cat, s.ident(ref.Target), cfg, log, currentPosition, metrics)
}

func init() {
	factory := func(ctx context.Context, cfg sink.Config) (sink.Sink, error) {
		return Open(ctx, cfg)
	}
	if err := driver.RegisterSink(driver.DefaultSinkType, factory); err != nil {
		panic(err)
	}
}
