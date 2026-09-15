// Package sink defines the destination catalog contract. A sink consumes
// core.Schema and commits rowchange.Batch; it knows nothing about any source.
// The contract is composed of small capability interfaces; the driver
// registry resolves a spec's sink into a concrete Sink.
package sink

import (
	"context"
	"log/slog"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
)

// Config is everything a sink needs, in neutral terms. Driver-specific
// knobs (warehouse, credentials, file size, codec) live in Options.
type Config struct {
	Type      string // "iceberg+rest" (empty defaults to it)
	URI       string // REST catalog endpoint / connection string
	Namespace string
	Options   map[string]string // warehouse, client_id, client_secret, scope, …
	// SourceKind is the pipeline's source driver kind ("mysql", "postgres",
	// "kafka"). It is a hint for decoding the opaque position strings a sink
	// persists, not a coupling: a sink that keeps one position per partition
	// (WK-001 §2.6) uses it to compare positions with position.Parse.
	// Empty means the default (MySQL GTID).
	SourceKind string
}

// secretOptionKeys are the Options keys whose values are credentials. They
// are redacted by LogValue so a naive slog of a Config cannot leak them.
var secretOptionKeys = map[string]bool{
	"client_secret": true,
	"password":      true,
	"token":         true,
	"secret":        true,
	"private_key":   true,
}

// LogValue implements slog.LogValuer: Options carries credentials, so
// logging the Config directly would leak them. The known secret keys are
// replaced with "[REDACTED]".
func (c Config) LogValue() slog.Value {
	opts := make(map[string]string, len(c.Options))
	for k, v := range c.Options {
		if secretOptionKeys[k] {
			opts[k] = "[REDACTED]"
			continue
		}
		opts[k] = v
	}
	return slog.GroupValue(
		slog.String("type", c.Type),
		slog.String("uri", c.URI),
		slog.String("namespace", c.Namespace),
		slog.Any("options", opts),
	)
}

// TableWriter commits one table's batches. The CDC position travels inside
// dataplane.Batch.Watermark (a serialized position string), and implementations
// MUST honour the invariant below — it is correctness, not style:
//
// The position must never advance past durably written data. How each sink
// achieves that is its own mechanism:
//
//   - Iceberg (transactional commits): delete-then-append in SEPARATE
//     commits, position written only on the LAST one. Staging equality
//     deletes and data rows in a single transaction produces two snapshots
//     in iceberg-go v0.6.0 with the delete holding the HIGHER sequence
//     number — it also deletes the freshly appended rows.
//   - ClickHouse (no multi-statement transaction): the position travels on
//     every row of ONE insert, so it can never separate from the data.
type TableWriter interface {
	// Commit writes the collapsed batch and the position. A batch that
	// fails must leave the table untouched — a partially applied batch is
	// indistinguishable from data loss on resume.
	Commit(ctx context.Context, b *dataplane.Batch) error

	Close() error
}

// Ensurer creates the target table from the canonical schema if absent, and
// validates compatibility if present. The sink derives its own target
// identifier and storage schema (e.g. Iceberg field mapping) internally.
// Mode is the write shape the table serves — upsert (versioned, dedup
// capable) or append (plain log) — so sinks whose storage engine is chosen
// at DDL time (ClickHouse MergeTree family) build a table that tells the
// truth about what it is. Sinks with mode-agnostic tables (Iceberg) may
// ignore it.
type Ensurer interface {
	EnsureTable(ctx context.Context, ref core.TableRef, schema core.Schema, partitionBy []string, cast core.CastPolicy, mode dataplane.WriteMode) error
}

// Writer opens the per-table committer. The primary key, cast plan and
// metadata columns come from the pipeline plan (the ref carries the PK and
// source table identity).
type Writer interface {
	Writer(ctx context.Context, ref core.TableRef, cast core.CastPolicy, meta []core.MetadataColumn) (TableWriter, error)
}

// Positioner reads the committed CDC position of a target table. An empty
// string means the table has never been written and needs the snapshot.
type Positioner interface {
	Position(ctx context.Context, ref core.TableRef) (string, error)
}

// PropertySetter writes arbitrary properties to a target table (snapshot
// progress, operator bookkeeping).
type PropertySetter interface {
	SetProperties(ctx context.Context, ref core.TableRef, props map[string]string) error
}

// PropertyGetter reads a target table's properties (snapshot progress
// resume). A missing table yields an empty map with no error.
type PropertyGetter interface {
	Properties(ctx context.Context, ref core.TableRef) (map[string]string, error)
}

// Closer releases the sink's catalog connection.
type Closer interface {
	Close() error
}

// ConcurrentWriter is implemented by sinks that can accept writes from more
// than one worker for the same table. A sink that cannot durably order or
// serialize concurrent writers to one table MUST NOT implement it: the
// coordinator refuses to boot a partitioned table (workers>1) whose sink
// does not.
//
// It is a declarative capability, deliberately separate from the data-plane
// interfaces: a sink may implement it while still returning false (its
// concurrent path not yet built), and the coordinator checks the VALUE, not
// the interface's presence.
type ConcurrentWriter interface {
	// SupportsConcurrentWriters reports whether this sink can serve N
	// workers writing the same table. It may inspect its own configuration
	// (e.g. Couchbase only in atomic commit mode).
	SupportsConcurrentWriters() bool
}

// StagingWriter is implemented by a sink whose data files can be written
// without committing them, so the coordinator can aggregate the N workers of
// a partitioned table into ONE commit cycle (WK-001 C5, the Flink
// IcebergStreamWriter/IcebergFilesCommitter model). Only the Iceberg sink
// implements it; the descriptor is opaque to the caller.
type StagingWriter interface {
	// WriteStaged writes the batch's data files and returns an opaque
	// descriptor. Nothing is visible in the table until CommitStaged.
	WriteStaged(ctx context.Context, b *dataplane.Batch) ([]byte, error)
}

// StagedCommitter commits one cycle's descriptors as a single unit: all the
// delete files across the cycle first, then all the data files, with the
// cycle's position on the last commit (WK-001 C5).
type StagedCommitter interface {
	CommitStaged(ctx context.Context, ref core.TableRef, staged [][]byte, pos string) error
}

// PositionSeeder is an optional capability: a sink that keeps a durable
// position per partition can seed a baseline for owners that have none, so
// the owner set is complete. Without it, an owner whose partition has no rows
// never records a position, and a Position() that requires complete coverage
// would re-snapshot the table on every boot (WK-001 §2.6).
type PositionSeeder interface {
	// SeedPositions records a baseline for every owner in owners that has no
	// committed position yet, using the minimum of the owners that do. It
	// must never overwrite a real commit, and is a no-op when no owner has a
	// position (a fresh table). Called by the coordinator after a table's
	// snapshot completes, for the owners whose partition had no rows.
	SeedPositions(ctx context.Context, ref core.TableRef, owners []string) error
}

// Sink is a destination catalog. It is the composition of the small
// capability interfaces above; a sink must satisfy all of them.
type Sink interface {
	Ensurer
	Writer
	Positioner
	PropertySetter
	PropertyGetter
	Closer
}
