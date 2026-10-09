package coordinator

import (
	"context"
	"fmt"
	"strings"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// requirePartitionKey rejects workers>1 for a table with no primary key:
// partitioning splits the key range, and a table without one has no way to
// divide it. Checked before resolvePartitionRanges so the error names the
// real cause instead of the chunker's cryptic empty-key failure.
func requirePartitionKey(t spec.Table, ref core.TableRef) error {
	if t.WorkerCount() > 1 && len(ref.PrimaryKey) == 0 {
		return fmt.Errorf("coordinator: %s: workers>1 requires a primary key: "+
			"partitioning splits the key range, and a table without one has no "+
			"way to divide it", t.Target)
	}
	return nil
}

// requireSnapshotBootstrap rejects a bootstrap block the coordinator does not
// implement. tables[].bootstrap is honored only by the collapsed runner: the
// coordinator snapshots every table without a committed position and starts
// the stream at the resume or current position (issue #405). Accepting
// adopt would silently run a full snapshot instead, and accepting an
// explicit start would skip the changes between that position and the
// current one. mode: snapshot with startAt: current is what the
// coordinator already does, so it stays valid.
func requireSnapshotBootstrap(t spec.Table) error {
	b := t.Bootstrap
	if b == nil {
		return nil
	}
	if b.Mode != "" && b.Mode != spec.BootstrapSnapshot {
		return fmt.Errorf("coordinator: %s: bootstrap.mode %q is not supported in distributed mode yet — "+
			"remove the bootstrap block to snapshot the table, or run this table in the collapsed runner", t.Target, b.Mode)
	}
	if b.StartAt == spec.StartAtExplicit {
		return fmt.Errorf("coordinator: %s: bootstrap.startAt %q is not supported in distributed mode yet — "+
			"the stream would start at the current source position and skip the changes since %q; "+
			"run this table in the collapsed runner", t.Target, b.StartAt, b.Position)
	}
	return nil
}

// requireConcurrentSink rejects workers>1 when the sink does not declare the
// ConcurrentWriter capability (or declares it false). snk is taken as any so
// the check is a pure capability probe — the coordinator never reaches into a
// concrete sink.
func requireConcurrentSink(t spec.Table, snk any) error {
	if t.WorkerCount() <= 1 {
		return nil
	}
	cw, ok := snk.(sink.ConcurrentWriter)
	if !ok || !cw.SupportsConcurrentWriters() {
		return fmt.Errorf("coordinator: %s: workers>1 is not supported by "+
			"this sink (it cannot order concurrent writers to one table); "+
			"use workers: 1, or for couchbase set sink.commitMode: atomic",
			t.Target)
	}
	return nil
}

// resolvePartitionRanges returns the ordered, contiguous PK ranges one
// table's Workers count requires — the SAME ranges the DBLog snapshot
// (each chunk aligned within its owning range) and live-stream routing
// both use, so a key never switches partition ownership between the two
// phases. Workers<=1 returns a single unbounded range without touching
// the database at all — the common, unpartitioned case pays no extra
// cost. Workers>1 requires the source's chunker to implement
// source.PartitionSource; a source that doesn't (Postgres, today) fails
// the boot loudly rather than silently running unpartitioned.
func (c *Coordinator) resolvePartitionRanges(_ context.Context, t spec.Table, ref source.TableRef) ([]source.Chunk, source.ChunkSource, error) {
	n := t.WorkerCount()
	if n <= 1 {
		// Unpartitioned: no chunker needed at boot; the snapshot builds it
		// lazily (the common case pays no chunker construction here).
		return []source.Chunk{{}}, nil, nil
	}
	if c.qsrc == nil {
		// A source with no SQL query surface (Kafka: coordinator.go's boot
		// makes QuerySource optional, issue #394) has no chunker at all —
		// partitioning it is a config error the operator must fix, not a
		// nil-qsrc panic on the line below.
		return nil, nil, fmt.Errorf("workers: %d: source %q has no SQL query surface to read chunks by; set workers.number to 1", n, c.cfg.Spec.Source.Kind)
	}
	chunker, err := c.qsrc.NewChunker(ref.Source, strings.Join(ref.PrimaryKey, ","), c.cfg.ChunkSize)
	if err != nil {
		return nil, nil, fmt.Errorf("workers: %d: chunker: %w", n, err)
	}
	// Ownership is the rendezvous hash, decided from the key, not a sampled
	// key range: no Partitions query is issued. The chunker only powers the
	// index range scan the fan-out reads by (snapshot_hash.go), so the source
	// no longer needs PartitionSource — just ChunkSource.
	return []source.Chunk{{}}, chunker, nil
}

// supervisionConfig maps the Config knobs to the supervisor defaults.
func supervisionConfig(cfg Config) SupervisorConfig {
	return SupervisorConfig{
		AckTimeout:      cfg.AckTimeout,
		MaxResets:       cfg.MaxResets,
		ResetWindow:     cfg.ResetWindow,
		DeliveryTimeout: cfg.WorkerDeliveryTimeout,
	}
}
