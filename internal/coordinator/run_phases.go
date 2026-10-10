package coordinator

import (
	"context"
	"fmt"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/internal/enrich"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// phaseEventlog opens the run's audit trail and writes job_started. It returns
// the shutdown func the caller defers, so the trail outlives every later phase
// and is sealed last (LIFO).
func (c *Coordinator) phaseEventlog(ctx context.Context) (func(), error) {
	cfg := c.cfg.Eventlog
	if cfg == nil {
		return func() {}, nil
	}
	// startEventlog opens the trail and attaches the log trail, so the run's
	// history (events AND logs) reaches S3; its stop func drains the log trail
	// before sealing the run.
	stop, err := c.startEventlog(ctx, *cfg)
	if err != nil {
		return nil, err
	}
	return stop, nil
}

// phaseSource opens the source adapter and its optional query connection, then
// validates the requested chunk parallelism. QuerySource is optional: it backs
// NewChunker, the snapshot/re-slice chunking surface, which only a relational,
// snapshot-capable source (Capabilities.Snapshot or ChunkQuery) ever calls. A
// source with neither, like Kafka, has no SQL query connection at all — see
// kafka.Source's own doc comment — and must boot with c.qsrc == nil; every call
// site gates on the source's capabilities before touching it (issue #394).
func (c *Coordinator) phaseSource() error {
	src, err := driver.OpenSource(c.cfg.Spec, source.Runtime{
		ServerID:         c.cfg.ServerID,
		Heartbeat:        c.cfg.Heartbeat,
		Logger:           c.log,
		OnDestructiveDDL: c.reportDestructiveDDL,
	})
	if err != nil {
		return err
	}
	c.src = src
	if qsrc, ok := src.(source.QuerySource); ok {
		c.qsrc = qsrc
	}
	// The parallel-chunk setting may not exceed the ceiling the source driver
	// declares — fail fast at boot, not mid-snapshot.
	if err := driver.ValidateParallelism(c.cfg.Spec.Source.Kind, c.cfg.MaxParallelChunks); err != nil {
		return fmt.Errorf("coordinator: %w", err)
	}
	return nil
}

// closeQuery closes the optional query connection opened by phaseSource.
func (c *Coordinator) closeQuery() {
	if c.qsrc != nil {
		_ = c.qsrc.CloseQuery()
	}
}

// phaseResolveTables expands the table list (a discovery pipeline enumerates it
// now), introspects each table, applies the cast policy and resolves the sink
// schema, registers enrichment columns, and indexes the boot lists for the
// per-batch hot paths. It returns the DDL-time schemas and the spec table index
// the later phases consume.
func (c *Coordinator) phaseResolveTables(ctx context.Context) (map[string]core.Schema, map[string]spec.Table, error) {
	// A discovery pipeline lists no tables: the source enumerates them now. The
	// write-back must land BEFORE the partition-range loop, which indexes the
	// expanded table list positionally against refs — a second list would
	// desync the ranges from the tables.
	tables, err := source.ExpandTables(ctx, c.src, c.cfg.Spec)
	if err != nil {
		return nil, nil, fmt.Errorf("coordinator: %w", err)
	}
	// The metrics server boots before run() and serves /statusz concurrently,
	// so the expanded list is stored under the lock those handlers read it
	// with — cfg.Spec is left untouched (issue #556).
	c.mu.Lock()
	c.tables = tables
	c.mu.Unlock()

	refs := make([]source.TableRef, 0, len(tables))
	// canonical holds the WIRE shape (source types; the workers encode it and
	// the sink casts). resolvedSchemas holds the sink's target shape (cast
	// types + metadata columns) for DDL.
	canonical := make(map[string]core.Schema, len(tables))
	resolvedSchemas := make(map[string]core.Schema, len(tables))
	tableBySource := make(map[string]spec.Table, len(tables))
	for _, t := range tables {
		ref, srcSchema, srcWarns, err := c.src.Introspect(ctx, t)
		if err != nil {
			return nil, nil, err
		}
		c.surfaceWarnings(ref.Source, srcWarns)
		cast, err := coreCastOf(t)
		if err != nil {
			return nil, nil, err
		}
		res, warns, err := core.ResolveSchema(srcSchema, cast, t.Metadata)
		if err != nil {
			return nil, nil, fmt.Errorf("coordinator: table %s: %w", t.Target, err)
		}
		c.surfaceWarnings(ref.Source, warns)
		refs = append(refs, ref)
		// Reference columns join BOTH shapes: the assignment/wire schema the
		// worker encodes against and the resolved schema EnsureTable creates
		// the table from. Without it, the first enriched batch carries a column
		// the table lacks and every sink silently drops it. Registered
		// decision: nullable strings until CR-069 resolves real types.
		//
		// The coordinator has no Stage of its own (it forwards enrich
		// declarations to the worker, which runs the real, long-lived join).
		// For a wildcard select, the real column names are only known once the
		// reference query runs — LoadWildcardColumns runs it synchronously
		// here, at boot, so canonical/resolvedSchemas are correct before
		// EnsureTable and before any worker session connects (#56). This is a
		// second, short-lived query against the reference beyond the worker's
		// own load — an accepted, disclosed cost of the coordinator/worker
		// split (no shared connection between the two processes).
		dests, err := enrich.LoadWildcardColumns(ctx, t.Enrich)
		if err != nil {
			return nil, nil, fmt.Errorf("coordinator: %s: enrich: %w", t.Source, err)
		}
		canonical[t.Source] = enrich.AddColumns(core.WireSchema(srcSchema, res), dests)
		resolvedSchemas[t.Source] = enrich.AddColumns(res, dests)
		tableBySource[t.Source] = t
	}
	c.refs = refs
	c.canonical = canonical
	// Index the boot lists so the per-batch lookups do not scan them
	// (issue #582).
	c.refByTarget = make(map[string]core.TableRef, len(refs))
	for _, ref := range refs {
		c.refByTarget[ref.Target] = ref
	}
	c.specBySource = make(map[string]spec.Table, len(tables))
	c.specByTarget = make(map[string]spec.Table, len(tables))
	for _, t := range tables {
		c.specBySource[t.Source] = t
		c.specByTarget[t.Target] = t
	}
	return resolvedSchemas, tableBySource, nil
}

// phaseValidateTables rejects modes the coordinator cannot run before any
// worker is provisioned. Incremental mode (#157) is implemented in the
// collapsed runner only. An upsert table with no key fails loudly BEFORE
// worker groups are resolved: a discovered keyless table would otherwise
// create worker Deployments and then abort, leaving orphaned resources across
// repeated boot failures.
func (c *Coordinator) phaseValidateTables(tableBySource map[string]spec.Table) error {
	for _, t := range c.tables {
		if t.Mode == spec.ModeIncremental {
			return fmt.Errorf("coordinator: %s: incremental mode is not supported in distributed mode yet — run this table in the collapsed runner", t.Target)
		}
	}
	for _, ref := range c.refs {
		if err := dataplane.RequireUpsertKey(ref.Target, ref.PrimaryKey, tableBySource[ref.Source].WriteMode.ChangeMode()); err != nil {
			return fmt.Errorf("coordinator: %w", err)
		}
	}
	return nil
}

// phaseResolveRouting resolves the worker groups, partition ranges, tickets and
// position indexes, then publishes the boot layout once. It returns the
// derived worker-group-to-target map the provisioning and maintenance phases
// consume.
//
// Resolve worker groups: one per partition, derived "<pipeline>-<target>-<index>"
// name (spec.Table.WorkerGroupNames) — there is no operator-chosen worker name,
// so two tables can never collide on one (each name embeds its own unique
// target).
//
// A table's partition RANGES are computed here, at boot, for every table — not
// only the ones needing a snapshot — because live-stream routing (enqueueBatch)
// depends on them from the first batch, resume or not. Workers<=1
// short-circuits to a single unbounded range with no chunker query at all, so
// this is a no-op for every unpartitioned table (the overwhelming common case
// today).
func (c *Coordinator) phaseResolveRouting(ctx context.Context) (map[string]string, error) {
	bootRanges := make(map[string][]source.Chunk, len(c.tables))
	bootOwners := make(map[string][]*workerState, len(c.tables))
	c.chunkers = make(map[string]source.ChunkSource, len(c.tables))
	// workerTarget maps every derived worker group name back to the table
	// target it belongs to — provisionWorkers uses it to pick that table's own
	// worker Pod template (one per table, rendered by the operator into the
	// coordinator's ConfigMap).
	workerTarget := make(map[string]string, len(c.tables))
	for i, t := range c.tables {
		if err := requirePartitionKey(t, c.refs[i]); err != nil {
			return nil, err
		}
		names := t.WorkerGroupNames(c.cfg.Spec.Pipeline)
		// Ranges are no longer sampled: ownership is the rendezvous hash
		// (route.OwnerOfKey), so resolvePartitionRanges returns the single
		// unbounded range the DBLog read covers and only builds the chunker.
		ranges, chunker, err := c.resolvePartitionRanges(ctx, t, c.refs[i])
		if err != nil {
			return nil, fmt.Errorf("coordinator: %s: %w", t.Source, err)
		}
		if len(ranges) == 0 {
			return nil, fmt.Errorf("coordinator: %s: no partition range resolved", t.Source)
		}
		bootRanges[t.Target] = ranges
		c.chunkers[t.Target] = chunker

		owners := make([]*workerState, len(names))
		for p, name := range names {
			owners[p] = c.bootWorker(name, c.refs[i])
			workerTarget[name] = t.Target
		}
		bootOwners[t.Target] = owners
	}
	// Publish the boot layout once: every runtime reader loads this snapshot,
	// and a re-slice swaps in a successor (issue #312).
	c.publishRouting(&routing{owners: bootOwners, ranges: bootRanges})
	if c.cfg.OnReady != nil {
		c.cfg.OnReady(c)
	}
	c.workerK8s = workerPodTemplateAvailable(workerTarget)
	return workerTarget, nil
}

// phaseProvisionWorkers provisions the Kubernetes worker Pods (if the operator
// rendered worker templates), emits a worker_created event per group, and
// starts the optional checkpoint writer. With no Kubernetes provisioning this
// makes zero API calls and only emits/starts what is configured.
func (c *Coordinator) phaseProvisionWorkers(ctx context.Context, workerTarget map[string]string) error {
	if err := c.provisionWorkers(ctx, workerTarget); err != nil {
		return fmt.Errorf("coordinator: %w", err)
	}
	for _, w := range c.workers {
		c.log.Info("coordinator worker group", "worker", w.name, "tables", len(w.refs))
		if err := c.emit(eventlog.KindWorkerCreated, map[string]any{
			"worker": w.name,
			"tables": tableNames(w.refs),
		}); err != nil {
			c.log.Warn("coordinator: eventlog emit", "err", err)
		}
	}
	if cfg := c.cfg.Checkpoint; cfg != nil {
		cp, err := newCheckpoint(ctx, *cfg)
		if err != nil {
			return fmt.Errorf("coordinator: checkpoint: %w", err)
		}
		c.cp = cp
		go cp.run(ctx, c.runID, c.indexSnapshot, c.log)
		c.log.Info("coordinator checkpoint", "uri", cfg.URI, "interval", cp.interval)
	}
	return nil
}

// phaseOpenSink opens the sink, checks the per-table concurrent-writer
// capability, and applies DDL. The coordinator owns DDL.
func (c *Coordinator) phaseOpenSink(ctx context.Context, resolvedSchemas map[string]core.Schema, tableBySource map[string]spec.Table) error {
	snk, err := driver.OpenSink(ctx, c.cfg.Spec)
	if err != nil {
		return fmt.Errorf("coordinator: catalog: %w", err)
	}
	c.snk = snk

	// A partitioned table needs a sink that can serve N concurrent writers.
	// Checked by CAPABILITY, not by sink type name: a plugin registers whatever
	// name it wants. The capability must be declared false until the sink's
	// concurrent path is actually built, so this refuses the boot instead of
	// letting N writers corrupt one table.
	for _, t := range c.tables {
		if err := requireConcurrentSink(t, c.snk); err != nil {
			return err
		}
	}

	for _, ref := range c.refs {
		tbl := tableBySource[ref.Source]
		// The cast policy must reach DDL: an empty policy here creates a table
		// whose types diverge from the collapsed runner's (audit #8).
		cast, err := coreCastOf(tbl)
		if err != nil {
			return err
		}
		mode := tbl.WriteMode.ChangeMode()
		if err := snk.EnsureTable(ctx, ref, resolvedSchemas[ref.Source], tbl.PartitionBy, cast, mode); err != nil {
			return fmt.Errorf("coordinator: ensure %s: %w", ref.Target, err)
		}
	}
	return nil
}

// closeSink closes the sink on every exit path between opening it and the run's
// return (audit #14).
func (c *Coordinator) closeSink() {
	if c.snk != nil {
		_ = c.snk.Close()
	}
}

// phaseMaintenanceResumeBaseline schedules Iceberg maintenance, resolves the
// snapshot tables and resume position, marks unfinished snapshots pending, and
// baselines every worker's confirmed position to the resume point. It returns
// the tables that need a snapshot.
func (c *Coordinator) phaseMaintenanceResumeBaseline(ctx context.Context, workerTarget map[string]string) ([]source.TableRef, position.Position, error) {
	// Iceberg table maintenance (issue #96): the coordinator SCHEDULES
	// maintenance but does not run it in its own process. With Kubernetes
	// worker provisioning available it provisions an ephemeral maintenance
	// worker per table (from the table's own worker pod template) and pushes
	// the due operations to it over the control stream; the worker dies when
	// the pass is done, so no maintenance work competes with the coordinator's
	// routing/commit path. Without Kubernetes there is no worker to launch —
	// the collapsed runner's in-process schedule is the only path there.
	// Checked against the NEUTRAL sink.Maintainable interface — never a
	// concrete sink package, which internal/architecture's
	// TestOrchestrationConsumesContracts forbids the coordinator from
	// importing. Maintenance is Iceberg-only (spec.Validate rejects it on any
	// other sink type), so a sink that does not implement the capability here
	// is a bug, not a configuration to tolerate: fail loudly, exactly like
	// requireConcurrentSink above. Skipped entirely when Maintenance is nil or
	// disabled.
	if err := c.startMaintenance(c.refs, workerTarget); err != nil {
		return nil, nil, err
	}

	resume, needsSnapshot, err := c.resumeFrom(ctx, c.refs)
	if err != nil {
		return nil, nil, err
	}
	c.log.Info("coordinator resume", "from", position.StringOrNone(resume), "snapshot_tables", len(needsSnapshot))
	// Before any worker can commit: a crash from here on must find these tables
	// unfinished, whatever positions the stream commits to them.
	if err := c.markSnapshotsPending(ctx, needsSnapshot); err != nil {
		return nil, nil, err
	}

	// Baseline every worker's confirmed position to the run's resume point.
	// confirmedPosition() takes the min over the map, so a worker that has not
	// acked yet must be IN the map holding that min back — omitting it let the
	// source slot advance past data a worker had not committed (WK-001 §2.2).
	// resume is nil on a fresh boot, which correctly holds the slot until every
	// worker has committed at least once.
	c.confirmedMu.Lock()
	for name := range c.workers {
		c.confirmed[name] = resume
	}
	c.confirmedMu.Unlock()
	return needsSnapshot, resume, nil
}
