package coordinator

import (
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/spec"
)

// assignmentFor builds one worker's table assignment with its own ticket.
// The table schema travels as Arrow IPC derived from the canonical schema —
// the same typed discipline as the Flight data plane, no JSON on the wire.
func (c *Coordinator) assignmentFor(w *workerState) (*pb.CoordinatorMessage, error) {
	// The assignment's epoch is the worker's CURRENT generation: the worker
	// echoes it in its ready Hello, and onHello rejects any other value. A
	// hardcoded 1 diverges from w.epoch's lifecycle (0 on first boot, ++ on
	// every reset) and silently drops every ready Hello's committed map
	// (audit #6).
	c.mu.Lock()
	epoch := w.epoch
	c.mu.Unlock()
	dsn, err := c.snapshotDSN()
	if err != nil {
		return nil, err
	}
	assign := &pb.Assignment{
		WorkerName: w.name,
		Epoch:      epoch,
		RunId:      c.runID,
		Ticket:     w.ticket,
		SourceKind: c.cfg.Spec.Source.Kind,
		SourceDsn:  dsn,
		ChunkSize:  uint32(c.cfg.ChunkSize),
		Batching: &pb.BatchConfig{
			MaxInterval: durationpb.New(2 * time.Second),
		},
	}
	// A structured postgres source that the DSN cannot fully express (an SSH
	// tunnel has a DialFunc, not a DSN) travels as its JSON block so the
	// worker rebuilds the same source (#170). A scoped SnapshotURI wins: the
	// operator deliberately gave the worker a directly reachable read-only
	// connection, and the replication credential stays coordinator-side
	// (D-CD1) — so the block is withheld then.
	if pg := c.cfg.Spec.Source.Postgres; pg != nil && c.cfg.Spec.Source.SnapshotURI == "" {
		b, err := json.Marshal(pg)
		if err != nil {
			return nil, fmt.Errorf("coordinator: postgres config: %w", err)
		}
		assign.Postgres = b
	}
	for _, ref := range w.refs {
		schemaB, err := transport.EncodeTableSchema(c.canonical[ref.Source])
		if err != nil {
			return nil, fmt.Errorf("coordinator: schema %s: %w", ref.Source, err)
		}
		ta := &pb.TableAssignment{
			SourceTable:       ref.Source,
			TargetTable:       ref.Target,
			WriteMode:         pb.WriteMode_WRITE_MODE_UPSERT,
			PrimaryKey:        ref.PrimaryKey,
			CreateIfNotExists: true,
			SchemaArrow:       schemaB,
			// A partitioned table on a staging sink: the worker stages its
			// data files and the coordinator commits the cycle (WK-001 C5).
			// This is the mode at ATTACH time; live batches carry the current
			// mode per batch (BatchMeta.staged), so a table that becomes
			// partitioned under this worker (issue #312) is staged from the
			// next batch even though this flag is stale.
			Staged: c.isStagedTable(ref.Target),
		}
		// The table's write shape travels with the assignment so the worker's
		// collapse and the coordinator's DDL agree: the per-table write mode
		// (a hardcoded UPSERT here silently flipped append tables to upsert
		// semantics, audit #10) and the cast/metadata JSON (a dropped cast
		// policy made the worker create a table divergent from the
		// coordinator's, audit #8).
		tbl, ok := c.specForSource(ref.Source)
		if ok {
			ta.WriteMode = writeModeToPB(tbl.WriteMode.ChangeMode())
			cast, cerr := coreCastOf(tbl)
			if cerr != nil {
				return nil, cerr
			}
			if castB, err := json.Marshal(cast); err != nil {
				return nil, fmt.Errorf("coordinator: cast %s: %w", ref.Source, err)
			} else {
				ta.CastPolicy = castB
			}
			if metaB, err := json.Marshal(tbl.Metadata); err != nil {
				return nil, fmt.Errorf("coordinator: metadata %s: %w", ref.Source, err)
			} else {
				ta.Metadata = metaB
			}
			// The source read projection travels too: the worker builds the
			// snapshot chunk SELECT, so it needs the column list (#162) and
			// the compiled predicate (#163). The predicate is shipped as the
			// structured filter and compiled by the worker's source, so the
			// same code path resolves it in both modes.
			ta.ColumnFilter = tbl.ColumnFilter
			// onDelete travels with the assignment: the worker applies it to
			// an append-only table (issue #264).
			ta.OnDelete = string(tbl.OnDelete)
			// The chunk column travels too: a partitioned table is chunked
			// by its (single-column) key even without an explicit
			// chunkColumn, and the worker's chunker must match the
			// coordinator's or the key bounds would be read as CTID.
			ta.ChunkColumn = tbl.ChunkColumn
			if ta.ChunkColumn == "" && tbl.WorkerCount() > 1 && len(ref.PrimaryKey) == 1 {
				ta.ChunkColumn = ref.PrimaryKey[0]
			}
			if tbl.Filter != nil {
				filterB, ferr := json.Marshal(tbl.Filter)
				if ferr != nil {
					return nil, fmt.Errorf("coordinator: filter %s: %w", ref.Source, ferr)
				}
				ta.Filter = filterB
			}
		}
		// Broadcast reference joins travel with the assignment: the worker
		// owns the join, the coordinator only forwards the declaration.
		for _, t := range c.cfg.Spec.Tables {
			if t.Source != ref.Source {
				continue
			}
			for _, e := range t.Enrich {
				ta.Enrich = append(ta.Enrich, &pb.EnrichRef{
					Table:           e.Table,
					SourceUri:       e.Source.URI,
					SourceQuery:     e.Source.Query,
					On:              e.On,
					Select:          e.Select,
					As:              e.As,
					JoinType:        e.JoinType,
					Refresh:         e.Refresh,
					OnColdStart:     e.OnColdStart,
					BufferMaxEvents: int64(e.BufferLimits.MaxEvents),
					BufferMaxWait:   e.BufferLimits.MaxWait,
				})
			}
			break
		}
		assign.Tables = append(assign.Tables, ta)
	}
	return &pb.CoordinatorMessage{Msg: &pb.CoordinatorMessage_Assign{Assign: assign}}, nil
}

// specForSource finds the spec table for a source name.
func (c *Coordinator) specForSource(src string) (spec.Table, bool) {
	for _, t := range c.cfg.Spec.Tables {
		if t.Source == src {
			return t, true
		}
	}
	return spec.Table{}, false
}

// writeModeToPB maps the dataplane write mode onto the wire enum.
func writeModeToPB(m dataplane.WriteMode) pb.WriteMode {
	if m == dataplane.AppendMode {
		return pb.WriteMode_WRITE_MODE_APPEND
	}
	return pb.WriteMode_WRITE_MODE_UPSERT
}

// coreCastOf parses the spec's cast map into the policy the coordinator's
// DDL and the worker's writes must both apply. Parse errors are ignored the
// same way the collapsed runner ignores them (the cast is re-validated on
// the write path); the coordinator must not diverge from the runner.
// snapshotDSN returns the connection string the WORKER uses for the snapshot
// chunk SELECT: the scoped read-only SnapshotURI when set, else the full
// source URI (pre-scoping behavior). When the source is configured with the
// structured source.postgres block instead of a URI, the block is rendered to
// its libpq DSN here so the worker can still open the query connection.
//
// An SSH-tunneled structured source cannot be expressed by this DSN. When
// SnapshotURI is unset, assignmentFor ships the whole source.postgres block
// instead (Assignment.postgres, issue #170) and the worker builds the tunnel
// from it; the DSN below is then only a fallback the worker ignores. The
// worker never opens a replication connection, so a deployment can grant it a
// SELECT-only user and keep the replication credential coordinator-side
// (D-CD1).
func (c *Coordinator) snapshotDSN() (string, error) {
	if u := c.cfg.Spec.Source.SnapshotURI; u != "" {
		return u, nil
	}
	if u := c.cfg.Spec.Source.URI; u != "" {
		return u, nil
	}
	if pg := c.cfg.Spec.Source.Postgres; pg != nil {
		return pg.DSN(), nil
	}
	return "", nil
}

// surfaceWarnings logs the advisory warnings from source introspection and
// cast resolution at boot — never swallowed, matching the runner (the
// core.Warning contract is operator-facing).
func (c *Coordinator) surfaceWarnings(table string, warns []core.Warning) {
	for _, w := range warns {
		c.log.Warn("schema", "table", table, "warning", w.Message)
	}
}

// coreCastOf parses the table's declared cast policy, failing loud: a
// swallowed parse error would ship an empty policy, creating a sink table
// whose types diverge from the spec (audit #8). The source Introspect
// validates the same string at boot, so this cannot normally fail — but the
// coordinator must not degrade silently if it ever does.
func coreCastOf(tbl spec.Table) (core.CastPolicy, error) {
	cast, err := core.ParseCastPolicy(tbl.Cast)
	if err != nil {
		return core.CastPolicy{}, fmt.Errorf("coordinator: table %s cast: %w", tbl.Target, err)
	}
	return cast, nil
}
