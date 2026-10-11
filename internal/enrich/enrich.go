// Package enrich is the table-level enrichment stage: a columnar broadcast
// hash join against small reference tables (CR-069). The reference is read
// WHOLE into a worker-local typed Arrow table (one per reference) plus a
// key→row index; ColumnarJoin matches a whole RecordBatch against it in
// O(rows) — Arrow in, Arrow out, no rowchange in the path — and the
// snapshot is swapped atomically on a full periodic re-read. There is no
// lookup per event against a database, no shuffle, no windowed state — the
// reference is small by contract, and if it stops being small the answer
// is a different tool, not a cache.
//
// Cold start (no snapshot loaded yet) is decided per batch: onColdStart=drop
// drops the whole batch, buffer/pass miss every row. The row path's per-row
// cold-start buffer is gone.
//
// Enrichment is point-in-time: the enriched columns depend on the
// reference image when the event passed, and the image is a snapshot, not
// CDC — a replay may therefore enrich with a different (newer) image.
// Pipelines with enrich report enrichment "point-in-time"; the source
// columns and the position stay deterministic either way.
//
// Column namespacing follows Spark DataFrame semantics: unrenamed columns
// are automatically prefixed with "{table}.{column}" (e.g., "users.name",
// "products.id") to prevent silent collisions when multiple references
// inject columns with the same name. The "as" map is keyed by the prefixed
// name (e.g., {"users.name": "user_name"}) for consistency.
package enrich

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/spec"
)

// DefaultRefresh re-reads a reference this often when the spec is silent.
//
// Behavior (not a limitation): a reference that boots late is first loaded
// within one refresh interval, and a reference that goes down mid-run comes
// hot again within one interval. At the default this is up to 5 minutes —
// size cold-start expectations accordingly, or set `refresh` shorter.
const DefaultRefresh = 5 * time.Minute

// DefaultMaxRows caps a reference image's row count when the spec is
// silent (MaxRows == 0). The broadcast join holds the reference whole —
// refTable's Arrow buffers plus one keyIndex entry per row — so this is
// the default backstop between "small by contract" and an unannounced
// OOM. Override per reference via spec.Enrich.MaxRows when the reference
// is legitimately larger (or smaller) than this.
const DefaultMaxRows = 5_000_000

// coldStartPolicy resolves the onColdStart grammar. The columnar join
// decides cold start PER BATCH: coldDrop returns the whole batch as
// dropped; coldBuffer and coldPass both miss every row (the per-row buffer
// the row path kept is gone — see website/docs/reference/semantics.md).
type coldStartPolicy int

const (
	coldBuffer coldStartPolicy = iota // default — miss every row until first load
	coldPass                          // identical to coldBuffer under the columnar join
	coldDrop                          // drop the whole batch until first load
)

// Stage enriches one table's changes across its declared references, in
// declaration order. ColumnarJoin runs on the worker's batcher goroutine;
// the refresh goroutines only swap reference snapshots atomically.
type Stage struct {
	refs []*refJoin

	stopped chan struct{}
	once    sync.Once // guards Stop against a concurrent double call
	wg      sync.WaitGroup
	log     *slog.Logger

	// Counters are the evidence the stage ran, and what it cost.
	misses       atomic.Int64 // left-join misses (rows that passed with NULLs)
	innerDropped atomic.Int64 // rows an inner join discarded

	// metrics is optional; when set, counters are mirrored to Prometheus.
	metrics *enrichMetrics
}

// enrichMetrics wraps the Prometheus counters for the enrichment stage.
// Nil-safe: if the pointer is nil, Inc calls are no-ops.
type enrichMetrics struct {
	misses  func(table, ref string)
	dropped func(table, ref string)
}

// snapshot is the immutable reference state swapped atomically on refresh.
// A single atomic.Load gives the hot path a consistent view — no lock, no
// partial reads.
type snapshot struct {
	// refTable holds the projected reference: column 0 is the join key,
	// columns 1..N are the dests in `dests` order. Typed Arrow — the
	// executor Take()s from it directly.
	refTable arrow.RecordBatch
	dests    []dest // projected reference columns, sorted by dest name
	// keyIndex maps a typed join value (the canonical Go value read from
	// the join-key column: string / int64 / uint64 / float64 / []byte
	// rendered as string / time.Time) to its row in refTable. Built once
	// per refresh — P4-config, not per-batch data. A cross-type mismatch
	// between reference and wire fails at boot (P4), so the batch side
	// reads the same Go type and hits here directly.
	keyIndex map[any]int32
}

// refType returns the wire Arrow type of a destination column, or nil if
// the destination is not projected by this snapshot.
func (s *snapshot) refType(as string) arrow.DataType {
	for i, d := range s.dests {
		if d.as == as {
			return s.refTable.Column(i + 1).DataType()
		}
	}
	return nil
}

// refJoin is one reference: its config and the hot lookup snapshot. The
// snapshot is swapped atomically — an in-flight batch finishes against the
// old image, never a half-built one. The mutex only protects the sticky
// first-load error; the hot path is lock-free.
type refJoin struct {
	cfg       spec.Enrich
	loader    Loader
	onKey     string         // event column name
	onKeyType arrow.DataType // event join column's wire Arrow type (P4 boot check)
	onRef     string         // reference column name
	policy    coldStartPolicy
	// refreshEvery is the validated re-read cadence, resolved once in New
	// so Start never re-parses (and never ignores a parse error).
	refreshEvery time.Duration
	// refDests are the FINAL destination names (renames applied) this
	// reference injects. Known at construction for explicit selects; for a
	// wildcard select, empty until refresh's first successful load
	// resolves the real names (see New, LoadWildcards). ColumnarJoin's miss
	// fallback writes NULLs into them; Stage.RefColumns exposes them so the
	// caller can extend the table's wire schema. Guarded by mu once
	// refresh can write it concurrently with a RefColumns() reader.
	refDests []string

	// snap is the hot-path state: image + dests + refTypes, swapped
	// atomically on refresh. Load() is lock-free; Store() is called only by
	// refresh (one goroutine per reference).
	snap atomic.Pointer[snapshot]

	// mu protects the sticky first-load error and refDests — both written
	// rarely (construction, and each successful refresh), never read on the
	// per-batch hot path (RefColumns is a boot/schema-owner call).
	mu       sync.Mutex
	firstErr error // sticky: a broken reference surfaces on the first batch

	// misses is the stage-level counter, shared by reference pointers.
	misses  *atomic.Int64
	metrics *enrichMetrics // shared with Stage; nil-safe
}

type dest struct {
	ref string // reference column
	as  string // destination event column
}

// New builds the stage and validates the declarations against the event's
// schema — the checks spec.Validate cannot make without schemas: the join's
// event side must exist, and the grammar is re-checked so a spec that
// reached us unvalidated fails loudly here. The event schema also carries
// the join column's type: when a reference loads, its join column's Arrow
// type is compared against the event join column's, and a mismatch fails
// the load loudly (P4 — the operator casts in the reference query; the
// join never coerces).
//
// Wildcard: with select ["*"] the reference destinations are only known at
// load time, so RefColumns is empty until the first load resolves them.
// The caller closes this drift window by calling LoadWildcards
// synchronously, right after New, before extending the table's wire
// schema and before Start — see LoadWildcards.
func New(cfgs []spec.Enrich, eventSchema core.Schema, log *slog.Logger) (*Stage, error) {
	if len(cfgs) == 0 {
		return nil, errors.New("enrich: no references declared")
	}
	evCols := make(map[string]bool, len(eventSchema.Columns))
	for _, c := range eventSchema.Columns {
		evCols[c.Name] = true
	}
	s := &Stage{stopped: make(chan struct{}), log: log}
	if s.log == nil {
		s.log = slog.Default()
	}
	// Cross-ref destination collision: two different refs projecting to
	// the same destination name silently overwrite each other.
	seenDests := make(map[string]string) // destination → first ref table
	for _, cfg := range cfgs {
		if len(cfg.On) != 1 {
			return nil, fmt.Errorf("enrich: reference %q: on: exactly one join pair is supported today", cfg.Table)
		}
		// Grammar validated at boot, not on the first event (audit #10): an
		// unknown join type silently became left, a bad maxWait silently
		// became "no limit", and a negative maxEvents silently became the
		// 100k default. joinType has no universal miss policy (issue #66):
		// spec.Validate() rejects "" as required, so this must too, or a
		// spec built and run without going through Validate() (a caller
		// constructing *spec.Spec directly) would silently get "left".
		switch cfg.JoinType {
		case "left", "left outer", "inner", "left semi", "left anti":
		default:
			return nil, fmt.Errorf("enrich: reference %q: join_type %q unknown (want left | left outer | inner | left semi | left anti)", cfg.Table, cfg.JoinType)
		}
		semiAnti := cfg.JoinType == "left semi" || cfg.JoinType == "left anti"
		if len(cfg.Select) == 0 && !semiAnti {
			return nil, fmt.Errorf("enrich: reference %q: select is required — declare the reference columns the event receives (\"*\" injects all)", cfg.Table)
		}
		if len(cfg.Select) > 0 && semiAnti {
			return nil, fmt.Errorf("enrich: reference %q: %s emits no reference columns — remove select", cfg.Table, cfg.JoinType)
		}
		rj := &refJoin{cfg: cfg}
		// bufferLimits is still validated as grammar (a spec that reached us
		// unvalidated fails loudly), but the cold-start row buffer it tuned
		// is gone: the columnar join decides cold start per batch, not per
		// row. See website/docs/reference/semantics.md.
		if cfg.BufferLimits.MaxWait != "" {
			if d, err := time.ParseDuration(cfg.BufferLimits.MaxWait); err != nil || d <= 0 {
				return nil, fmt.Errorf("enrich: reference %q: bufferLimits.maxWait %q is not a positive duration", cfg.Table, cfg.BufferLimits.MaxWait)
			}
		}
		if cfg.BufferLimits.MaxEvents < 0 {
			return nil, fmt.Errorf("enrich: reference %q: bufferLimits.maxEvents %d must be >= 0", cfg.Table, cfg.BufferLimits.MaxEvents)
		}
		if cfg.MaxRows < 0 {
			return nil, fmt.Errorf("enrich: reference %q: maxRows %d must be >= 0", cfg.Table, cfg.MaxRows)
		}
		if cfg.MaxRows > math.MaxInt32 {
			return nil, fmt.Errorf("enrich: reference %q: maxRows %d exceeds the int32 row-index limit (%d) — the broadcast join indexes rows as int32", cfg.Table, cfg.MaxRows, math.MaxInt32)
		}
		for ev, ref := range cfg.On {
			if !evCols[ev] {
				return nil, fmt.Errorf("enrich: reference %q: on: event column %q is not in the table's schema", cfg.Table, ev)
			}
			rj.onKey, rj.onRef = ev, ref
			// The event join column's wire Arrow type — refresh compares the
			// reference's join column against it (P4).
			if col, ok := eventSchema.Column(ev); ok {
				dt, aerr := transport.KindToArrow(col.Type)
				if aerr != nil {
					return nil, fmt.Errorf("enrich: reference %q: join column %q has no wire type: %w", cfg.Table, ev, aerr)
				}
				rj.onKeyType = dt
			}
		}
		// Default destinations claim seenDests too (RV-07): a rename from
		// ANOTHER reference must not steal a name a default projection
		// ("<table>.<col>") would produce. Wildcard stays out — with "*"
		// the destinations are only known at load time, and the per-ref
		// destSeen check in buildImage covers that case.
		// refDests carries the FINAL destination names (rename applied) —
		// what the miss fallback writes NULLs into and what RefColumns
		// exposes for the wire. RefColumnsFor is the single source of this
		// computation: the coordinator applies the same extension without
		// building a stage.
		rj.refDests = RefColumnsFor([]spec.Enrich{cfg})
		if star := len(cfg.Select) == 1 && cfg.Select[0] == "*"; !star {
			for _, s := range cfg.Select {
				dest := cfg.Table + "." + s
				if _, renamed := cfg.As[dest]; renamed {
					continue // this ref's own rename overrides the default
				}
				if firstRef, exists := seenDests[dest]; exists && firstRef != cfg.Table {
					return nil, fmt.Errorf("enrich: destination %q is claimed by reference %q and also by reference %q", dest, firstRef, cfg.Table)
				}
				seenDests[dest] = cfg.Table
			}
		}
		// The as map is keyed by the prefixed name ("users.name"); a dangling
		// or unselected key is a spec typo that would silently no-op (audit
		// #10/#11). Destination names must not collide with the event's own
		// columns or with each other — a rename overwriting a user column
		// silently destroyed the event value.
		prefix := cfg.Table + "."
		seenAs := make(map[string]bool, len(cfg.As))
		for name, as := range cfg.As {
			if !strings.HasPrefix(name, prefix) {
				return nil, fmt.Errorf("enrich: reference %q: as key %q must be prefixed %q", cfg.Table, name, prefix)
			}
			refCol := strings.TrimPrefix(name, prefix)
			selected := len(cfg.Select) == 1 && cfg.Select[0] == "*"
			for _, s := range cfg.Select {
				if s == refCol {
					selected = true
					break
				}
			}
			if !selected {
				return nil, fmt.Errorf("enrich: reference %q: as key %q is not in select", cfg.Table, name)
			}
			if evCols[as] {
				return nil, fmt.Errorf("enrich: reference %q: as %q collides with the event column of the same name", cfg.Table, as)
			}
			if seenAs[as] {
				return nil, fmt.Errorf("enrich: reference %q: two renames project to the same destination %q", cfg.Table, as)
			}
			seenAs[as] = true
			// Cross-ref collision: another ref already claims this destination.
			if firstRef, exists := seenDests[as]; exists {
				return nil, fmt.Errorf("enrich: destination %q is claimed by reference %q and also by reference %q", as, firstRef, cfg.Table)
			}
			seenDests[as] = cfg.Table
		}
		switch cfg.OnColdStart {
		case "", "buffer":
			rj.policy = coldBuffer
		case "pass":
			rj.policy = coldPass
		case "drop":
			rj.policy = coldDrop
		default:
			return nil, fmt.Errorf("enrich: reference %q: onColdStart %q unknown (want buffer | pass | drop)", cfg.Table, cfg.OnColdStart)
		}
		interval, err := rj.refreshInterval()
		if err != nil {
			return nil, err
		}
		rj.refreshEvery = interval
		rj.misses = &s.misses
		s.refs = append(s.refs, rj)
	}
	return s, nil
}

// SetMetrics wires Prometheus counters into the stage. Call after New;
// nil-safe (metrics pointer is stored, not dereferenced).
func (s *Stage) SetMetrics(misses, dropped func(table, ref string)) {
	m := &enrichMetrics{misses: misses, dropped: dropped}
	s.metrics = m
	for _, rj := range s.refs {
		rj.metrics = m
	}
}

// Ready reports whether every reference has completed its first load. Until
// then ColumnarJoin runs the cold-start policy (drop the batch, or miss
// every row). Callers may poll this at boot to avoid a burst of cold-start
// misses; the pipeline does not require it.
func (s *Stage) Ready() bool {
	for _, rj := range s.refs {
		if rj.snap.Load() == nil {
			return false
		}
	}
	return true
}

// RefColumns returns the destination columns this stage injects — resolved
// at construction for explicit selects; for a wildcard select, empty until
// the reference's first load resolves the real names (refresh updates them
// — see LoadWildcards to force that load synchronously, before Start, so
// RefColumns is correct from the first batch). The caller adds them to the
// table's WIRE schema as nullable columns BEFORE the pipeline starts, so
// every batch travels with the full column set and the drift check sees
// one stable shape. A left-join miss writes NULL into these columns; the
// schema declares them nullable, so the wire is never violated.
func (s *Stage) RefColumns() []string {
	var out []string
	for _, rj := range s.refs {
		out = append(out, rj.refDestNames()...)
	}
	return out
}

// isWildcard reports whether cfg selects every reference column ("*").
func isWildcard(cfg spec.Enrich) bool {
	return len(cfg.Select) == 1 && cfg.Select[0] == "*"
}

// LoadWildcards runs the first load, synchronously, for every WILDCARD
// reference in the stage — explicit-select references are untouched; they
// keep Start's fully asynchronous first load. Call after New (and any
// UseLoader overrides), before Start: closing #56(a)'s schema-drift gap
// means the caller reads RefColumns() (now resolved for wildcard too)
// before extending the table's wire schema, which must happen before the
// pipeline starts accepting batches.
//
// Returns the first error encountered, wrapped with the failing
// reference's table name — the caller should fail boot rather than call
// Start, exactly like any other boot-time schema error. References are
// tried in declaration order so a repeated failure always names the same
// reference first.
func (s *Stage) LoadWildcards(ctx context.Context) error {
	for _, rj := range s.refs {
		if !isWildcard(rj.cfg) {
			continue
		}
		rj.refresh(ctx, s.log)
		if err := rj.stickyErr(); err != nil {
			return fmt.Errorf("enrich: reference %q: %w", rj.cfg.Table, err)
		}
	}
	return nil
}

// UseLoader overrides the SQL loader for one reference (test seam). Must be
// called before Start — the loader field has no lock.
func (s *Stage) UseLoader(refTable string, l Loader) error {
	for _, rj := range s.refs {
		if rj.cfg.Table == refTable {
			rj.loader = l
			return nil
		}
	}
	return fmt.Errorf("enrich: no reference named %q", refTable)
}

// arrowValueAt was removed: the reference index is built with readArrowValue
// (columnar.go) so the index builder and the batch lookup read a column's
// value identically — arrowValueAt lacked the Int32/Float32 cases
// readArrowValue has (issue #640).
