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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

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

// lookup returns the row index for a typed join value and whether it hit.
func (s *snapshot) lookup(key any) (int32, bool) {
	if s.refTable == nil {
		return 0, false
	}
	idx, ok := s.keyIndex[normalizeKey(key)]
	return idx, ok
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
		// 100k default.
		switch cfg.JoinType {
		case "", "left", "left outer", "inner", "left semi", "left anti":
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
			if d, err := time.ParseDuration(cfg.BufferLimits.MaxWait); err != nil || d < 0 {
				return nil, fmt.Errorf("enrich: reference %q: bufferLimits.maxWait %q is not a non-negative duration", cfg.Table, cfg.BufferLimits.MaxWait)
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

// RefColumnsFor returns the FINAL destination names (renames applied) the
// given declarations inject, for explicit selects — the wire-schema extension
// a caller must apply BEFORE the pipeline starts, so every batch travels with
// the full column set and the drift check sees one stable shape. Empty for a
// wildcard select: this function does no I/O and the real names are only
// known once the reference query runs — see LoadWildcardColumns (no Stage)
// or Stage.LoadWildcards + Stage.RefColumns (with a Stage) for the
// wildcard-aware equivalent.
//
// Exported because the pipeline's three schema owners — the collapsed runner,
// the worker's assignment path and the coordinator's assignment builder —
// must all apply the SAME extension, and the coordinator builds no stage.
func RefColumnsFor(cfgs []spec.Enrich) []string {
	var out []string
	for _, cfg := range cfgs {
		if len(cfg.Select) == 1 && cfg.Select[0] == "*" {
			continue
		}
		for _, s := range cfg.Select {
			dest := cfg.Table + "." + s
			if as, ok := cfg.As[dest]; ok {
				dest = as
			}
			out = append(out, dest)
		}
	}
	return out
}

// LoadWildcardColumns runs the first load, synchronously, for every
// WILDCARD reference in cfgs and returns the complete RefColumns set for
// cfgs: RefColumnsFor's explicit-select names plus the real names just
// discovered for each wildcard reference. For a caller with no Stage of
// its own — the coordinator, which forwards enrich declarations to the
// worker instead of running the join itself, so it has no long-lived
// place to keep a Stage — but still needs the real wildcard columns before
// it extends a table's wire schema for sink DDL.
//
// Each wildcard reference gets its own short-lived loader (opened and
// closed here); this is deliberately independent of whatever Stage the
// worker later builds for the same declarations — the two processes share
// no connection, so the worker's real, long-lived load still runs a second
// time. Accepted, disclosed cost of the coordinator/worker split; not
// avoidable without a protocol change.
func LoadWildcardColumns(ctx context.Context, cfgs []spec.Enrich) ([]string, error) {
	out := RefColumnsFor(cfgs)
	for _, cfg := range cfgs {
		if !isWildcard(cfg) {
			continue
		}
		maxRows := cfg.MaxRows
		if maxRows == 0 {
			maxRows = DefaultMaxRows
		}
		l, err := NewSQLLoader(cfg.Source.URI, cfg.Source.Query, "", maxRows)
		if err != nil {
			return nil, fmt.Errorf("enrich: reference %q: %w", cfg.Table, err)
		}
		rec, err := l.Load(ctx)
		if err != nil {
			_ = l.Close()
			return nil, fmt.Errorf("enrich: reference %q: %w", cfg.Table, err)
		}
		snap, err := buildImage(&refJoin{cfg: cfg, onRef: onRefOf(cfg)}, rec)
		rec.Release()
		if err != nil {
			_ = l.Close()
			return nil, fmt.Errorf("enrich: reference %q: %w", cfg.Table, err)
		}
		if err := l.Close(); err != nil {
			return nil, fmt.Errorf("enrich: reference %q: close: %w", cfg.Table, err)
		}
		for _, d := range snap.dests {
			out = append(out, d.as)
		}
	}
	return out, nil
}

// onRefOf returns the reference-side column name of cfg's single join
// pair — New already validates cfg.On has exactly one entry; this mirrors
// that resolution for a throwaway refJoin used only for column discovery.
func onRefOf(cfg spec.Enrich) string {
	for _, ref := range cfg.On {
		return ref
	}
	return ""
}

// AddColumns returns cs extended with exactly the given destination names
// (deduplicated; nullable strings — the registered type decision until
// CR-069 resolves real types). The sibling of AddRefColumns for a caller
// that already resolved the real destination names itself — e.g. via
// LoadWildcardColumns, or Stage.RefColumns() after LoadWildcards — instead
// of recomputing them from cfgs (which RefColumnsFor cannot do for a
// wildcard select).
func AddColumns(cs core.Schema, dests []string) core.Schema {
	for _, dest := range dests {
		if _, exists := cs.Column(dest); !exists {
			cs.Columns = append(cs.Columns, core.Column{
				Name: dest,
				Type: core.ColumnType{Kind: core.KindString, Nullable: true},
			})
		}
	}
	return cs
}

// AddRefColumns returns cs extended with the declarations' EXPLICIT-select
// reference destination columns — see RefColumnsFor for why a wildcard
// select contributes nothing here. Callers apply it to every schema shape
// that feeds a sink table or a drift check BEFORE the pipeline starts:
// without it, the first enriched batch carries columns the table lacks and
// every sink silently drops them (they project by the table's own columns,
// never by the wire's). A caller with a wildcard reference must resolve its
// real columns first (LoadWildcardColumns, or Stage.RefColumns() after
// LoadWildcards) and use AddColumns instead.
func AddRefColumns(cs core.Schema, cfgs []spec.Enrich) core.Schema {
	return AddColumns(cs, RefColumnsFor(cfgs))
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

// refreshInterval resolves the re-read cadence.
func (rj *refJoin) refreshInterval() (time.Duration, error) {
	if rj.cfg.Refresh == "" {
		return DefaultRefresh, nil
	}
	d, err := time.ParseDuration(rj.cfg.Refresh)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("enrich: reference %q: refresh %q is not a positive duration", rj.cfg.Table, rj.cfg.Refresh)
	}
	return d, nil
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

// Start launches the first load (skipped for a reference LoadWildcards
// already warmed) and the refresh loop per reference. For any reference
// Start itself loads for the first time, this is asynchronous: the
// pipeline boots without waiting for it, and the cold-start policy governs
// whatever arrives early.
func (s *Stage) Start(ctx context.Context) {
	for _, rj := range s.refs {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			t := time.NewTicker(rj.refreshEvery)
			defer t.Stop()
			// First load immediately, UNLESS LoadWildcards already warmed
			// this reference — a wildcard ref loaded synchronously at boot
			// must not pay a second, redundant query here. A failure is
			// sticky (it surfaces on the first event) and the ticker keeps
			// retrying, so a reference database that boots late still comes
			// hot.
			if rj.snap.Load() == nil {
				rj.refresh(ctx, s.log)
			}
			for {
				select {
				case <-ctx.Done():
					return
				case <-s.stopped:
					return
				case <-t.C:
					rj.refresh(ctx, s.log)
				}
			}
		}()
	}
}

// Stop ends the refresh loops and closes SQL loaders. Idempotent: a
// concurrent second call is a no-op (a double close of the stopped channel
// panics).
func (s *Stage) Stop() {
	s.once.Do(func() {
		close(s.stopped)
		s.wg.Wait()
		for _, rj := range s.refs {
			if rj.loader != nil {
				_ = rj.loader.Close()
			}
		}
		// The current snapshot's refTable is left to GC — the default
		// allocator's buffers are plain Go memory, and a concurrent join
		// may still hold the pointer past Stop in a badly-ordered shutdown.
	})
}

// refresh re-reads the reference and swaps the snapshot atomically. The
// FIRST successful load also resolves the projection and flips hot —
// parking the cold-start queue for the next Apply to release.
func (rj *refJoin) refresh(ctx context.Context, log *slog.Logger) {
	if rj.loader == nil {
		maxRows := rj.cfg.MaxRows
		if maxRows == 0 {
			maxRows = DefaultMaxRows
		}
		l, err := NewSQLLoader(rj.cfg.Source.URI, rj.cfg.Source.Query, rj.onRef, maxRows)
		if err != nil {
			rj.setFirstErr(err)
			log.Error("enrich: reference loader failed (will retry)", "reference", rj.cfg.Table, "err", err)
			return
		}
		rj.loader = l
	}
	rec, err := rj.loader.Load(ctx)
	if err != nil {
		rj.setFirstErr(err)
		log.Error("enrich: reference load failed (keeping previous image, will retry)", "reference", rj.cfg.Table, "err", err)
		return
	}
	defer rec.Release()
	snap, err := buildImage(rj, rec)
	if err != nil {
		rj.setFirstErr(err)
		log.Error("enrich: reference image rejected", "reference", rj.cfg.Table, "err", err)
		return
	}
	// P4: the join never coerces. A non-empty reference whose join column's
	// Arrow type differs from the event join column's is a rejected load —
	// the operator casts in the reference query.
	if snap.refTable != nil && rj.onKeyType != nil {
		refKeyType := snap.refTable.Column(0).DataType()
		if !arrow.TypeEqual(refKeyType, rj.onKeyType) {
			jerr := fmt.Errorf(
				"enrich: reference %q: join column %q is %s but the event column %q is %s — cast in the reference query so both sides match",
				rj.cfg.Table, rj.onRef, refKeyType, rj.onKey, rj.onKeyType)
			rj.setFirstErr(jerr)
			log.Error("enrich: reference join column type mismatch", "reference", rj.cfg.Table, "err", jerr)
			return
		}
	}
	// Atomic swap: the hot path reads this with a single Load(), no lock.
	// The displaced snapshot's refTable is NOT released here — a concurrent
	// ColumnarJoin may hold the old pointer, and the default (Go) allocator
	// reclaims the buffers by GC once no reader references them. Explicit
	// release only happens in Stop, when no join can be in flight.
	rj.snap.Store(snap)
	// refDests was only ever set once, at New, from RefColumnsFor — empty
	// for a wildcard select, because the real names are unknown without I/O.
	// Now that a load just resolved them (buildImage's dests, star or not),
	// refresh it here so Stage.RefColumns() (read by every schema owner)
	// stops lying about a wildcard reference the moment it goes hot. Guarded
	// by mu — the same lock firstErr already uses — because refresh's
	// goroutine (Start's ticker) is the only writer, but RefColumns() can
	// read concurrently from another goroutine.
	names := make([]string, len(snap.dests))
	for i, d := range snap.dests {
		names[i] = d.as
	}
	rj.mu.Lock()
	rj.refDests = names
	// Clear the sticky first-load error on ANY success: "Sticky ONLY before
	// the first success" means a transient boot failure must not poison the
	// stage forever once the reference comes hot (audit #1).
	rj.firstErr = nil
	rj.mu.Unlock()
	log.Info("enrich: reference loaded", "reference", rj.cfg.Table, "rows", len(snap.keyIndex))
}

// setFirstErr records a load failure. Sticky ONLY before the first
// success: a broken join declaration is permanent, not transient — the
// pipeline pauses on the first event instead of silently passing
// unenriched forever. Once hot, a failed refresh is tolerated (the
// previous image stays authoritative) and refresh clears the error on the
// next success.
func (rj *refJoin) setFirstErr(err error) {
	rj.mu.Lock()
	defer rj.mu.Unlock()
	if rj.snap.Load() == nil && rj.firstErr == nil {
		rj.firstErr = err
	}
}

// buildImage resolves the projection once and indexes the reference with
// ONLY the projected columns, already carrying their FINAL names (as
// renames at load, not per event). Semantics:
//
//   - select lists the reference columns the event receives; a listed
//     column missing from the query result is a rejected load.
//   - select ["*"] projects everything including the join column. The
//     unrenamed join column preserves the source's own value (the
//     reference value is projected but not applied); use "as" to
//     inject the reference value under a distinct name.
//   - as renames a selected column; unrenamed columns get a
//     table-prefixed name (e.g., "produtos.id") to avoid silent
//     collisions when multiple references inject columns with the same
//     name. This matches Spark DataFrame semantics.
//
// The on-reference column is validated to exist and be unique; it is
// projected only when explicitly listed in select or under "*".
func buildImage(rj *refJoin, rec arrow.RecordBatch) (*snapshot, error) {
	star := len(rj.cfg.Select) == 1 && rj.cfg.Select[0] == "*"

	// A legitimately empty reference (every join misses) is not a broken
	// load (audit #3): go hot with an empty index. The on/select columns
	// cannot be validated against an empty result; a later non-empty
	// refresh does.
	if rec == nil || rec.NumRows() == 0 {
		return &snapshot{keyIndex: map[any]int32{}}, nil
	}

	maxRows := rj.cfg.MaxRows
	if maxRows == 0 {
		maxRows = DefaultMaxRows
	}
	if int(rec.NumRows()) > maxRows {
		return nil, fmt.Errorf(
			"enrich: reference %q: %d rows exceeds maxRows %d — the broadcast join holds the whole reference in RAM; raise maxRows on the reference declaration or shrink the reference query",
			rj.cfg.Table, rec.NumRows(), maxRows)
	}

	schema := rec.Schema()
	colIdx := make(map[string]int, schema.NumFields()) // P4-config: name → column index
	for i := 0; i < schema.NumFields(); i++ {
		colIdx[schema.Field(i).Name] = i
	}
	if _, ok := colIdx[rj.onRef]; !ok {
		return nil, fmt.Errorf("on: reference column %q is not in the query result", rj.onRef)
	}
	if !star {
		for _, s := range rj.cfg.Select {
			if _, ok := colIdx[s]; !ok {
				return nil, fmt.Errorf("select: reference column %q is not in the query result", s)
			}
		}
	}

	// Resolve the projection: reference column → destination name. Under
	// the star it is every column (including the join column). Unrenamed
	// columns get a table-prefixed name (Spark-style); "as" overrides.
	var dests []dest
	destSeen := map[string]bool{}
	addDest := func(refCol string) error {
		for _, d := range dests {
			if d.ref == refCol {
				return nil
			}
		}
		name := fmt.Sprintf("%s.%s", rj.cfg.Table, refCol)
		if as, ok := rj.cfg.As[name]; ok {
			name = as
		}
		if destSeen[name] {
			return fmt.Errorf("reference %q: two columns project to the same destination %q", rj.cfg.Table, name)
		}
		destSeen[name] = true
		dests = append(dests, dest{ref: refCol, as: name})
		return nil
	}
	if star {
		for i := 0; i < schema.NumFields(); i++ {
			if err := addDest(schema.Field(i).Name); err != nil {
				return nil, err
			}
		}
	} else {
		for _, s := range rj.cfg.Select {
			if err := addDest(s); err != nil {
				return nil, err
			}
		}
	}
	sort.Slice(dests, func(i, j int) bool { return dests[i].as < dests[j].as })

	// Build the refTable: column 0 = join key, columns 1..N = dests. Each
	// column is retained from the loaded record (zero-copy).
	joinCol := rec.Column(colIdx[rj.onRef])
	fields := make([]arrow.Field, len(dests)+1)
	cols := make([]arrow.Array, len(dests)+1)
	fields[0] = arrow.Field{Name: rj.onRef, Type: joinCol.DataType(), Nullable: true}
	joinCol.Retain()
	cols[0] = joinCol
	for i, d := range dests {
		c := rec.Column(colIdx[d.ref])
		fields[i+1] = arrow.Field{Name: d.as, Type: c.DataType(), Nullable: true}
		c.Retain()
		cols[i+1] = c
	}
	refTable := array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, rec.NumRows())
	for _, c := range cols {
		c.Release() // refTable holds its own refs
	}

	// keyIndex: typed join value → row. A NULL join key is unreachable
	// (audit #7). A duplicate is a rejected load — the error cites the
	// column and the count, never a value, never a position (P5).
	keyIndex := make(map[any]int32, int(rec.NumRows()))
	kc := refTable.Column(0)
	dupCount := 0
	//allow:rowloop snapshot key index; built once per refresh, not per batch.
	for i := 0; i < int(rec.NumRows()); i++ {
		if kc.IsNull(i) {
			continue
		}
		k := normalizeKey(arrowValueAt(kc, i))
		if _, dup := keyIndex[k]; dup {
			dupCount++
			continue
		}
		keyIndex[k] = int32(i)
	}
	if dupCount > 0 {
		refTable.Release()
		return nil, fmt.Errorf("enrich: reference %q: %d duplicate join key(s) in column %q",
			rj.cfg.Table, dupCount, rj.onRef)
	}

	return &snapshot{refTable: refTable, dests: dests, keyIndex: keyIndex}, nil
}

// normalizeKey collapses the driver's int-width variants so the same
// logical key from a SQL reference (MySQL UNSIGNED → uint64) and from the
// binlog (int64) hits the same index entry — the int family shares a
// non-negative space. string vs int stay distinct; float widths stay
// distinct (0.1f ≠ 0.1). []byte becomes string. A cross-type mismatch
// between reference and wire is caught at boot (P4), so this only has to
// reconcile the numeric-width noise.
func normalizeKey(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	case int:
		if t >= 0 {
			return uint64(t)
		}
		return int64(t)
	case int32:
		if t >= 0 {
			return uint64(t)
		}
		return int64(t)
	case int64:
		if t >= 0 {
			return uint64(t)
		}
		return t
	case uint32:
		return uint64(t)
	case uint:
		return uint64(t)
	default:
		return v
	}
}

// arrowValueAt returns the canonical Go value at row i of an Arrow column,
// nil when null. Only the reference value types are handled.
func arrowValueAt(col arrow.Array, i int) any {
	if col.IsNull(i) {
		return nil
	}
	switch a := col.(type) {
	case *array.String:
		return a.Value(i)
	case *array.Binary:
		return a.Value(i)
	case *array.Boolean:
		return a.Value(i)
	case *array.Int64:
		return a.Value(i)
	case *array.Uint64:
		return a.Value(i)
	case *array.Float64:
		return a.Value(i)
	case *array.Timestamp:
		return a.Value(i).ToTime(arrow.Microsecond)
	default:
		return nil
	}
}

func (rj *refJoin) stickyErr() error {
	rj.mu.Lock()
	defer rj.mu.Unlock()
	return rj.firstErr
}

// refDestNames reads refDests under mu — refresh can write it concurrently
// (LoadWildcards, or the async refresh ticker) with the join's cold-start
// fallback path reading it, so this is not a lock-free field any more.
func (rj *refJoin) refDestNames() []string {
	rj.mu.Lock()
	defer rj.mu.Unlock()
	return rj.refDests
}
