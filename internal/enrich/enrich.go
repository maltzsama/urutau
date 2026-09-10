// Package enrich is the table-level enrichment stage: a columnar broadcast
// hash join against small reference tables (CR-069). The reference is read
// WHOLE into a worker-local map (one map per reference), ColumnarJoin
// matches a whole RecordBatch against it in O(rows) — Arrow in, Arrow out,
// no rowchange in the path — and the map is swapped atomically on a full
// periodic re-read. There is no lookup per event against a database, no
// shuffle, no windowed state — the reference is small by contract, and if
// it stops being small the answer is a different tool, not a cache.
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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/spec"
)

// DefaultRefresh re-reads a reference this often when the spec is silent.
//
// Behavior (not a limitation): a reference that boots late is first loaded
// within one refresh interval, and a reference that goes down mid-run comes
// hot again within one interval. At the default this is up to 5 minutes —
// size cold-start expectations accordingly, or set `refresh` shorter.
const DefaultRefresh = 5 * time.Minute

// coldStartPolicy resolves the onColdStart grammar. The columnar join
// decides cold start PER BATCH: coldDrop returns the whole batch as
// dropped; coldBuffer and coldPass both miss every row (the per-row buffer
// the row path kept is gone — see docs/semantics.md).
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
// Bundling image, dests and refTypes ensures the hot path reads a consistent
// view with a single atomic.Load — no lock, no partial reads.
type snapshot struct {
	image map[string]map[string]any // normalized join key → reference row
	dests []dest                    // projected reference columns
	// refTypes is the Arrow type each destination column lands as on the
	// wire, derived from the first non-null value seen for it in the load.
	// A destination that was NULL in every reference row gets a String
	// placeholder; the next load carrying a real value swaps the whole
	// snapshot and corrects it (a reference column changing Arrow type
	// between loads — the sink tolerates the widening or the first load is
	// synchronous).
	refTypes map[string]arrow.DataType
}

// refJoin is one reference: its config and the hot lookup snapshot. The
// snapshot is swapped atomically — an in-flight batch finishes against the
// old image, never a half-built one. The mutex only protects the sticky
// first-load error; the hot path is lock-free.
type refJoin struct {
	cfg    spec.Enrich
	loader Loader
	onKey  string // event column name
	onRef  string // reference column name
	policy coldStartPolicy
	// refreshEvery is the validated re-read cadence, resolved once in New
	// so Start never re-parses (and never ignores a parse error).
	refreshEvery time.Duration
	// refDests are the FINAL destination names (renames applied) this
	// reference injects, known at construction for explicit selects.
	// ColumnarJoin's miss fallback writes NULLs into them; Stage.RefColumns
	// exposes them so the caller can extend the table's wire schema. Empty
	// for a wildcard select — those destinations are only known at load
	// time (the documented exception in New).
	refDests []string

	// snap is the hot-path state: image + dests + refTypes, swapped
	// atomically on refresh. Load() is lock-free; Store() is called only by
	// refresh (one goroutine per reference).
	snap atomic.Pointer[snapshot]

	// mu protects the sticky first-load error — written rarely (first
	// load), never on the hot path.
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
// known columns — the checks spec.Validate cannot make without schemas:
// the join's event side must exist, and the grammar is re-checked so a
// spec that reached us unvalidated fails loudly here.
//
// Wildcard exception: with select ["*"] the reference destinations are only
// known at load time, so RefColumns is empty and a miss before the first
// non-empty load injects no columns — the table's schema can drift between
// the first batches and the first load. Documented trade-off: the
// cold-start policy governs it, or declare the columns explicitly.
func New(cfgs []spec.Enrich, eventColumns []string, log *slog.Logger) (*Stage, error) {
	if len(cfgs) == 0 {
		return nil, errors.New("enrich: no references declared")
	}
	evCols := make(map[string]bool, len(eventColumns))
	for _, c := range eventColumns {
		evCols[c] = true
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
		if len(cfg.Select) == 0 {
			return nil, fmt.Errorf("enrich: reference %q: select is required — declare the reference columns the event receives (\"*\" injects all)", cfg.Table)
		}
		// Grammar validated at boot, not on the first event (audit #10): an
		// unknown join type silently became left, a bad maxWait silently
		// became "no limit", and a negative maxEvents silently became the
		// 100k default.
		switch cfg.JoinType {
		case "", "left", "inner":
		default:
			return nil, fmt.Errorf("enrich: reference %q: join_type %q unknown (want left | inner)", cfg.Table, cfg.JoinType)
		}
		rj := &refJoin{cfg: cfg}
		// bufferLimits is still validated as grammar (a spec that reached us
		// unvalidated fails loudly), but the cold-start row buffer it tuned
		// is gone: the columnar join decides cold start per batch, not per
		// row. See docs/semantics.md.
		if cfg.BufferLimits.MaxWait != "" {
			if d, err := time.ParseDuration(cfg.BufferLimits.MaxWait); err != nil || d < 0 {
				return nil, fmt.Errorf("enrich: reference %q: bufferLimits.maxWait %q is not a non-negative duration", cfg.Table, cfg.BufferLimits.MaxWait)
			}
		}
		if cfg.BufferLimits.MaxEvents < 0 {
			return nil, fmt.Errorf("enrich: reference %q: bufferLimits.maxEvents %d must be >= 0", cfg.Table, cfg.BufferLimits.MaxEvents)
		}
		for ev, ref := range cfg.On {
			if !evCols[ev] {
				return nil, fmt.Errorf("enrich: reference %q: on: event column %q is not in the table's schema", cfg.Table, ev)
			}
			rj.onKey, rj.onRef = ev, ref
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
// wildcard select: those destinations are only known at load time (the
// documented exception in New).
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

// AddRefColumns returns cs extended with the declarations' reference
// destination columns (deduplicated; nullable strings — the registered type
// decision until CR-069 resolves real types). Callers apply it to every
// schema shape that feeds a sink table or a drift check BEFORE the pipeline
// starts: without it, the first enriched batch carries columns the table
// lacks and every sink silently drops them (they project by the table's own
// columns, never by the wire's).
func AddRefColumns(cs core.Schema, cfgs []spec.Enrich) core.Schema {
	for _, dest := range RefColumnsFor(cfgs) {
		if _, exists := cs.Column(dest); !exists {
			cs.Columns = append(cs.Columns, core.Column{
				Name: dest,
				Type: core.ColumnType{Kind: core.KindString, Nullable: true},
			})
		}
	}
	return cs
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
// at construction for explicit selects, empty for a wildcard select (those
// destinations are only known at load time; see New for the documented
// exception). The caller adds them to the table's WIRE schema as nullable
// columns BEFORE the pipeline starts, so every batch travels with the full
// column set and the drift check sees one stable shape. A left-join miss
// writes NULL into these columns; the schema declares them nullable, so the
// wire is never violated.
func (s *Stage) RefColumns() []string {
	var out []string
	for _, rj := range s.refs {
		out = append(out, rj.refDests...)
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

// Start launches the first load and the refresh loop per reference —
// asynchronously: the pipeline boots without waiting for the references,
// and the cold-start policy governs whatever arrives early.
func (s *Stage) Start(ctx context.Context) {
	for _, rj := range s.refs {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			t := time.NewTicker(rj.refreshEvery)
			defer t.Stop()
			// First load immediately; a failure is sticky (it surfaces on
			// the first event) and the ticker keeps retrying, so a
			// reference database that boots late still comes hot.
			rj.refresh(ctx, s.log)
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
	})
}

// refresh re-reads the reference and swaps the snapshot atomically. The
// FIRST successful load also resolves the projection and flips hot —
// parking the cold-start queue for the next Apply to release.
func (rj *refJoin) refresh(ctx context.Context, log *slog.Logger) {
	if rj.loader == nil {
		l, err := NewSQLLoader(rj.cfg.Source.URI, rj.cfg.Source.Query)
		if err != nil {
			rj.setFirstErr(err)
			log.Error("enrich: reference loader failed (will retry)", "reference", rj.cfg.Table, "err", err)
			return
		}
		rj.loader = l
	}
	rows, err := rj.loader.Load(ctx)
	if err != nil {
		rj.setFirstErr(err)
		log.Error("enrich: reference load failed (keeping previous image, will retry)", "reference", rj.cfg.Table, "err", err)
		return
	}
	image, dests, refTypes, err := buildImage(rj, rows)
	if err != nil {
		rj.setFirstErr(err)
		log.Error("enrich: reference image rejected", "reference", rj.cfg.Table, "err", err)
		return
	}
	// Atomic swap: the hot path reads this with a single Load(), no lock.
	rj.snap.Store(&snapshot{image: image, dests: dests, refTypes: refTypes})
	// Clear the sticky first-load error on ANY success: "Sticky ONLY before
	// the first success" means a transient boot failure must not poison the
	// stage forever once the reference comes hot (audit #1).
	rj.mu.Lock()
	rj.firstErr = nil
	rj.mu.Unlock()
	log.Info("enrich: reference loaded", "reference", rj.cfg.Table, "rows", len(image))
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
func buildImage(rj *refJoin, rows []map[string]any) (map[string]map[string]any, []dest, map[string]arrow.DataType, error) {
	star := len(rj.cfg.Select) == 1 && rj.cfg.Select[0] == "*"

	// A legitimately empty reference (every join misses) is not a broken
	// load (audit #3): build an empty image so the stage goes hot with an
	// all-miss map. The on/select columns cannot be validated against an
	// empty result; a later non-empty refresh does.
	if len(rows) == 0 {
		return make(map[string]map[string]any), nil, nil, nil
	}

	if _, ok := rows[0][rj.onRef]; !ok {
		return nil, nil, nil, fmt.Errorf("on: reference column %q is not in the query result", rj.onRef)
	}
	available := map[string]bool{}
	for col := range rows[0] {
		available[col] = true
	}
	if !star {
		for _, s := range rj.cfg.Select {
			if !available[s] {
				return nil, nil, nil, fmt.Errorf("select: reference column %q is not in the query result", s)
			}
		}
	}

	// Resolve the projection: reference column → destination name. Under
	// the star it is every column except the join key. Unrenamed columns
	// get a table-prefixed name (Spark-style) to avoid silent collisions
	// when multiple references inject columns with the same name. The "as"
	// map is keyed by the prefixed name (e.g., "users.id") for consistency.
	projection := map[string]string{}
	var dests []dest
	destSeen := map[string]bool{}
	addDest := func(refCol string) error {
		if _, done := projection[refCol]; done {
			return nil
		}
		name := fmt.Sprintf("%s.%s", rj.cfg.Table, refCol)
		if as, ok := rj.cfg.As[name]; ok {
			name = as
		}
		if destSeen[name] {
			return fmt.Errorf("reference %q: two columns project to the same destination %q", rj.cfg.Table, name)
		}
		projection[refCol] = name
		destSeen[name] = true
		dests = append(dests, dest{ref: refCol, as: name})
		return nil
	}
	if star {
		for col := range available {
			if err := addDest(col); err != nil {
				return nil, nil, nil, err
			}
		}
	} else {
		for _, s := range rj.cfg.Select {
			if err := addDest(s); err != nil {
				return nil, nil, nil, err
			}
		}
	}
	// Deterministic order: the join writes by name, but reproducible
	// column order costs nothing and makes traces comparable.
	sort.Slice(dests, func(i, j int) bool { return dests[i].as < dests[j].as })

	// refTypes: the Arrow type each destination lands as, from the first
	// non-null value seen. A dest that is null in every row falls back to
	// String (a placeholder the next non-null load replaces). A value whose
	// Go type has no wire mapping is a rejected load — teaching the way out.
	refTypes := make(map[string]arrow.DataType, len(dests))
	image := make(map[string]map[string]any, len(rows))
	for _, row := range rows {
		// A NULL join key never matches (SQL semantics): a reference row
		// with a NULL on-column is unreachable by any event (audit #7).
		if row[rj.onRef] == nil {
			continue
		}
		k := joinKey(row[rj.onRef])
		if _, dup := image[k]; dup {
			return nil, nil, nil, fmt.Errorf("duplicate join key %v — the reference must be unique on %q", row[rj.onRef], rj.onRef)
		}
		projected := make(map[string]any, len(dests))
		for refCol, name := range projection {
			v := row[refCol]
			if v != nil {
				if _, seen := refTypes[name]; !seen {
					dt, err := refValueArrowType(v)
					if err != nil {
						return nil, nil, nil, fmt.Errorf("reference %q column %q: %w", rj.cfg.Table, refCol, err)
					}
					refTypes[name] = dt
				}
			}
			projected[name] = v
		}
		image[k] = projected
	}
	for _, d := range dests {
		if _, ok := refTypes[d.as]; !ok {
			refTypes[d.as] = arrow.BinaryTypes.String
		}
	}
	return image, dests, refTypes, nil
}

// refValueArrowType maps a reference value's Go type to the Arrow type its
// destination column lands as on the wire. The reference SQL loader yields
// string / int64 / float64 / []byte / bool / time.Time (drivers may also
// hand int/int32/uint64); anything else has no wire contract.
func refValueArrowType(v any) (arrow.DataType, error) {
	switch v.(type) {
	case string:
		return arrow.BinaryTypes.String, nil
	case []byte:
		return arrow.BinaryTypes.Binary, nil
	case bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case int, int32, int64:
		return arrow.PrimitiveTypes.Int64, nil
	case uint64:
		return arrow.PrimitiveTypes.Uint64, nil
	case float32, float64:
		return arrow.PrimitiveTypes.Float64, nil
	case time.Time:
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, nil
	default:
		return nil, fmt.Errorf("value type %T is not representable on the wire", v)
	}
}

func (rj *refJoin) stickyErr() error {
	rj.mu.Lock()
	defer rj.mu.Unlock()
	return rj.firstErr
}

// joinKey renders a join value into the map key. Numeric families
// normalize (drivers disagree on int widths), []byte becomes string; a
// string "5" and an int64 5 stay DISTINCT — the cast lives in the
// reference query's SQL, not in silent coercion.
// intKey places a signed integer in the shared non-negative space when it
// is non-negative, else in its own negative space.
//
// float32 and float64 of the same literal are DIFFERENT VALUES (0.1f
// upcasts to 0.10000000149011612, not 0.1) and therefore different keys.
// Same doctrine as string-vs-int: the cast lives in the reference query's
// SQL, not in silent coercion. The int family is the exception — signed
// and unsigned share the non-negative space because the VALUE is the same
// and only the width differs.
func intKey(t int64) string {
	if t >= 0 {
		return "n:" + strconv.FormatInt(t, 10)
	}
	return "i:" + strconv.FormatInt(t, 10)
}

func joinKey(v any) string {
	switch t := v.(type) {
	case nil:
		return "nil"
	case string:
		return "s:" + t
	case []byte:
		return "s:" + string(t)
	// Signed and unsigned integers share one space for non-negative values:
	// the same logical key loads as uint64 from a SQL reference (MySQL
	// UNSIGNED) and decodes as int64 from the binlog, and a signed/unsigned
	// split would make the join never match. Negative values have no
	// unsigned counterpart, so they keep their own space.
	case int:
		return intKey(int64(t))
	case int32:
		return intKey(int64(t))
	case int64:
		return intKey(t)
	case uint:
		return "n:" + strconv.FormatUint(uint64(t), 10)
	case uint32:
		return "n:" + strconv.FormatUint(uint64(t), 10)
	case uint64:
		return "n:" + strconv.FormatUint(t, 10)
	case float32:
		return "f:" + strconv.FormatFloat(float64(t), 'g', -1, 64)
	case float64:
		return "f:" + strconv.FormatFloat(t, 'g', -1, 64)
	default:
		return rowchange.KeyString([]any{v})
	}
}
