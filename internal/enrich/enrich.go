// Package enrich is the table-level enrichment stage: a broadcast hash
// join against small reference tables. The reference is read WHOLE into a
// worker-local map (one map per reference), every batch matches against it
// in O(rows), and the map is swapped atomically on a full periodic re-read.
// There is no lookup per event against a database, no shuffle, no windowed
// state — the reference is small by contract, and if it stops being small
// the answer is a different tool, not a cache.
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/spec"
)

// DefaultRefresh re-reads a reference this often when the spec is silent.
const DefaultRefresh = 5 * time.Minute

// defaultMaxEvents caps the cold-start buffer when the spec leaves
// BufferLimits.MaxEvents at zero (the Go int zero value).
const defaultMaxEvents = 100_000

// coldStartPolicy resolves the onColdStart grammar.
type coldStartPolicy int

const (
	coldBuffer coldStartPolicy = iota // default
	coldPass
	coldDrop
)

// Stage enriches one table's changes across its declared references, in
// declaration order. Apply runs on the worker's batcher goroutine; the
// refresh goroutines only swap images and park the drained cold-start
// queue for the next Apply to release.
type Stage struct {
	refs []*refJoin

	stopped chan struct{}
	wg      sync.WaitGroup
	log     *slog.Logger

	// Counters are the evidence the stage ran, and what it cost.
	misses       atomic.Int64 // left-join misses (events that passed with NULLs)
	innerDropped atomic.Int64 // events an inner join discarded
	evicted      atomic.Int64 // cold-start evacuations (maxEvents / maxWait)

	// metrics is optional; when set, counters are mirrored to Prometheus.
	metrics *enrichMetrics
}

// enrichMetrics wraps the Prometheus counters for the enrichment stage.
// Nil-safe: if the pointer is nil, Inc calls are no-ops.
type enrichMetrics struct {
	misses  func(table, ref string)
	dropped func(table, ref string)
	evicted func(table, ref string)
}

// snapshot is the immutable reference state swapped atomically on refresh.
// Bundling hot, image, and dests ensures the hot path reads a consistent
// view with a single atomic.Load — no lock, no partial reads.
type snapshot struct {
	hot   bool
	image map[string]map[string]any // normalized join key → reference row
	dests []dest                    // projected reference columns
	star  bool                      // true when select is ["*"]
}

// refJoin is one reference: its config, the hot lookup image, and the
// cold-start queue. The snapshot is swapped atomically — an in-flight
// batch finishes against the old map, never a half-built one. The mutex
// only protects cold-start state (queue, pendingDrain) and the sticky
// error; the hot path is lock-free.
type refJoin struct {
	cfg    spec.Enrich
	loader Loader
	onKey  string // event column name
	onRef  string // reference column name
	policy coldStartPolicy

	// snap is the hot-path state: image + dests + hot flag, swapped
	// atomically on refresh. Load() is lock-free; Store() is called
	// only by refresh (one goroutine per reference).
	snap atomic.Pointer[snapshot]

	// mu protects cold-start state and the sticky error — written
	// rarely (cold start, first load), never on the hot path.
	mu    sync.Mutex
	queue []buffered
	// pendingDrain is the queue captured at the hot flip, released into
	// the pipeline by the next Apply (the batcher goroutine owns Apply).
	pendingDrain []buffered

	firstErr error // sticky: a broken reference surfaces on the first event

	// misses is the stage-level counter, shared by reference pointers.
	misses  *atomic.Int64
	metrics *enrichMetrics // shared with Stage; nil-safe
}

type dest struct {
	ref string // reference column
	as  string // destination event column
}

// buffered is an event parked by a cold-start buffer.
type buffered struct {
	c      change.Change
	next   int // reference index the event still has to pass
	queued time.Time
}

// New builds the stage and validates the declarations against the event's
// known columns — the checks spec.Validate cannot make without schemas:
// the join's event side must exist, and the grammar is re-checked so a
// spec that reached us unvalidated fails loudly here.
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
	for _, cfg := range cfgs {
		if len(cfg.On) != 1 {
			return nil, fmt.Errorf("enrich: reference %q: on: exactly one join pair is supported today", cfg.Table)
		}
		if len(cfg.Select) == 0 {
			return nil, fmt.Errorf("enrich: reference %q: select is required — declare the reference columns the event receives (\"*\" injects all)", cfg.Table)
		}
		rj := &refJoin{cfg: cfg}
		for ev, ref := range cfg.On {
			if !evCols[ev] {
				return nil, fmt.Errorf("enrich: reference %q: on: event column %q is not in the table's schema", cfg.Table, ev)
			}
			rj.onKey, rj.onRef = ev, ref
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
		if _, err := rj.refreshInterval(); err != nil {
			return nil, err
		}
		rj.misses = &s.misses
		s.refs = append(s.refs, rj)
	}
	return s, nil
}

// SetMetrics wires Prometheus counters into the stage. Call after New;
// nil-safe (metrics pointer is stored, not dereferenced).
func (s *Stage) SetMetrics(misses, dropped, evicted func(table, ref string)) {
	m := &enrichMetrics{misses: misses, dropped: dropped, evicted: evicted}
	s.metrics = m
	for _, rj := range s.refs {
		rj.metrics = m
	}
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

// UseLoader overrides the SQL loader for one reference (test seam).
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
		rj := rj
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			interval, _ := rj.refreshInterval()
			t := time.NewTicker(interval)
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

// Stop ends the refresh loops and closes SQL loaders.
func (s *Stage) Stop() {
	select {
	case <-s.stopped:
		return
	default:
	}
	close(s.stopped)
	s.wg.Wait()
	for _, rj := range s.refs {
		if rj.loader != nil {
			_ = rj.loader.Close()
		}
	}
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
	image, dests, star, err := buildImage(rj, rows)
	if err != nil {
		rj.setFirstErr(err)
		log.Error("enrich: reference image rejected", "reference", rj.cfg.Table, "err", err)
		return
	}
	// Atomic swap: the hot path reads this with a single Load(), no lock.
	wasHot := rj.snap.Load() != nil
	rj.snap.Store(&snapshot{hot: true, image: image, dests: dests, star: star})
	// Cold-start queue flip: captured under mu for the batcher to drain.
	if !wasHot {
		rj.mu.Lock()
		rj.pendingDrain = rj.queue
		rj.queue = nil
		rj.mu.Unlock()
	}
	log.Info("enrich: reference loaded", "reference", rj.cfg.Table, "rows", len(image))
}

// setFirstErr records a load failure. Sticky ONLY before the first
// success: a broken join declaration is permanent, not transient — the
// pipeline pauses on the first event instead of silently passing
// unenriched forever. Once hot, a failed refresh is tolerated (the
// previous image stays authoritative); the caller logs it.
func (rj *refJoin) setFirstErr(err error) {
	rj.mu.Lock()
	defer rj.mu.Unlock()
	if snap := rj.snap.Load(); snap == nil || !snap.hot {
		if rj.firstErr == nil {
			rj.firstErr = err
		}
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
func buildImage(rj *refJoin, rows []map[string]any) (map[string]map[string]any, []dest, bool, error) {
	if len(rows) == 0 {
		return nil, nil, false, fmt.Errorf("reference query returned no rows")
	}
	if _, ok := rows[0][rj.onRef]; !ok {
		return nil, nil, false, fmt.Errorf("on: reference column %q is not in the query result", rj.onRef)
	}

	star := len(rj.cfg.Select) == 1 && rj.cfg.Select[0] == "*"
	sel := make(map[string]bool, len(rj.cfg.Select))
	for _, s := range rj.cfg.Select {
		sel[s] = true
	}
	available := map[string]bool{}
	for col := range rows[0] {
		available[col] = true
	}
	if !star {
		for _, s := range rj.cfg.Select {
			if !available[s] {
				return nil, nil, false, fmt.Errorf("select: reference column %q is not in the query result", s)
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
	addDest := func(refCol string) {
		if _, done := projection[refCol]; done {
			return
		}
		name := fmt.Sprintf("%s.%s", rj.cfg.Table, refCol)
		if as, ok := rj.cfg.As[name]; ok {
			name = as
		}
		projection[refCol] = name
		dests = append(dests, dest{ref: refCol, as: name})
	}
	if star {
		for col := range available {
			addDest(col)
		}
	} else {
		for _, s := range rj.cfg.Select {
			addDest(s)
		}
	}
	// Deterministic order: the join writes by name, but reproducible
	// column order costs nothing and makes traces comparable.
	sort.Slice(dests, func(i, j int) bool { return dests[i].as < dests[j].as })

	image := make(map[string]map[string]any, len(rows))
	for _, row := range rows {
		k := joinKey(row[rj.onRef])
		if _, dup := image[k]; dup {
			return nil, nil, false, fmt.Errorf("duplicate join key %v — the reference must be unique on %q", row[rj.onRef], rj.onRef)
		}
		projected := make(map[string]any, len(dests))
		for refCol, name := range projection {
			projected[name] = row[refCol]
		}
		image[k] = projected
	}
	return image, dests, star, nil
}

// Apply runs the batch through every reference in order and returns the
// events that survived: joined, miss-marked, or drained from a cold-start
// buffer. Dropped events (inner miss, drop policy) vanish — dropping IS
// their effect, and the batch position advances past them.
func (s *Stage) Enrich(changes []change.Change) ([]change.Change, error) {
	out := make([]change.Change, 0, len(changes))
	// Release cold-start queues first: FIFO order beats the new traffic.
	for i, rj := range s.refs {
		for _, b := range rj.takeDrained() {
			// MaxWait: an event parked past the cap follows the join type
			// at drain time — latency bound, not a third policy.
			if wait := rj.maxWait(); wait > 0 && time.Since(b.queued) > wait {
				s.evicted.Add(1)
				if s.metrics != nil {
					s.metrics.evicted(rj.cfg.Table, rj.cfg.Table)
				}
				s.forceMiss(i, b.c, &out)
				continue
			}
			s.applyFrom(i, b.c, &out)
		}
	}
	for _, c := range changes {
		s.applyFrom(0, c, &out)
	}
	// A sticky first-load error pauses the pipeline on the first event —
	// never silently pass unenriched rows.
	for _, rj := range s.refs {
		if err := rj.stickyErr(); err != nil {
			return nil, fmt.Errorf("enrich: reference %q: %w", rj.cfg.Table, err)
		}
	}
	return out, nil
}

// applyFrom pushes one change through references starting at i.
func (s *Stage) applyFrom(i int, c change.Change, out *[]change.Change) {
	for ; i < len(s.refs); i++ {
		rj := s.refs[i]
		switch rj.apply(&c) {
		case applied:
			// continue to the next reference
		case dropped:
			s.innerDropped.Add(1)
			if s.metrics != nil {
				s.metrics.dropped(rj.cfg.Table, rj.cfg.Table)
			}
			return
		case parked:
			s.enqueue(rj, i, c, out)
			return
		}
	}
	*out = append(*out, c)
}

// forceMiss joins a change against reference i as a miss (whatever the
// event was, it will not be matched) and continues downstream.
func (s *Stage) forceMiss(i int, c change.Change, out *[]change.Change) {
	if c.After == nil {
		return // nothing to enrich; a key-only delete just vanishes
	}
	rj := s.refs[i]
	snap := rj.snap.Load()
	var dests []dest
	var star bool
	if snap != nil {
		dests = snap.dests
		star = snap.star
	}
	if rj.join(&c, nil, dests, star) == applied {
		s.applyFrom(i+1, c, out)
	}
}

// enqueue parks a change in a cold-start buffer, evacuating the oldest
// when MaxEvents is exceeded. An evacuated change follows the join type —
// the buffer's bounds are a latency/memory contract, not a third policy.
func (s *Stage) enqueue(rj *refJoin, at int, c change.Change, out *[]change.Change) {
	rj.mu.Lock()
	max := rj.cfg.BufferLimits.MaxEvents
	if max <= 0 {
		max = defaultMaxEvents
	}
	var evicted []change.Change
	for max > 0 && len(rj.queue) >= max {
		evicted = append(evicted, rj.queue[0].c)
		rj.queue = rj.queue[1:]
	}
	rj.queue = append(rj.queue, buffered{c: c, next: at, queued: time.Now()})
	rj.mu.Unlock()
	for _, e := range evicted {
		s.evicted.Add(1)
		if s.metrics != nil {
			s.metrics.evicted(rj.cfg.Table, rj.cfg.Table)
		}
		s.forceMiss(at, e, out)
	}
}

type applyResult int

const (
	applied applyResult = iota
	dropped
	parked
)

// apply joins one change against this reference, mutating After in place.
func (rj *refJoin) apply(c *change.Change) applyResult {
	snap := rj.snap.Load() // lock-free read

	if snap == nil || !snap.hot {
		switch rj.policy {
		case coldDrop:
			return dropped
		case coldPass:
			// Cold map: every lookup misses; the miss follows the join
			// type. Reference columns are unknown until the first load,
			// so a cold left-miss marks the row without adding NULLs.
			return rj.join(c, nil, nil, false)
		default: // coldBuffer
			return parked
		}
	}
	if c.After == nil {
		// A key-only delete carries nothing to enrich.
		return applied
	}
	return rj.join(c, snap.image[joinKey(c.After[rj.onKey])], snap.dests, snap.star)
}

// join materializes the hit or the miss. A miss in a left join passes the
// event with NULL reference columns and marks it; an inner join drops it.
// That grammar is the ONLY miss policy — cold start, eviction and expiry
// all route through it.
func (rj *refJoin) join(c *change.Change, row map[string]any, dests []dest, star bool) applyResult {
	if row == nil {
		rj.misses.Add(1)
		if rj.metrics != nil {
			rj.metrics.misses(rj.cfg.Table, rj.cfg.Table)
		}
		if rj.cfg.JoinType == "inner" {
			return dropped
		}
		for _, d := range dests {
			c.After[d.as] = nil
		}
		c.EnrichMiss = true
		return applied
	}
	for _, d := range dests {
		// With table-prefixed names (Spark-style), the reference's join
		// column is injected as "table.column" while the source retains
		// its original name. Both coexist without collision. When the
		// user renames via "as", the reference value is used under the
		// new name.
		c.After[d.as] = row[d.as] // the image is already projected + renamed
	}
	return applied
}

func (rj *refJoin) stickyErr() error {
	rj.mu.Lock()
	defer rj.mu.Unlock()
	return rj.firstErr
}

// maxWait parses the optional latency cap. The value is checked at drain
// time (Enrich), not per-event during the buffer — events that exceed
// maxWait are evicted when the cold-start queue is released, not on a
// background timer. This is a deliberate simplicity trade-off: the drain
// is a single pass that handles all events at once.
func (rj *refJoin) maxWait() time.Duration {
	if rj.cfg.BufferLimits.MaxWait == "" {
		return 0
	}
	d, err := time.ParseDuration(rj.cfg.BufferLimits.MaxWait)
	if err != nil {
		return 0
	}
	return d
}

// takeDrained returns (and clears) the queue captured at the hot flip.
func (rj *refJoin) takeDrained() []buffered {
	rj.mu.Lock()
	q := rj.pendingDrain
	rj.pendingDrain = nil
	rj.mu.Unlock()
	return q
}

// joinKey renders a join value into the map key. Numeric families
// normalize (drivers disagree on int widths), []byte becomes string; a
// string "5" and an int64 5 stay DISTINCT — the cast lives in the
// reference query's SQL, not in silent coercion.
func joinKey(v any) string {
	switch t := v.(type) {
	case nil:
		return "nil"
	case string:
		return "s:" + t
	case []byte:
		return "s:" + string(t)
	case int:
		return "i:" + strconv.FormatInt(int64(t), 10)
	case int32:
		return "i:" + strconv.FormatInt(int64(t), 10)
	case int64:
		return "i:" + strconv.FormatInt(t, 10)
	case uint:
		return "u:" + strconv.FormatUint(uint64(t), 10)
	case uint32:
		return "u:" + strconv.FormatUint(uint64(t), 10)
	case uint64:
		return "u:" + strconv.FormatUint(t, 10)
	case float32:
		return "f:" + strconv.FormatFloat(float64(t), 'g', -1, 64)
	case float64:
		return "f:" + strconv.FormatFloat(t, 'g', -1, 64)
	default:
		return change.KeyString([]any{v})
	}
}
