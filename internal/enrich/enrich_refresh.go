package enrich

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

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
		k := normalizeKey(readArrowValue(kc, i))
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
