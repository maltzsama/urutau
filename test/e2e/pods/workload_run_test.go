package pods

// The executing half of the production-readiness workload (issue #384): it
// runs what workload_gen_test.go builds against the real MySQL source, keeps
// the oracle in step with what actually committed, and compares
// MySQL source state → oracle → Iceberg logical state.

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// maxInsertBytes bounds one multi-row INSERT, well under MySQL 8.4's default
// max_allowed_packet (64 MiB).
const maxInsertBytes = 4 << 20

// workload drives the three tables' streams and owns their oracles.
type workload struct {
	profile workloadProfile
	seed    uint64
	db      *sql.DB
	tables  []*prTable
	gens    []*tableGen
	stats   []*tableStats

	started  time.Time
	liveFrom time.Time
	liveTo   time.Time
	finalPos string

	stopCh chan struct{}
	wg     sync.WaitGroup

	mu   sync.Mutex
	errs []string // oracle reconciliation failures; any one fails the run

	// Filled after the run, for the diagnostics file.
	checks    []tableCheck
	sinkRows  []*dist
	sinkBytes []*dist
}

// newWorkload builds a workload. Each table gets its own random stream,
// derived from the run seed and the table's position.
func newWorkload(seed uint64, p workloadProfile, tables []*prTable, db *sql.DB) *workload {
	w := &workload{profile: p, seed: seed, db: db, tables: tables, started: time.Now()}
	for i, t := range tables {
		w.gens = append(w.gens, newTableGen(t, p, seed, uint64(i)+1))
		w.stats = append(w.stats, newTableStats())
	}
	return w
}

func (w *workload) fail(format string, args ...any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.errs = append(w.errs, fmt.Sprintf(format, args...))
}

// createTables drops and recreates the source tables.
func (w *workload) createTables(ctx context.Context) error {
	for _, t := range w.tables {
		if _, err := w.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+t.Name); err != nil {
			return fmt.Errorf("drop %s: %w", t.Name, err)
		}
		if _, err := w.db.ExecContext(ctx, fmt.Sprintf(t.DDL, t.Name)); err != nil {
			return fmt.Errorf("create %s: %w", t.Name, err)
		}
	}
	return nil
}

// dropTables removes the source tables, best effort.
func (w *workload) dropTables() {
	for _, t := range w.tables {
		_, _ = w.db.Exec("DROP TABLE IF EXISTS " + t.Name)
	}
}

// seedInitial inserts profile.InitialRows rows per table through the same
// generator the live streams use, and records the accounts table's expected
// partition boundary from the seeded key range.
func (w *workload) seedInitial(ctx context.Context) error {
	errs := make(chan error, len(w.gens))
	for _, g := range w.gens {
		go func() {
			for left := w.profile.InitialRows; left > 0; {
				n := min(500, left)
				muts, undo := g.buildTxn(n, true)
				if _, err := execTxn(ctx, w.db, g.t, muts); err != nil {
					undo()
					errs <- fmt.Errorf("seed %s: %w", g.t.Name, err)
					return
				}
				g.commit(muts)
				left -= n
			}
			g.setAccountsBoundary()
			errs <- nil
		}()
	}
	var first error
	for range w.gens {
		if err := <-errs; err != nil && first == nil {
			first = err
		}
	}
	return first
}

// execTxn runs one transaction. ambiguous reports a failed COMMIT, after
// which the transaction may or may not have committed.
func execTxn(ctx context.Context, db *sql.DB, t *prTable, muts []mutation) (ambiguous bool, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	rollback := func(err error) (bool, error) {
		_ = tx.Rollback()
		return false, err
	}
	for i := 0; i < len(muts); {
		m := muts[i]
		switch m.Kind {
		case opInsert:
			// Consecutive inserts go out as one multi-row INSERT: one binlog
			// rows event carrying many rows, as a bulk writer produces.
			j, bytes := i, 0
			for j < len(muts) && muts[j].Kind == opInsert && (j == i || bytes+muts[j].Bytes <= maxInsertBytes) {
				bytes += muts[j].Bytes
				j++
			}
			row := "(" + strings.TrimSuffix(strings.Repeat("?,", len(t.insertCols)), ",") + ")"
			q := "INSERT INTO " + t.Name + " (" + strings.Join(t.insertCols, ",") + ") VALUES " +
				strings.TrimSuffix(strings.Repeat(row+",", j-i), ",")
			var args []any
			for _, m := range muts[i:j] {
				args = append(args, m.Key...)
				args = append(args, m.Vals...)
			}
			if _, err := tx.ExecContext(ctx, q, args...); err != nil {
				return rollback(fmt.Errorf("insert into %s: %w", t.Name, err))
			}
			i = j
			continue
		case opUpdate:
			set := strings.Join(t.valueCols, "=?,") + "=?"
			args := append(append([]any{}, m.Vals...), m.Key...)
			if err := execOne(ctx, tx, "UPDATE "+t.Name+" SET "+set+" WHERE "+keyPredicate(t), args); err != nil {
				return rollback(fmt.Errorf("update %s %s: %w", t.Name, m.ID, err))
			}
		case opDelete:
			if err := execOne(ctx, tx, "DELETE FROM "+t.Name+" WHERE "+keyPredicate(t), m.Key); err != nil {
				return rollback(fmt.Errorf("delete %s %s: %w", t.Name, m.ID, err))
			}
		}
		i++
	}
	if err := tx.Commit(); err != nil {
		return true, fmt.Errorf("commit %s: %w", t.Name, err)
	}
	return false, nil
}

// execOne runs a statement that must touch exactly one row: the oracle said
// the key is live, so zero rows means the oracle and MySQL disagree.
func execOne(ctx context.Context, tx *sql.Tx, q string, args []any) error {
	res, err := tx.ExecContext(ctx, q, args...)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%d rows affected, want 1 (oracle and source disagree)", n)
	}
	return nil
}

func keyPredicate(t *prTable) string {
	return strings.Join(t.PK, "=? AND ") + "=?"
}

// start launches one stream per table. Mutations begin at once, so they
// overlap the pipeline's boot and snapshot.
func (w *workload) start(ctx context.Context) {
	w.liveFrom = time.Now()
	w.stopCh = make(chan struct{})
	for i := range w.gens {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.runStream(ctx, i)
		}()
	}
}

// stop ends the streams and records the expected final source position: the
// GTID set executed once the last generated mutation committed.
func (w *workload) stop(ctx context.Context) error {
	close(w.stopCh)
	w.wg.Wait()
	w.liveTo = time.Now()
	return w.db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&w.finalPos)
}

// runStream is one table's live stream: draw a regime, then transactions
// until the regime ends, forever until stop. The events table also runs
// backlog episodes: the first one inside the first third of the window, the
// next ones at random intervals.
func (w *workload) runStream(ctx context.Context, i int) {
	g, s, p := w.gens[i], w.stats[i], w.profile
	backlogged := g.t.Kind == eventsKind
	next := w.liveFrom.Add(time.Duration((0.15 + 0.2*g.r.Float64()) * float64(p.Duration)))
	var open *episode
	g.regime = newRegime(g.r, p, g.t, time.Now(), false)
	burst := 0
	for {
		select {
		case <-w.stopCh:
			if open != nil {
				open.End = time.Since(w.liveFrom)
				s.Backlog = append(s.Backlog, *open)
			}
			// An upper bound on this stream's last position: other streams
			// may commit between its last transaction and this read. The
			// driver cannot report one transaction's own GTID (that needs
			// session_track_gtids), so the authoritative expected position
			// is the global one stop reads once every stream has ended.
			var pos string
			if err := w.db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&pos); err != nil {
				w.fail("%s: gtid_executed at stop: %v", g.t.Name, err)
			}
			s.GTIDAtStop, s.LiveRows = pos, g.live.len()
			return
		default:
		}
		now := time.Now()
		if now.After(g.regime.Until) {
			if open != nil {
				open.End = now.Sub(w.liveFrom)
				s.Backlog = append(s.Backlog, *open)
				open = nil
			}
			backlog := backlogged && !now.Before(next)
			g.regime = newRegime(g.r, p, g.t, now, backlog)
			if backlog {
				open = &episode{Start: now.Sub(w.liveFrom)}
				next = now.Add(time.Duration(g.r.ExpFloat64() * float64(p.Duration/4)))
			}
		}
		n := g.regime.txnRows(g.r, p.MaxTxnRows)
		if s.Txns == 3 {
			n = p.MaxTxnRows // one jumbo transaction per stream, whatever the regimes draw
		}
		muts, undo := g.buildTxn(n, false)
		ambiguous, err := execTxn(ctx, w.db, g.t, muts)
		switch {
		case err == nil:
			g.commit(muts)
			s.recordTxn(g, muts, time.Now())
		case ambiguous:
			s.AmbiguousCommits++
			undo()
			if rerr := w.reconcile(ctx, g, muts); rerr != nil {
				w.fail("%s: reconcile after %v: %v", g.t.Name, err, rerr)
			}
		default:
			undo()
			if ctx.Err() != nil {
				return
			}
			s.TxnErrors++
			w.fail("%s: transaction failed: %v", g.t.Name, err)
		}
		if burst > 0 {
			burst--
			continue
		}
		if g.r.Float64() < g.regime.BurstProb {
			burst = 1 + g.r.IntN(20)
		}
		select {
		case <-w.stopCh:
		case <-time.After(g.regime.delay(g.r, len(muts))):
		}
	}
}

// reconcile settles the oracle after an ambiguous COMMIT by reading each
// touched key back from MySQL, the ground truth.
func (w *workload) reconcile(ctx context.Context, g *tableGen, muts []mutation) error {
	seen := map[string]bool{}
	for _, m := range muts {
		if seen[m.ID] {
			continue
		}
		seen[m.ID] = true
		where := make([]string, len(g.t.PK))
		for i, k := range g.t.PK {
			where[i] = k + "=?"
		}
		rows, dups, err := readCanon(ctx, w.db, g.t, false, " WHERE "+strings.Join(where, " AND "), m.Key...)
		if err != nil {
			return err
		}
		if len(dups) > 0 {
			return fmt.Errorf("key %s is duplicated in the source", m.ID)
		}
		if r, ok := rows[m.ID]; ok {
			g.rows[m.ID] = r
			delete(g.deleted, m.ID)
			g.live.add(m.ID, m.Key)
		} else {
			if _, was := g.rows[m.ID]; was {
				g.deleted[m.ID] = m.Rev
			}
			delete(g.rows, m.ID)
			g.live.remove(m.ID)
		}
	}
	return nil
}

// ── reading and comparing state ─────────────────────────────────────────

// readCanon reads a table's canonical state, keyed by oracle id: from MySQL
// (trino false, the source table) or from Trino (the Iceberg target). A key
// read twice is returned in dups.
func readCanon(ctx context.Context, db *sql.DB, t *prTable, trino bool, where string, args ...any) (map[string]oracleRow, []string, error) {
	var exprs []string
	for _, c := range append(append([]prColumn{}, t.Keys...), t.Vals...) {
		if trino {
			exprs = append(exprs, c.Trino)
		} else {
			exprs = append(exprs, c.MySQL)
		}
	}
	from := t.Name
	if trino {
		from = t.Target
	}
	rows, err := db.QueryContext(ctx, "SELECT "+strings.Join(exprs, ", ")+" FROM "+from+where, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("read %s: %w", from, err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]oracleRow{}
	var dups []string
	cells := make([]sql.NullString, len(exprs))
	ptrs := make([]any, len(exprs))
	for i := range cells {
		ptrs[i] = &cells[i]
	}
	texts := make([]string, len(exprs))
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, fmt.Errorf("scan %s: %w", from, err)
		}
		for i, c := range cells {
			texts[i] = nullText
			if c.Valid {
				texts[i] = c.String
			}
		}
		nk := len(t.Keys)
		id := keyID(texts[:nk])
		rev, err := strconv.ParseInt(texts[nk], 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("%s key %s: rev %q: %w", from, id, texts[nk], err)
		}
		if _, dup := out[id]; dup {
			dups = append(dups, id)
		}
		out[id] = oracleRow{Rev: rev, Image: strings.Join(texts[nk:], imageSep)}
	}
	return out, dups, rows.Err()
}

// idList is a category of differing keys: the full count and a sample.
type idList struct {
	Count  int      `json:"count"`
	Sample []string `json:"sample,omitempty"`
}

func (l *idList) add(id string) {
	l.Count++
	if len(l.Sample) < 20 {
		l.Sample = append(l.Sample, id)
	}
}

// stateDiff classifies how an observed state differs from the expected one.
type stateDiff struct {
	Missing            idList `json:"missing"`            // expected live, absent
	Extra              idList `json:"extra"`              // present, never written by this run
	ResurrectedDeletes idList `json:"resurrectedDeletes"` // present, but this run deleted it
	Stale              idList `json:"stale"`              // an older revision of the row
	Incorrect          idList `json:"incorrect"`          // current or newer revision with the wrong content
	Duplicated         idList `json:"duplicated"`         // one key, several rows
}

func (d *stateDiff) empty() bool {
	return d.Missing.Count+d.Extra.Count+d.ResurrectedDeletes.Count+d.Stale.Count+d.Incorrect.Count+d.Duplicated.Count == 0
}

func (d *stateDiff) String() string {
	return fmt.Sprintf("missing=%d%v extra=%d%v resurrected=%d%v stale=%d%v incorrect=%d%v duplicated=%d%v",
		d.Missing.Count, d.Missing.Sample, d.Extra.Count, d.Extra.Sample,
		d.ResurrectedDeletes.Count, d.ResurrectedDeletes.Sample, d.Stale.Count, d.Stale.Sample,
		d.Incorrect.Count, d.Incorrect.Sample, d.Duplicated.Count, d.Duplicated.Sample)
}

// diffStates compares an observed state to the expected one. deleted holds
// the keys the run deleted, to tell a resurrected delete from a stray row.
func diffStates(want, got map[string]oracleRow, dups []string, deleted map[string]int64) *stateDiff {
	d := &stateDiff{}
	for id, w := range want {
		g, ok := got[id]
		switch {
		case !ok:
			d.Missing.add(id)
		case g.Image == w.Image:
		case g.Rev < w.Rev:
			d.Stale.add(id)
		default:
			d.Incorrect.add(id)
		}
	}
	for id := range got {
		if _, ok := want[id]; ok {
			continue
		}
		if _, was := deleted[id]; was {
			d.ResurrectedDeletes.add(id)
		} else {
			d.Extra.add(id)
		}
	}
	for _, id := range dups {
		d.Duplicated.add(id)
	}
	return d
}

// tableCheck is one table's final comparison.
type tableCheck struct {
	OracleVsSource *stateDiff // the workload's own bookkeeping; must be empty
	SourceVsSink   *stateDiff // the pipeline; must be empty
}

// compare reads MySQL and Iceberg for every table and diffs them against
// each other and against the oracle.
func (w *workload) compare(ctx context.Context, trino *sql.DB) ([]tableCheck, error) {
	out := make([]tableCheck, len(w.tables))
	for i, g := range w.gens {
		src, srcDups, err := readCanon(ctx, w.db, g.t, false, "")
		if err != nil {
			return nil, err
		}
		sink, sinkDups, err := readCanon(ctx, trino, g.t, true, "")
		if err != nil {
			return nil, err
		}
		out[i] = tableCheck{
			OracleVsSource: diffStates(g.rows, src, srcDups, g.deleted),
			SourceVsSink:   diffStates(src, sink, sinkDups, g.deleted),
		}
	}
	return out, nil
}

// settle polls until every table's Iceberg state equals its MySQL state, or
// the profile's settle time runs out, and returns the last comparison.
// reconnect re-establishes the Trino port-forward after a failed read: over a
// long run the forward can drop, and without it every retry fails the same
// way until the deadline.
func (w *workload) settle(ctx context.Context, trino *sql.DB, logf func(string, ...any), reconnect func() error) ([]tableCheck, error) {
	deadline := time.Now().Add(w.profile.Settle)
	for {
		checks, err := w.compare(ctx, trino)
		if err != nil {
			// Trino may briefly fail while a table is being created or
			// committed; only a persistent failure ends the wait.
			if time.Now().After(deadline) {
				return nil, err
			}
			logf("settle: %v (retrying)", err)
			if reconnect != nil {
				if rerr := reconnect(); rerr != nil {
					logf("settle: reconnect: %v", rerr)
				}
			}
		} else {
			settled := true
			for _, c := range checks {
				settled = settled && c.SourceVsSink.empty()
			}
			if settled || time.Now().After(deadline) {
				return checks, nil
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Second):
		}
	}
}

// sinkCommits reads the rows and bytes each Iceberg commit added, from the
// target's snapshot summaries: one snapshot per Urutau worker commit.
func sinkCommits(ctx context.Context, trino *sql.DB, t *prTable) (rows, bytes *dist, err error) {
	// element_at, not summary[...]: a snapshot that added no data file (a
	// delete-only commit) has no added-records key, and [] fails on it.
	q := `SELECT element_at(summary, 'added-records'), element_at(summary, 'added-files-size') FROM "` + t.Target + `$snapshots"`
	res, err := trino.QueryContext(ctx, q)
	if err != nil {
		return nil, nil, fmt.Errorf("snapshots of %s: %w", t.Target, err)
	}
	defer func() { _ = res.Close() }()
	rows, bytes = newDist(), newDist()
	for res.Next() {
		var r, b sql.NullString
		if err := res.Scan(&r, &b); err != nil {
			return nil, nil, err
		}
		if v, err := strconv.ParseFloat(r.String, 64); r.Valid && err == nil {
			rows.add(v)
		}
		if v, err := strconv.ParseFloat(b.String, 64); b.Valid && err == nil {
			bytes.add(v)
		}
	}
	return rows, bytes, res.Err()
}

// report assembles the diagnostics file.
func (w *workload) report() *runReport {
	rep := &runReport{
		Seed: w.seed, Profile: w.profile, Started: w.started,
		LiveFrom: w.liveFrom, LiveTo: w.liveTo, ExpectedPosition: w.finalPos,
	}
	for i, g := range w.gens {
		s := w.stats[i]
		tr := tableReport{
			Table: g.t.Name, Target: g.t.Target, Kind: g.t.Kind, Workers: g.t.Workers, Stats: s,
			RowsPerTxn: s.rowsPerTxn.summary(), BytesPerTxn: s.bytesPerTxn.summary(), PayloadBytes: s.payload.summary(),
		}
		if !w.liveFrom.IsZero() && !w.liveTo.IsZero() {
			tr.MutationsPerSecond = s.mutationsPerSecond(w.liveFrom, w.liveTo)
		}
		if g.accountsBoundary != 0 {
			tr.PartitionBoundary = strconv.FormatUint(g.accountsBoundary, 10)
		}
		if i < len(w.checks) {
			tr.OracleVsSource, tr.SourceVsSink = w.checks[i].OracleVsSource, w.checks[i].SourceVsSink
		}
		if i < len(w.sinkRows) {
			tr.SinkRowsPerCommit, tr.SinkBytesPerCommit = w.sinkRows[i].summary(), w.sinkBytes[i].summary()
		}
		rep.Tables = append(rep.Tables, tr)
	}
	return rep
}

// coverageProblems checks that the run exercised what #384 requires: every
// operation on every table, mutations on every side of 2^63 and of the
// accounts partition boundary, a backlog episode, small and large
// transactions, and small and large payloads.
func (w *workload) coverageProblems() []string {
	var out []string
	// A run narrowed with URUTAU_E2E_TABLES does not cover the omitted
	// tables, so it is never a pass of the matrix.
	present := map[tableKind]bool{}
	for _, g := range w.gens {
		present[g.t.Kind] = true
	}
	for _, k := range []tableKind{accountsKind, itemsKind, eventsKind} {
		if !present[k] {
			out = append(out, fmt.Sprintf("table kind %s was not run (URUTAU_E2E_TABLES narrowed the run)", k))
		}
	}
	ops := []string{opInsert.String(), opUpdate.String(), opDelete.String()}
	txnMin, txnMax := -1.0, 0.0
	for i, g := range w.gens {
		s := w.stats[i]
		for _, op := range ops {
			if s.Ops[op] == 0 {
				out = append(out, fmt.Sprintf("%s: no %s committed", g.t.Name, op))
			}
		}
		if g.t.Kind == accountsKind {
			for _, side := range []string{"below-2^63", "2^63-to-boundary", "above-boundary"} {
				for _, op := range ops {
					if s.SideOps[side][op] == 0 {
						out = append(out, fmt.Sprintf("%s: no %s on the %s side", g.t.Name, op, side))
					}
				}
			}
		}
		if g.t.Kind == eventsKind {
			if len(s.Backlog) == 0 {
				out = append(out, g.t.Name+": no backlog episode")
			}
			pl := s.payload.summary()
			if pl.Min > 1024 || pl.Max < float64(w.profile.MaxPayload)/2 {
				out = append(out, fmt.Sprintf("%s: payloads spanned only [%v, %v] bytes", g.t.Name, pl.Min, pl.Max))
			}
		}
		if r := s.rowsPerTxn.summary(); r.N > 0 {
			if txnMin < 0 || r.Min < txnMin {
				txnMin = r.Min
			}
			txnMax = max(txnMax, r.Max)
		}
	}
	if txnMin > 1 || txnMax < float64(w.profile.MaxTxnRows) {
		out = append(out, fmt.Sprintf("transactions spanned only [%v, %v] rows", txnMin, txnMax))
	}
	return out
}
