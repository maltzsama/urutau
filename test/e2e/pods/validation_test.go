package pods

// The production-readiness hard gates beyond row state (issue #387): each
// table's committed cdc.position, checked independently of its rows, and
// per-table progress over time (from #355).

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/maltzsama/urutau/position"
)

// progressSample is one table's state at one instant.
type progressSample struct {
	At        time.Time `json:"at"`
	Produced  int64     `json:"produced"`            // row mutations the workload committed so far
	Committed string    `json:"committed,omitempty"` // the table's cdc.position
}

// progressSampler polls each table's committed position every interval and
// checks the gates that need the history: no position regression, and no
// table starved while the others advance.
type progressSampler struct {
	w        *workload
	trino    *sql.DB
	interval time.Duration
	// starve is how long a table may go without advancing while its source
	// changes and another table advances. Restarts pause every table at
	// once, which is not starvation.
	starve time.Duration

	mu       sync.Mutex
	samples  map[string][]progressSample
	problems []string

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func newProgressSampler(w *workload, trino *sql.DB, interval, starve time.Duration) *progressSampler {
	return &progressSampler{w: w, trino: trino, interval: interval, starve: starve, samples: map[string][]progressSample{}}
}

// committedPosition reads a table's cdc.position table property ("" when
// the table does not exist yet or has none).
func committedPosition(ctx context.Context, trino *sql.DB, target string) (string, error) {
	var pos sql.NullString
	err := trino.QueryRowContext(ctx, `SELECT value FROM "`+target+`$properties" WHERE key = 'cdc.position'`).Scan(&pos)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return pos.String, err
}

func (s *progressSampler) start(ctx context.Context) {
	s.stopCh = make(chan struct{})
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			s.sample(ctx)
			select {
			case <-s.stopCh:
				return
			case <-ctx.Done():
				return
			case <-time.After(s.interval):
			}
		}
	}()
}

func (s *progressSampler) stop() {
	close(s.stopCh)
	s.wg.Wait()
}

func (s *progressSampler) sample(ctx context.Context) {
	now := time.Now()
	for i, g := range s.w.gens {
		pos, err := committedPosition(ctx, s.trino, g.t.Target)
		if err != nil {
			continue // not created yet, or Trino busy: the next poll retries
		}
		cur := progressSample{At: now, Produced: s.w.produced[i].Load(), Committed: pos}
		s.mu.Lock()
		hist := s.samples[g.t.Target]
		if n := len(hist); n > 0 && regressed(hist[n-1].Committed, pos) {
			s.problems = append(s.problems, fmt.Sprintf("%s: committed position regressed from %s to %s at %s",
				g.t.Target, hist[n-1].Committed, pos, now.Format(time.RFC3339)))
		}
		s.samples[g.t.Target] = append(hist, cur)
		s.mu.Unlock()
	}
	s.checkStarvation(now)
}

// regressed reports whether next, a later committed position, fails to
// contain prev. Unknown or unparsable positions never count.
func regressed(prev, next string) bool {
	if prev == "" || next == "" || prev == next {
		return false
	}
	p, perr := position.ParseGTID(prev)
	n, nerr := position.ParseGTID(next)
	return perr == nil && nerr == nil && !n.Contains(p)
}

// checkStarvation fails a table whose committed position has not moved for
// starve while its source kept changing, if another table's did move in that
// same stretch.
func (s *progressSampler) checkStarvation(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	since := now.Add(-s.starve)
	advanced := map[string]bool{}
	for table, hist := range s.samples {
		first, last, ok := window(hist, since)
		advanced[table] = ok && first.Committed != last.Committed
	}
	for table, hist := range s.samples {
		first, last, ok := window(hist, since)
		if !ok || advanced[table] || last.Produced == first.Produced {
			continue
		}
		for other, adv := range advanced {
			if other != table && adv {
				s.problems = append(s.problems, fmt.Sprintf("%s: starved — committed position %s unchanged for %s while its source produced %d mutations and %s advanced",
					table, last.Committed, s.starve, last.Produced-first.Produced, other))
				break
			}
		}
	}
}

// window returns the first sample at or before since and the last sample,
// when the history spans the whole window.
func window(hist []progressSample, since time.Time) (first, last progressSample, ok bool) {
	if len(hist) < 2 || hist[0].At.After(since) {
		return first, last, false
	}
	for i := len(hist) - 1; i >= 0; i-- {
		if !hist[i].At.After(since) {
			return hist[i], hist[len(hist)-1], true
		}
	}
	return first, last, false
}

// report returns the progress history and the problems found while sampling.
func (s *progressSampler) report() (map[string][]progressSample, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string][]progressSample, len(s.samples))
	for k, v := range s.samples {
		out[k] = append([]progressSample(nil), v...)
	}
	return out, append([]string(nil), s.problems...)
}

// positionProblems checks each table's committed position once the run has
// drained, independently of its rows:
//   - it is at or before what MySQL executed (never beyond the workload);
//   - it covers everything MySQL had executed right before the table's last
//     generated transaction, so that transaction was the last one pending.
func positionProblems(ctx context.Context, w *workload, trino *sql.DB) []string {
	var out []string
	var executed string
	if err := w.db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&executed); err != nil {
		return []string{fmt.Sprintf("read gtid_executed: %v", err)}
	}
	exec, err := position.ParseGTID(executed)
	if err != nil {
		return []string{fmt.Sprintf("parse gtid_executed %q: %v", executed, err)}
	}
	for i, g := range w.gens {
		pos, err := committedPosition(ctx, trino, g.t.Target)
		if err != nil {
			out = append(out, fmt.Sprintf("%s: read cdc.position: %v", g.t.Target, err))
			continue
		}
		if pos == "" {
			out = append(out, g.t.Target+": no committed cdc.position")
			continue
		}
		committed, err := position.ParseGTID(pos)
		if err != nil {
			out = append(out, fmt.Sprintf("%s: cdc.position %q: %v", g.t.Target, pos, err))
			continue
		}
		if !exec.Contains(committed) {
			out = append(out, fmt.Sprintf("%s: committed position %s is beyond what MySQL executed (%s)", g.t.Target, pos, executed))
		}
		before := w.stats[i].GTIDBeforeLast
		if before == "" {
			continue
		}
		lower, err := position.ParseGTID(before)
		if err != nil {
			out = append(out, fmt.Sprintf("%s: gtid before last txn %q: %v", g.t.Target, before, err))
			continue
		}
		if !committed.Contains(lower) || committed.Compare(lower) == 0 {
			out = append(out, fmt.Sprintf("%s: committed position %s does not reach past %s, what MySQL had executed right before the table's last transaction",
				g.t.Target, pos, before))
		}
	}
	return out
}
