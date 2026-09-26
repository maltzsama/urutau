package pods

// Observed distributions and the diagnostics file of a production-readiness
// run (issue #384): what the workload actually did, so a failure can be
// replayed (the seed) and judged (the distributions).

import (
	"encoding/json"
	"fmt"
	"math"
	"math/bits"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"
)

// reservoirCap bounds the samples a dist keeps for its percentiles.
const reservoirCap = 8192

// dist accumulates one observed distribution: exact count, sum, min and max,
// power-of-two buckets, and a uniform reservoir for percentiles.
type dist struct {
	n         int64
	sum       float64
	lo, hi    float64
	buckets   map[int]int64 // bucket b holds values in [2^(b-1), 2^b); 0 holds values < 1
	reservoir []float64
	r         *rand.Rand
}

func newDist() *dist {
	return &dist{buckets: map[int]int64{}, r: rand.New(rand.NewPCG(1, 2))}
}

func (d *dist) add(v float64) {
	if d.n == 0 || v < d.lo {
		d.lo = v
	}
	if d.n == 0 || v > d.hi {
		d.hi = v
	}
	d.n++
	d.sum += v
	b := 0
	if v >= 1 {
		b = bits.Len64(uint64(v))
	}
	d.buckets[b]++
	if len(d.reservoir) < reservoirCap {
		d.reservoir = append(d.reservoir, v)
	} else if j := d.r.Int64N(d.n); j < reservoirCap {
		d.reservoir[j] = v
	}
}

// distSummary is a dist as it lands in the diagnostics file.
type distSummary struct {
	N       int64            `json:"n"`
	Min     float64          `json:"min"`
	Max     float64          `json:"max"`
	Mean    float64          `json:"mean"`
	P50     float64          `json:"p50"`
	P90     float64          `json:"p90"`
	P99     float64          `json:"p99"`
	Buckets map[string]int64 `json:"buckets"` // "[lo,hi)" → count
}

func (d *dist) summary() distSummary {
	s := distSummary{N: d.n, Min: d.lo, Max: d.hi, Buckets: map[string]int64{}}
	if d.n == 0 {
		return s
	}
	s.Mean = d.sum / float64(d.n)
	sorted := slices.Clone(d.reservoir)
	slices.Sort(sorted)
	q := func(p float64) float64 { return sorted[int(math.Ceil(p*float64(len(sorted))))-1] }
	s.P50, s.P90, s.P99 = q(0.50), q(0.90), q(0.99)
	for b, c := range d.buckets {
		if b == 0 {
			s.Buckets["[0,1)"] = c
			continue
		}
		s.Buckets[fmt.Sprintf("[%d,%d)", uint64(1)<<(b-1), uint64(1)<<b)] = c
	}
	return s
}

// episode is one backlog episode, as offsets from the start of the live window.
type episode struct {
	Start time.Duration `json:"start"`
	End   time.Duration `json:"end"`
}

// tableStats is what one stream observed.
type tableStats struct {
	Ops              map[string]int64            `json:"ops"` // insert/update/delete
	Txns             int64                       `json:"txns"`
	TxnErrors        int64                       `json:"txnErrors"`
	AmbiguousCommits int64                       `json:"ambiguousCommits"`
	SideOps          map[string]map[string]int64 `json:"sideOps,omitempty"` // accounts: side → op → count
	Backlog          []episode                   `json:"backlogEpisodes,omitempty"`
	// GTIDAtStop is gtid_executed read when this stream stopped: an upper
	// bound on its last mutation's position, not that position itself.
	GTIDAtStop string `json:"gtidExecutedAtStreamStop"`
	// GTIDBeforeLast is gtid_executed read right before the stream's last
	// committed transaction: the table's final committed position must be
	// strictly past it.
	GTIDBeforeLast string `json:"gtidExecutedBeforeLastTxn"`
	LiveRows       int    `json:"liveRows"`

	rowsPerTxn  *dist
	bytesPerTxn *dist
	payload     *dist
	perSecond   map[int64]int64 // unix second → committed row mutations
}

func newTableStats() *tableStats {
	return &tableStats{
		Ops: map[string]int64{}, SideOps: map[string]map[string]int64{},
		rowsPerTxn: newDist(), bytesPerTxn: newDist(), payload: newDist(),
		perSecond: map[int64]int64{},
	}
}

// recordTxn records a committed live transaction.
func (s *tableStats) recordTxn(g *tableGen, muts []mutation, at time.Time) {
	s.Txns++
	bytes := 0
	for _, m := range muts {
		s.Ops[m.Kind.String()]++
		bytes += m.Bytes
		if m.Kind != opDelete {
			s.payload.add(float64(m.Bytes))
		}
		if g.t.Kind == accountsKind {
			side := g.accountsSide(m.Key)
			if s.SideOps[side] == nil {
				s.SideOps[side] = map[string]int64{}
			}
			s.SideOps[side][m.Kind.String()]++
		}
	}
	s.rowsPerTxn.add(float64(len(muts)))
	s.bytesPerTxn.add(float64(bytes))
	s.perSecond[at.Unix()] += int64(len(muts))
}

// mutationsPerSecond summarizes the per-second rate over [from, to], counting
// the seconds with no commit as zero.
func (s *tableStats) mutationsPerSecond(from, to time.Time) distSummary {
	d := newDist()
	for sec := from.Unix(); sec <= to.Unix(); sec++ {
		d.add(float64(s.perSecond[sec]))
	}
	return d.summary()
}

// tableReport is one table's section of the diagnostics file.
type tableReport struct {
	Table              string      `json:"table"`
	Target             string      `json:"target"`
	Kind               tableKind   `json:"kind"`
	Workers            int         `json:"workers"`
	PartitionBoundary  string      `json:"expectedPartitionBoundary,omitempty"`
	Stats              *tableStats `json:"stats"`
	MutationsPerSecond distSummary `json:"mutationsPerSecond"`
	RowsPerTxn         distSummary `json:"rowsPerTransaction"`
	BytesPerTxn        distSummary `json:"bytesPerTransaction"`
	PayloadBytes       distSummary `json:"payloadBytes"`
	SinkRowsPerCommit  distSummary `json:"sinkRowsPerCommit"`
	SinkBytesPerCommit distSummary `json:"sinkBytesPerCommit"`
	OracleVsSource     *stateDiff  `json:"oracleVsSource,omitempty"`
	SourceVsSink       *stateDiff  `json:"sourceVsSink,omitempty"`
}

// runReport is the diagnostics file of one run.
type runReport struct {
	Seed             uint64          `json:"seed"`
	Profile          workloadProfile `json:"profile"`
	Started          time.Time       `json:"started"`
	LiveFrom         time.Time       `json:"liveFrom"`
	LiveTo           time.Time       `json:"liveTo"`
	ExpectedPosition string          `json:"expectedPosition"` // gtid_executed after the last generated mutation
	Tables           []tableReport   `json:"tables"`
	Failure          string          `json:"failure,omitempty"`
	Chaos            *chaosReport    `json:"chaos,omitempty"`
	// Progress is each table's sampled progress over time (#355).
	Progress map[string][]progressSample `json:"progress,omitempty"`
}

// writeReport persists the report under URUTAU_E2E_ARTIFACTS (or the system
// temp dir) and returns its path.
func writeReport(rep *runReport) (string, error) {
	dir := os.Getenv("URUTAU_E2E_ARTIFACTS")
	if dir == "" {
		dir = filepath.Join(os.TempDir(), "urutau-e2e")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return "", err
	}
	name := "production-readiness-" + rep.Profile.Name + "-" + strconv.FormatUint(rep.Seed, 10) + ".json"
	path := filepath.Join(dir, name)
	return path, os.WriteFile(path, b, 0o644)
}
