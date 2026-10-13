package pods

// Measurements for the delete-mode benchmark (issue #750): what a row-level
// delete costs the writer, and what it costs the reader until a compaction
// applies it.

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// readSample is one sink table as a reader finds it: its files, and the
// time of a full scan.
type readSample struct {
	DataFiles   int64 `json:"dataFiles"`
	DeleteFiles int64 `json:"deleteFiles"`
	// ScanSeconds are consecutive runs of the same full scan; the first may
	// pay for a cold cache.
	ScanSeconds []float64 `json:"scanSeconds"`
}

// readCheck is one table's read cost with the run's deletes in place, and
// again once a compaction has applied them.
type readCheck struct {
	Table             string     `json:"table"`
	Before            readSample `json:"beforeCompaction"`
	After             readSample `json:"afterCompaction"`
	CompactionSeconds float64    `json:"compactionSeconds"`
	Error             string     `json:"error,omitempty"`
}

// readScans is how many times the scan is timed on each side.
const readScans = 3

// sampleRead counts the table's data and delete files and times a full scan.
// The scan reads a column of every live row, so every delete is applied:
// count(*) alone could be answered from metadata.
func sampleRead(ctx context.Context, trino *sql.DB, target string) (readSample, error) {
	var s readSample
	rows, err := trino.QueryContext(ctx, `SELECT content, count(*) FROM "`+target+`$files" GROUP BY content`)
	if err != nil {
		return s, fmt.Errorf("files of %s: %w", target, err)
	}
	for rows.Next() {
		var content, n int64
		if err := rows.Scan(&content, &n); err != nil {
			_ = rows.Close()
			return s, err
		}
		// 0 is data; 1 (position deletes and deletion vectors) and 2
		// (equality deletes) are what a reader must apply.
		if content == 0 {
			s.DataFiles += n
		} else {
			s.DeleteFiles += n
		}
	}
	if err := rows.Close(); err != nil {
		return s, err
	}
	for range readScans {
		var n, rev sql.NullInt64
		start := time.Now()
		if err := trino.QueryRowContext(ctx, "SELECT count(*), sum(rev) FROM "+target).Scan(&n, &rev); err != nil {
			return s, fmt.Errorf("scan %s: %w", target, err)
		}
		s.ScanSeconds = append(s.ScanSeconds, time.Since(start).Seconds())
	}
	return s, nil
}

// checkReads measures every table before and after Trino compacts it. It
// runs once the run has settled: the tables are final and nothing writes.
func checkReads(ctx context.Context, trino *sql.DB, tables []*prTable) []readCheck {
	out := make([]readCheck, len(tables))
	for i, t := range tables {
		rc := readCheck{Table: t.Target}
		var err error
		if rc.Before, err = sampleRead(ctx, trino, t.Target); err == nil {
			start := time.Now()
			_, err = trino.ExecContext(ctx, "ALTER TABLE "+t.Target+" EXECUTE optimize")
			rc.CompactionSeconds = time.Since(start).Seconds()
		}
		if err == nil {
			rc.After, err = sampleRead(ctx, trino, t.Target)
		}
		if err != nil {
			rc.Error = err.Error()
		}
		out[i] = rc
	}
	return out
}

// commitLatency is a table's worker commit durations, from the workers' own
// histogram (urutau_worker_commit_duration_seconds).
type commitLatency struct {
	Table       string  `json:"table"`
	Commits     float64 `json:"commits"`
	MeanSeconds float64 `json:"meanSeconds"`
	// AtMost maps a bucket's upper bound, in seconds, to the commits that
	// took at most that long (cumulative, as Prometheus keeps them).
	AtMost map[string]float64 `json:"atMostSeconds"`
}

var commitMetricRE = regexp.MustCompile(`^urutau_worker_commit_duration_seconds_(sum|count|bucket)\{([^}]*)\} ([0-9.eE+-]+)$`)

// foldCommitMetrics adds one Pod's /metrics text into the per-table commit
// latencies.
func foldCommitMetrics(text string, into map[string]*commitLatency, sums map[string]float64) {
	sc := bufio.NewScanner(strings.NewReader(text))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		m := commitMetricRE.FindStringSubmatch(sc.Text())
		if m == nil {
			continue
		}
		v, err := strconv.ParseFloat(m[3], 64)
		if err != nil {
			continue
		}
		var table, le string
		for _, kv := range strings.Split(m[2], ",") {
			k, val, _ := strings.Cut(kv, "=")
			switch val = strings.Trim(val, `"`); k {
			case "table":
				table = val
			case "le":
				le = val
			}
		}
		c := into[table]
		if c == nil {
			c = &commitLatency{Table: table, AtMost: map[string]float64{}}
			into[table] = c
		}
		switch m[1] {
		case "sum":
			sums[table] += v
		case "count":
			c.Commits += v
		case "bucket":
			c.AtMost[le] += v
		}
	}
}

// commitLatencies scrapes the commit histogram of every Pod under test. The
// histogram lives in the worker process: a worker replaced during the run
// took its own with it.
func commitLatencies(ns, prefix string) ([]commitLatency, error) {
	out, err := kubectlCmd("", "-n", ns, "get", "pods", "-o", "jsonpath={.items[*].metadata.name}")
	if err != nil {
		return nil, err
	}
	into, sums := map[string]*commitLatency{}, map[string]float64{}
	for _, pod := range strings.Fields(out) {
		if !strings.HasPrefix(pod, prefix) {
			continue
		}
		text, err := kubectlCmd("", "get", "--raw", "/api/v1/namespaces/"+ns+"/pods/"+pod+":8080/proxy/metrics")
		if err != nil {
			continue // the coordinator and a Pod on its way out serve no worker histogram
		}
		foldCommitMetrics(text, into, sums)
	}
	var res []commitLatency
	for table, c := range into {
		if c.Commits > 0 {
			c.MeanSeconds = sums[table] / c.Commits
		}
		res = append(res, *c)
	}
	return res, nil
}

// positionCost is what resolving positions cost a table's positional
// commits: the data files each one read for its keys.
type positionCost struct {
	Table       string      `json:"table"`
	Commits     int64       `json:"commits"`
	Keys        distSummary `json:"keysPerCommit"`
	FilesRead   distSummary `json:"filesReadPerCommit"`
	FilesMarked distSummary `json:"filesMarkedPerCommit"`
}

var positionLogRE = regexp.MustCompile(`positions resolved.*keys=(\d+) files_read=(\d+) files_marked=(\d+)`)

// positionCosts reads the workers' "positions resolved" lines from the run's
// collected logs. Empty for a run in equality mode, which resolves none.
func positionCosts(logDir string, tables []*prTable) ([]positionCost, error) {
	files, err := filepath.Glob(filepath.Join(logDir, "*.log"))
	if err != nil {
		return nil, err
	}
	type acc struct{ keys, read, marked *dist }
	by := map[string]*acc{}
	for _, f := range files {
		fh, err := os.Open(f)
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(fh)
		sc.Buffer(make([]byte, 1<<20), 4<<20)
		for sc.Scan() {
			line := sc.Text()
			m := positionLogRE.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			for _, t := range tables {
				if !strings.Contains(line, t.Target) {
					continue
				}
				a := by[t.Target]
				if a == nil {
					a = &acc{newDist(), newDist(), newDist()}
					by[t.Target] = a
				}
				k, _ := strconv.ParseFloat(m[1], 64)
				r, _ := strconv.ParseFloat(m[2], 64)
				mk, _ := strconv.ParseFloat(m[3], 64)
				a.keys.add(k)
				a.read.add(r)
				a.marked.add(mk)
			}
		}
		_ = fh.Close()
	}
	var out []positionCost
	for _, t := range tables {
		if a := by[t.Target]; a != nil {
			out = append(out, positionCost{Table: t.Target, Commits: a.keys.n, Keys: a.keys.summary(), FilesRead: a.read.summary(), FilesMarked: a.marked.summary()})
		}
	}
	return out, nil
}
