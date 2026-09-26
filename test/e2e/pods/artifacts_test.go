package pods

// Failure artifacts for the production-readiness runs (issue #387): logs
// that survive Pod replacement, and a dump of the cluster and table state
// taken before teardown.

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// runArtifactsDir is the per-run directory under URUTAU_E2E_ARTIFACTS (or the
// system temp dir), created on first use.
func runArtifactsDir(profile string, seed uint64) (string, error) {
	base := os.Getenv("URUTAU_E2E_ARTIFACTS")
	if base == "" {
		base = filepath.Join(os.TempDir(), "urutau-e2e")
	}
	dir := filepath.Join(base, fmt.Sprintf("production-readiness-%s-%d", profile, seed))
	return dir, os.MkdirAll(dir, 0o755)
}

// logCollector follows every container of a pipeline's Pods into files, one
// per Pod and restart, so a container's log outlives its replacement (a
// restarted container's own log is gone after the next restart, and a
// deleted Pod's with it).
type logCollector struct {
	dir, ns, pipeline string

	mu      sync.Mutex
	started map[string]bool // "pod/container/restartCount"

	stopCh   chan struct{}
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	stopOnce sync.Once
}

func newLogCollector(dir, ns, pipeline string) *logCollector {
	return &logCollector{dir: filepath.Join(dir, "logs"), ns: ns, pipeline: pipeline, started: map[string]bool{}}
}

func (l *logCollector) start() error {
	if err := os.MkdirAll(l.dir, 0o755); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	l.stopCh = make(chan struct{})
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		for {
			l.poll(ctx)
			select {
			case <-l.stopCh:
				return
			case <-time.After(3 * time.Second):
			}
		}
	}()
	return nil
}

// poll starts a follower for every running container not followed yet:
// every container of every Pod, one follower per restart.
func (l *logCollector) poll(ctx context.Context) {
	out, err := kubectlCmd("", "-n", l.ns, "get", "pods", "-l", "urutau.io/pipeline="+l.pipeline, "-o",
		`jsonpath={range .items[*]}{.metadata.name}{"\t"}{.status.phase}{"\t"}{range .status.containerStatuses[*]}{.name}{"="}{.restartCount}{","}{end}{"\n"}{end}`)
	if err != nil {
		return
	}
	for _, line := range strings.Split(out, "\n") {
		f := strings.Split(line, "\t")
		if len(f) != 3 || f[1] != "Running" {
			continue
		}
		pod := f[0]
		for _, cs := range strings.Split(strings.TrimSuffix(f[2], ","), ",") {
			container, restarts, ok := strings.Cut(cs, "=")
			if !ok || container == "" {
				continue
			}
			l.follow(ctx, pod, container, restarts)
		}
	}
}

// follow starts one follower for a container's current run, unless one is
// already running. The run is marked followed only once the file and the
// process are up, so a failed start is retried on the next poll.
func (l *logCollector) follow(ctx context.Context, pod, container, restarts string) {
	key := pod + "/" + container + "/" + restarts
	l.mu.Lock()
	seen := l.started[key]
	l.mu.Unlock()
	if seen {
		return
	}
	file, err := os.Create(filepath.Join(l.dir, pod+"-"+container+"-r"+restarts+".log"))
	if err != nil {
		return
	}
	cmd := exec.CommandContext(ctx, "kubectl", "-n", l.ns, "logs", "-f", pod, "-c", container)
	cmd.Stdout, cmd.Stderr = file, file
	if err := cmd.Start(); err != nil {
		_ = file.Close()
		return
	}
	l.mu.Lock()
	l.started[key] = true
	l.mu.Unlock()
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		_ = cmd.Wait()
		_ = file.Close()
	}()
}

// stop ends every follower, giving them a moment to flush the tail.
func (l *logCollector) stop() {
	l.stopOnce.Do(func() {
		close(l.stopCh)
		time.Sleep(2 * time.Second)
		l.cancel()
		l.wg.Wait()
	})
}

// races scans every collected log for a race report and returns the files
// that hold one, with the report's first lines.
func (l *logCollector) races() []string {
	var out []string
	files, _ := filepath.Glob(filepath.Join(l.dir, "*.log"))
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		var report []string
		for sc.Scan() {
			line := sc.Text()
			if strings.Contains(line, "WARNING: DATA RACE") || (len(report) > 0 && len(report) < 12) {
				report = append(report, line)
			}
		}
		_ = f.Close()
		if len(report) > 0 {
			out = append(out, filepath.Base(path)+":\n"+strings.Join(report, "\n"))
		}
	}
	return out
}

// dumpDiagnostics writes, into dir/diagnostics, the cluster objects and the
// Iceberg state a failure is diagnosed from. Best effort: a failing command
// writes its error in place of its output.
func dumpDiagnostics(ctx context.Context, dir, ns, pipeline string, trino *sql.DB, tables []*prTable) {
	d := filepath.Join(dir, "diagnostics")
	if err := os.MkdirAll(d, 0o755); err != nil {
		return
	}
	sel := "urutau.io/pipeline=" + pipeline
	for name, args := range map[string][]string{
		"cdcpipeline.yaml":   {"-n", ns, "get", "cdcpipelines", pipeline, "-o", "yaml"},
		"statefulsets.yaml":  {"-n", ns, "get", "statefulsets", "-l", sel, "-o", "yaml"},
		"pods.yaml":          {"-n", ns, "get", "pods", "-l", sel, "-o", "yaml"},
		"pods-describe.txt":  {"-n", ns, "describe", "pods", "-l", sel},
		"events.txt":         {"-n", ns, "get", "events", "--sort-by=.lastTimestamp"},
		"services.yaml":      {"-n", ns, "get", "services", "-o", "yaml"},
		"scaledobjects.yaml": {"-n", ns, "get", "scaledobjects", "-o", "yaml"},
		"hpa.yaml":           {"-n", ns, "get", "hpa", "-o", "yaml"},
		"chaos.yaml":         {"-n", ns, "get", "podchaos,networkchaos,stresschaos", "-o", "yaml"},
		"data-services.txt":  {"-n", dataNS, "get", "pods", "-o", "wide"},
	} {
		out, err := kubectlCmd("", args...)
		if err != nil {
			out = "error: " + err.Error()
		}
		_ = os.WriteFile(filepath.Join(d, name), []byte(out+"\n"), 0o644)
	}
	if trino == nil {
		return
	}
	for _, tb := range tables {
		var b strings.Builder
		pos, err := committedPosition(ctx, trino, tb.Target)
		fmt.Fprintf(&b, "cdc.position: %s (err %v)\n\nproperties:\n", pos, err)
		dumpQuery(ctx, trino, &b, `SELECT key, value FROM "`+tb.Target+`$properties"`)
		b.WriteString("\nsnapshots:\n")
		dumpQuery(ctx, trino, &b, `SELECT CAST(committed_at AS VARCHAR), CAST(snapshot_id AS VARCHAR), operation, element_at(summary, 'cdc.position'), element_at(summary, 'added-records') FROM "`+tb.Target+`$snapshots" ORDER BY committed_at`)
		_ = os.WriteFile(filepath.Join(d, "iceberg-"+tb.Target+".txt"), []byte(b.String()), 0o644)
	}
}

// dumpQuery writes a query's rows, tab-separated, or its error.
func dumpQuery(ctx context.Context, db *sql.DB, b *strings.Builder, q string) {
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		fmt.Fprintf(b, "error: %v\n", err)
		return
	}
	defer func() { _ = rows.Close() }()
	cols, _ := rows.Columns()
	cells := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range cells {
		ptrs[i] = &cells[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			fmt.Fprintf(b, "error: %v\n", err)
			return
		}
		parts := make([]string, len(cells))
		for i, c := range cells {
			parts[i] = c.String
		}
		b.WriteString(strings.Join(parts, "\t") + "\n")
	}
}

// dumpStates writes the oracle, the MySQL state and the Iceberg state of each
// table, one "key<TAB>rev<TAB>image" line per row, sorted, for a diff by hand.
func dumpStates(ctx context.Context, dir string, w *workload, trino *sql.DB) {
	d := filepath.Join(dir, "diagnostics")
	_ = os.MkdirAll(d, 0o755)
	write := func(name string, rows map[string]oracleRow) {
		lines := make([]string, 0, len(rows))
		for id, r := range rows {
			lines = append(lines, id+"\t"+strconv.FormatInt(r.Rev, 10)+"\t"+r.Image)
		}
		sort.Strings(lines)
		_ = os.WriteFile(filepath.Join(d, name), []byte(strings.Join(lines, "\n")+"\n"), 0o644)
	}
	for _, g := range w.gens {
		write("oracle-"+g.t.Name+".tsv", g.rows)
		if src, _, err := readCanon(ctx, w.db, g.t, false, ""); err == nil {
			write("mysql-"+g.t.Name+".tsv", src)
		}
		if trino != nil {
			if sink, _, err := readCanon(ctx, trino, g.t, true, ""); err == nil {
				write("iceberg-"+g.t.Target+".tsv", sink)
			}
		}
	}
}
