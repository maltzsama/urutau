package pods

// The benchmark's measurements (issue #375), none of which asks the engine
// under test anything: latency is read from the sink's rows, resources from
// the kubelet.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// revMark is one committed transaction as the latency probe needs it: the
// highest rev it wrote, and when MySQL committed it.
type revMark struct {
	rev int64
	at  time.Time
}

// latencyProbe measures end-to-end latency: from a transaction's commit in
// MySQL to its rows being visible in the Iceberg table.
//
// Every insert and update writes a rev that only grows within its table. The
// probe polls max(rev) of each sink table; a transaction is visible once the
// highest rev the sink has shown reaches the highest rev it wrote. The
// highest ever seen is kept, not the current maximum: the row carrying it may
// be deleted later, and a sink that applied a later change has applied the
// earlier ones. A transaction of deletes alone writes no rev and is not
// measured.
//
// The resolution is the poll interval plus the query's own time: a latency
// is the time of the first poll that saw the transaction, so it is an upper
// bound by at most one poll.
type latencyProbe struct {
	trino    *sql.DB
	tables   []*prTable
	interval time.Duration
	from     time.Time // commits before it are warm-up: not measured

	mu      sync.Mutex
	marks   [][]revMark // per table, oldest first
	seen    []int64     // highest rev the sink has shown, per table
	lat     []*dist     // seconds, per table
	errs    int
	lastErr string

	stopCh chan struct{}
	done   chan struct{}
}

func newLatencyProbe(trino *sql.DB, tables []*prTable, interval time.Duration, from time.Time) *latencyProbe {
	p := &latencyProbe{
		trino: trino, tables: tables, interval: interval, from: from,
		marks: make([][]revMark, len(tables)), seen: make([]int64, len(tables)),
		stopCh: make(chan struct{}), done: make(chan struct{}),
	}
	for range tables {
		p.lat = append(p.lat, newDist())
	}
	return p
}

// onCommit records a committed transaction of table i. It is the workload's
// commit hook, called on that table's stream.
func (p *latencyProbe) onCommit(i int, muts []mutation, at time.Time) {
	var rev int64
	for _, m := range muts {
		if m.Kind != opDelete && m.Rev > rev {
			rev = m.Rev
		}
	}
	if rev == 0 || at.Before(p.from) {
		return
	}
	p.mu.Lock()
	p.marks[i] = append(p.marks[i], revMark{rev: rev, at: at})
	p.mu.Unlock()
}

// observe credits table i's sink having shown rev at seenAt: every waiting
// transaction it covers gets its latency.
func (p *latencyProbe) observe(i int, rev int64, seenAt time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if rev > p.seen[i] {
		p.seen[i] = rev
	}
	n := 0
	for _, m := range p.marks[i] {
		if m.rev > p.seen[i] {
			break
		}
		p.lat[i].add(seenAt.Sub(m.at).Seconds())
		n++
	}
	p.marks[i] = p.marks[i][n:]
}

func (p *latencyProbe) poll(ctx context.Context) {
	for i, t := range p.tables {
		var rev sql.NullInt64
		err := p.trino.QueryRowContext(ctx, "SELECT max(rev) FROM "+t.Target).Scan(&rev)
		seenAt := time.Now()
		if err != nil {
			// The table does not exist until the engine creates it, and
			// Trino may be restarting: counted, not fatal.
			p.mu.Lock()
			p.errs++
			p.lastErr = err.Error()
			p.mu.Unlock()
			continue
		}
		if rev.Valid {
			p.observe(i, rev.Int64, seenAt)
		}
	}
}

func (p *latencyProbe) start(ctx context.Context) {
	go func() {
		defer close(p.done)
		tick := time.NewTicker(p.interval)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-p.stopCh:
				return
			case <-tick.C:
				p.poll(ctx)
			}
		}
	}()
}

// stop ends the polling after one last poll, so what became visible since
// the previous one is counted.
func (p *latencyProbe) stop(ctx context.Context) {
	select {
	case <-p.stopCh:
		return
	default:
		close(p.stopCh)
	}
	<-p.done
	p.poll(ctx)
}

// pending is how many measured transactions the sink has not shown yet.
func (p *latencyProbe) pending() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, m := range p.marks {
		n += len(m)
	}
	return n
}

// latencyReport is one table's end-to-end latency, in seconds.
type latencyReport struct {
	Table string `json:"table"`
	// Transactions measured, and those the sink had not shown when the run
	// ended: a non-zero NotVisible means the percentiles understate.
	Seconds    distSummary `json:"seconds"`
	NotVisible int         `json:"notVisible"`
}

func (p *latencyProbe) report() ([]latencyReport, int, string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]latencyReport, len(p.tables))
	for i, t := range p.tables {
		out[i] = latencyReport{Table: t.Target, Seconds: p.lat[i].summary(), NotVisible: len(p.marks[i])}
	}
	return out, p.errs, p.lastErr
}

// ── resources ───────────────────────────────────────────────────────────

// kubeletSummary is the part of the kubelet's /stats/summary the probe reads.
type kubeletSummary struct {
	Pods []struct {
		PodRef struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace"`
		} `json:"podRef"`
		Containers []struct {
			CPU struct {
				UsageNanoCores uint64 `json:"usageNanoCores"`
			} `json:"cpu"`
			Memory struct {
				WorkingSetBytes uint64 `json:"workingSetBytes"`
			} `json:"memory"`
		} `json:"containers"`
		// Network is absent on a runtime whose kubelet does not report it
		// per Pod (cri-dockerd).
		Network *struct {
			RxBytes uint64 `json:"rxBytes"`
			TxBytes uint64 `json:"txBytes"`
		} `json:"network"`
	} `json:"pods"`
}

// podUsage is one Pod's usage in one sample.
type podUsage struct {
	cpuCores float64
	memBytes float64
	rx, tx   uint64
	hasNet   bool // the kubelet reported the Pod's network counters
}

// usageOf extracts the usage of the Pods of ns whose name starts with prefix.
func usageOf(s *kubeletSummary, ns, prefix string) map[string]podUsage {
	out := map[string]podUsage{}
	for _, p := range s.Pods {
		if p.PodRef.Namespace != ns || !strings.HasPrefix(p.PodRef.Name, prefix) {
			continue
		}
		u := podUsage{}
		if p.Network != nil {
			u.rx, u.tx, u.hasNet = p.Network.RxBytes, p.Network.TxBytes, true
		}
		for _, c := range p.Containers {
			u.cpuCores += float64(c.CPU.UsageNanoCores) / 1e9
			u.memBytes += float64(c.Memory.WorkingSetBytes)
		}
		out[p.PodRef.Name] = u
	}
	return out
}

// podResources is what one Pod used over the run.
type podResources struct {
	cpu, mem       *dist
	rx, tx         uint64 // bytes since the Pod was first seen
	lastRx, lastTx uint64 // the counters of the previous sample
	seen           bool
	firstSeen      time.Time
	lastSeenAt     time.Time
}

// addNet folds one sample of the Pod's cumulative network counters in. A
// counter lower than the last one belongs to a new Pod sandbox (the Pod was
// replaced under the same name): it starts over, and what it shows is new.
func (r *podResources) addNet(rx, tx uint64) {
	if r.seen {
		if rx >= r.lastRx {
			r.rx += rx - r.lastRx
		} else {
			r.rx += rx
		}
		if tx >= r.lastTx {
			r.tx += tx - r.lastTx
		} else {
			r.tx += tx
		}
	}
	r.lastRx, r.lastTx, r.seen = rx, tx, true
}

// resourceProbe samples the CPU, memory and network of the Pods under test
// from the kubelet, which sees every container whatever engine runs in it.
type resourceProbe struct {
	ns, prefix string
	interval   time.Duration
	nodes      []string

	// netHelper, when set, is a Pod in the host's PID namespace that reads
	// the network counters the kubelet does not report (netFromHost).
	netHelper string

	mu      sync.Mutex
	pods    map[string]*podResources
	netSeen bool // some sample carried network counters
	total   struct{ cpu, mem, count *dist }
	errs    int
	lastErr string

	stopCh chan struct{}
	done   chan struct{}
}

func newResourceProbe(ns, prefix string, interval time.Duration) *resourceProbe {
	p := &resourceProbe{ns: ns, prefix: prefix, interval: interval, pods: map[string]*podResources{}, stopCh: make(chan struct{}), done: make(chan struct{})}
	p.total.cpu, p.total.mem, p.total.count = newDist(), newDist(), newDist()
	return p
}

// record folds one sample of every matching Pod into the probe.
func (p *resourceProbe) record(sample map[string]podUsage, at time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var cpu, mem float64
	for name, u := range sample {
		r := p.pods[name]
		if r == nil {
			r = &podResources{cpu: newDist(), mem: newDist(), firstSeen: at}
			p.pods[name] = r
		}
		r.cpu.add(u.cpuCores)
		r.mem.add(u.memBytes)
		if u.hasNet {
			r.addNet(u.rx, u.tx)
			p.netSeen = true
		}
		r.lastSeenAt = at
		cpu += u.cpuCores
		mem += u.memBytes
	}
	p.total.cpu.add(cpu)
	p.total.mem.add(mem)
	p.total.count.add(float64(len(sample)))
}

func (p *resourceProbe) fail(err error) {
	p.mu.Lock()
	p.errs++
	p.lastErr = err.Error()
	p.mu.Unlock()
}

func (p *resourceProbe) sample() {
	merged := map[string]podUsage{}
	for _, node := range p.nodes {
		out, err := kubectlCmd("", "get", "--raw", "/api/v1/nodes/"+node+"/proxy/stats/summary")
		if err != nil {
			p.fail(err)
			continue
		}
		var s kubeletSummary
		if err := json.Unmarshal([]byte(out), &s); err != nil {
			p.fail(err)
			continue
		}
		for name, u := range usageOf(&s, p.ns, p.prefix) {
			merged[name] = u
		}
	}
	p.netFromHost(merged)
	p.record(merged, time.Now())
}

// netHelperNS and netHelperImage are where the network helper runs and what
// it runs: the harness's own image, which is on the node already and has a
// shell.
const (
	netHelperNS    = dataNS
	netHelperImage = "urutau-e2e:dev"
)

// startNetHelper starts a Pod in the host's PID namespace. From there the
// network counters of any Pod on the node can be read from /proc, whatever
// the Pod's image holds and whatever the container runtime reports.
func (p *resourceProbe) startNetHelper() error {
	name := "bench-netprobe-" + strconv.FormatInt(time.Now().UnixNano()%2176782336, 36)
	manifest := fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata: {name: %s, namespace: %s, labels: {app: urutau-bench-netprobe}}
spec:
  hostPID: true
  restartPolicy: Never
  terminationGracePeriodSeconds: 0
  containers:
    - name: probe
      image: %s
      imagePullPolicy: IfNotPresent
      command: ["sleep", "86400"]
      securityContext: {privileged: true, runAsUser: 0}
`, name, netHelperNS, netHelperImage)
	if _, err := kubectlCmd(manifest, "apply", "-f", "-"); err != nil {
		return err
	}
	if _, err := kubectlCmd("", "-n", netHelperNS, "wait", "--for=condition=Ready", "pod/"+name, "--timeout=90s"); err != nil {
		_, _ = kubectlCmd("", "-n", netHelperNS, "delete", "pod", name, "--ignore-not-found", "--wait=false")
		return err
	}
	p.netHelper = name
	return nil
}

// netScript prints, for each Pod UID it is given, the receive and transmit
// byte counters of every interface but the loopback, as seen by one process
// of that Pod. A Pod's cgroup path carries its UID, with dashes or
// underscores depending on the cgroup driver.
const netScript = `for uid in "$@"; do
  u=$(echo "$uid" | tr - _)
  for f in /proc/[0-9]*/cgroup; do
    if grep -qe "pod$uid" -e "pod$u" "$f" 2>/dev/null; then
      awk -v uid="$uid" 'NR>2 && $1!="lo:" {rx+=$2; tx+=$10} END {print uid, rx+0, tx+0}' "${f%%cgroup}net/dev" 2>/dev/null && break
    fi
  done
done`

// netFromHost fills the network counters the kubelet left out, read through
// the helper.
func (p *resourceProbe) netFromHost(sample map[string]podUsage) {
	if p.netHelper == "" {
		return
	}
	var missing []string
	for name, u := range sample {
		if !u.hasNet {
			missing = append(missing, name)
		}
	}
	if len(missing) == 0 {
		return
	}
	out, err := kubectlCmd("", "-n", p.ns, "get", "pods", "-o", `jsonpath={range .items[*]}{.metadata.name} {.metadata.uid}{"\n"}{end}`)
	if err != nil {
		p.fail(err)
		return
	}
	byUID := map[string]string{}
	args := []string{"-n", netHelperNS, "exec", p.netHelper, "--", "sh", "-c", netScript, "netprobe"}
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) == 2 && strings.HasPrefix(f[0], p.prefix) {
			byUID[f[1]] = f[0]
			args = append(args, f[1])
		}
	}
	if len(byUID) == 0 {
		return
	}
	out, err = kubectlCmd("", args...)
	if err != nil {
		p.fail(err)
		return
	}
	for _, line := range strings.Split(out, "\n") {
		var uid string
		var rx, tx uint64
		if n, _ := fmt.Sscan(line, &uid, &rx, &tx); n != 3 {
			continue
		}
		if u, ok := sample[byUID[uid]]; ok && !u.hasNet {
			u.rx, u.tx, u.hasNet = rx, tx, true
			sample[byUID[uid]] = u
		}
	}
}

func (p *resourceProbe) start(ctx context.Context) error {
	out, err := kubectlCmd("", "get", "nodes", "-o", "jsonpath={.items[*].metadata.name}")
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}
	p.nodes = strings.Fields(out)
	if len(p.nodes) == 0 {
		return fmt.Errorf("no nodes")
	}
	// The helper reads one node's /proc: enough for a single-node cluster,
	// which is where the kubelet's own counters are missing (minikube on
	// the docker runtime). A multi-node cluster on containerd has them.
	if len(p.nodes) == 1 {
		if err := p.startNetHelper(); err != nil {
			p.fail(fmt.Errorf("network helper: %w", err))
		}
	}
	go func() {
		defer close(p.done)
		tick := time.NewTicker(p.interval)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-p.stopCh:
				return
			case <-tick.C:
				p.sample()
			}
		}
	}()
	return nil
}

func (p *resourceProbe) stop() {
	select {
	case <-p.stopCh:
		return
	default:
		close(p.stopCh)
	}
	<-p.done
	if p.netHelper != "" {
		_, _ = kubectlCmd("", "-n", netHelperNS, "delete", "pod", p.netHelper, "--ignore-not-found", "--wait=false")
	}
}

// podReport is one Pod's resource usage over the run.
type podReport struct {
	Pod        string      `json:"pod"`
	CPUCores   distSummary `json:"cpuCores"`
	MemBytes   distSummary `json:"memoryWorkingSetBytes"`
	NetRxBytes uint64      `json:"networkRxBytes"`
	NetTxBytes uint64      `json:"networkTxBytes"`
	Seconds    float64     `json:"observedSeconds"`
}

// resourceReport is the resource usage of the Pods under test.
type resourceReport struct {
	Namespace string `json:"namespace"`
	PodPrefix string `json:"podPrefix"`
	// Total is the sum over the Pods present in each sample.
	TotalCPUCores distSummary `json:"totalCpuCores"`
	TotalMemBytes distSummary `json:"totalMemoryWorkingSetBytes"`
	PodCount      distSummary `json:"podCount"`
	Pods          []podReport `json:"pods"`
	// NetworkMeasured is false when no sample carried network counters: the
	// byte counts are then zero because nothing was read, not because
	// nothing was sent.
	NetworkMeasured bool   `json:"networkMeasured"`
	SampleErrors    int    `json:"sampleErrors"`
	LastError       string `json:"lastError,omitempty"`
}

func (p *resourceProbe) report() resourceReport {
	p.mu.Lock()
	defer p.mu.Unlock()
	rep := resourceReport{
		Namespace: p.ns, PodPrefix: p.prefix,
		TotalCPUCores: p.total.cpu.summary(), TotalMemBytes: p.total.mem.summary(), PodCount: p.total.count.summary(),
		NetworkMeasured: p.netSeen, SampleErrors: p.errs, LastError: p.lastErr,
	}
	for name, r := range p.pods {
		rep.Pods = append(rep.Pods, podReport{
			Pod: name, CPUCores: r.cpu.summary(), MemBytes: r.mem.summary(),
			NetRxBytes: r.rx, NetTxBytes: r.tx,
			Seconds: r.lastSeenAt.Sub(r.firstSeen).Seconds(),
		})
	}
	return rep
}
