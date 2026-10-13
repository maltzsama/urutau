package pods

// Cluster-free tests of the benchmark harness (issue #375), run by plain
// `go test`.

import (
	"encoding/base64"
	"math/rand/v2"
	"strings"
	"testing"
	"time"
)

const benchConfigJSON = `{
  "name": "unit", "engine": "urutau", "seed": 42,
  "initialRows": 1000, "duration": "2m", "settle": "5m", "warmup": "30s",
  "meanRate": 50, "maxTxnRows": 100, "maxPayloadBytes": 4096,
  "tables": [
    {"kind": "events", "meanRate": 400, "maxTxnRows": 20, "maxPayloadBytes": 65536,
     "mix": {"insert": 9, "update": 1, "delete": 0}},
    {"kind": "accounts", "workers": 3, "maxPayloadBytes": 999999}
  ]
}`

func TestBenchConfigParsesAndShapesTheTables(t *testing.T) {
	c, err := parseBenchConfig([]byte(benchConfigJSON))
	if err != nil {
		t.Fatal(err)
	}
	p := c.profile()
	if p.InitialRows != 1000 || p.Duration != 2*time.Minute || p.Settle != 5*time.Minute || p.MeanRate != 50 {
		t.Fatalf("profile = %+v", p)
	}
	tables := c.shape(productionTables("x", p))
	if len(tables) != 2 || tables[0].Kind != eventsKind || tables[1].Kind != accountsKind {
		t.Fatalf("tables = %v, want events then accounts: the configuration's order, and only its tables", tableNames(tables))
	}
	ev, acc := tables[0], tables[1]
	if ev.meanRate(p) != 400 || ev.maxTxnRows(p) != 20 || ev.maxPayload != 65536 || ev.baseMix() != [3]float64{9, 1, 0} {
		t.Fatalf("events keeps the run-wide load: rate %v, txn %d, payload %d, mix %v", ev.meanRate(p), ev.maxTxnRows(p), ev.maxPayload, ev.baseMix())
	}
	// accounts sets no load of its own: the run-wide one, and the default mix.
	if acc.meanRate(p) != 50 || acc.maxTxnRows(p) != 100 || acc.baseMix() != [3]float64{0.45, 0.35, 0.20} || acc.Workers != 3 {
		t.Fatalf("accounts: rate %v, txn %d, mix %v, workers %d", acc.meanRate(p), acc.maxTxnRows(p), acc.baseMix(), acc.Workers)
	}
	// Its payload column is VARCHAR(2048): a larger bound cannot be honoured.
	if acc.maxPayload != 2048 {
		t.Fatalf("accounts payload bound = %d, want the column's 2048", acc.maxPayload)
	}
}

// A table's own mix and rate reach the regimes the stream draws.
func TestBenchTableTuneDrivesTheRegimes(t *testing.T) {
	c, err := parseBenchConfig([]byte(benchConfigJSON))
	if err != nil {
		t.Fatal(err)
	}
	p := c.profile()
	ev := c.shape(productionTables("x", p))[0]
	r := rand.New(rand.NewPCG(1, 2))
	var rate float64
	for range 2000 {
		g := newRegime(r, p, ev, time.Now(), false)
		if g.Mix[opDelete] != 0 {
			t.Fatalf("a regime drew deletes (%v) for a table whose mix has none", g.Mix)
		}
		if g.Rate < 400*0.05 || g.Rate > 400*8 {
			t.Fatalf("regime rate %v outside 0.05x-8x of the table's 400/s", g.Rate)
		}
		rate += g.Rate
	}
	if mean := rate / 2000; mean < 200 || mean > 1200 {
		t.Fatalf("mean regime rate %v: not drawn around the table's own 400/s", mean)
	}
}

func TestBenchConfigRejectsWhatItCannotRun(t *testing.T) {
	for _, tc := range []struct{ name, from, to, want string }{
		{"a misspelt knob", `"meanRate": 50`, `"meanRate": 50, "meanRatee": 1`, `unknown field`},
		{"an unknown engine", `"engine": "urutau"`, `"engine": "flink"`, `engine "flink"`},
		{"external without the Pods to measure", `"engine": "urutau"`, `"engine": "external"`, `podPrefix`},
		{"a warm-up as long as the run", `"warmup": "30s"`, `"warmup": "2m"`, `warmup must be shorter`},
		{"an unknown table", `"kind": "accounts"`, `"kind": "orders"`, `table kind "orders"`},
		{"a table listed twice", `"kind": "accounts"`, `"kind": "events"`, `listed twice`},
	} {
		_, err := parseBenchConfig([]byte(strings.Replace(benchConfigJSON, tc.from, tc.to, 1)))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want one naming %q", tc.name, err, tc.want)
		}
	}
}

// The configuration travels into the in-cluster Job as base64.
func TestLoadBenchConfigAcceptsBase64(t *testing.T) {
	t.Setenv("URUTAU_BENCH_CONFIG", base64.StdEncoding.EncodeToString([]byte(benchConfigJSON)))
	c, err := loadBenchConfig()
	if err != nil || c.Name != "unit" || c.Seed != 42 {
		t.Fatalf("config = %+v, %v", c, err)
	}
}

// A transaction is visible once the highest rev the sink has shown reaches
// the highest rev it wrote; the highest ever seen counts, not the current
// one; deletes and warm-up commits are not measured.
func TestLatencyProbeCreditsWhatTheSinkHasShown(t *testing.T) {
	t0 := time.Now()
	tables := []*prTable{{Target: "a"}}
	p := newLatencyProbe(nil, tables, time.Second, t0)

	p.onCommit(0, []mutation{{Kind: opInsert, Rev: 5}}, t0.Add(-time.Second)) // warm-up
	p.onCommit(0, []mutation{{Kind: opDelete}}, t0.Add(time.Second))          // no rev
	p.onCommit(0, []mutation{{Kind: opInsert, Rev: 10}, {Kind: opUpdate, Rev: 12}}, t0.Add(1*time.Second))
	p.onCommit(0, []mutation{{Kind: opUpdate, Rev: 20}}, t0.Add(2*time.Second))
	if p.pending() != 2 {
		t.Fatalf("%d transactions waiting, want the two measured ones", p.pending())
	}

	p.observe(0, 11, t0.Add(3*time.Second)) // rev 12 not shown yet
	if p.pending() != 2 {
		t.Fatal("a transaction was credited before its highest rev was visible")
	}
	p.observe(0, 15, t0.Add(4*time.Second))
	// The row carrying rev 15 is deleted: max(rev) drops, nothing is un-seen.
	p.observe(0, 9, t0.Add(5*time.Second))
	if p.pending() != 1 {
		t.Fatalf("%d waiting, want 1", p.pending())
	}
	p.observe(0, 20, t0.Add(7*time.Second))
	rep, _, _ := p.report()
	if rep[0].NotVisible != 0 || rep[0].Seconds.N != 2 || rep[0].Seconds.Min != 3 || rep[0].Seconds.Max != 5 {
		t.Fatalf("latency = %+v, want two transactions at 3 s and 5 s", rep[0])
	}
}

func TestResourceProbeSumsThePodsUnderTest(t *testing.T) {
	var s kubeletSummary
	add := func(ns, name string, cores, mem, rx, tx uint64) {
		s.Pods = append(s.Pods, struct {
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
			Network struct {
				RxBytes uint64 `json:"rxBytes"`
				TxBytes uint64 `json:"txBytes"`
			} `json:"network"`
		}{})
		p := &s.Pods[len(s.Pods)-1]
		p.PodRef.Name, p.PodRef.Namespace = name, ns
		p.Containers = make([]struct {
			CPU struct {
				UsageNanoCores uint64 `json:"usageNanoCores"`
			} `json:"cpu"`
			Memory struct {
				WorkingSetBytes uint64 `json:"workingSetBytes"`
			} `json:"memory"`
		}, 1)
		p.Containers[0].CPU.UsageNanoCores, p.Containers[0].Memory.WorkingSetBytes = cores, mem
		p.Network.RxBytes, p.Network.TxBytes = rx, tx
	}
	add("pod-e2e", "bench-x-coordinator-0", 500_000_000, 100, 10, 20)
	add("pod-e2e", "bench-x-raw-a-0", 1_500_000_000, 300, 30, 40)
	add("pod-e2e", "other-0", 9_000_000_000, 999, 1, 1)
	add("e2e", "bench-x-not-this-namespace", 9_000_000_000, 999, 1, 1)

	u := usageOf(&s, "pod-e2e", "bench-x-")
	if len(u) != 2 || u["bench-x-raw-a-0"].cpuCores != 1.5 || u["bench-x-coordinator-0"].memBytes != 100 {
		t.Fatalf("usage = %+v, want the two Pods of the pipeline", u)
	}

	p := newResourceProbe("pod-e2e", "bench-x-", time.Second)
	now := time.Now()
	p.record(u, now)
	// The worker is replaced under the same name: its counters start over.
	u["bench-x-raw-a-0"] = podUsage{cpuCores: 1, memBytes: 200, rx: 5, tx: 6}
	u["bench-x-coordinator-0"] = podUsage{cpuCores: 0.5, memBytes: 100, rx: 25, tx: 50}
	p.record(u, now.Add(10*time.Second))
	rep := p.report()
	if rep.TotalCPUCores.Max != 2 || rep.TotalCPUCores.Min != 1.5 || rep.PodCount.Max != 2 {
		t.Fatalf("totals = cpu %+v pods %+v", rep.TotalCPUCores, rep.PodCount)
	}
	for _, pod := range rep.Pods {
		switch pod.Pod {
		case "bench-x-coordinator-0":
			if pod.NetRxBytes != 15 || pod.NetTxBytes != 30 {
				t.Fatalf("coordinator network = %d/%d, want the growth 15/30", pod.NetRxBytes, pod.NetTxBytes)
			}
		case "bench-x-raw-a-0":
			if pod.NetRxBytes != 5 || pod.NetTxBytes != 6 {
				t.Fatalf("replaced worker network = %d/%d, want what the new sandbox showed, 5/6", pod.NetRxBytes, pod.NetTxBytes)
			}
		}
	}
}
