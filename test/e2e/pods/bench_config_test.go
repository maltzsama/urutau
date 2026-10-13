package pods

// The benchmark harness (issue #375): the production-readiness workload,
// driven by a configuration instead of a fixed profile, with the
// measurements a benchmark needs. It generates the load and measures; which
// engine carries the changes from MySQL to Iceberg is a setting, so the same
// workload runs against any CDC engine without modification.
//
// This file is the pure half: the configuration and how it shapes the
// workload. Nothing here touches a cluster (bench_unit_test.go).

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// benchDuration is a time.Duration written as "30s" or "10m" in JSON.
type benchDuration time.Duration

func (d *benchDuration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration: want a string such as \"10m\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = benchDuration(v)
	return nil
}

func (d benchDuration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// benchMix is the insert/update/delete weights of one table. The regimes
// vary around it; it need not sum to one.
type benchMix struct {
	Insert float64 `json:"insert"`
	Update float64 `json:"update"`
	Delete float64 `json:"delete"`
}

// benchTable is one table's own workload distribution. A zero field keeps
// the run-wide value.
type benchTable struct {
	// Kind is the table: accounts (unsigned 64-bit key), items (string key)
	// or events (composite key, large payloads, backlog episodes).
	Kind string `json:"kind"`
	// MeanRate is the long-run mean mutations/s. The rate varies between
	// 0.05x and 8x of it from one regime to the next.
	MeanRate float64 `json:"meanRate,omitempty"`
	// MaxTxnRows is the largest transaction, in row mutations.
	MaxTxnRows int `json:"maxTxnRows,omitempty"`
	// MaxPayloadBytes bounds the table's payload column.
	MaxPayloadBytes int `json:"maxPayloadBytes,omitempty"`
	// Mix is the insert/update/delete weights.
	Mix *benchMix `json:"mix,omitempty"`
	// Workers is the table's worker count on an engine that partitions it.
	Workers int `json:"workers,omitempty"`
	// PartitionBy is the Iceberg table's partition expressions (engine
	// "urutau"), e.g. ["identity(tenant_id)"] or ["bucket(16, event_id)"].
	PartitionBy []string `json:"partitionBy,omitempty"`
	// HotShare is the share of the key space that is hot, and HotWeight the
	// share of updates and deletes that go to it. For events the hot keys
	// are whole tenants, so a table partitioned by tenant has hot
	// partitions. Zero: updates are spread uniformly.
	HotShare  float64 `json:"hotShare,omitempty"`
	HotWeight float64 `json:"hotWeight,omitempty"`
}

// benchConfig is one benchmark run. With its seed it reproduces every random
// draw of the workload.
type benchConfig struct {
	Name string `json:"name"`
	// Seed replays a run's random choices; 0 draws one from the clock, and
	// the results record it.
	Seed uint64 `json:"seed,omitempty"`
	// Engine is what carries the changes: "urutau" applies a CDCPipeline and
	// measures its Pods; "external" expects another engine to be started
	// against the same tables, and only generates the load and measures.
	Engine string `json:"engine"`
	// Image is the Urutau image (engine "urutau"); empty is the e2e default.
	Image string `json:"image,omitempty"`
	// PodPrefix names the Pods whose resources are measured. Empty is the
	// pipeline's own Pods for "urutau"; "external" must set it.
	PodPrefix string `json:"podPrefix,omitempty"`
	// PodNamespace is the namespace of those Pods (default: the pipeline's).
	PodNamespace string `json:"podNamespace,omitempty"`
	// DeleteMode is the Urutau sink's row-delete mode: "equality" (the
	// default) or "positional".
	DeleteMode string `json:"deleteMode,omitempty"`
	// ReadCheck times a full scan of each sink table once the run has
	// settled, compacts the table (Trino's optimize), and times it again.
	ReadCheck bool `json:"readCheck,omitempty"`

	InitialRows int           `json:"initialRows"`
	Duration    benchDuration `json:"duration"`
	Settle      benchDuration `json:"settle"`
	// Warmup is the start of the live window left out of the latency and
	// throughput figures: the snapshot and the first catch-up.
	Warmup benchDuration `json:"warmup,omitempty"`

	// Run-wide load, for the tables that do not set their own.
	MeanRate        float64 `json:"meanRate"`
	MaxTxnRows      int     `json:"maxTxnRows"`
	MaxPayloadBytes int     `json:"maxPayloadBytes"`

	Tables []benchTable `json:"tables"`
}

// loadBenchConfig reads URUTAU_BENCH_CONFIG: a JSON document, its base64, or
// the path of a file holding it.
func loadBenchConfig() (*benchConfig, error) {
	v := strings.TrimSpace(os.Getenv("URUTAU_BENCH_CONFIG"))
	if v == "" {
		return nil, errors.New("URUTAU_BENCH_CONFIG is not set: give the benchmark's JSON configuration, or the path of a file holding it")
	}
	raw := []byte(v)
	if !strings.HasPrefix(v, "{") {
		// Base64 is how run-in-cluster.sh carries a file into the Job's
		// environment; anything else is a path.
		if b, err := base64.StdEncoding.DecodeString(v); err == nil && strings.HasPrefix(strings.TrimSpace(string(b)), "{") {
			raw = b
		} else if b, err := os.ReadFile(v); err == nil {
			raw = b
		} else {
			return nil, fmt.Errorf("URUTAU_BENCH_CONFIG: %w", err)
		}
	}
	return parseBenchConfig(raw)
}

func parseBenchConfig(raw []byte) (*benchConfig, error) {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields() // a misspelt knob must not silently keep its default
	var c benchConfig
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("benchmark config: %w", err)
	}
	return &c, c.validate()
}

func (c *benchConfig) validate() error {
	var problems []string
	bad := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	if c.Name == "" {
		bad("name is required")
	}
	switch c.Engine {
	case "urutau":
	case "external":
		if c.PodPrefix == "" {
			bad(`engine "external" needs podPrefix: the Pods whose resources are measured`)
		}
	default:
		bad(`engine %q: want "urutau" or "external"`, c.Engine)
	}
	switch c.DeleteMode {
	case "", "equality", "positional":
	default:
		bad(`deleteMode %q: want "equality" or "positional"`, c.DeleteMode)
	}
	if c.DeleteMode != "" && c.Engine != "urutau" {
		bad("deleteMode is the Urutau sink's; it means nothing to an external engine")
	}
	if c.InitialRows <= 0 {
		bad("initialRows must be positive")
	}
	if c.Duration <= 0 || c.Settle <= 0 {
		bad("duration and settle must be positive")
	}
	if c.Warmup < 0 || c.Warmup >= c.Duration {
		bad("warmup must be shorter than duration")
	}
	if c.MeanRate <= 0 || c.MaxTxnRows <= 0 || c.MaxPayloadBytes < 16 {
		bad("meanRate and maxTxnRows must be positive, and maxPayloadBytes at least 16")
	}
	if len(c.Tables) == 0 {
		bad("tables: at least one table")
	}
	seen := map[string]bool{}
	for _, t := range c.Tables {
		switch tableKind(t.Kind) {
		case accountsKind, itemsKind, eventsKind:
		default:
			bad("table kind %q: want accounts, items or events", t.Kind)
		}
		if seen[t.Kind] {
			bad("table kind %q is listed twice", t.Kind)
		}
		seen[t.Kind] = true
		if t.MeanRate < 0 || t.MaxTxnRows < 0 || t.MaxPayloadBytes < 0 || t.Workers < 0 {
			bad("table %s: negative value", t.Kind)
		}
		if t.HotShare < 0 || t.HotShare > 1 || t.HotWeight < 0 || t.HotWeight > 1 {
			bad("table %s: hotShare and hotWeight are shares, between 0 and 1", t.Kind)
		}
		if (t.HotShare > 0) != (t.HotWeight > 0) {
			bad("table %s: hotShare and hotWeight go together", t.Kind)
		}
		if len(t.PartitionBy) > 0 && c.Engine != "urutau" {
			bad("table %s: partitionBy is the Urutau pipeline's; an external engine lays its own tables out", t.Kind)
		}
		if m := t.Mix; m != nil && (m.Insert < 0 || m.Update < 0 || m.Delete < 0 || m.Insert+m.Update+m.Delete == 0) {
			bad("table %s: mix weights must be non-negative and not all zero", t.Kind)
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("benchmark config: %s", strings.Join(problems, "; "))
	}
	return nil
}

// profile is the run-wide workload profile of the configuration.
func (c *benchConfig) profile() workloadProfile {
	return workloadProfile{
		Name: "bench-" + c.Name, InitialRows: c.InitialRows, MeanRate: c.MeanRate,
		Duration: time.Duration(c.Duration), MaxTxnRows: c.MaxTxnRows,
		MaxPayload: c.MaxPayloadBytes, Settle: time.Duration(c.Settle),
	}
}

// shape returns the run's tables, in the configuration's order, each with
// its own distribution. all is productionTables' result for the profile.
func (c *benchConfig) shape(all []*prTable) []*prTable {
	var out []*prTable
	for _, bt := range c.Tables {
		for _, t := range all {
			if string(t.Kind) != bt.Kind {
				continue
			}
			tune := &tableTune{MeanRate: bt.MeanRate, MaxTxnRows: bt.MaxTxnRows, HotShare: bt.HotShare, HotWeight: bt.HotWeight}
			t.partitionBy = bt.PartitionBy
			if bt.Mix != nil {
				tune.Mix = [3]float64{bt.Mix.Insert, bt.Mix.Update, bt.Mix.Delete}
			}
			t.tune = tune
			// The events payload is a MEDIUMTEXT; the other tables' payload
			// columns are sized by their DDL and can only be given less.
			if bt.MaxPayloadBytes > 0 && (t.Kind == eventsKind || bt.MaxPayloadBytes < t.maxPayload) {
				t.maxPayload = bt.MaxPayloadBytes
			}
			if bt.Workers > 0 {
				t.Workers = bt.Workers
			}
			out = append(out, t)
		}
	}
	return out
}
