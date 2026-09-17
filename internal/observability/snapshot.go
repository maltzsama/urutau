package observability

import (
	dto "github.com/prometheus/client_model/go"
)

// WorkerSnapshot is a plain snapshot of the worker-side series the dashboard
// reports back to the coordinator — the ones the coordinator cannot derive
// from acks. Counters are cumulative totals, not deltas.
type WorkerSnapshot struct {
	CommitFailures   map[string]int64
	DeletesDropped   map[string]int64
	SnapshotProgress map[string]float64
	CommitLatencyMs  map[string]float64
	Enrich           map[string]EnrichCounts // key: table + "\x00" + reference
}

// EnrichCounts is one reference's enrich counters.
type EnrichCounts struct {
	Misses       int64
	InnerDropped int64
	Evicted      int64
}

// WorkerSnapshot gathers the worker series from the registry.
func (m *Metrics) WorkerSnapshot() WorkerSnapshot {
	snap := WorkerSnapshot{
		CommitFailures:   map[string]int64{},
		DeletesDropped:   map[string]int64{},
		SnapshotProgress: map[string]float64{},
		CommitLatencyMs:  map[string]float64{},
		Enrich:           map[string]EnrichCounts{},
	}
	fams, err := m.reg.Gather()
	if err != nil {
		return snap
	}
	for _, f := range fams {
		switch f.GetName() {
		case "urutau_worker_commit_failures_total":
			for _, mt := range f.GetMetric() {
				snap.CommitFailures[labelValue(mt, "table")] = int64(mt.GetCounter().GetValue())
			}
		case "urutau_worker_deletes_dropped_total":
			for _, mt := range f.GetMetric() {
				snap.DeletesDropped[labelValue(mt, "table")] = int64(mt.GetCounter().GetValue())
			}
		case "urutau_worker_snapshot_progress_ratio":
			for _, mt := range f.GetMetric() {
				snap.SnapshotProgress[labelValue(mt, "table")] = mt.GetGauge().GetValue()
			}
		case "urutau_worker_commit_latency_ms":
			for _, mt := range f.GetMetric() {
				snap.CommitLatencyMs[labelValue(mt, "table")] = mt.GetGauge().GetValue()
			}
		case "urutau_enrich_misses_total", "urutau_enrich_inner_dropped_total", "urutau_enrich_evicted_total":
			for _, mt := range f.GetMetric() {
				key := labelValue(mt, "table") + "\x00" + labelValue(mt, "reference")
				ec := snap.Enrich[key]
				v := int64(mt.GetCounter().GetValue())
				switch f.GetName() {
				case "urutau_enrich_misses_total":
					ec.Misses = v
				case "urutau_enrich_inner_dropped_total":
					ec.InnerDropped = v
				case "urutau_enrich_evicted_total":
					ec.Evicted = v
				}
				snap.Enrich[key] = ec
			}
		}
	}
	return snap
}

func labelValue(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}
