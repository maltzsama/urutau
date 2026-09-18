package observability

import "testing"

func TestWorkerSnapshotExtractsSeries(t *testing.T) {
	m := New()
	m.CommitFailures.WithLabelValues("raw.orders").Inc()
	m.CommitFailures.WithLabelValues("raw.orders").Inc()
	m.DeletesDropped.WithLabelValues("raw.orders").Add(3)
	m.SnapshotProgress.WithLabelValues("raw.orders").Set(0.5)
	m.EnrichEvicted.WithLabelValues("raw.orders", "ref").Add(2)

	s := m.WorkerSnapshot()
	if s.CommitFailures["raw.orders"] != 2 {
		t.Errorf("commit failures = %v", s.CommitFailures)
	}
	if s.DeletesDropped["raw.orders"] != 3 {
		t.Errorf("deletes dropped = %v", s.DeletesDropped)
	}
	if s.SnapshotProgress["raw.orders"] != 0.5 {
		t.Errorf("snapshot progress = %v", s.SnapshotProgress)
	}
	if ec := s.Enrich["raw.orders\x00ref"]; ec.Evicted != 2 {
		t.Errorf("enrich = %+v", ec)
	}
}
