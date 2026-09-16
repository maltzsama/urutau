package clickhouse

import (
	"testing"

	"github.com/maltzsama/urutau/sink"
)

// ClickHouse does not implement Iceberg table maintenance (issue #96): the
// runner/coordinator capability gate (snk.(sink.Maintainable)) must actually
// gate something. This is the negative half of the proof; the positive half
// (iceberg.Sink DOES implement it) lives in
// internal/sink/iceberg/maintainer_test.go.
func TestSinkDoesNotImplementMaintainable(t *testing.T) {
	var s sink.Sink = &Sink{}
	if _, ok := s.(sink.Maintainable); ok {
		t.Error("clickhouse.Sink must NOT implement sink.Maintainable — it has no maintenance mechanism, and if this ever changes it needs a deliberate implementation, not an accidental method-set match")
	}
}
