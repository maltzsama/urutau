package plugin

import (
	"testing"

	"github.com/maltzsama/urutau/sink"
)

// C0 (WK-001): the plugin sink must NOT declare sink.ConcurrentWriter. It
// persists no position (Position returns ""), so the coordinator cannot
// safely serve N concurrent workers to one table and must refuse to boot a
// partitioned table whose sink lacks the capability. This guards against a
// future edit adding the interface without the underlying mechanism.
func TestPluginSinkHasNoConcurrentWriter(t *testing.T) {
	var s any = &SinkAdapter{}
	if _, ok := s.(sink.ConcurrentWriter); ok {
		t.Fatal("plugin sink must not implement sink.ConcurrentWriter (it persists no position)")
	}
}
