package coordinator

import (
	"testing"

	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/spec"
)

// The assignment carries the spec's sink type, namespace and options to the
// worker, and never its credentials.
func TestSinkAssignmentCarriesTheSpecSinkWithoutCredentials(t *testing.T) {
	s := &spec.Spec{Sink: spec.Sink{
		Type:         "couchbase",
		URI:          "couchbase://couchbase.e2e.svc.cluster.local",
		Namespace:    "lakehouse",
		ClientID:     "urutau",
		ClientSecret: "urutaupass",
		CommitMode:   "atomic",
	}}
	got := sinkAssignment(s)

	if got.Type != "couchbase" || got.Namespace != "lakehouse" {
		t.Errorf("type=%q namespace=%q, want couchbase/lakehouse", got.Type, got.Namespace)
	}
	if v := got.Options[driver.OptCommitMode]; v != "atomic" {
		t.Errorf("commit mode = %q, want atomic", v)
	}
	for _, k := range []string{driver.OptClientID, driver.OptClientSecret} {
		if v, ok := got.Options[k]; ok {
			t.Errorf("option %s = %q travels in the assignment; credentials must not", k, v)
		}
	}
	if _, ok := got.Options[driver.OptWarehouse]; ok {
		t.Error("an empty option travels in the assignment; it would erase the worker's own value")
	}
}
