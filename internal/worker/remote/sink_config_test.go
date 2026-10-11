package remote

import (
	"testing"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"

	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/sink"
)

// A worker's own config only knows the URI and the credentials. The sink
// type, namespace and options of the pipeline spec must come from the
// assignment, or every sink is opened as the default Iceberg catalog.
func TestWorkerSinkConfigTakesTheAssignedSink(t *testing.T) {
	cfg := RemoteConfig{
		Namespace: "raw",
		Sink: sink.Config{
			URI: "couchbase://couchbase.e2e.svc.cluster.local",
			Options: map[string]string{
				driver.OptClientID:     "urutau",
				driver.OptClientSecret: "urutaupass",
				driver.OptWarehouse:    "",
			},
		},
	}
	got := workerSinkConfig(cfg, &pb.SinkAssignment{
		Type:      "couchbase",
		Namespace: "lakehouse",
		Options: map[string]string{
			driver.OptCommitMode:   "atomic",
			driver.OptClientID:     "from-the-wire",
			driver.OptClientSecret: "from-the-wire",
		},
	})

	if got.Type != "couchbase" {
		t.Errorf("Type = %q, want couchbase", got.Type)
	}
	if got.Namespace != "lakehouse" {
		t.Errorf("Namespace = %q, want lakehouse", got.Namespace)
	}
	if got.URI != cfg.Sink.URI {
		t.Errorf("URI = %q, want the worker's own %q", got.URI, cfg.Sink.URI)
	}
	if v := got.Options[driver.OptCommitMode]; v != "atomic" {
		t.Errorf("commit mode = %q, want atomic", v)
	}
	if v := got.Options[driver.OptClientID]; v != "urutau" {
		t.Errorf("client id = %q, want the worker's own (credentials never come from the assignment)", v)
	}
	if v := got.Options[driver.OptClientSecret]; v != "urutaupass" {
		t.Errorf("client secret = %q, want the worker's own", v)
	}
}

// An assignment from a coordinator that predates the sink field leaves the
// worker's own config as it was.
func TestWorkerSinkConfigWithoutAssignedSink(t *testing.T) {
	cfg := RemoteConfig{
		Namespace: "raw",
		Sink: sink.Config{
			URI:     "http://polaris:8181/api/catalog",
			Options: map[string]string{driver.OptWarehouse: "quickstart_catalog"},
		},
	}
	got := workerSinkConfig(cfg, nil)

	if got.Type != "" || got.Namespace != "raw" || got.URI != cfg.Sink.URI {
		t.Errorf("got type=%q namespace=%q uri=%q, want the worker's own config", got.Type, got.Namespace, got.URI)
	}
	if v := got.Options[driver.OptWarehouse]; v != "quickstart_catalog" {
		t.Errorf("warehouse = %q, want quickstart_catalog", v)
	}
}

// An option the spec leaves empty must not erase the worker's own value.
func TestWorkerSinkConfigKeepsOwnValueForEmptyAssignedOption(t *testing.T) {
	cfg := RemoteConfig{Sink: sink.Config{Options: map[string]string{driver.OptWarehouse: "quickstart_catalog"}}}
	got := workerSinkConfig(cfg, &pb.SinkAssignment{Options: map[string]string{driver.OptWarehouse: ""}})

	if v := got.Options[driver.OptWarehouse]; v != "quickstart_catalog" {
		t.Errorf("warehouse = %q, want quickstart_catalog", v)
	}
}

// The worker reads a snapshot window's position from the replication slot, so
// the slot name must survive the trip from the pipeline spec to the source the
// worker rebuilds — with a URI source and with the structured postgres block.
func TestSourceSpecForCarriesTheSlotName(t *testing.T) {
	s, err := sourceSpecFor("postgres", "host=db", nil, "urutau_orders")
	if err != nil {
		t.Fatalf("sourceSpecFor: %v", err)
	}
	if s.SlotName != "urutau_orders" {
		t.Errorf("URI source: SlotName = %q, want urutau_orders", s.SlotName)
	}

	s, err = sourceSpecFor("postgres", "", []byte(`{"host":"db"}`), "urutau_orders")
	if err != nil {
		t.Fatalf("sourceSpecFor with a postgres block: %v", err)
	}
	if s.SlotName != "urutau_orders" {
		t.Errorf("structured source: SlotName = %q, want urutau_orders", s.SlotName)
	}
}
