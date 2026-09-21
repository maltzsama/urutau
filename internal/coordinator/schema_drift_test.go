package coordinator

import (
	"testing"

	"github.com/maltzsama/urutau/internal/dashboard"
	"github.com/maltzsama/urutau/internal/eventlog"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// #272: a worker's schema-drift report must be recorded so the log/event trail
// says why the worker is stopping.
func TestOnSchemaDriftRecordsEvent(t *testing.T) {
	c, _ := coordHarness()
	c.dashEvents = dashboard.NewEvents(10)

	c.onSchemaDrift("w0", &pb.SchemaDrift{Table: "raw.orders", Column: "extra", Kind: "added"})

	evs := c.dashEvents.List("", "", 10)
	if len(evs) != 1 || evs[0].Type != eventlog.KindSchemaDrift || evs[0].Table != "raw.orders" {
		t.Fatalf("events = %+v, want one schema_drift for raw.orders", evs)
	}
}
