package coordinator

import "github.com/maltzsama/urutau/source"

// setRouteForTest publishes one table's owners into the routing snapshot.
// Production routing is published by boot and by ScaleTable.
func (c *Coordinator) setRouteForTest(target string, owners []*workerState) {
	c.publishRouting(c.loadRouting().WithOwners(target, owners))
}

// setRangesForTest replaces the whole ranges map in the routing snapshot,
// matching the old c.partitionRanges = map[...] assignment.
func (c *Coordinator) setRangesForTest(ranges map[string][]source.Chunk) {
	c.publishRouting(c.loadRouting().WithRanges(ranges))
}
