package coordinator

// workerFor returns the worker registered under name, or nil. It reads under
// c.mu: registerOwner/retireOwner mutate the registry at runtime during a
// re-slice (issue #312), so an unlocked read races them (issue #479). The
// returned pointer stays valid even if the worker is retired concurrently (the
// GC keeps it alive), which is what the ack path needs — it only prunes that
// worker's own sent list.
func (c *Coordinator) workerFor(name string) *workerState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.workers[name]
}

// workersSnapshot returns the registered workers at one instant, for a reader
// that must touch several of them (shutdown, dashboard summary) without
// holding c.mu across the loop.
func (c *Coordinator) workersSnapshot() []*workerState {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*workerState, 0, len(c.workers))
	for _, w := range c.workers {
		out = append(out, w)
	}
	return out
}
