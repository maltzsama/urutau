package runner

// release closes every resource the runner owns. The collapsed runner owns the
// sink; without this a ClickHouse/Couchbase/plugin connection leaks on every
// run (issue #487). The coordinator already closes its own sink.
func (r *Runner) release() {
	if r.rdr != nil {
		r.rdr.Close()
	}
	if r.closeQuery != nil {
		r.closeQuery()
	}
	for _, st := range r.enrichStages {
		st.Stop()
	}
	if r.snk != nil {
		_ = r.snk.Close()
	}
}
