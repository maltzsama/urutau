package runner

import (
	"context"
)

// emit posts one lifecycle event to the audit trail, best-effort by contract:
// a lost event is logged and the pipeline carries on.
func (r *Runner) emit(kind string, fields map[string]any) {
	if r.ev == nil {
		return
	}
	if err := r.ev.Emit(context.Background(), kind, fields); err != nil {
		r.log.Warn("eventlog: emit failed", "kind", kind, "err", err)
	}
}

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
