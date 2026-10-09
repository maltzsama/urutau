package runner

import (
	"context"
	"sync"

	"github.com/maltzsama/urutau/source"
)

// ddlReporter routes the source reader's destructive-DDL reports to the
// runner's audit trail. The reader captures its callback at OpenSource time —
// before newRunner opens the eventlog — so the sink is set once the trail
// exists. No report can precede the stream's start, so nothing is dropped in
// practice.
type ddlReporter struct {
	mu     sync.Mutex
	report func(source.DestructiveDDL)
}

// ReportDestructiveDDL implements the reader's callback.
func (d *ddlReporter) ReportDestructiveDDL(x source.DestructiveDDL) {
	d.mu.Lock()
	f := d.report
	d.mu.Unlock()
	if f != nil {
		f(x)
	}
}

// set installs the destination once the eventlog is open.
func (d *ddlReporter) set(f func(source.DestructiveDDL)) {
	d.mu.Lock()
	d.report = f
	d.mu.Unlock()
}

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
