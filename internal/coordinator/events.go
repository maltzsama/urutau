package coordinator

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// emitLog writes an event and logs any failure (best-effort trail).
func (c *Coordinator) emitLog(kind string, fields map[string]any) {
	if err := c.emit(kind, fields); err != nil {
		c.log.Warn("coordinator: eventlog emit", "kind", kind, "err", err)
	}
}

// terminalFields builds a job_stopped / job_terminated event's fields: the
// coarse reason code plus the underlying error text, so a postmortem has more
// than "reason": "crashloop" to go on (issue #351). err may be nil.
func terminalFields(reason string, err error) map[string]any {
	f := map[string]any{"reason": reason}
	if err != nil {
		f["error"] = err.Error()
	}
	return f
}

// statusz renders the live coordinator state for /statusz (design §13.4).
func (c *Coordinator) statusz(w http.ResponseWriter, r *http.Request) {
	type workerStatus struct {
		Phase     string            `json:"phase"`
		Epoch     uint64            `json:"epoch"`
		Attached  bool              `json:"attached"`
		Inflight  int64             `json:"inflight_bytes"`
		Committed map[string]string `json:"committed,omitempty"`
	}
	st := map[string]any{
		"run_id": c.runID,
	}
	// Point-in-time enrichment is visible to the operator: enriched columns
	// are NOT reproducible by replay (the reference is a snapshot, not
	// CDC), and that trade is declared, not hidden.
	for _, t := range c.cfg.Spec.Tables {
		if len(t.Enrich) > 0 {
			st["enrichment"] = "point-in-time"
			break
		}
	}
	ws := map[string]*workerStatus{}
	// One lock across the whole iteration: statusz runs from the metrics
	// server, which boots BEFORE run() populates c.workers — a per-entry
	// lock still races the map write on boot (audit #13).
	c.mu.Lock()
	for name, w := range c.workers {
		// Phase reflects reality: the field used to hardcode "attached" for
		// every worker, which lied about detached/pending ones.
		phase := "detached"
		if w.attached {
			phase = "attached"
		}
		ws[name] = &workerStatus{
			Phase:     phase,
			Epoch:     w.epoch,
			Attached:  w.attached,
			Inflight:  c.budget.inFlight(name),
			Committed: w.committed,
		}
	}
	c.mu.Unlock()
	st["workers"] = ws
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(st); err != nil {
		c.log.Warn("statusz encode", "err", err)
	}
}

// emit writes one event to the audit trail when configured; best-effort by
// contract (a lost trail must never fail the pipeline). The dashboard's
// recent-events ring is fed here too, unconditionally, so the UI shows events
// even when no audit trail is configured.
func (c *Coordinator) emit(kind string, fields map[string]any) error {
	if c.dashEvents != nil {
		ev := c.dashEvents.Record(kind, fields)
		if c.dash != nil {
			c.dash.PublishEvent(ev)
		}
	}
	if c.ev == nil {
		return nil
	}
	// Bounded: the audit-trail upload must not hang the caller (the ack hot
	// path already fires-and-forgets, but emit is also called synchronously
	// on boot/terminal paths).
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.ev.Emit(ctx, kind, fields); err != nil {
		return err
	}
	return nil
}
