package dashboard

import (
	"sync"
	"time"
)

// Event is one operational event, newest-first in the Events view.
type Event struct {
	TS      string         `json:"ts"`
	Type    string         `json:"type"`
	Worker  string         `json:"worker,omitempty"`
	Table   string         `json:"table,omitempty"`
	Message string         `json:"message"`
	Fields  map[string]any `json:"fields,omitempty"`
}

// Events is a bounded in-memory ring of the coordinator's recent events. The
// coordinator feeds it from its emit hook — the same place the S3 audit trail
// is written — so the dashboard and the durable trail never diverge.
type Events struct {
	mu   sync.Mutex
	buf  []Event
	next int
	full bool
}

// NewEvents returns an Events ring of at most capacity records.
func NewEvents(capacity int) *Events {
	if capacity <= 0 {
		capacity = 1000
	}
	return &Events{buf: make([]Event, capacity)}
}

// Record captures one event. worker/table are lifted out of fields when
// present; the rest stay as Fields for the UI's chips.
func (e *Events) Record(kind string, fields map[string]any) {
	ev := Event{
		TS:      time.Now().UTC().Format(time.RFC3339Nano),
		Type:    kind,
		Message: kind,
		Fields:  fields,
	}
	if v, ok := fields["worker"].(string); ok {
		ev.Worker = v
	}
	if v, ok := fields["table"].(string); ok {
		ev.Table = v
	}
	e.mu.Lock()
	e.buf[e.next] = ev
	e.next = (e.next + 1) % len(e.buf)
	if e.next == 0 {
		e.full = true
	}
	e.mu.Unlock()
}

// List returns up to limit events, newest first, filtered by type and worker
// (an empty filter matches everything).
func (e *Events) List(typ, worker string, limit int) []Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	if limit <= 0 || limit > len(e.buf) {
		limit = len(e.buf)
	}
	count := e.next
	if e.full {
		count = len(e.buf)
	}
	out := make([]Event, 0, limit)
	for i := 0; i < count && len(out) < limit; i++ {
		idx := (e.next - 1 - i + len(e.buf)) % len(e.buf)
		ev := e.buf[idx]
		if typ != "" && ev.Type != typ {
			continue
		}
		if worker != "" && ev.Worker != worker {
			continue
		}
		out = append(out, ev)
	}
	return out
}
