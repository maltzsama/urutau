package dashboard

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/maltzsama/urutau/internal/logging"
)

func TestEventsListNewestFirstAndFilters(t *testing.T) {
	e := NewEvents(4)
	e.Record("job_started", nil)
	e.Record("commit", map[string]any{"worker": "w-0", "table": "raw.orders"})
	e.Record("worker_reset", map[string]any{"worker": "w-0"})

	all := e.List("", "", 10)
	if len(all) != 3 {
		t.Fatalf("List = %d, want 3", len(all))
	}
	if all[0].Type != "worker_reset" {
		t.Errorf("newest first: got %q, want worker_reset", all[0].Type)
	}
	if all[1].Worker != "w-0" || all[1].Table != "raw.orders" {
		t.Errorf("worker/table not lifted from fields: %+v", all[1])
	}
	if byType := e.List("commit", "", 10); len(byType) != 1 || byType[0].Type != "commit" {
		t.Errorf("type filter: %v", byType)
	}
	if byWorker := e.List("", "w-0", 10); len(byWorker) != 2 {
		t.Errorf("worker filter: got %d, want 2", len(byWorker))
	}
}

func TestEventsEvictsOldest(t *testing.T) {
	e := NewEvents(2)
	e.Record("a", nil)
	e.Record("b", nil)
	e.Record("c", nil)
	got := e.List("", "", 10)
	if len(got) != 2 || got[0].Type != "c" || got[1].Type != "b" {
		t.Fatalf("ring did not evict oldest: %v", got)
	}
}

type fakeState struct {
	summary   PipelineSummary
	tables    []TableStatus
	workers   []WorkerStatus
	cancelled bool
	restarted string
}

func (f *fakeState) Summary() PipelineSummary { return f.summary }
func (f *fakeState) Tables() []TableStatus    { return f.tables }
func (f *fakeState) Workers() []WorkerStatus  { return f.workers }
func (f *fakeState) Cancel() error            { f.cancelled = true; return nil }
func (f *fakeState) RestartWorker(n string) error {
	f.restarted = n
	return nil
}

func TestHandlerEndpoints(t *testing.T) {
	st := &fakeState{
		summary: PipelineSummary{Pipeline: "shop"},
		tables:  []TableStatus{{Target: "raw.orders"}},
		workers: []WorkerStatus{{Name: "w-0"}},
	}
	events := NewEvents(8)
	events.Record("commit", map[string]any{"table": "raw.orders"})

	// A real log buffer so the /logs path is exercised end to end.
	logger, logBuf, err := logging.NewBuffered("debug", "text", 8)
	if err != nil {
		t.Fatalf("NewBuffered: %v", err)
	}
	logger.Info("hello", "table", "raw.orders")

	h := New(st, events, logBuf, slog.Default())
	mux := http.NewServeMux()
	h.Register(mux)

	do := func(method, path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
		return rec
	}

	for _, path := range []string{"/api/v1/pipeline", "/api/v1/tables", "/api/v1/workers", "/api/v1/events", "/healthz", "/readyz"} {
		if rec := do("GET", path); rec.Code != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", path, rec.Code)
		}
	}

	var logs []logEntry
	if rec := do("GET", "/api/v1/logs"); rec.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/logs = %d", rec.Code)
	} else if err := json.Unmarshal(rec.Body.Bytes(), &logs); err != nil {
		t.Fatalf("logs decode: %v", err)
	}
	if len(logs) != 1 || logs[0].Msg != "hello" || logs[0].Attrs["table"] != "raw.orders" {
		t.Errorf("logs = %+v, want the one captured line", logs)
	}

	if rec := do("POST", "/api/v1/actions/cancel"); rec.Code != http.StatusOK {
		t.Errorf("cancel = %d", rec.Code)
	}
	if !st.cancelled {
		t.Error("cancel did not reach the state")
	}
	if rec := do("POST", "/api/v1/actions/restart/w-0"); rec.Code != http.StatusOK {
		t.Errorf("restart = %d", rec.Code)
	}
	if st.restarted != "w-0" {
		t.Errorf("restart = %q, want w-0", st.restarted)
	}

	if rec := do("GET", "/api/v1/workers/nope"); rec.Code != http.StatusNotFound {
		t.Errorf("unknown worker = %d, want 404", rec.Code)
	}
}
