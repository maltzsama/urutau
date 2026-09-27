package historyserver

import (
	"net/http"
	"testing"

	"github.com/maltzsama/urutau/internal/eventlog"
)

// A terminated run's trail carries its lifecycle events AND its structured
// logs; ?kind narrows what a caller pages through without touching the run's
// completeness signals (which always describe the whole trail).
func TestReadRunFiltersKind(t *testing.T) {
	events := []eventlog.Event{
		{Kind: "job_started", RunID: "r1"},
		{Kind: eventlog.KindLog, RunID: "r1", Fields: map[string]any{"level": "INFO", "msg": "listening"}},
		{Kind: "commit", RunID: "r1"},
		{Kind: eventlog.KindLog, RunID: "r1", Fields: map[string]any{"level": "ERROR", "msg": "boom"}},
	}
	srv := testServer(fakeStore{events: events, sealed: true, emitted: 4}, 0)
	defer srv.Close()

	type page struct {
		Events []struct {
			Kind   string         `json:"kind"`
			Fields map[string]any `json:"fields"`
		} `json:"events"`
		Sealed  bool `json:"sealed"`
		Emitted int  `json:"emitted"`
	}
	kinds := func(query string) []string {
		t.Helper()
		var got page
		getJSON(t, srv.URL+"/api/v1/pipelines/shop/runs/r1/events"+query, &got)
		if !got.Sealed || got.Emitted != 4 {
			t.Errorf("%s: completeness = sealed:%v emitted:%d, want true/4 — a filter must not rewrite it",
				query, got.Sealed, got.Emitted)
		}
		out := make([]string, 0, len(got.Events))
		for _, e := range got.Events {
			out = append(out, e.Kind)
		}
		return out
	}

	if got := kinds(""); len(got) != 4 {
		t.Fatalf("unfiltered kinds = %v, want all 4", got)
	}
	if got := kinds("?kind=log"); len(got) != 2 || got[0] != eventlog.KindLog || got[1] != eventlog.KindLog {
		t.Fatalf("?kind=log = %v, want the two log records", got)
	}
	if got := kinds("?kind=event"); len(got) != 2 || got[0] != "job_started" || got[1] != "commit" {
		t.Fatalf("?kind=event = %v, want the two lifecycle events", got)
	}
	if got := kinds("?kind=all"); len(got) != 4 {
		t.Fatalf("?kind=all = %v, want all 4", got)
	}

	// A bogus filter is a caller error, not a silent full trail.
	resp, err := http.Get(srv.URL + "/api/v1/pipelines/shop/runs/r1/events?kind=nope")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}
