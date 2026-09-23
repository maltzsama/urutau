package historyserver

import (
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/eventlog"
)

type fakeStore struct {
	pipelines []eventlog.PipelineSummary
	runs      []eventlog.RunSummary
	events    []eventlog.Event
	// Completeness signals the fake reports (issue #333).
	sealed  bool
	emitted int
	dropped bool
	missing int
	// The terminal outcome the fake reports (issue #350).
	outcome eventlog.Outcome
	// listErr/readErr, when set, are returned by the corresponding method.
	listErr error
	readErr error
}

func (f fakeStore) ListPipelines(context.Context) ([]eventlog.PipelineSummary, error) {
	return f.pipelines, nil
}
func (f fakeStore) ListRuns(context.Context, string) ([]eventlog.RunSummary, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.runs, nil
}
func (f fakeStore) ReadRunTrail(context.Context, string, string) (eventlog.Trail, error) {
	if f.readErr != nil {
		return eventlog.Trail{}, f.readErr
	}
	return eventlog.Trail{
		Events: f.events, Sealed: f.sealed, Emitted: f.emitted, Dropped: f.dropped, Missing: f.missing,
		Outcome: f.outcome,
	}, nil
}

func testServer(store Store, limit int) *httptest.Server {
	if limit <= 0 {
		limit = defaultPageLimit
	}
	s := &server{store: store, log: slog.Default(), pageLimit: limit}
	return httptest.NewServer(s.routes())
}

func getJSON(t *testing.T, url string, into any) *http.Response {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if into != nil {
		if err := json.Unmarshal(body, into); err != nil {
			t.Fatalf("decode %s: %v", body, err)
		}
	}
	return resp
}

// The SPA is embedded and served at /, and it drives the same API.
func TestServesSPA(t *testing.T) {
	srv := testServer(fakeStore{}, 0)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("content-type = %q, want text/html", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "/api/v1") {
		t.Fatal("the SPA must call the API")
	}
}

func TestListPipelines(t *testing.T) {
	srv := testServer(fakeStore{pipelines: []eventlog.PipelineSummary{{Name: "shop"}}}, 0)
	defer srv.Close()
	var got struct {
		Pipelines []struct{ Name string } `json:"pipelines"`
	}
	getJSON(t, srv.URL+"/api/v1/pipelines", &got)
	if len(got.Pipelines) != 1 || got.Pipelines[0].Name != "shop" {
		t.Fatalf("pipelines = %+v", got.Pipelines)
	}
}

func TestListRuns(t *testing.T) {
	started := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	srv := testServer(fakeStore{runs: []eventlog.RunSummary{{ID: "r1", Started: started}}}, 0)
	defer srv.Close()
	var got struct {
		Runs []struct {
			ID      string `json:"id"`
			Started string `json:"started"`
		} `json:"runs"`
	}
	getJSON(t, srv.URL+"/api/v1/pipelines/shop/runs", &got)
	if len(got.Runs) != 1 || got.Runs[0].ID != "r1" || got.Runs[0].Started != "2026-01-01T00:00:00Z" {
		t.Fatalf("runs = %+v", got.Runs)
	}
}

func TestReadRunPaginates(t *testing.T) {
	events := []eventlog.Event{
		{Kind: "job_started", RunID: "r1", Fields: map[string]any{"pipeline": "shop"}},
		{Kind: "commit", RunID: "r1"},
	}
	srv := testServer(fakeStore{events: events}, 1)
	defer srv.Close()

	var page struct {
		Events []struct {
			Kind string `json:"kind"`
		} `json:"events"`
		NextCursor string `json:"nextCursor"`
	}
	getJSON(t, srv.URL+"/api/v1/pipelines/shop/runs/r1/events", &page)
	if len(page.Events) != 1 || page.Events[0].Kind != "job_started" || page.NextCursor != "1" {
		t.Fatalf("page 1 = %+v cursor=%q", page.Events, page.NextCursor)
	}

	var page2 struct {
		Events []struct {
			Kind string `json:"kind"`
		} `json:"events"`
		NextCursor string `json:"nextCursor"`
	}
	getJSON(t, srv.URL+"/api/v1/pipelines/shop/runs/r1/events?cursor=1", &page2)
	if len(page2.Events) != 1 || page2.Events[0].Kind != "commit" || page2.NextCursor != "" {
		t.Fatalf("page 2 = %+v cursor=%q", page2.Events, page2.NextCursor)
	}

	resp, err := http.Get(srv.URL + "/api/v1/pipelines/shop/runs/r1/events?cursor=bad")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad cursor status = %d, want 400", resp.StatusCode)
	}
}

// #329: a missing run is 404, not a 200 with an empty list.
func TestReadRunNotFound(t *testing.T) {
	srv := testServer(fakeStore{readErr: eventlog.ErrNotFound}, 0)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/api/v1/pipelines/shop/runs/nope/events")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// #329: an unknown pipeline is 404 on the runs list.
func TestListRunsNotFound(t *testing.T) {
	srv := testServer(fakeStore{listErr: eventlog.ErrNotFound}, 0)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/api/v1/pipelines/nope/runs")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// #335: an unsafe path segment is rejected with 400 before the store is hit.
func TestReadRunRejectsUnsafeID(t *testing.T) {
	srv := testServer(fakeStore{}, 0)
	defer srv.Close()
	for _, path := range []string{
		"/api/v1/pipelines/..%2F..%2Fetc/runs",
		"/api/v1/pipelines/shop/runs/..%2F..%2Fother%2Frun-x/events",
	} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s status = %d, want 400", path, resp.StatusCode)
		}
	}
}

// #333: the completeness signals travel with the events, so the SPA can flag a
// truncated or abandoned run.
func TestReadRunReportsCompleteness(t *testing.T) {
	srv := testServer(fakeStore{
		events:  []eventlog.Event{{Kind: "job_started", RunID: "r1"}},
		sealed:  true,
		emitted: 5,
		missing: 2,
	}, 0)
	defer srv.Close()
	var got struct {
		Sealed  bool `json:"sealed"`
		Emitted int  `json:"emitted"`
		Dropped bool `json:"dropped"`
		Missing int  `json:"missing"`
	}
	getJSON(t, srv.URL+"/api/v1/pipelines/shop/runs/r1/events", &got)
	if !got.Sealed || got.Emitted != 5 || got.Missing != 2 || got.Dropped {
		t.Fatalf("completeness = %+v, want sealed=true emitted=5 missing=2 dropped=false", got)
	}
}

// #350: the run's terminal outcome travels with the events, independent of
// the completeness signals (a sealed run can still have failed).
func TestReadRunReportsOutcome(t *testing.T) {
	srv := testServer(fakeStore{
		events:  []eventlog.Event{{Kind: "job_terminated", RunID: "r1", Fields: map[string]any{"reason": "crashloop"}}},
		sealed:  true,
		outcome: eventlog.OutcomeFailed,
	}, 0)
	defer srv.Close()
	var got struct {
		Sealed  bool   `json:"sealed"`
		Outcome string `json:"outcome"`
	}
	getJSON(t, srv.URL+"/api/v1/pipelines/shop/runs/r1/events", &got)
	if !got.Sealed || got.Outcome != "failed" {
		t.Fatalf("outcome = %q sealed=%v, want failed/sealed (sealed != succeeded)", got.Outcome, got.Sealed)
	}
}

// #350: the runs list carries each run's outcome so it reads without opening
// each run.
func TestListRunsReportsOutcome(t *testing.T) {
	srv := testServer(fakeStore{
		runs:    []eventlog.RunSummary{{ID: "r1"}},
		outcome: eventlog.OutcomeSucceeded,
	}, 0)
	defer srv.Close()
	var got struct {
		Runs []struct {
			ID      string `json:"id"`
			Outcome string `json:"outcome"`
		} `json:"runs"`
	}
	getJSON(t, srv.URL+"/api/v1/pipelines/shop/runs", &got)
	if len(got.Runs) != 1 || got.Runs[0].Outcome != "succeeded" {
		t.Fatalf("runs = %+v, want outcome succeeded", got.Runs)
	}
}

// #334: the SPA must never interpolate trail contents into innerHTML — a
// malicious kind would execute in the viewer's browser. Regression guard: no
// `innerHTML = `…${…}“ (a plain-string innerHTML is fine).
func TestSPANoInnerHTMLInterpolation(t *testing.T) {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		t.Fatal(err)
	}
	b, err := fs.ReadFile(sub, "index.html")
	if err != nil {
		t.Fatal(err)
	}
	if innerHTMLInterp.Match(b) {
		t.Fatal("index.html interpolates into innerHTML; build the cell with textContent (issue #334)")
	}
}

var innerHTMLInterp = regexp.MustCompile("innerHTML\\s*=\\s*`[^`]*\\$\\{")
