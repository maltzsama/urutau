package historyserver

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/eventlog"
)

type fakeStore struct {
	pipelines []eventlog.PipelineSummary
	runs      []eventlog.RunSummary
	events    []eventlog.Event
}

func (f fakeStore) ListPipelines(context.Context) ([]eventlog.PipelineSummary, error) {
	return f.pipelines, nil
}
func (f fakeStore) ListRuns(context.Context, string) ([]eventlog.RunSummary, error) {
	return f.runs, nil
}
func (f fakeStore) ReadRun(context.Context, string, string) ([]eventlog.Event, error) {
	return f.events, nil
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
