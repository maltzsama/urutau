//go:build !faultinject

package observability

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// A release build does not serve the profiler.
func TestReleaseBuildHasNoProfiler(t *testing.T) {
	srv := httptest.NewServer(New().Handler(nil))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/debug/pprof/heap")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("/debug/pprof/heap in a release build: %s, want 404", resp.Status)
	}
}
