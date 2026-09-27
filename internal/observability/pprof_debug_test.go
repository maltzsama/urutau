//go:build faultinject

package observability

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// The test build serves the heap profile on the metrics mux.
func TestDebugBuildServesTheHeapProfile(t *testing.T) {
	srv := httptest.NewServer(New().Handler(nil))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/debug/pprof/heap")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/debug/pprof/heap: %s", resp.Status)
	}
}
