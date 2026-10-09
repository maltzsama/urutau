package observability

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// #604: PprofHandler exposes the profiler for an explicit, loopback-only
// mount; it is not part of Handler's default routes.
func TestPprofHandlerServes(t *testing.T) {
	srv := httptest.NewServer(PprofHandler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/debug/pprof/heap")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/debug/pprof/heap = %s, want 200", resp.Status)
	}
}
