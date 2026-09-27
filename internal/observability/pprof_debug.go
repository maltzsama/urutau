//go:build faultinject

package observability

import (
	"net/http"
	"net/http/pprof"
)

// The race e2e image (build/Dockerfile.race, -tags faultinject) serves the Go
// profiler on the metrics port, so a test can read a live Pod's heap
// (`/debug/pprof/heap`). A release build never compiles this file.
func init() {
	debugRoutes = append(debugRoutes, func(mux *http.ServeMux) {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	})
}
