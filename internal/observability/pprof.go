package observability

import (
	"net/http"
	"net/http/pprof"
)

// PprofHandler returns a mux serving the Go profiler. Handler does NOT mount
// it — a release build serves no profiler by default — so a caller must mount
// it explicitly, behind a flag, on a loopback-only address (issue #604).
func PprofHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}
