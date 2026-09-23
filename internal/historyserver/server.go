// Package historyserver serves a read-only, request/response API over
// terminated pipeline runs, backed by the durable eventlog trail in S3. It is
// deliberately not the live dashboard: a terminated run has nothing to push,
// so this is plain polling, and discovery is S3-only (no Kubernetes API, no
// database).
package historyserver

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/maltzsama/urutau/internal/eventlog"
)

// staticFS is the embedded single-page frontend: a pipeline picker → run
// picker → read-only event view, all plain fetch() polling.
//
//go:embed static
var staticFS embed.FS

// Config tunes the server.
type Config struct {
	// Root is the shared eventlog root the trail was written under.
	Root eventlog.RootConfig
	// Listen is the HTTP address, e.g. ":8080".
	Listen string
	// PageLimit caps the events returned per request (default 1000).
	PageLimit int
	// RetainedRuns caps how many terminated runs' decoded trails stay cached
	// in memory (default 32). A sealed run is immutable, so caching it turns a
	// repeat read — and every page after the first — into a map lookup
	// (issues #330, #331).
	RetainedRuns int
	Logger       *slog.Logger
}

// Store is the read side the server needs. The eventlog package satisfies it
// through eventlogStore; tests supply a fake.
type Store interface {
	ListPipelines(ctx context.Context) ([]eventlog.PipelineSummary, error)
	ListRuns(ctx context.Context, pipeline string) ([]eventlog.RunSummary, error)
	ReadRunTrail(ctx context.Context, pipeline, runID string) (eventlog.Trail, error)
}

const defaultPageLimit = 1000

// Run serves the API until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.PageLimit <= 0 {
		cfg.PageLimit = defaultPageLimit
	}
	s := &server{
		store:     newCachedStore(&eventlogStore{root: cfg.Root}, cfg.RetainedRuns),
		log:       cfg.Logger,
		pageLimit: cfg.PageLimit,
	}

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           s.routes(),
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		cfg.Logger.Info("history-server listening", "addr", cfg.Listen)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

type server struct {
	store     Store
	log       *slog.Logger
	pageLimit int
}

func (s *server) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/pipelines", s.listPipelines)
	mux.HandleFunc("GET /api/v1/pipelines/{name}/runs", s.listRuns)
	mux.HandleFunc("GET /api/v1/pipelines/{name}/runs/{runId}/events", s.readRun)
	// The SPA. staticFS is embedded at build time, so a missing subdir is a
	// build error, not a runtime one.
	sub, _ := fs.Sub(staticFS, "static")
	mux.Handle("GET /", http.FileServer(http.FS(sub)))
	return mux
}

type pipelineJSON struct {
	Name string `json:"name"`
}

type runJSON struct {
	ID      string `json:"id"`
	Started string `json:"started,omitempty"`
}

type eventJSON struct {
	Timestamp time.Time      `json:"timestamp"`
	RunID     string         `json:"runId"`
	Kind      string         `json:"kind"`
	Fields    map[string]any `json:"fields,omitempty"`
}

func (s *server) listPipelines(w http.ResponseWriter, r *http.Request) {
	pipes, err := s.store.ListPipelines(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	out := make([]pipelineJSON, 0, len(pipes))
	for _, p := range pipes {
		out = append(out, pipelineJSON{Name: p.Name})
	}
	s.writeJSON(w, map[string]any{"pipelines": out})
}

func (s *server) listRuns(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	// Reject an unsafe segment at the boundary: it is embedded in an S3 key
	// prefix, and confinement must not rest on S3's key semantics (#335).
	if !eventlog.ValidSegment(name) {
		http.Error(w, "invalid pipeline id", http.StatusBadRequest)
		return
	}
	runs, err := s.store.ListRuns(r.Context(), name)
	if err != nil {
		s.fail(w, err)
		return
	}
	out := make([]runJSON, 0, len(runs))
	for _, run := range runs {
		rj := runJSON{ID: run.ID}
		if !run.Started.IsZero() {
			rj.Started = run.Started.UTC().Format(time.RFC3339)
		}
		out = append(out, rj)
	}
	s.writeJSON(w, map[string]any{"runs": out})
}

func (s *server) readRun(w http.ResponseWriter, r *http.Request) {
	name, runID := r.PathValue("name"), r.PathValue("runId")
	if !eventlog.ValidSegment(name) || !eventlog.ValidSegment(runID) {
		http.Error(w, "invalid pipeline or run id", http.StatusBadRequest)
		return
	}
	trail, err := s.store.ReadRunTrail(r.Context(), name, runID)
	if err != nil {
		s.fail(w, err)
		return
	}
	page, next, err := paginate(trail.Events, r.URL.Query().Get("cursor"), s.pageLimit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	out := make([]eventJSON, 0, len(page))
	for _, e := range page {
		out = append(out, eventJSON{Timestamp: e.Timestamp, RunID: e.RunID, Kind: e.Kind, Fields: e.Fields})
	}
	// The completeness signals travel with every page: a truncated or
	// abandoned run must not render as a clean one (issues #329, #333).
	resp := map[string]any{
		"events":  out,
		"sealed":  trail.Sealed,
		"emitted": trail.Emitted,
		"dropped": trail.Dropped,
		"missing": trail.Missing,
	}
	if next != "" {
		resp["nextCursor"] = next
	}
	s.writeJSON(w, resp)
}

// paginate slices items from an offset cursor. The cursor is opaque to the
// client; today it is a decimal offset into the (already materialized) events.
func paginate[T any](items []T, cursor string, limit int) ([]T, string, error) {
	off := 0
	if cursor != "" {
		v, err := strconv.Atoi(cursor)
		if err != nil || v < 0 {
			return nil, "", fmt.Errorf("invalid cursor %q", cursor)
		}
		off = v
	}
	if off > len(items) {
		off = len(items)
	}
	end := off + limit
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = strconv.Itoa(end)
	}
	return items[off:end], next, nil
}

func (s *server) writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.log.Warn("history-server: encode", "err", err)
	}
}

func (s *server) fail(w http.ResponseWriter, err error) {
	// A missing pipeline/run is a 404 and a bad identifier a 400, so a typo
	// and a genuine S3 outage do not look alike to monitoring (#329, #335).
	switch {
	case errors.Is(err, eventlog.ErrNotFound):
		http.Error(w, "not found", http.StatusNotFound)
		return
	case errors.Is(err, eventlog.ErrInvalidID):
		http.Error(w, "invalid identifier", http.StatusBadRequest)
		return
	}
	s.log.Warn("history-server: request failed", "err", err)
	http.Error(w, "internal error", http.StatusInternalServerError)
}

// eventlogStore adapts the eventlog read primitives to Store.
type eventlogStore struct {
	root eventlog.RootConfig
}

func (s *eventlogStore) ListPipelines(ctx context.Context) ([]eventlog.PipelineSummary, error) {
	return eventlog.ListPipelines(ctx, s.root)
}

func (s *eventlogStore) ListRuns(ctx context.Context, pipeline string) ([]eventlog.RunSummary, error) {
	return eventlog.ListRuns(ctx, s.root, pipeline)
}

func (s *eventlogStore) ReadRunTrail(ctx context.Context, pipeline, runID string) (eventlog.Trail, error) {
	return eventlog.ReadRunTrail(ctx, s.root, pipeline, runID)
}
