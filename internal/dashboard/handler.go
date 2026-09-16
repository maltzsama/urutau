package dashboard

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/maltzsama/urutau/internal/logging"
)

// LogSource is the coordinator's log tail (implemented by *logging.Buffer).
type LogSource interface {
	Tail(minLevel slog.Level, limit int) []logging.Record
}

// logEntry is the wire shape of one coordinator log line.
type logEntry struct {
	TS    string         `json:"ts"`
	Level string         `json:"level"`
	Msg   string         `json:"msg"`
	Attrs map[string]any `json:"attrs,omitempty"`
}

// Handler serves the dashboard's JSON API and the embedded SPA. It reads
// coordinator state through State and the coordinator's log tail through
// LogSource; it never imports the coordinator.
type Handler struct {
	state  State
	events *Events
	logs   LogSource
	log    *slog.Logger
}

// New builds a Handler. logs may be nil (no log tail).
func New(state State, events *Events, logs LogSource, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}
	return &Handler{state: state, events: events, logs: logs, log: log}
}

// Register mounts every route on mux: the /api/v1/* endpoints, the probes, and
// the SPA catch-all. /metrics and /statusz are registered separately by the
// caller and win over the catch-all (more specific patterns).
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/pipeline", h.pipeline)
	mux.HandleFunc("GET /api/v1/tables", h.tables)
	mux.HandleFunc("GET /api/v1/tables/{name}", h.table)
	mux.HandleFunc("GET /api/v1/workers", h.workers)
	mux.HandleFunc("GET /api/v1/workers/{name}", h.worker)
	mux.HandleFunc("GET /api/v1/events", h.listEvents)
	mux.HandleFunc("GET /api/v1/logs", h.listLogs)
	mux.HandleFunc("POST /api/v1/actions/cancel", h.cancel)
	mux.HandleFunc("POST /api/v1/actions/restart/{worker}", h.restart)
	mux.HandleFunc("GET /healthz", h.ok)
	mux.HandleFunc("GET /readyz", h.ok)
	mux.Handle("/", spaHandler())
}

func (h *Handler) pipeline(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, h.state.Summary())
}

func (h *Handler) tables(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, h.state.Tables())
}

func (h *Handler) table(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	for _, t := range h.state.Tables() {
		if t.Target == name || t.Source == name {
			writeJSON(w, t)
			return
		}
	}
	http.Error(w, "no such stream", http.StatusNotFound)
}

func (h *Handler) workers(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, h.state.Workers())
}

func (h *Handler) worker(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("worker")
	for _, ws := range h.state.Workers() {
		if ws.Name == name {
			writeJSON(w, ws)
			return
		}
	}
	http.Error(w, "no such worker", http.StatusNotFound)
}

func (h *Handler) listEvents(w http.ResponseWriter, r *http.Request) {
	limit := queryInt(r, "limit", 200)
	writeJSON(w, h.events.List(r.URL.Query().Get("type"), r.URL.Query().Get("worker"), limit))
}

func (h *Handler) listLogs(w http.ResponseWriter, r *http.Request) {
	if h.logs == nil {
		writeJSON(w, []logEntry{})
		return
	}
	minLevel, err := parseLevel(r.URL.Query().Get("level"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	recs := h.logs.Tail(minLevel, queryInt(r, "limit", 500))
	out := make([]logEntry, len(recs))
	for i, rec := range recs {
		out[i] = logEntry{
			TS:    rec.Time.UTC().Format(time.RFC3339Nano),
			Level: rec.Level.String(),
			Msg:   rec.Message,
			Attrs: rec.Attrs,
		}
	}
	writeJSON(w, out)
}

func (h *Handler) cancel(w http.ResponseWriter, _ *http.Request) {
	if err := h.state.Cancel(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]bool{"ok": true})
}

func (h *Handler) restart(w http.ResponseWriter, r *http.Request) {
	if err := h.state.RestartWorker(r.PathValue("worker")); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]bool{"ok": true})
}

func (h *Handler) ok(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func writeJSON(w http.ResponseWriter, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

func queryInt(r *http.Request, key string, def int) int {
	if s := r.URL.Query().Get(key); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			return n
		}
	}
	return def
}

// parseLevel maps a UI level filter to a slog.Level floor. "" (or "all") means
// debug, so nothing is filtered.
func parseLevel(s string) (slog.Level, error) {
	switch s {
	case "", "all", "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning", "warnings":
		return slog.LevelWarn, nil
	case "error", "errors":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown level %q", s)
	}
}
