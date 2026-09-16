package dashboard

import (
	"encoding/json"
	"fmt"
	"io"
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

// Handler serves the dashboard's JSON API, the SSE stream, and the embedded
// SPA. It reads coordinator state through State and the coordinator's log tail
// through LogSource; it never imports the coordinator.
type Handler struct {
	state  State
	events *Events
	logs   LogSource
	log    *slog.Logger
	hub    *Hub
}

// New builds a Handler. logs may be nil (no log tail).
func New(state State, events *Events, logs LogSource, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}
	return &Handler{state: state, events: events, logs: logs, log: log, hub: NewHub()}
}

// Register mounts every route on mux: the /api/v1/* endpoints, the SSE stream,
// the probes, and the SPA catch-all. /metrics and /statusz are registered
// separately by the caller and win over the catch-all (more specific patterns).
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/pipeline", h.pipeline)
	mux.HandleFunc("GET /api/v1/tables", h.tables)
	mux.HandleFunc("GET /api/v1/tables/{name}", h.table)
	mux.HandleFunc("GET /api/v1/workers", h.workers)
	mux.HandleFunc("GET /api/v1/workers/{name}", h.worker)
	mux.HandleFunc("GET /api/v1/events", h.listEvents)
	mux.HandleFunc("GET /api/v1/logs", h.listLogs)
	mux.HandleFunc("GET /api/v1/stream", h.stream)
	mux.HandleFunc("POST /api/v1/actions/cancel", h.cancel)
	mux.HandleFunc("POST /api/v1/actions/restart/{worker}", h.restart)
	mux.HandleFunc("GET /healthz", h.ok)
	mux.HandleFunc("GET /readyz", h.ok)
	mux.Handle("/", spaHandler())
}

// PublishState pushes the current pipeline/tables/workers snapshot to every
// connected SSE subscriber. The coordinator calls it when that state changes
// (an ack, a worker-metrics report, a maintenance result, a session change).
func (h *Handler) PublishState() {
	h.hub.Publish("state", h.statePayload())
}

// PublishEvent pushes one new event.
func (h *Handler) PublishEvent(e Event) {
	h.hub.Publish("event", e)
}

// PublishLog pushes one new coordinator log line.
func (h *Handler) PublishLog(r logging.Record) {
	h.hub.Publish("log", logEntryOf(r))
}

// statePayload is the pipeline/tables/workers snapshot shared by the SSE
// "snapshot" and "state" events.
func (h *Handler) statePayload() map[string]any {
	return map[string]any{
		"pipeline": h.state.Summary(),
		"tables":   h.state.Tables(),
		"workers":  h.state.Workers(),
	}
}

// snapshot is the full state a subscriber receives on connect.
func (h *Handler) snapshot() map[string]any {
	snap := h.statePayload()
	snap["events"] = h.events.List("", "", 200)
	snap["logs"] = h.logTail(500)
	return snap
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
		out[i] = logEntryOf(rec)
	}
	writeJSON(w, out)
}

// logTail renders the last n log records as wire entries (nil with no source).
func (h *Handler) logTail(n int) []logEntry {
	if h.logs == nil {
		return nil
	}
	recs := h.logs.Tail(slog.LevelDebug, n)
	out := make([]logEntry, len(recs))
	for i, rec := range recs {
		out[i] = logEntryOf(rec)
	}
	return out
}

func logEntryOf(rec logging.Record) logEntry {
	return logEntry{
		TS:    rec.Time.UTC().Format(time.RFC3339Nano),
		Level: rec.Level.String(),
		Msg:   rec.Message,
		Attrs: jsonSafeAttrs(rec.Attrs),
	}
}

// stream is the Server-Sent Events endpoint: it sends a full snapshot on
// connect and then forwards every state/event/log change until the client
// goes away. The browser's EventSource reconnects on its own.
func (h *Handler) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // don't let a proxy buffer the stream

	if b, err := json.Marshal(h.snapshot()); err == nil {
		writeSSE(w, "snapshot", b)
		flusher.Flush()
	}

	ch, unsubscribe := h.hub.Subscribe()
	defer unsubscribe()
	// A periodic comment keeps idle proxies from timing the stream out.
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case msg := <-ch:
			writeSSE(w, msg.event, msg.data)
			flusher.Flush()
		case <-ticker.C:
			_, _ = io.WriteString(w, ": ping\n\n")
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func writeSSE(w io.Writer, event string, data []byte) {
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
}

// jsonSafeAttrs recursively replaces values json.Marshal cannot encode (maps
// with non-string keys, channels, funcs, …) with their text form, so one odd
// attr can never 500 the whole logs endpoint. The log buffer already stores
// JSON-safe values; this is defense in depth.
func jsonSafeAttrs(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = jsonSafeValue(v)
	}
	return out
}

func jsonSafeValue(v any) any {
	switch t := v.(type) {
	case nil, bool, string,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, json.Number:
		return v
	case map[string]any:
		return jsonSafeAttrs(t)
	case []any:
		out := make([]any, len(t))
		for i, vv := range t {
			out[i] = jsonSafeValue(vv)
		}
		return out
	default:
		return fmt.Sprint(v)
	}
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
