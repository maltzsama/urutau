package logging

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// Record is one captured log line in structured form — what the dashboard
// renders, not the text handler's output. Attrs are flattened (nested groups
// become dotted keys), which is what a UI chip list wants.
type Record struct {
	Time    time.Time
	Level   slog.Level
	Message string
	Attrs   map[string]any
}

// Buffer is a bounded in-memory ring of the most recent Records. It is fed by
// the tee handler NewBuffered installs, so the process keeps logging to stderr
// unchanged while the last N lines stay available to the dashboard. Safe for
// concurrent use.
type Buffer struct {
	mu       sync.Mutex
	buf      []Record
	next     int
	full     bool
	onAppend func(Record)
}

// SetOnAppend installs a hook called after every record is appended (outside
// the buffer lock). The dashboard uses it to push new log lines to its SSE
// subscribers. Nil disables it.
func (b *Buffer) SetOnAppend(f func(Record)) {
	b.mu.Lock()
	b.onAppend = f
	b.mu.Unlock()
}

// NewBuffer returns a Buffer holding at most capacity records. A non-positive
// capacity falls back to a small default, so a misconfigured caller still gets
// a working (bounded) buffer.
func NewBuffer(capacity int) *Buffer {
	if capacity <= 0 {
		capacity = 2000
	}
	return &Buffer{buf: make([]Record, capacity)}
}

// add appends one record, evicting the oldest once full, then fires the append
// hook (if any) outside the lock.
func (b *Buffer) add(r Record) {
	b.mu.Lock()
	b.buf[b.next] = r
	b.next = (b.next + 1) % len(b.buf)
	if b.next == 0 {
		b.full = true
	}
	f := b.onAppend
	b.mu.Unlock()
	if f != nil {
		f(r)
	}
}

// Tail returns up to limit records at or above minLevel, newest first.
func (b *Buffer) Tail(minLevel slog.Level, limit int) []Record {
	b.mu.Lock()
	defer b.mu.Unlock()
	if limit <= 0 || limit > len(b.buf) {
		limit = len(b.buf)
	}
	count := b.next
	if b.full {
		count = len(b.buf)
	}
	out := make([]Record, 0, limit)
	for i := 0; i < count && len(out) < limit; i++ {
		idx := (b.next - 1 - i + len(b.buf)) % len(b.buf)
		r := b.buf[idx]
		if r.Level < minLevel {
			continue
		}
		out = append(out, r)
	}
	return out
}

// teeHandler writes every record to the base handler (stderr, unchanged) and
// also captures it in the buffer. Handler-level attrs (WithAttrs) are
// flattened with the group path active at the time they were added, so a
// later WithGroup does not retroactively nest them — matching slog semantics.
type teeHandler struct {
	base   slog.Handler
	buf    *Buffer
	groups []string
	pre    map[string]any
}

func (h *teeHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.base.Enabled(ctx, l)
}

func (h *teeHandler) Handle(ctx context.Context, r slog.Record) error {
	if err := h.base.Handle(ctx, r); err != nil {
		return err
	}
	attrs := make(map[string]any, len(h.pre)+r.NumAttrs())
	for k, v := range h.pre {
		attrs[k] = v
	}
	r.Attrs(func(a slog.Attr) bool {
		addAttrs(attrs, h.groups, []slog.Attr{a})
		return true
	})
	h.buf.add(Record{Time: r.Time, Level: r.Level, Message: r.Message, Attrs: attrs})
	return nil
}

func (h *teeHandler) WithAttrs(as []slog.Attr) slog.Handler {
	pre := make(map[string]any, len(h.pre)+len(as))
	for k, v := range h.pre {
		pre[k] = v
	}
	addAttrs(pre, h.groups, as)
	return &teeHandler{base: h.base.WithAttrs(as), buf: h.buf, groups: h.groups, pre: pre}
}

func (h *teeHandler) WithGroup(name string) slog.Handler {
	groups := append(append([]string{}, h.groups...), name)
	return &teeHandler{base: h.base.WithGroup(name), buf: h.buf, groups: groups, pre: h.pre}
}

// addAttrs flattens attrs into m, prefixing group paths. A slog.Group attr
// recurses (a named group extends the path, an inline one does not). Values
// are stored JSON-safe: a KindAny attr can carry an arbitrary Go value (e.g. a
// map with non-string keys) that json.Marshal refuses, and the dashboard
// serializes these records, so those are rendered as text.
func addAttrs(m map[string]any, groups []string, attrs []slog.Attr) {
	for _, a := range attrs {
		if a.Value.Kind() == slog.KindGroup {
			inner := a.Value.Group()
			if len(inner) == 0 {
				continue
			}
			g := groups
			if a.Key != "" {
				g = append(append([]string{}, groups...), a.Key)
			}
			addAttrs(m, g, inner)
			continue
		}
		key := a.Key
		if len(groups) > 0 {
			key = strings.Join(groups, ".") + "." + key
		}
		m[key] = jsonSafeValue(a.Value)
	}
}

// jsonSafeValue renders a slog value for the wire. The typed kinds (string,
// int, bool, time, …) keep their Go value; KindAny — where an arbitrary Go
// value lives — is rendered as text, since it may not be JSON-serializable.
func jsonSafeValue(v slog.Value) any {
	if v.Kind() == slog.KindAny {
		return v.String()
	}
	return v.Any()
}
