package logging

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"
)

func discardLogger(buf *Buffer) *slog.Logger {
	return slog.New(&teeHandler{base: slog.NewTextHandler(io.Discard, nil), buf: buf})
}

func TestBufferTailsNewestFirstAndFiltersByLevel(t *testing.T) {
	buf := NewBuffer(4)
	log := discardLogger(buf)
	log.Info("one")
	log.Warn("two", "worker", "w-0")
	log.Error("three")

	all := buf.Tail(slog.LevelInfo, 10)
	if len(all) != 3 {
		t.Fatalf("Tail(info) = %d records, want 3", len(all))
	}
	if all[0].Message != "three" || all[2].Message != "one" {
		t.Errorf("Tail must be newest-first: got %q..%q", all[0].Message, all[2].Message)
	}
	if all[0].Level != slog.LevelError {
		t.Errorf("level = %v, want error", all[0].Level)
	}
	if got := all[1].Attrs["worker"]; got != "w-0" {
		t.Errorf("attrs = %v, want worker=w-0", all[1].Attrs)
	}

	warn := buf.Tail(slog.LevelWarn, 10)
	if len(warn) != 2 || warn[0].Message != "three" || warn[1].Message != "two" {
		t.Errorf("Tail(warn) = %v, want [three two]", warn)
	}
}

func TestBufferEvictsOldestAndHonorsLimit(t *testing.T) {
	buf := NewBuffer(2)
	log := discardLogger(buf)
	log.Info("a")
	log.Info("b")
	log.Info("c")

	got := buf.Tail(slog.LevelInfo, 10)
	if len(got) != 2 || got[0].Message != "c" || got[1].Message != "b" {
		t.Fatalf("ring did not evict oldest: %v", got)
	}
	if one := buf.Tail(slog.LevelInfo, 1); len(one) != 1 || one[0].Message != "c" {
		t.Errorf("limit=1 = %v, want [c]", one)
	}
}

func TestTeeHandlerFlattensGroupsAndHandlerAttrs(t *testing.T) {
	buf := NewBuffer(4)
	log := discardLogger(buf).With("run", "r-1").WithGroup("cdc")

	log.Info("msg", "table", "raw.orders")

	got := buf.Tail(slog.LevelInfo, 1)
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	// Handler attrs are ungrouped; the WithGroup prefixes the record's attrs.
	if got[0].Attrs["run"] != "r-1" {
		t.Errorf("handler attr missing: %v", got[0].Attrs)
	}
	if got[0].Attrs["cdc.table"] != "raw.orders" {
		t.Errorf("grouped attr = %v, want cdc.table", got[0].Attrs)
	}
}

func TestNewBufferedCaptures(t *testing.T) {
	log, buf, err := NewBuffered("debug", "json", 8)
	if err != nil {
		t.Fatalf("NewBuffered: %v", err)
	}
	log.Debug("hello", "k", "v")
	got := buf.Tail(slog.LevelDebug, 10)
	if len(got) != 1 || got[0].Message != "hello" || got[0].Attrs["k"] != "v" {
		t.Fatalf("NewBuffered did not capture: %v", got)
	}
}

// A KindAny attr can carry a Go value json.Marshal refuses (a map with
// non-string keys, e.g. the mysql driver's tag maps). The buffer must store a
// JSON-safe rendering, or the dashboard's /logs endpoint 500s and the SPA
// never finishes loading.
func TestTeeHandlerJSONSafeAttrs(t *testing.T) {
	buf := NewBuffer(4)
	log := discardLogger(buf)
	log.Info("x", "tags", map[struct{ A int }]int{{A: 1}: 2})

	got := buf.Tail(slog.LevelInfo, 1)
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	if _, ok := got[0].Attrs["tags"].(string); !ok {
		t.Fatalf("tags attr = %#v, want a JSON-safe string", got[0].Attrs["tags"])
	}
	if _, err := json.Marshal(got[0].Attrs); err != nil {
		t.Fatalf("attrs are not JSON-marshalable: %v", err)
	}
}
