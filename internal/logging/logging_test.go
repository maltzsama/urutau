package logging

import (
	"context"
	"log/slog"
	"testing"
)

func TestNewLevels(t *testing.T) {
	cases := map[string]slog.Level{
		"":      slog.LevelInfo,
		"info":  slog.LevelInfo,
		"DEBUG": slog.LevelDebug,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
	}
	for in, want := range cases {
		log, err := New(in, "text")
		if err != nil {
			t.Fatalf("New(%q): %v", in, err)
		}
		if got := log.Enabled(context.Background(), want); !got {
			t.Fatalf("level %q did not enable %v", in, want)
		}
	}
}

func TestNewFormats(t *testing.T) {
	for _, f := range []string{"", "text", "json", "JSON"} {
		if _, err := New("info", f); err != nil {
			t.Fatalf("New(format=%q): %v", f, err)
		}
	}
}

func TestNewRejectsUnknown(t *testing.T) {
	if _, err := New("trace", "text"); err == nil {
		t.Fatal("unknown level must error")
	}
	if _, err := New("info", "xml"); err == nil {
		t.Fatal("unknown format must error")
	}
}
