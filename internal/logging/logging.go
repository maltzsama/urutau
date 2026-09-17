// Package logging builds the process-wide slog logger from CLI flags, so
// every binary in the tree configures its handler and level the same way.
package logging

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// New returns a slog.Logger for the given level (debug|info|warn|error) and
// format (text|json). Empty values fall back to info/text.
func New(level, format string) (*slog.Logger, error) {
	lvl, err := parseLevel(level)
	if err != nil {
		return nil, err
	}
	h, err := baseHandler(format, lvl)
	if err != nil {
		return nil, err
	}
	return slog.New(h), nil
}

// NewBuffered is New plus a Buffer mirroring the last capacity records, for the
// dashboard's log tail. The logger's stderr output is unchanged.
func NewBuffered(level, format string, capacity int) (*slog.Logger, *Buffer, error) {
	lvl, err := parseLevel(level)
	if err != nil {
		return nil, nil, err
	}
	h, err := baseHandler(format, lvl)
	if err != nil {
		return nil, nil, err
	}
	buf := NewBuffer(capacity)
	return slog.New(&teeHandler{base: h, buf: buf}), buf, nil
}

// baseHandler builds the stderr handler for a format and level.
func baseHandler(format string, lvl slog.Level) (slog.Handler, error) {
	opts := &slog.HandlerOptions{Level: lvl}
	switch strings.ToLower(format) {
	case "", "text":
		return slog.NewTextHandler(os.Stderr, opts), nil
	case "json":
		return slog.NewJSONHandler(os.Stderr, opts), nil
	default:
		return nil, fmt.Errorf("logging: unknown format %q (want text|json)", format)
	}
}

func parseLevel(level string) (slog.Level, error) {
	switch strings.ToLower(level) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("logging: unknown level %q (want debug|info|warn|error)", level)
	}
}
