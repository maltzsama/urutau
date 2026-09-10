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
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	switch strings.ToLower(format) {
	case "", "text":
		h = slog.NewTextHandler(os.Stderr, opts)
	case "json":
		h = slog.NewJSONHandler(os.Stderr, opts)
	default:
		return nil, fmt.Errorf("logging: unknown format %q (want text|json)", format)
	}
	return slog.New(h), nil
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
