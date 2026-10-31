// Package log configures the process-wide slog logger.
package log

import (
	"log/slog"
	"os"
	"strings"
)

// Setup installs a JSON (or text) slog handler as the default logger and
// returns it. service is attached to every record.
func Setup(service, level, format string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	if format == "text" {
		h = slog.NewTextHandler(os.Stderr, opts)
	} else {
		h = slog.NewJSONHandler(os.Stderr, opts)
	}
	l := slog.New(h).With("service", service)
	slog.SetDefault(l)
	return l
}
