package filelog

import (
	"log/slog"
	"os"
	"path/filepath"
)

// ProgramLogLevel values.
const (
	LevelTrace    = slog.Level(-8)
	LevelDebug    = slog.LevelDebug
	LevelInfo     = slog.LevelInfo
	LevelWarn     = slog.LevelWarn
	LevelError    = slog.LevelError
	LevelCritical = slog.Level(12)
)

// SetupProgramLog configures the global slog logger.
// output: "console" → stderr, or a file path.
// level: trace/debug/info/warn/error/critical.
func SetupProgramLog(output, level string) error {
	var w *os.File
	if output == "console" || output == "" {
		w = os.Stderr
	} else {
		if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
			return err
		}
		f, err := os.Create(output)
		if err != nil {
			return err
		}
		w = f
	}

	lvl := parseLevel(level)
	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: lvl,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Rename level to match PRD naming
			if a.Key == slog.LevelKey {
				var level slog.Level
					switch v := a.Value.Any().(type) {
					case slog.Level:
						level = v
					case string:
						level = parseLevel(v)
					default:
						return a
					}
				switch {
				case level >= LevelCritical:
					a.Value = slog.StringValue("critical")
				default:
					// Keep default slog names: debug, info, warn, error
				}
			}
			return a
		},
	})

	logger := slog.New(handler)
	slog.SetDefault(logger)
	return nil
}

func parseLevel(s string) slog.Level {
	switch s {
	case "trace":
		return LevelTrace
	case "debug":
		return LevelDebug
	case "info":
		return LevelInfo
	case "warn":
		return LevelWarn
	case "error":
		return LevelError
	case "critical":
		return LevelCritical
	default:
		return LevelInfo
	}
}
