package filelog

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Emitter writes NDJSON FileLog events to a writer.
type Emitter struct {
	w       io.Writer
	taskID  string
	enabled bool
}

// NewEmitter creates a FileLog emitter.
// If output is "console", writes to stdout; if a file path, creates/opens the file.
func NewEmitter(taskID, output string, enabled bool) (*Emitter, error) {
	if !enabled {
		return &Emitter{enabled: false}, nil
	}

	var w io.Writer
	if output == "console" {
		w = os.Stdout
	} else {
		if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
			return nil, err
		}
		f, err := os.Create(output)
		if err != nil {
			return nil, err
		}
		w = f
	}

	return &Emitter{
		w:       w,
		taskID:  taskID,
		enabled: true,
	}, nil
}

// Emit writes a structured FileLog event as NDJSON.
func (e *Emitter) Emit(eventType string, data any) {
	if !e.enabled || e.w == nil {
		return
	}

	entry := map[string]any{
		"timestamp": time.Now().Format(time.RFC3339Nano),
		"event":     eventType,
		"taskId":    e.taskID,
	}

	// Merge data fields into entry
	if m, ok := data.(map[string]any); ok {
		for k, v := range m {
			entry[k] = v
		}
	} else {
		entry["data"] = data
	}

	line, err := json.Marshal(entry)
	if err != nil {
		return
	}
	_, _ = e.w.Write(line)
	_, _ = e.w.Write([]byte("\n"))
}

// Close flushes and closes the underlying writer if it's a file.
func (e *Emitter) Close() error {
	if closer, ok := e.w.(io.Closer); ok && e.enabled {
		return closer.Close()
	}
	return nil
}
