package filelog

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestEmitterConsole(t *testing.T) {
	var buf bytes.Buffer
	e := &Emitter{w: &buf, taskID: "task-test", enabled: true}

	e.Emit(EventFileComplete, FileCompleteData{
		RelPath:  "dir/file.txt",
		FileSize: 1024,
	})

	if buf.Len() == 0 {
		t.Fatal("expected output")
	}

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("invalid json: %v\n%s", err, buf.String())
	}
	if entry["event"] != EventFileComplete {
		t.Fatalf("expected event %s, got %v", EventFileComplete, entry["event"])
	}
	if entry["taskId"] != "task-test" {
		t.Fatalf("expected taskId task-test, got %v", entry["taskId"])
	}
}

func TestEmitterDisabled(t *testing.T) {
	e := &Emitter{enabled: false}
	// Should not panic
	e.Emit(EventFileComplete, nil)
}

func TestEmitterMapData(t *testing.T) {
	var buf bytes.Buffer
	e := &Emitter{w: &buf, taskID: "t1", enabled: true}

	e.Emit(EventWalkProgress, map[string]any{
		"discoveredCount": 100,
		"currentPath":     "a/b",
	})

	var entry map[string]any
	json.Unmarshal(buf.Bytes(), &entry)
	if entry["discoveredCount"] != float64(100) {
		t.Fatalf("expected 100, got %v", entry["discoveredCount"])
	}
}

func TestNewEmitterFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file.log")

	e, err := NewEmitter("task-1", path, true)
	if err != nil {
		t.Fatalf("new emitter: %v", err)
	}

	e.Emit(EventCopyPlan, map[string]any{"src": "/data"})

	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("readfile: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected log output")
	}
}
