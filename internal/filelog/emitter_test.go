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

	e.Emit(EventFileComplete, map[string]any{
		"action":    "copy",
		"result":    "done",
		"errorCode": "",
		"relPath":   "dir/file.txt",
		"fileType":  "regular",
		"fileSize":  1024,
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
	if entry["action"] != "copy" {
		t.Fatalf("expected action copy, got %v", entry["action"])
	}
	if entry["relPath"] != "dir/file.txt" {
		t.Fatalf("expected relPath dir/file.txt, got %v", entry["relPath"])
	}
}

func TestEmitterDisabled(t *testing.T) {
	e := &Emitter{enabled: false}
	e.Emit(EventFileComplete, nil)
}

func TestEmitterMapData(t *testing.T) {
	var buf bytes.Buffer
	e := &Emitter{w: &buf, taskID: "t1", enabled: true}

	e.Emit(EventProgressSummary, map[string]any{
		"phase":    "copy",
		"finished": false,
	})

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("invalid json: %v\n%s", err, buf.String())
	}
	if entry["phase"] != "copy" {
		t.Fatalf("expected copy, got %v", entry["phase"])
	}
}

func TestNewEmitterFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file.log")

	e, err := NewEmitter("task-1", path, true)
	if err != nil {
		t.Fatalf("new emitter: %v", err)
	}

	e.Emit(EventFileComplete, map[string]any{"src": "/data"})

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
