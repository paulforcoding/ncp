package task

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGenerateTaskID(t *testing.T) {
	id := GenerateTaskID()
	if len(id) == 0 {
		t.Fatal("task ID is empty")
	}
	// Format: task-YYYYMMDD-HHMMSS-XXXX
	if id[:5] != "task-" {
		t.Fatalf("expected task- prefix, got %s", id[:5])
	}
}

func TestGenerateRunID(t *testing.T) {
	id := GenerateRunID()
	if len(id) == 0 {
		t.Fatal("run ID is empty")
	}
	if id[:4] != "run-" {
		t.Fatalf("expected run- prefix, got %s", id[:4])
	}
}

func TestNewMeta(t *testing.T) {
	meta := NewMeta("task-1234-abcd", "/src", "/dst", []string{"ncp", "copy", "/src", "/dst"}, JobTypeCopy)

	if meta.TaskID != "task-1234-abcd" {
		t.Fatalf("taskID mismatch: %s", meta.TaskID)
	}
	if meta.SrcBase != "/src" {
		t.Fatalf("srcBase mismatch: %s", meta.SrcBase)
	}
	if meta.DstBase != "/dst" {
		t.Fatalf("dstBase mismatch: %s", meta.DstBase)
	}
	if meta.PID != os.Getpid() {
		t.Fatalf("pid mismatch: got %d, want %d", meta.PID, os.Getpid())
	}
	if len(meta.Runs) != 1 {
		t.Fatalf("expected 1 run, got %d", len(meta.Runs))
	}
	run := meta.Runs[0]
	if run.JobType != JobTypeCopy {
		t.Fatalf("jobType mismatch: %s", run.JobType)
	}
	if run.Status != RunStatusRunning {
		t.Fatalf("status mismatch: %s", run.Status)
	}
	if run.FinishedAt != nil {
		t.Fatal("finishedAt should be nil for running")
	}
}

func TestMetaPath(t *testing.T) {
	got := MetaPath("/progress", "task-1234-abcd")
	want := filepath.Join("/progress", "task-1234-abcd", "meta.json")
	if got != want {
		t.Fatalf("meta path mismatch: got %s, want %s", got, want)
	}
}

func TestWriteAndReadMeta(t *testing.T) {
	dir := t.TempDir()
	meta := NewMeta("task-test-001", "/src", "/dst", []string{"ncp", "copy"}, JobTypeCopy)

	if err := WriteMetaTo(meta, dir); err != nil {
		t.Fatalf("write meta: %v", err)
	}

	read, err := ReadMeta(dir, "task-test-001")
	if err != nil {
		t.Fatalf("read meta: %v", err)
	}

	if read.TaskID != meta.TaskID {
		t.Fatalf("taskID mismatch: %s vs %s", read.TaskID, meta.TaskID)
	}
	if read.SrcBase != meta.SrcBase {
		t.Fatalf("srcBase mismatch")
	}
	if read.DstBase != meta.DstBase {
		t.Fatalf("dstBase mismatch")
	}
	if read.PID != meta.PID {
		t.Fatalf("pid mismatch")
	}
	if len(read.Runs) != 1 {
		t.Fatalf("runs mismatch: %d", len(read.Runs))
	}
}

func TestUpdateRunFinished(t *testing.T) {
	dir := t.TempDir()
	meta := NewMeta("task-test-002", "/src", "/dst", nil, JobTypeCopy)
	WriteMetaTo(meta, dir)

	// Mark as done
	if err := UpdateRunFinished(meta, 0, dir); err != nil {
		t.Fatalf("update run: %v", err)
	}

	read, _ := ReadMeta(dir, "task-test-002")
	run := read.Runs[0]
	if run.Status != RunStatusDone {
		t.Fatalf("expected done status, got %s", run.Status)
	}
	if run.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", run.ExitCode)
	}
	if run.FinishedAt == nil {
		t.Fatal("finishedAt should not be nil")
	}
}

func TestUpdateRunFinished_Failed(t *testing.T) {
	dir := t.TempDir()
	meta := NewMeta("task-test-003", "/src", "/dst", nil, JobTypeCopy)
	WriteMetaTo(meta, dir)

	UpdateRunFinished(meta, 2, dir)

	read, _ := ReadMeta(dir, "task-test-003")
	run := read.Runs[0]
	if run.Status != RunStatusFailed {
		t.Fatalf("expected failed status, got %s", run.Status)
	}
	if run.ExitCode != 2 {
		t.Fatalf("expected exit code 2, got %d", run.ExitCode)
	}
}

func TestAppendRun(t *testing.T) {
	dir := t.TempDir()
	meta := NewMeta("task-test-004", "/src", "/dst", nil, JobTypeCopy)
	WriteMetaTo(meta, dir)

	// Finish first run
	UpdateRunFinished(meta, 0, dir)

	// Append a second run (e.g. resume)
	if err := AppendRun(meta, JobTypeCopy, dir); err != nil {
		t.Fatalf("append run: %v", err)
	}

	read, _ := ReadMeta(dir, "task-test-004")
	if len(read.Runs) != 2 {
		t.Fatalf("expected 2 runs, got %d", len(read.Runs))
	}
	if read.Runs[1].JobType != JobTypeCopy {
		t.Fatalf("second run jobType mismatch: %s", read.Runs[1].JobType)
	}
	if read.Runs[1].Status != RunStatusRunning {
		t.Fatalf("second run should be running, got %s", read.Runs[1].Status)
	}
}

func TestLastJobType(t *testing.T) {
	meta := &Meta{Runs: []RunRecord{{JobType: JobTypeCopy}}}
	if got := LastJobType(meta); got != JobTypeCopy {
		t.Fatalf("expected copy, got %s", got)
	}

	meta.Runs = append(meta.Runs, RunRecord{JobType: JobTypeCksum})
	if got := LastJobType(meta); got != JobTypeCksum {
		t.Fatalf("expected cksum, got %s", got)
	}

	empty := &Meta{Runs: nil}
	if got := LastJobType(empty); got != JobTypeCopy {
		t.Fatalf("expected copy default, got %s", got)
	}
}

func TestMetaJSONRoundtrip(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond) // JSON loses sub-ms
	meta := &Meta{
		TaskID:    "task-roundtrip",
		SrcBase:   "/data/src",
		DstBase:   "/data/dst",
		CreatedAt: now,
		CmdArgs:   []string{"ncp", "copy", "/data/src", "/data/dst"},
		PID:       12345,
		Hostname:  "testhost",
		Runs: []RunRecord{
			{
				ID:        "run-001",
				JobType:   JobTypeCopy,
				StartedAt: now,
				Status:    RunStatusDone,
				ExitCode:  0,
			},
		},
	}

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var back Meta
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if back.TaskID != meta.TaskID {
		t.Fatalf("taskID mismatch")
	}
	if back.Runs[0].JobType != JobTypeCopy {
		t.Fatalf("jobType mismatch")
	}
}
