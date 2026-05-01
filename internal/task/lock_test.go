//go:build unix

package task

import (
	"os"
	"testing"
)

func TestAcquireTaskLock(t *testing.T) {
	dir := t.TempDir()

	lock, err := AcquireTaskLock(dir)
	if err != nil {
		t.Fatalf("acquire lock: %v", err)
	}

	// Second lock should fail
	_, err = AcquireTaskLock(dir)
	if err == nil {
		t.Fatal("expected second lock to fail")
	}

	// Release and re-acquire should succeed
	lock.Release()
	lock2, err := AcquireTaskLock(dir)
	if err != nil {
		t.Fatalf("re-acquire lock after release: %v", err)
	}
	lock2.Release()
}

func TestIsProcessAlive(t *testing.T) {
	// Current process should be alive
	if !IsProcessAlive(os.Getpid()) {
		t.Fatal("current process should be alive")
	}

	// PID 1 (init) should be alive on Unix
	if !IsProcessAlive(1) {
		t.Log("PID 1 not alive (may be expected in some environments)")
	}

	// Unlikely PID should be dead
	if IsProcessAlive(999999) {
		t.Log("PID 999999 reported alive (unexpected)")
	}

	// Negative PID
	if IsProcessAlive(-1) {
		t.Fatal("negative PID should not be alive")
	}
}

func TestCheckTaskNotRunning(t *testing.T) {
	dir := t.TempDir()

	// Create a meta for a task
	meta := NewMeta("task-lock-test", "/src", "/dst", nil, JobTypeCopy)
	meta.PID = os.Getpid()
	WriteMetaTo(meta, dir)

	// Task with our own PID should be detected as running
	_, _, err := CheckTaskNotRunning(dir, "task-lock-test")
	if err == nil {
		t.Fatal("expected error for running task")
	}

	// Set PID to a dead process
	meta.PID = 999999
	WriteMetaTo(meta, dir)

	// Now it should succeed
	_, lock, err := CheckTaskNotRunning(dir, "task-lock-test")
	if err != nil {
		t.Fatalf("expected success for dead PID: %v", err)
	}
	if lock != nil {
		lock.Release()
	}
}

func TestCheckTaskNotRunning_MissingTask(t *testing.T) {
	dir := t.TempDir()

	_, _, err := CheckTaskNotRunning(dir, "nonexistent-task")
	if err == nil {
		t.Fatal("expected error for missing task")
	}
}
