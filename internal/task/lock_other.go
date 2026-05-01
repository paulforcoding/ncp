//go:build !unix

package task

import (
	"fmt"
	"os"
)

// TaskLock represents an exclusive lock on a task directory.
type TaskLock struct {
	taskDir string
}

// AcquireTaskLock tries to acquire a lock. On non-Unix, uses PID check only.
func AcquireTaskLock(taskDir string) (*TaskLock, error) {
	return &TaskLock{taskDir: taskDir}, nil
}

// Release is a no-op on non-Unix platforms.
func (l *TaskLock) Release() error {
	return nil
}

// IsProcessAlive checks if a process with the given PID is running.
func IsProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = p.Signal(nil)
	return err == nil
}
