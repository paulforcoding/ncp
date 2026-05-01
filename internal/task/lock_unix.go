//go:build unix

package task

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// TaskLock represents an exclusive lock on a task directory.
type TaskLock struct {
	flockFile *os.File
	taskDir   string
}

// AcquireTaskLock tries to acquire an exclusive flock on taskDir/ncp.lock.
// Returns error if another process holds the lock.
func AcquireTaskLock(taskDir string) (*TaskLock, error) {
	if err := os.MkdirAll(taskDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir task dir: %w", err)
	}
	lockPath := taskDir + "/ncp.lock"
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("task is locked by another process: %w", err)
	}
	return &TaskLock{flockFile: f, taskDir: taskDir}, nil
}

// Release unlocks and closes the flock file.
func (l *TaskLock) Release() error {
	if l.flockFile != nil {
		unix.Flock(int(l.flockFile.Fd()), unix.LOCK_UN)
		return l.flockFile.Close()
	}
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
	err = p.Signal(syscall.Signal(0))
	return err == nil
}
