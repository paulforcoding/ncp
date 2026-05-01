//go:build unix || darwin || linux

package copy

import (
	"syscall"
	"time"
)

func setFileMtime(path string, mtime time.Time) error {
	ts := []syscall.Timespec{
		{Sec: mtime.Unix(), Nsec: int64(mtime.Nanosecond())},
		{Sec: mtime.Unix(), Nsec: int64(mtime.Nanosecond())},
	}
	return syscall.UtimesNano(path, ts)
}
