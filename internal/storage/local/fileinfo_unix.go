//go:build unix || darwin || linux

package local

import (
	"io/fs"
	"syscall"
)

func fileOwner(info fs.FileInfo) (uid, gid int) {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		return int(stat.Uid), int(stat.Gid)
	}
	return 0, 0
}
