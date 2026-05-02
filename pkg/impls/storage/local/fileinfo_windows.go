//go:build windows

package local

import "io/fs"

func fileOwner(info fs.FileInfo) (uid, gid int) {
	return 0, 0
}
