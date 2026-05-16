//go:build linux

package ncpserver

import "golang.org/x/sys/unix"

func setXattr(path, key, value string) error {
	return unix.Setxattr(path, key, []byte(value), 0)
}
