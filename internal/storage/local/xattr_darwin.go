//go:build darwin

package local

import (
	"syscall"
)

func setXattr(path, key, value string) error {
	return syscall.Setxattr(path, key, []byte(value), 0)
}
