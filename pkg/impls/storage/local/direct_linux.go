//go:build linux

package local

import "golang.org/x/sys/unix"

const syscall_O_DIRECT = unix.O_DIRECT
