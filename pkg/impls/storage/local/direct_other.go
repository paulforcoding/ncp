//go:build darwin || windows

package local

const syscall_O_DIRECT = 0 // O_DIRECT not supported on this platform
