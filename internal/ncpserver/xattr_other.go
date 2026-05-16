//go:build !darwin && !linux

package ncpserver

import "fmt"

func setXattr(path, key, value string) error {
	return fmt.Errorf("xattr not supported on this platform")
}
