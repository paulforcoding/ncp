//go:build windows

package local

func setXattr(path, key, value string) error {
	// xattr not supported on Windows
	return nil
}
