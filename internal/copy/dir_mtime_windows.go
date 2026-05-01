//go:build windows

package copy

import (
	"os"
	"time"
)

func setFileMtime(path string, mtime time.Time) error {
	return os.Chtimes(path, mtime, mtime)
}
