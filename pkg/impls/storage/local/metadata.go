package local

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/zp001/ncp/pkg/interfaces/storage"
)

// setMetadata applies chmod, chown, utime, and setxattr to a file or directory.
func setMetadata(base, relPath string, attr storage.FileAttr) error {
	fullPath := filepath.Join(base, relPath)

	// chmod
	if attr.Mode != 0 {
		if err := os.Chmod(fullPath, attr.Mode); err != nil {
			return fmt.Errorf("chmod %s: %w", relPath, err)
		}
	}

	// chown
	if attr.Uid != 0 || attr.Gid != 0 {
		if err := os.Chown(fullPath, attr.Uid, attr.Gid); err != nil {
			// Non-fatal: may lack permission
			return fmt.Errorf("chown %s: %w", relPath, err)
		}
	}

	// utime (atime + mtime)
	if !attr.Mtime.IsZero() {
		atime := attr.Atime
		if atime.IsZero() {
			atime = attr.Mtime
		}
		if err := os.Chtimes(fullPath, atime, attr.Mtime); err != nil {
			return fmt.Errorf("chtimes %s: %w", relPath, err)
		}
	}

	// xattr
	for key, val := range attr.Xattr {
		if err := setXattr(fullPath, key, val); err != nil {
			return fmt.Errorf("setxattr %s %s: %w", relPath, key, err)
		}
	}

	return nil
}
