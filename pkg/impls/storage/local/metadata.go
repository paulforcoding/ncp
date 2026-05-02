package local

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/zp001/ncp/pkg/model"
)

// setMetadata applies chmod, chown, utime, and setxattr to a file or directory.
func setMetadata(base, relPath string, meta model.FileMetadata) error {
	fullPath := filepath.Join(base, relPath)

	// chmod
	if meta.Mode != 0 {
		if err := os.Chmod(fullPath, meta.Mode); err != nil {
			return fmt.Errorf("chmod %s: %w", relPath, err)
		}
	}

	// chown
	if meta.Uid != 0 || meta.Gid != 0 {
		if err := os.Chown(fullPath, meta.Uid, meta.Gid); err != nil {
			// Non-fatal: may lack permission
			return fmt.Errorf("chown %s: %w", relPath, err)
		}
	}

	// utime (atime + mtime)
	if meta.Mtime != 0 {
		atime := time.Unix(0, meta.Atime)
		mtime := time.Unix(0, meta.Mtime)
		if err := os.Chtimes(fullPath, atime, mtime); err != nil {
			return fmt.Errorf("chtimes %s: %w", relPath, err)
		}
	}

	// xattr
	for key, val := range meta.Xattr {
		if err := setXattr(fullPath, key, val); err != nil {
			return fmt.Errorf("setxattr %s %s: %w", relPath, key, err)
		}
	}

	return nil
}
