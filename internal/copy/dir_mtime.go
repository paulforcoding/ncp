package copy

import (
	"os"
	"path/filepath"

	"github.com/zp001/ncp/internal/progress/pebble"
	"github.com/zp001/ncp/internal/storage"
	"github.com/zp001/ncp/pkg/model"
)

// EnsureDirMtime sets directory mtime from source after copy is complete.
// DB does not store fileType, so we stat the source to identify directories.
// Iterates DB in reverse order (deep→shallow) because writing files into
// a directory updates its mtime — must set parent dirs after children.
func EnsureDirMtime(store *pebble.Store, src storage.Source, dstBase string) error {
	it, err := store.Iter()
	if err != nil {
		return err
	}
	defer it.Close()

	// Collect directory relative paths (stat source to identify dirs)
	var dirs []string
	for it.First(); it.Valid(); it.Next() {
		key := it.Key()
		if isInternalKey(key) {
			continue
		}
		cs, _ := it.Value()
		if cs != model.CopyDone {
			continue
		}
		srcPath := filepath.Join(src.Base(), key)
		info, err := os.Stat(srcPath)
		if err != nil || !info.IsDir() {
			continue
		}
		dirs = append(dirs, key)
	}

	// Apply in reverse order (deep→shallow)
	for i := len(dirs) - 1; i >= 0; i-- {
		relPath := dirs[i]
		srcInfo, err := os.Stat(filepath.Join(src.Base(), relPath))
		if err != nil {
			continue
		}
		dstPath := filepath.Join(dstBase, relPath)
		setFileMtime(dstPath, srcInfo.ModTime())
	}

	return nil
}
