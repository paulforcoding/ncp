package copy

import (
	"context"
	"path/filepath"

	"github.com/zp001/ncp/pkg/interfaces/progress"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// EnsureDirMtime sets directory mtime from source after copy is complete.
// DB does not store fileType, so we stat the source to identify directories.
// Iterates DB in reverse order (deep→shallow) because writing files into
// a directory updates its mtime — must set parent dirs after children.
func EnsureDirMtime(ctx context.Context, store progress.ProgressStore, src storage.Source, dstBase string) error {
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
		item, err := src.Stat(ctx, key)
		if err != nil || item.FileType != model.FileDir {
			continue
		}
		dirs = append(dirs, key)
	}

	// Apply in reverse order (deep→shallow)
	for i := len(dirs) - 1; i >= 0; i-- {
		relPath := dirs[i]
		item, err := src.Stat(ctx, relPath)
		if err != nil {
			continue
		}
		dstPath := filepath.Join(dstBase, relPath)
		_ = setFileMtime(dstPath, item.Attr.Mtime)
	}

	return nil
}
