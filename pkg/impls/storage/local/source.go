package local

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// Source implements storage.Source for the local filesystem.
type Source struct {
	base string // absolute base directory, no trailing slash
}

// NewSource creates a local Source rooted at base.
func NewSource(base string) (*Source, error) {
	abs, err := filepath.Abs(base)
	if err != nil {
		return nil, fmt.Errorf("local source abs path: %w", err)
	}
	clean := filepath.Clean(abs)
	if clean == "/" {
		return nil, fmt.Errorf("local source: copying the entire filesystem root is not allowed")
	}
	return &Source{base: abs}, nil
}

// Walk traverses the directory tree rooted at base using DFS
// (filepath.Walk is DFS), guaranteeing DB key lexicographic order
// is shallow-to-deep. Reverse iteration = deep-to-shallow
// (used by EnsureDirMtime).
func (s *Source) Walk(ctx context.Context, fn func(context.Context, storage.DiscoverItem) error) error {
	return filepath.Walk(s.base, func(path string, info fs.FileInfo, err error) error {
		// Respect context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			return nil
		}

		mode := info.Mode()

		// Skip special file types: device, named pipe, socket
		if mode&fs.ModeDevice != 0 || mode&fs.ModeNamedPipe != 0 || mode&fs.ModeSocket != 0 {
			return nil
		}

		var ft model.FileType
		switch {
		case info.IsDir():
			ft = model.FileDir
		case mode&fs.ModeSymlink != 0:
			ft = model.FileSymlink
		default:
			ft = model.FileRegular
		}

		relPath, err := s.toRelPath(path)
		if err != nil {
			return nil
		}

		// Skip the root directory itself (relPath is empty), but allow single-file roots
		if relPath == "" {
			if info.IsDir() {
				return nil
			}
			// root is a file: let it fall through to emit below with empty RelPath
		}

		uid, gid := fileOwner(info)

		item := storage.DiscoverItem{
			RelPath:  relPath,
			FileType: ft,
			Size:     info.Size(),
			Attr: storage.FileAttr{
				Mode:  mode,
				Uid:   uid,
				Gid:   gid,
				Mtime: info.ModTime(),
			},
		}

		// Read symlink target at walk time so Replicator doesn't need Source access
		if ft == model.FileSymlink {
			target, err := os.Readlink(path)
			if err != nil {
				return nil // skip unreadable symlinks
			}
			item.Attr.SymlinkTarget = target
		}

		return fn(ctx, item)
	})
}

// Open opens a local file for reading.
func (s *Source) Open(ctx context.Context, relPath string) (storage.FileReader, error) {
	fullPath := filepath.Join(s.base, relPath)
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, fmt.Errorf("local open %s: %w", relPath, err)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("local open stat %s: %w", relPath, err)
	}

	uid, gid := fileOwner(info)
	return &Reader{
		f:    f,
		size: info.Size(),
		attr: storage.FileAttr{
			Mode:  info.Mode(),
			Uid:   uid,
			Gid:   gid,
			Mtime: info.ModTime(),
		},
	}, nil
}

// toRelPath converts an absolute path to a relative path from base,
// using forward slashes (filepath.ToSlash) for DB key consistency.
func (s *Source) toRelPath(absPath string) (string, error) {
	rel, err := filepath.Rel(s.base, absPath)
	if err != nil {
		return "", fmt.Errorf("rel path: %w", err)
	}
	if rel == "." {
		return "", nil
	}
	return filepath.ToSlash(rel), nil
}

// Base returns the source base directory.
func (s *Source) URI() string { return s.base }

// BeginTask is a no-op for local sources.
func (s *Source) BeginTask(ctx context.Context, taskID string) error { return nil }

// EndTask is a no-op for local sources.
func (s *Source) EndTask(ctx context.Context, summary storage.TaskSummary) error { return nil }

// Stat rebuilds a DiscoverItem by stat-ing the source path.
// Used by Walker.dispatchRemaining to re-enqueue discovered items
// whose full metadata wasn't stored in DB.
func (s *Source) Stat(_ context.Context, relPath string) (storage.DiscoverItem, error) {
	fullPath := filepath.Join(s.base, relPath)
	info, err := os.Lstat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return storage.DiscoverItem{}, fmt.Errorf("local stat %s: %w", relPath, storage.ErrNotFound)
		}
		return storage.DiscoverItem{}, fmt.Errorf("local stat %s: %w", relPath, err)
	}

	mode := info.Mode()
	var ft model.FileType
	switch {
	case info.IsDir():
		ft = model.FileDir
	case mode&fs.ModeSymlink != 0:
		ft = model.FileSymlink
	default:
		ft = model.FileRegular
	}

	uid, gid := fileOwner(info)

	item := storage.DiscoverItem{
		RelPath:  relPath,
		FileType: ft,
		Size:     info.Size(),
		Attr: storage.FileAttr{
			Mode:  mode,
			Uid:   uid,
			Gid:   gid,
			Mtime: info.ModTime(),
		},
	}

	if ft == model.FileSymlink {
		target, err := os.Readlink(fullPath)
		if err != nil {
			return storage.DiscoverItem{}, fmt.Errorf("local stat readlink %s: %w", relPath, err)
		}
		item.Attr.SymlinkTarget = target
	}

	return item, nil
}
