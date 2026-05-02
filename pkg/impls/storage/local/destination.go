package local

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// Destination implements storage.Destination for the local filesystem.
type Destination struct {
	base string
	cfg  WriterConfig
}

// NewDestination creates a local Destination rooted at base with default IO config.
func NewDestination(base string) (*Destination, error) {
	return NewDestinationWithConfig(base, DefaultWriterConfig())
}

// NewDestinationWithConfig creates a local Destination with custom IO config.
func NewDestinationWithConfig(base string, cfg WriterConfig) (*Destination, error) {
	abs, err := filepath.Abs(base)
	if err != nil {
		return nil, fmt.Errorf("local destination abs path: %w", err)
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, fmt.Errorf("local destination mkdir %s: %w", abs, err)
	}
	return &Destination{base: abs, cfg: cfg}, nil
}

// OpenFile creates or opens a file for writing (pwrite semantics).
func (d *Destination) OpenFile(relPath string, size int64, mode os.FileMode, uid, gid int) (storage.Writer, error) {
	fullPath := filepath.Join(d.base, relPath)

	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		return nil, fmt.Errorf("local mkdirall %s: %w", filepath.Dir(fullPath), err)
	}

	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if d.cfg.DirectIO {
		flags |= syscall_O_DIRECT
	}

	f, err := os.OpenFile(fullPath, flags, mode)
	if err != nil {
		return nil, fmt.Errorf("local openfile %s: %w", relPath, err)
	}

	if size > 0 {
		_ = f.Truncate(size)
	}

	return &Writer{f: f, cfg: d.cfg, size: size}, nil
}

// Mkdir creates a directory (recursively).
func (d *Destination) Mkdir(relPath string, mode os.FileMode, uid, gid int) error {
	fullPath := filepath.Join(d.base, relPath)
	if err := os.MkdirAll(fullPath, mode); err != nil {
		return fmt.Errorf("local mkdir %s: %w", relPath, err)
	}
	return nil
}

// Symlink creates a symbolic link (does not follow).
func (d *Destination) Symlink(relPath string, target string) error {
	fullPath := filepath.Join(d.base, relPath)

	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		return fmt.Errorf("local mkdirall for symlink %s: %w", relPath, err)
	}

	os.Remove(fullPath)

	if err := os.Symlink(target, fullPath); err != nil {
		return fmt.Errorf("local symlink %s -> %s: %w", relPath, target, err)
	}
	return nil
}

// SetMetadata applies POSIX metadata to a file or directory.
func (d *Destination) SetMetadata(relPath string, meta model.FileMetadata) error {
	return setMetadata(d.base, relPath, meta)
}

// Restat returns metadata for an existing file on the destination (for skip-by-mtime).
func (d *Destination) Restat(relPath string) (model.DiscoverItem, error) {
	fullPath := filepath.Join(d.base, relPath)
	info, err := os.Lstat(fullPath)
	if err != nil {
		return model.DiscoverItem{}, err
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

	item := model.DiscoverItem{
		RelPath:  relPath,
		FileType: ft,
		FileSize: info.Size(),
		Mode:     uint32(mode.Perm()),
		Uid:      uid,
		Gid:      gid,
		Mtime:    info.ModTime().UnixNano(),
	}

	if ft == model.FileSymlink {
		target, err := os.Readlink(fullPath)
		if err != nil {
			return model.DiscoverItem{}, fmt.Errorf("local restat readlink %s: %w", relPath, err)
		}
		item.LinkTarget = target
	}

	return item, nil
}

// Base returns the destination base directory.
func (d *Destination) Base() string { return d.base }
