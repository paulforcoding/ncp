package storage

import "os"

// Writer writes data to a destination file (pwrite semantics).
// Close accepts a checksum; local implementations ignore it,
// remote/OSS implementations use it for integrity verification.
type Writer interface {
	WriteAt(p []byte, offset int64) (n int, err error)
	Sync() error
	Close(checksum []byte) error
}

// FileMetadata carries POSIX metadata to restore on the destination.
type FileMetadata struct {
	Mode  os.FileMode // Permission bits
	Uid   int         // Owner UID
	Gid   int         // Owner GID
	Atime int64       // Access time (unix nano)
	Mtime int64       // Modification time (unix nano)
	Xattr map[string]string // Extended attributes
}

// Destination writes files to a storage backend.
type Destination interface {
	OpenFile(relPath string, size int64, mode os.FileMode, uid, gid int) (Writer, error)
	Mkdir(relPath string, mode os.FileMode, uid, gid int) error
	Symlink(relPath string, target string) error
	SetMetadata(relPath string, meta FileMetadata) error
}
