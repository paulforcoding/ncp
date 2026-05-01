package model

import "os"

// FileMetadata carries POSIX metadata to restore on the destination.
type FileMetadata struct {
	Mode  os.FileMode        // Permission bits
	Uid   int                // Owner UID
	Gid   int                // Owner GID
	Atime int64              // Access time (unix nano)
	Mtime int64              // Modification time (unix nano)
	Xattr map[string]string  // Extended attributes
}
