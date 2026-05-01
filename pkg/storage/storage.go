package storage

import (
	"context"
	"io"
	"os"

	"github.com/zp001/ncp/pkg/model"
)

// Walker traverses a source and invokes fn for each discovered item.
type Walker interface {
	Walk(ctx context.Context, fn func(model.DiscoverItem) error) error
}

// Reader reads data from a source file at a given offset (pread semantics).
type Reader interface {
	io.ReaderAt
	io.Closer
}

// Source combines Walker and Reader creation for a storage backend.
type Source interface {
	Walker
	Open(relPath string) (Reader, error)
	Restat(relPath string) (model.DiscoverItem, error)
	Base() string
}

// Writer writes data to a destination file (pwrite semantics).
type Writer interface {
	WriteAt(p []byte, offset int64) (n int, err error)
	Sync() error
	Close(checksum []byte) error
}

// Destination writes files to a storage backend.
type Destination interface {
	OpenFile(relPath string, size int64, mode os.FileMode, uid, gid int) (Writer, error)
	Mkdir(relPath string, mode os.FileMode, uid, gid int) error
	Symlink(relPath string, target string) error
	SetMetadata(relPath string, meta model.FileMetadata) error
}

// TaskFinalizer is optionally implemented by Destination implementations
// that need to notify a remote server when the replicator is done.
type TaskFinalizer interface {
	Done() error
}
