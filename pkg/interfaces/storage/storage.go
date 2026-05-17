package storage

import (
	"context"
	"os"
	"time"

	"github.com/zp001/ncp/pkg/model"
)

// Walker traverses a source and invokes fn for each discovered item.
type Walker interface {
	Walk(ctx context.Context, fn func(ctx context.Context, item DiscoverItem) error) error
}

// Stater queries metadata for a single relPath. Backends must return an error
// wrapping ErrNotFound when the entry does not exist; callers use errors.Is.
type Stater interface {
	Stat(ctx context.Context, relPath string) (DiscoverItem, error)
}

// TaskBoundary marks the start and end of a task.
type TaskBoundary interface {
	BeginTask(ctx context.Context, taskID string) error
	EndTask(ctx context.Context, summary TaskSummary) error
}

// TaskSummary aggregates task-level metrics passed to EndTask.
type TaskSummary struct {
	Files    int64
	Bytes    int64
	Errors   int64
	Duration time.Duration
}

// FileReader reads data from a source file as a stream.
type FileReader interface {
	Read(ctx context.Context, p []byte) (int, error)
	Close(ctx context.Context) error
	Size() int64
	Attr() FileAttr
}

// Source combines Walker, Stater, FileReader creation, and task boundary
// notification for a storage backend.
type Source interface {
	Walker
	Stater
	TaskBoundary
	Open(ctx context.Context, relPath string) (FileReader, error)
	URI() string
	ComputeHash(ctx context.Context, relPath string, algo model.CksumAlgorithm, chunkSize int64) (HashResult, error)
}

// FileWriter writes data to a destination file as a stream.
type FileWriter interface {
	Write(ctx context.Context, p []byte) (n int, err error)
	Commit(ctx context.Context, checksum []byte) error
	Abort(ctx context.Context) error
	BytesWritten() int64
}

// Destination writes files to a storage backend. It embeds Stater and
// TaskBoundary so that callers no longer need to do a type assertion for
// skip-by-mtime or task finalization.
type Destination interface {
	Stater
	TaskBoundary
	OpenFile(ctx context.Context, relPath string, size int64, mode os.FileMode, uid, gid int) (FileWriter, error)
	Mkdir(ctx context.Context, relPath string, mode os.FileMode, uid, gid int) error
	Symlink(ctx context.Context, relPath string, target string) error
	SetMetadata(ctx context.Context, relPath string, attr FileAttr) error
	ExistsDir(ctx context.Context) (bool, error)
}
