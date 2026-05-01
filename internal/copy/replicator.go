package copy

import (
	"fmt"
	"io"
	"os"

	"github.com/zp001/ncp/internal/storage"
	"github.com/zp001/ncp/pkg/model"
)

// Replicator copies files from a Source to a Destination.
// Multiple Replicators share the same discoverCh for natural load balancing.
type Replicator struct {
	id      int
	src     storage.Source
	dst     storage.Destination
	fileLog FileLogger
	ioSize  int // 0 means use tiered IOSize
}

// NewReplicator creates a Replicator with the given ID.
func NewReplicator(id int, src storage.Source, dst storage.Destination, fileLog FileLogger, ioSize int) *Replicator {
	return &Replicator{
		id:      id,
		src:     src,
		dst:     dst,
		fileLog: fileLog,
		ioSize:  ioSize,
	}
}

// Run consumes items from discoverCh and sends results to resultCh.
func (r *Replicator) Run(discoverCh <-chan model.DiscoverItem, resultCh chan<- model.FileResult) {
	for item := range discoverCh {
		result := r.copyOne(item)
		resultCh <- result
	}
}

func (r *Replicator) copyOne(item model.DiscoverItem) model.FileResult {
	switch item.FileType {
	case model.FileDir:
		return r.copyDir(item)
	case model.FileSymlink:
		return r.copySymlink(item)
	case model.FileRegular:
		return r.copyFile(item)
	default:
		return model.FileResult{
			RelPath:    item.RelPath,
			CopyStatus: model.CopyError,
			Err:        fmt.Errorf("unknown file type %d", item.FileType),
		}
	}
}

func (r *Replicator) copyDir(item model.DiscoverItem) model.FileResult {
	err := r.dst.Mkdir(item.RelPath, os.FileMode(item.Mode), item.Uid, item.Gid)
	status := model.CopyDone
	if err != nil {
		status = model.CopyError
	}
	return model.FileResult{RelPath: item.RelPath, CopyStatus: status, Err: err}
}

func (r *Replicator) copySymlink(item model.DiscoverItem) model.FileResult {
	if item.LinkTarget == "" {
		return model.FileResult{
			RelPath:    item.RelPath,
			CopyStatus: model.CopyError,
			Err:        fmt.Errorf("symlink target empty for %s", item.RelPath),
		}
	}
	err := r.dst.Symlink(item.RelPath, item.LinkTarget)
	status := model.CopyDone
	if err != nil {
		status = model.CopyError
	}
	return model.FileResult{RelPath: item.RelPath, CopyStatus: status, Err: err}
}

func (r *Replicator) copyFile(item model.DiscoverItem) model.FileResult {
	reader, err := r.src.Open(item.RelPath)
	if err != nil {
		return model.FileResult{RelPath: item.RelPath, CopyStatus: model.CopyError, Err: err}
	}
	defer reader.Close()

	writer, err := r.dst.OpenFile(item.RelPath, item.FileSize, os.FileMode(item.Mode), item.Uid, item.Gid)
	if err != nil {
		return model.FileResult{RelPath: item.RelPath, CopyStatus: model.CopyError, Err: err}
	}
	defer writer.Close(nil)

	bufSize := r.ioSize
	if bufSize <= 0 {
		bufSize = model.ResolveIOSize(model.DefaultIOSizeTiers(), item.FileSize)
	}
	buf := make([]byte, bufSize)

	var offset int64
	for {
		n, readErr := reader.ReadAt(buf, offset)
		if n > 0 {
			written, writeErr := writer.WriteAt(buf[:n], offset)
			if writeErr != nil {
				return model.FileResult{RelPath: item.RelPath, CopyStatus: model.CopyError, Err: writeErr}
			}
			offset += int64(written)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return model.FileResult{RelPath: item.RelPath, CopyStatus: model.CopyError, Err: readErr}
		}
	}

	if err := writer.Close(nil); err != nil {
		return model.FileResult{RelPath: item.RelPath, CopyStatus: model.CopyError, Err: err}
	}

	return model.FileResult{RelPath: item.RelPath, CopyStatus: model.CopyDone}
}
