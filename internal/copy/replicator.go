package copy

import (
	"fmt"
	"io"
	"os"

	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// Replicator copies files from a Source to a Destination.
type Replicator struct {
	id          int
	src         storage.Source
	dst         storage.Destination
	fileLog     FileLogger
	ioSize      int
	cksumAlgo   model.CksumAlgorithm
	metrics     *ThroughputMeter
	skipByMtime bool
}

// NewReplicator creates a Replicator with the given ID.
func NewReplicator(id int, src storage.Source, dst storage.Destination, fileLog FileLogger, ioSize int, cksumAlgo model.CksumAlgorithm, metrics *ThroughputMeter, skipByMtime bool) *Replicator {
	return &Replicator{
		id:          id,
		src:         src,
		dst:         dst,
		fileLog:     fileLog,
		ioSize:      ioSize,
		cksumAlgo:   cksumAlgo,
		metrics:     metrics,
		skipByMtime: skipByMtime,
	}
}

// Run consumes items from discoverCh and sends results to resultCh.
func (r *Replicator) Run(discoverCh <-chan model.DiscoverItem, resultCh chan<- model.FileResult) {
	for item := range discoverCh {
		result := r.copyOne(item)
		resultCh <- result
	}
	if f, ok := r.dst.(storage.TaskFinalizer); ok {
		f.Done()
	}
}

func (r *Replicator) copyOne(item model.DiscoverItem) model.FileResult {
	if r.skipByMtime {
		if skipped, _ := ShouldSkipCopy(r.dst, item); skipped {
			return model.FileResult{
				RelPath:    item.RelPath,
				FileType:   item.FileType,
				FileSize:   item.FileSize,
				CopyStatus: model.CopyDone,
				Skipped:    true,
				Algorithm:  string(r.cksumAlgo),
			}
		}
	}

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
			FileType:   item.FileType,
			FileSize:   item.FileSize,
			CopyStatus: model.CopyError,
			Algorithm:  string(r.cksumAlgo),
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
	return model.FileResult{
		RelPath:    item.RelPath,
		FileType:   item.FileType,
		FileSize:   0,
		CopyStatus: status,
		Algorithm:  string(r.cksumAlgo),
		Err:        err,
	}
}

func (r *Replicator) copySymlink(item model.DiscoverItem) model.FileResult {
	if item.LinkTarget == "" {
		return model.FileResult{
			RelPath:    item.RelPath,
			FileType:   item.FileType,
			FileSize:   0,
			CopyStatus: model.CopyError,
			Algorithm:  string(r.cksumAlgo),
			Err:        fmt.Errorf("symlink target empty for %s", item.RelPath),
		}
	}
	err := r.dst.Symlink(item.RelPath, item.LinkTarget)
	status := model.CopyDone
	if err != nil {
		status = model.CopyError
	}
	return model.FileResult{
		RelPath:    item.RelPath,
		FileType:   item.FileType,
		FileSize:   0,
		CopyStatus: status,
		Algorithm:  string(r.cksumAlgo),
		Err:        err,
	}
}

func (r *Replicator) copyFile(item model.DiscoverItem) model.FileResult {
	reader, err := r.src.Open(item.RelPath)
	if err != nil {
		return model.FileResult{
			RelPath:    item.RelPath,
			FileType:   item.FileType,
			FileSize:   item.FileSize,
			CopyStatus: model.CopyError,
			Algorithm:  string(r.cksumAlgo),
			Err:        err,
		}
	}
	defer reader.Close()

	writer, err := r.dst.OpenFile(item.RelPath, item.FileSize, os.FileMode(item.Mode), item.Uid, item.Gid)
	if err != nil {
		return model.FileResult{
			RelPath:    item.RelPath,
			FileType:   item.FileType,
			FileSize:   item.FileSize,
			CopyStatus: model.CopyError,
			Algorithm:  string(r.cksumAlgo),
			Err:        err,
		}
	}

	bufSize := r.ioSize
	if bufSize <= 0 {
		bufSize = model.ResolveIOSize(model.DefaultIOSizeTiers(), item.FileSize)
	}
	buf := make([]byte, bufSize)

	var offset int64
	h := NewHasher(r.cksumAlgo)
	for {
		n, readErr := reader.ReadAt(buf, offset)
		if n > 0 {
			h.Write(buf[:n])
			written, writeErr := writer.WriteAt(buf[:n], offset)
			if writeErr != nil {
				writer.Close(nil)
				return model.FileResult{
					RelPath:    item.RelPath,
					FileType:   item.FileType,
					FileSize:   item.FileSize,
					CopyStatus: model.CopyError,
					Algorithm:  string(r.cksumAlgo),
					Err:        writeErr,
				}
			}
			offset += int64(written)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			writer.Close(nil)
			return model.FileResult{
				RelPath:    item.RelPath,
				FileType:   item.FileType,
				FileSize:   item.FileSize,
				CopyStatus: model.CopyError,
				Algorithm:  string(r.cksumAlgo),
				Err:        readErr,
			}
		}
	}

	checksumBytes := h.Sum(nil)
	if err := writer.Close(checksumBytes); err != nil {
		return model.FileResult{
			RelPath:    item.RelPath,
			FileType:   item.FileType,
			FileSize:   item.FileSize,
			CopyStatus: model.CopyError,
			Algorithm:  string(r.cksumAlgo),
			Err:        err,
		}
	}

	// Preserve file mtime
	if item.Mtime != 0 {
		if err := r.dst.SetMetadata(item.RelPath, model.FileMetadata{Mtime: item.Mtime}); err != nil {
			return model.FileResult{
				RelPath:    item.RelPath,
				FileType:   item.FileType,
				FileSize:   item.FileSize,
				CopyStatus: model.CopyError,
				Algorithm:  string(r.cksumAlgo),
				Err:        err,
			}
		}
	}

	checksumHex := fmt.Sprintf("%x", checksumBytes)

	return model.FileResult{
		RelPath:    item.RelPath,
		FileType:   item.FileType,
		FileSize:   item.FileSize,
		CopyStatus: model.CopyDone,
		Checksum:   checksumHex,
		Algorithm:  string(r.cksumAlgo),
	}
}
