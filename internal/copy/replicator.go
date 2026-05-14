package copy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

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
func (r *Replicator) Run(ctx context.Context, discoverCh <-chan storage.DiscoverItem, resultCh chan<- model.FileResult) {
	for item := range discoverCh {
		result := r.copyOne(ctx, item)
		resultCh <- result
	}
	files, bytes := r.metrics.Totals()
	if err := r.dst.EndTask(ctx, storage.TaskSummary{Files: files, Bytes: bytes}); err != nil {
		slog.Error("replicator task end failed", "replicatorId", r.id, "error", err)
	}
}

func (r *Replicator) copyOne(ctx context.Context, item storage.DiscoverItem) model.FileResult {
	if r.skipByMtime {
		if skipped, _ := ShouldSkipCopy(ctx, r.dst, item); skipped {
			return model.FileResult{
				RelPath:    item.RelPath,
				FileType:   item.FileType,
				FileSize:   item.Size,
				CopyStatus: model.CopyDone,
				Skipped:    true,
				Algorithm:  string(r.cksumAlgo),
			}
		}
	}

	switch item.FileType {
	case model.FileDir:
		return r.copyDir(ctx, item)
	case model.FileSymlink:
		return r.copySymlink(ctx, item)
	case model.FileRegular:
		return r.copyFile(ctx, item)
	default:
		return model.FileResult{
			RelPath:    item.RelPath,
			FileType:   item.FileType,
			FileSize:   item.Size,
			CopyStatus: model.CopyError,
			Algorithm:  string(r.cksumAlgo),
			Err:        fmt.Errorf("unknown file type %d", item.FileType),
		}
	}
}

func (r *Replicator) copyDir(ctx context.Context, item storage.DiscoverItem) model.FileResult {
	err := r.dst.Mkdir(ctx, item.RelPath, item.Attr.Mode, item.Attr.Uid, item.Attr.Gid)
	if err != nil {
		return model.FileResult{
			RelPath:     item.RelPath,
			FileType:    item.FileType,
			FileSize:    0,
			CopyStatus:  model.CopyError,
			Algorithm:   string(r.cksumAlgo),
			Err:         err,
			MetadataErr: err,
		}
	}

	// Set directory metadata (mode, uid/gid, xattr).
	// Mtime is not set here because writing files into a directory updates its mtime;
	// directory mtime is restored after all children are done (see EnsureDirMtime / Job layer).
	dirAttr := item.Attr
	dirAttr.Mtime = time.Time{}
	dirAttr.Atime = time.Time{}
	var metaErr error
	if err := r.dst.SetMetadata(ctx, item.RelPath, dirAttr); err != nil {
		metaErr = err
	}

	return model.FileResult{
		RelPath:     item.RelPath,
		FileType:    item.FileType,
		FileSize:    0,
		CopyStatus:  model.CopyDone,
		Algorithm:   string(r.cksumAlgo),
		MetadataErr: metaErr,
	}
}

func (r *Replicator) copySymlink(ctx context.Context, item storage.DiscoverItem) model.FileResult {
	if item.Attr.SymlinkTarget == "" {
		return model.FileResult{
			RelPath:    item.RelPath,
			FileType:   item.FileType,
			FileSize:   0,
			CopyStatus: model.CopyError,
			Algorithm:  string(r.cksumAlgo),
			Err:        fmt.Errorf("symlink target empty for %s", item.RelPath),
		}
	}
	err := r.dst.Symlink(ctx, item.RelPath, item.Attr.SymlinkTarget)
	status := model.CopyDone
	if err != nil {
		status = model.CopyError
	}
	return model.FileResult{
		RelPath:     item.RelPath,
		FileType:    item.FileType,
		FileSize:    0,
		CopyStatus:  status,
		Algorithm:   string(r.cksumAlgo),
		Err:         err,
		MetadataErr: err,
	}
}

func (r *Replicator) copyFile(ctx context.Context, item storage.DiscoverItem) model.FileResult {
	reader, err := r.src.Open(ctx, item.RelPath)
	if err != nil {
		return model.FileResult{
			RelPath:    item.RelPath,
			FileType:   item.FileType,
			FileSize:   item.Size,
			CopyStatus: model.CopyError,
			Algorithm:  string(r.cksumAlgo),
			Err:        err,
		}
	}
	defer reader.Close(ctx)

	writer, err := r.dst.OpenFile(ctx, item.RelPath, item.Size, item.Attr.Mode, item.Attr.Uid, item.Attr.Gid)
	if err != nil {
		return model.FileResult{
			RelPath:    item.RelPath,
			FileType:   item.FileType,
			FileSize:   item.Size,
			CopyStatus: model.CopyError,
			Algorithm:  string(r.cksumAlgo),
			Err:        err,
		}
	}

	bufSize := r.ioSize
	if bufSize <= 0 {
		bufSize = model.ResolveIOSize(model.DefaultIOSizeTiers(), item.Size)
	}
	buf := make([]byte, bufSize)

	h := NewHasher(r.cksumAlgo)
	for {
		n, readErr := reader.Read(ctx, buf)
		if n > 0 {
			h.Write(buf[:n])
			if _, writeErr := writer.Write(ctx, buf[:n]); writeErr != nil {
				_ = writer.Abort(ctx)
				return model.FileResult{
					RelPath:    item.RelPath,
					FileType:   item.FileType,
					FileSize:   item.Size,
					CopyStatus: model.CopyError,
					Algorithm:  string(r.cksumAlgo),
					Err:        writeErr,
				}
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			_ = writer.Abort(ctx)
			return model.FileResult{
				RelPath:    item.RelPath,
				FileType:   item.FileType,
				FileSize:   item.Size,
				CopyStatus: model.CopyError,
				Algorithm:  string(r.cksumAlgo),
				Err:        readErr,
			}
		}
	}

	checksumBytes := h.Sum(nil)
	if err := writer.Commit(ctx, checksumBytes); err != nil {
		return model.FileResult{
			RelPath:    item.RelPath,
			FileType:   item.FileType,
			FileSize:   item.Size,
			CopyStatus: model.CopyError,
			Algorithm:  string(r.cksumAlgo),
			Err:        err,
		}
	}

	checksumHex := fmt.Sprintf("%x", checksumBytes)

	// Preserve file metadata (mode, uid/gid, atime, mtime, xattr)
	if err := r.dst.SetMetadata(ctx, item.RelPath, item.Attr); err != nil {
		return model.FileResult{
			RelPath:     item.RelPath,
			FileType:    item.FileType,
			FileSize:    item.Size,
			CopyStatus:  model.CopyError,
			Checksum:    checksumHex,
			Algorithm:   string(r.cksumAlgo),
			Err:         err,
			MetadataErr: err,
		}
	}

	return model.FileResult{
		RelPath:    item.RelPath,
		FileType:   item.FileType,
		FileSize:   item.Size,
		CopyStatus: model.CopyDone,
		Checksum:   checksumHex,
		Algorithm:  string(r.cksumAlgo),
	}
}
