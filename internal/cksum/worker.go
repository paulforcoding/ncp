package cksum

import (
	"context"
	"fmt"
	"io"

	"github.com/zp001/ncp/internal/copy"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// CksumWorker compares source and destination files by computing checksums.
type CksumWorker struct {
	id          int
	src         storage.Source
	dst         storage.Source
	fileLog     copy.FileLogger
	ioSize      int
	cksumAlgo   model.CksumAlgorithm
	skipByMtime bool
}

// NewCksumWorker creates a CksumWorker.
func NewCksumWorker(id int, src, dst storage.Source, fileLog copy.FileLogger, ioSize int, cksumAlgo model.CksumAlgorithm, skipByMtime bool) *CksumWorker {
	return &CksumWorker{
		id:          id,
		src:         src,
		dst:         dst,
		fileLog:     fileLog,
		ioSize:      ioSize,
		cksumAlgo:   cksumAlgo,
		skipByMtime: skipByMtime,
	}
}

// Run consumes items from cksumCh and sends results to resultCh.
func (w *CksumWorker) Run(ctx context.Context, cksumCh <-chan storage.DiscoverItem, resultCh chan<- model.FileResult) {
	for item := range cksumCh {
		result := w.cksumOne(ctx, item)
		resultCh <- result
	}
}

func (w *CksumWorker) cksumOne(ctx context.Context, item storage.DiscoverItem) model.FileResult {
	if w.skipByMtime {
		if skipped, _ := ShouldSkipCksum(ctx, w.dst, item); skipped {
			return model.FileResult{
				RelPath:     item.RelPath,
				FileType:    item.FileType,
				FileSize:    item.Size,
				CksumStatus: model.CksumPass,
				Skipped:     true,
				Algorithm:   string(w.cksumAlgo),
			}
		}
	}

	switch item.FileType {
	case model.FileRegular:
		return w.cksumFile(ctx, item)
	case model.FileDir:
		return w.cksumDir(ctx, item)
	case model.FileSymlink:
		return w.cksumSymlink(ctx, item)
	default:
		return model.FileResult{
			RelPath:     item.RelPath,
			FileType:    item.FileType,
			FileSize:    item.Size,
			CksumStatus: model.CksumError,
			Algorithm:   string(w.cksumAlgo),
			Err:         fmt.Errorf("unknown file type %d", item.FileType),
		}
	}
}

func (w *CksumWorker) cksumFile(ctx context.Context, item storage.DiscoverItem) model.FileResult {
	srcHash, err := w.computeHash(ctx, w.src, item.RelPath)
	if err != nil {
		return model.FileResult{
			RelPath:     item.RelPath,
			FileType:    item.FileType,
			FileSize:    item.Size,
			CksumStatus: model.CksumError,
			Algorithm:   string(w.cksumAlgo),
			SrcHash:     srcHash,
			Err:         fmt.Errorf("src read: %w", err),
		}
	}

	dstHash, err := w.computeHash(ctx, w.dst, item.RelPath)
	if err != nil {
		return model.FileResult{
			RelPath:     item.RelPath,
			FileType:    item.FileType,
			FileSize:    item.Size,
			CksumStatus: model.CksumMismatch,
			Algorithm:   string(w.cksumAlgo),
			SrcHash:     srcHash,
			DstHash:     dstHash,
			Err:         fmt.Errorf("dst read: %w", err),
		}
	}

	if srcHash == dstHash {
		return model.FileResult{
			RelPath:     item.RelPath,
			FileType:    item.FileType,
			FileSize:    item.Size,
			CksumStatus: model.CksumPass,
			Algorithm:   string(w.cksumAlgo),
			SrcHash:     srcHash,
			DstHash:     dstHash,
		}
	}

	return model.FileResult{
		RelPath:     item.RelPath,
		FileType:    item.FileType,
		FileSize:    item.Size,
		CksumStatus: model.CksumMismatch,
		Algorithm:   string(w.cksumAlgo),
		SrcHash:     srcHash,
		DstHash:     dstHash,
		Err:         fmt.Errorf("%s mismatch: src=%s dst=%s", w.cksumAlgo, srcHash, dstHash),
	}
}

func (w *CksumWorker) computeHash(ctx context.Context, src storage.Source, relPath string) (string, error) {
	reader, err := src.Open(ctx, relPath)
	if err != nil {
		return "", err
	}
	defer reader.Close(ctx)

	bufSize := w.ioSize
	if bufSize <= 0 {
		bufSize = 128 * 1024
	}
	buf := make([]byte, bufSize)

	h := copy.NewHasher(w.cksumAlgo)
	for {
		n, readErr := reader.Read(ctx, buf)
		if n > 0 {
			h.Write(buf[:n])
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return "", readErr
		}
	}

	return copy.SumToHex(h), nil
}

func (w *CksumWorker) cksumDir(ctx context.Context, item storage.DiscoverItem) model.FileResult {
	dstItem, err := w.dst.Stat(ctx, item.RelPath)
	if err != nil {
		return model.FileResult{
			RelPath:     item.RelPath,
			FileType:    item.FileType,
			FileSize:    0,
			CksumStatus: model.CksumMismatch,
			Algorithm:   string(w.cksumAlgo),
			Err:         fmt.Errorf("dst stat dir: %w", err),
		}
	}
	if dstItem.FileType != model.FileDir {
		return model.FileResult{
			RelPath:     item.RelPath,
			FileType:    item.FileType,
			FileSize:    0,
			CksumStatus: model.CksumMismatch,
			Algorithm:   string(w.cksumAlgo),
			Err:         fmt.Errorf("dst is not a directory"),
		}
	}
	return model.FileResult{
		RelPath:     item.RelPath,
		FileType:    item.FileType,
		FileSize:    0,
		CksumStatus: model.CksumPass,
		Algorithm:   string(w.cksumAlgo),
	}
}

func (w *CksumWorker) cksumSymlink(ctx context.Context, item storage.DiscoverItem) model.FileResult {
	dstItem, err := w.dst.Stat(ctx, item.RelPath)
	if err != nil {
		return model.FileResult{
			RelPath:     item.RelPath,
			FileType:    item.FileType,
			FileSize:    0,
			CksumStatus: model.CksumMismatch,
			Algorithm:   string(w.cksumAlgo),
			Err:         fmt.Errorf("dst stat symlink: %w", err),
		}
	}
	if dstItem.FileType != model.FileSymlink {
		return model.FileResult{
			RelPath:     item.RelPath,
			FileType:    item.FileType,
			FileSize:    0,
			CksumStatus: model.CksumMismatch,
			Algorithm:   string(w.cksumAlgo),
			Err:         fmt.Errorf("dst is not a symlink"),
		}
	}
	if dstItem.Attr.SymlinkTarget != item.Attr.SymlinkTarget {
		return model.FileResult{
			RelPath:     item.RelPath,
			FileType:    item.FileType,
			FileSize:    0,
			CksumStatus: model.CksumMismatch,
			Algorithm:   string(w.cksumAlgo),
			Err:         fmt.Errorf("symlink target mismatch: src=%q dst=%q", item.Attr.SymlinkTarget, dstItem.Attr.SymlinkTarget),
		}
	}
	return model.FileResult{
		RelPath:     item.RelPath,
		FileType:    item.FileType,
		FileSize:    0,
		CksumStatus: model.CksumPass,
		Algorithm:   string(w.cksumAlgo),
	}
}
