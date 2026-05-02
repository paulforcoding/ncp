package cksum

import (
	"fmt"
	"io"

	"github.com/zp001/ncp/internal/copy"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// CksumWorker compares source and destination files by computing checksums.
type CksumWorker struct {
	id        int
	src       storage.Source
	dst       storage.Source
	fileLog   copy.FileLogger
	ioSize    int
	cksumAlgo model.CksumAlgorithm
}

// NewCksumWorker creates a CksumWorker.
func NewCksumWorker(id int, src, dst storage.Source, fileLog copy.FileLogger, ioSize int, cksumAlgo model.CksumAlgorithm) *CksumWorker {
	return &CksumWorker{
		id:        id,
		src:       src,
		dst:       dst,
		fileLog:   fileLog,
		ioSize:    ioSize,
		cksumAlgo: cksumAlgo,
	}
}

// Run consumes items from cksumCh and sends results to resultCh.
func (w *CksumWorker) Run(cksumCh <-chan model.DiscoverItem, resultCh chan<- model.FileResult) {
	for item := range cksumCh {
		result := w.cksumOne(item)
		resultCh <- result
	}
}

func (w *CksumWorker) cksumOne(item model.DiscoverItem) model.FileResult {
	switch item.FileType {
	case model.FileRegular:
		return w.cksumFile(item)
	case model.FileDir:
		return w.cksumDir(item)
	case model.FileSymlink:
		return w.cksumSymlink(item)
	default:
		return model.FileResult{
			RelPath:     item.RelPath,
			FileType:    item.FileType,
			FileSize:    item.FileSize,
			CksumStatus: model.CksumError,
			Algorithm:   string(w.cksumAlgo),
			Err:         fmt.Errorf("unknown file type %d", item.FileType),
		}
	}
}

func (w *CksumWorker) cksumFile(item model.DiscoverItem) model.FileResult {
	srcHash, err := w.computeHash(w.src, item.RelPath)
	if err != nil {
		return model.FileResult{
			RelPath:     item.RelPath,
			FileType:    item.FileType,
			FileSize:    item.FileSize,
			CksumStatus: model.CksumError,
			Algorithm:   string(w.cksumAlgo),
			SrcHash:     srcHash,
			Err:         fmt.Errorf("src read: %w", err),
		}
	}

	dstHash, err := w.computeHash(w.dst, item.RelPath)
	if err != nil {
		return model.FileResult{
			RelPath:     item.RelPath,
			FileType:    item.FileType,
			FileSize:    item.FileSize,
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
			FileSize:    item.FileSize,
			CksumStatus: model.CksumPass,
			Algorithm:   string(w.cksumAlgo),
			SrcHash:     srcHash,
			DstHash:     dstHash,
		}
	}

	return model.FileResult{
		RelPath:     item.RelPath,
		FileType:    item.FileType,
		FileSize:    item.FileSize,
		CksumStatus: model.CksumMismatch,
		Algorithm:   string(w.cksumAlgo),
		SrcHash:     srcHash,
		DstHash:     dstHash,
		Err:         fmt.Errorf("%s mismatch: src=%s dst=%s", w.cksumAlgo, srcHash, dstHash),
	}
}

func (w *CksumWorker) computeHash(src storage.Source, relPath string) (string, error) {
	reader, err := src.Open(relPath)
	if err != nil {
		return "", err
	}
	defer reader.Close()

	bufSize := w.ioSize
	if bufSize <= 0 {
		bufSize = 128 * 1024
	}
	buf := make([]byte, bufSize)

	h := copy.NewHasher(w.cksumAlgo)
	var offset int64
	for {
		n, readErr := reader.ReadAt(buf, offset)
		if n > 0 {
			h.Write(buf[:n])
			offset += int64(n)
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

func (w *CksumWorker) cksumDir(item model.DiscoverItem) model.FileResult {
	dstItem, err := w.dst.Restat(item.RelPath)
	if err != nil {
		return model.FileResult{
			RelPath:     item.RelPath,
			FileType:    item.FileType,
			FileSize:    0,
			CksumStatus: model.CksumMismatch,
			Algorithm:   string(w.cksumAlgo),
			Err:         fmt.Errorf("dst restat dir: %w", err),
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

func (w *CksumWorker) cksumSymlink(item model.DiscoverItem) model.FileResult {
	dstItem, err := w.dst.Restat(item.RelPath)
	if err != nil {
		return model.FileResult{
			RelPath:     item.RelPath,
			FileType:    item.FileType,
			FileSize:    0,
			CksumStatus: model.CksumMismatch,
			Algorithm:   string(w.cksumAlgo),
			Err:         fmt.Errorf("dst restat symlink: %w", err),
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
	if dstItem.LinkTarget != item.LinkTarget {
		return model.FileResult{
			RelPath:     item.RelPath,
			FileType:    item.FileType,
			FileSize:    0,
			CksumStatus: model.CksumMismatch,
			Algorithm:   string(w.cksumAlgo),
			Err:         fmt.Errorf("symlink target mismatch: src=%q dst=%q", item.LinkTarget, dstItem.LinkTarget),
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
