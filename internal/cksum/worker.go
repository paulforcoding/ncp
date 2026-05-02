package cksum

import (
	"crypto/md5"
	"fmt"
	"io"

	"github.com/zp001/ncp/internal/copy"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// CksumWorker compares source and destination files by computing MD5 checksums.
type CksumWorker struct {
	id      int
	src     storage.Source
	dst     storage.Source
	fileLog copy.FileLogger
	ioSize  int
}

// NewCksumWorker creates a CksumWorker.
func NewCksumWorker(id int, src, dst storage.Source, fileLog copy.FileLogger, ioSize int) *CksumWorker {
	return &CksumWorker{
		id:      id,
		src:     src,
		dst:     dst,
		fileLog: fileLog,
		ioSize:  ioSize,
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
			CksumStatus: model.CksumError,
			Err:         fmt.Errorf("unknown file type %d", item.FileType),
		}
	}
}

func (w *CksumWorker) cksumFile(item model.DiscoverItem) model.FileResult {
	srcMD5, err := w.computeMD5(w.src, item.RelPath)
	if err != nil {
		return model.FileResult{
			RelPath:     item.RelPath,
			CksumStatus: model.CksumError,
			Err:         fmt.Errorf("src read: %w", err),
		}
	}

	dstMD5, err := w.computeMD5(w.dst, item.RelPath)
	if err != nil {
		// Destination file doesn't exist or unreadable
		return model.FileResult{
			RelPath:     item.RelPath,
			CksumStatus: model.CksumMismatch,
			Err:         fmt.Errorf("dst read: %w", err),
		}
	}

	if srcMD5 == dstMD5 {
		return model.FileResult{
			RelPath:     item.RelPath,
			CksumStatus: model.CksumPass,
		}
	}

	return model.FileResult{
		RelPath:     item.RelPath,
		CksumStatus: model.CksumMismatch,
		Err:         fmt.Errorf("md5 mismatch: src=%s dst=%s", srcMD5, dstMD5),
	}
}

func (w *CksumWorker) computeMD5(src storage.Source, relPath string) (string, error) {
	reader, err := src.Open(relPath)
	if err != nil {
		return "", err
	}
	defer reader.Close()

	bufSize := w.ioSize
	if bufSize <= 0 {
		bufSize = 128 * 1024 // 128KB default for checksumming
	}
	buf := make([]byte, bufSize)

	h := md5.New()
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

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func (w *CksumWorker) cksumDir(item model.DiscoverItem) model.FileResult {
	dstItem, err := w.dst.Restat(item.RelPath)
	if err != nil {
		return model.FileResult{
			RelPath:     item.RelPath,
			CksumStatus: model.CksumMismatch,
			Err:         fmt.Errorf("dst restat dir: %w", err),
		}
	}
	if dstItem.FileType != model.FileDir {
		return model.FileResult{
			RelPath:     item.RelPath,
			CksumStatus: model.CksumMismatch,
			Err:         fmt.Errorf("dst is not a directory"),
		}
	}
	return model.FileResult{
		RelPath:     item.RelPath,
		CksumStatus: model.CksumPass,
	}
}

func (w *CksumWorker) cksumSymlink(item model.DiscoverItem) model.FileResult {
	dstItem, err := w.dst.Restat(item.RelPath)
	if err != nil {
		return model.FileResult{
			RelPath:     item.RelPath,
			CksumStatus: model.CksumMismatch,
			Err:         fmt.Errorf("dst restat symlink: %w", err),
		}
	}
	if dstItem.FileType != model.FileSymlink {
		return model.FileResult{
			RelPath:     item.RelPath,
			CksumStatus: model.CksumMismatch,
			Err:         fmt.Errorf("dst is not a symlink"),
		}
	}
	if dstItem.LinkTarget != item.LinkTarget {
		return model.FileResult{
			RelPath:     item.RelPath,
			CksumStatus: model.CksumMismatch,
			Err:         fmt.Errorf("symlink target mismatch: src=%q dst=%q", item.LinkTarget, dstItem.LinkTarget),
		}
	}
	return model.FileResult{
		RelPath:     item.RelPath,
		CksumStatus: model.CksumPass,
	}
}
