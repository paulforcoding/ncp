package cksum

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"
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
	g, gCtx := errgroup.WithContext(ctx)

	var srcRes, dstRes storage.HashResult
	g.Go(func() error {
		res, err := w.src.ComputeHash(gCtx, item.RelPath, w.cksumAlgo, storage.CksumChunkSize)
		srcRes = res
		return err
	})
	g.Go(func() error {
		res, err := w.dst.ComputeHash(gCtx, item.RelPath, w.cksumAlgo, storage.CksumChunkSize)
		dstRes = res
		return err
	})

	if err := g.Wait(); err != nil {
		status := model.CksumError
		errMsg := fmt.Sprintf("hash error: %v", err)
		// If either side returned ErrChecksum (OSS multipart without ncp-md5), treat as mismatch
		if errors.Is(srcRes.Err, storage.ErrChecksum) || errors.Is(dstRes.Err, storage.ErrChecksum) {
			status = model.CksumMismatch
			errMsg = fmt.Sprintf("checksum unavailable: src=%v dst=%v", srcRes.Err, dstRes.Err)
		}
		return model.FileResult{
			RelPath:     item.RelPath,
			FileType:    item.FileType,
			FileSize:    item.Size,
			CksumStatus: status,
			Algorithm:   string(w.cksumAlgo),
			SrcHash:     srcRes.WholeFileHash,
			DstHash:     dstRes.WholeFileHash,
			Err:         fmt.Errorf("%s", errMsg),
		}
	}

	return compareHashResults(item, srcRes, dstRes, w.cksumAlgo)
}

func compareHashResults(item storage.DiscoverItem, srcRes, dstRes storage.HashResult, algo model.CksumAlgorithm) model.FileResult {
	srcHash := srcRes.WholeFileHash
	dstHash := dstRes.WholeFileHash

	// Per-chunk comparison if both sides have chunk hashes
	if len(srcRes.ChunkHashes) > 0 && len(dstRes.ChunkHashes) > 0 {
		if len(srcRes.ChunkHashes) != len(dstRes.ChunkHashes) {
			return model.FileResult{
				RelPath:     item.RelPath,
				FileType:    item.FileType,
				FileSize:    item.Size,
				CksumStatus: model.CksumMismatch,
				Algorithm:   string(algo),
				SrcHash:     srcHash,
				DstHash:     dstHash,
				Err:         fmt.Errorf("%s mismatch: chunk count src=%d dst=%d", algo, len(srcRes.ChunkHashes), len(dstRes.ChunkHashes)),
			}
		}
		for i, sh := range srcRes.ChunkHashes {
			if sh != dstRes.ChunkHashes[i] {
				return model.FileResult{
					RelPath:     item.RelPath,
					FileType:    item.FileType,
					FileSize:    item.Size,
					CksumStatus: model.CksumMismatch,
					Algorithm:   string(algo),
					SrcHash:     srcHash,
					DstHash:     dstHash,
					Err:         fmt.Errorf("%s mismatch at chunk %d: src=%s dst=%s", algo, i, sh, dstRes.ChunkHashes[i]),
				}
			}
		}
		return model.FileResult{
			RelPath:     item.RelPath,
			FileType:    item.FileType,
			FileSize:    item.Size,
			CksumStatus: model.CksumPass,
			Algorithm:   string(algo),
			SrcHash:     srcHash,
			DstHash:     dstHash,
		}
	}

	// Fall back to whole-file hash comparison (e.g. OSS with etag-md5)
	if srcHash == dstHash {
		return model.FileResult{
			RelPath:     item.RelPath,
			FileType:    item.FileType,
			FileSize:    item.Size,
			CksumStatus: model.CksumPass,
			Algorithm:   string(algo),
			SrcHash:     srcHash,
			DstHash:     dstHash,
		}
	}

	return model.FileResult{
		RelPath:     item.RelPath,
		FileType:    item.FileType,
		FileSize:    item.Size,
		CksumStatus: model.CksumMismatch,
		Algorithm:   string(algo),
		SrcHash:     srcHash,
		DstHash:     dstHash,
		Err:         fmt.Errorf("%s mismatch: src=%s dst=%s", algo, srcHash, dstHash),
	}
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
