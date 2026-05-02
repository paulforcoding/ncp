package copy

import (
	"context"
	"time"
	"sync/atomic"

	"github.com/zp001/ncp/pkg/interfaces/progress"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

const batchThreshold = 1000

// Walker traverses a source directory, writes progress to DB,
// and pushes discovered items to discoverCh.
type Walker struct {
	src         storage.Source
	store       progress.ProgressStore
	walkDone    atomic.Bool
	fileLog     FileLogger
	logInterval time.Duration
}

// FileLogger emits structured events for FileLog output.
type FileLogger interface {
	Emit(eventType string, data any)
}

// NewWalker creates a Walker.
func NewWalker(src storage.Source, store progress.ProgressStore, fileLog FileLogger, logInterval time.Duration) *Walker {
	return &Walker{
		src:         src,
		store:       store,
		fileLog:     fileLog,
		logInterval: logInterval,
	}
}

// WalkComplete returns whether the walk has finished and synced.
func (w *Walker) WalkComplete() bool {
	return w.walkDone.Load()
}

// Run traverses the source, writes progress, and pushes to discoverCh.
func (w *Walker) Run(ctx context.Context, discoverCh chan<- model.DiscoverItem) error {
	defer func() {
		w.dispatchRemaining(discoverCh)
		close(discoverCh)
	}()

	pushEnabled := true
	batch := w.store.Batch()
	batchCount := 0
	walkCount := 0
	lastProgressTime := time.Now()

	err := w.src.Walk(ctx, func(item model.DiscoverItem) error {
		walkCount++

		if pushEnabled {
			select {
			case discoverCh <- item:
				batch.Set(item.RelPath, model.CopyDispatched, model.CksumNone)
			default:
				pushEnabled = false
				batch.Set(item.RelPath, model.CopyDiscovered, model.CksumNone)
			}
		} else {
			batch.Set(item.RelPath, model.CopyDiscovered, model.CksumNone)
		}

		batchCount++
		if batchCount >= batchThreshold {
			if err := batch.Commit(false); err != nil {
				return err
			}
			batch.Close()
			batch = w.store.Batch()
			batchCount = 0
		}

		if w.fileLog != nil && w.logInterval > 0 && time.Since(lastProgressTime) >= w.logInterval {
			w.fileLog.Emit("walk_progress", map[string]any{
				"discoveredCount": walkCount,
				"currentPath":     item.RelPath,
			})
			lastProgressTime = time.Now()
		}

		return nil
	})

	if batchCount > 0 {
		if cerr := batch.Commit(false); cerr != nil && err == nil {
			err = cerr
		}
	}
	batch.Close()

	if err != nil {
		// Walk was cancelled or failed — do NOT write __walk_complete.
		// Next resume will see no __walk_complete and start fresh.
		return err
	}

	if err := w.store.SetWalkComplete(int64(walkCount)); err != nil {
		return err
	}

	w.walkDone.Store(true)
	return nil
}

// dispatchRemaining pushes discovered (not yet dispatched) items to discoverCh.
// Called after walk is complete, so channel will drain without blocking.
// Since DB only stores 2-byte status, we re-stat each path to rebuild full DiscoverItem.
func (w *Walker) dispatchRemaining(discoverCh chan<- model.DiscoverItem) {
	it, err := w.store.Iter()
	if err != nil {
		return
	}
	defer it.Close()

	for it.First(); it.Valid(); it.Next() {
		key := it.Key()
		if isInternalKey(key) {
			continue
		}
		cs, _ := it.Value()
		if cs != model.CopyDiscovered {
			continue
		}
		item, err := w.src.Restat(key)
		if err != nil {
			continue
		}
		discoverCh <- item
	}
}

// ResumeFromDB restores discoverCh from DB for a completed walk.
// Supports scenario D (cksum→copy): files with cksumStatus=pass are skipped,
// files with cksumStatus=mismatch/error are re-copied.
func (w *Walker) ResumeFromDB(discoverCh chan<- model.DiscoverItem) {
	it, err := w.store.Iter()
	if err != nil {
		return
	}
	defer it.Close()

	for it.First(); it.Valid(); it.Next() {
		key := it.Key()
		if isInternalKey(key) {
			continue
		}
		cs, cks := it.Value()
		if shouldSkipForCopyResume(cs, cks) {
			continue
		}
		item, err := w.src.Restat(key)
		if err != nil {
			continue
		}
		discoverCh <- item
	}
	close(discoverCh)
	w.walkDone.Store(true)
}

// shouldSkipForCopyResume determines if a file should be skipped during copy resume.
// Skip if: copy done AND (cksum passed OR no checksum result).
// Re-copy if: cksum mismatch/error, or copy not done.
func shouldSkipForCopyResume(cs model.CopyStatus, cks model.CksumStatus) bool {
	if cks == model.CksumPass {
		return true // Verified by checksum — skip
	}
	if cs == model.CopyDone && cks == model.CksumNone {
		return true // Copy done, no checksum issues — skip
	}
	return false // Needs copy or re-copy
}

// ResumeFromDBForCksum pushes files needing checksum verification to cksumCh.
// Only includes files with copyStatus=done and cksumStatus≠pass.
// Files with copyStatus=error are skipped (not worth verifying).
func (w *Walker) ResumeFromDBForCksum(cksumCh chan<- model.DiscoverItem) {
	it, err := w.store.Iter()
	if err != nil {
		return
	}
	defer it.Close()

	for it.First(); it.Valid(); it.Next() {
		key := it.Key()
		if isInternalKey(key) {
			continue
		}
		cs, cks := it.Value()
		// Only verify successfully copied files that haven't passed checksum
		if cs != model.CopyDone {
			continue
		}
		if cks == model.CksumPass {
			continue
		}
		item, err := w.src.Restat(key)
		if err != nil {
			continue
		}
		cksumCh <- item
	}
	close(cksumCh)
	w.walkDone.Store(true)
}

func isInternalKey(key string) bool {
	return len(key) >= 2 && key[0] == '_' && key[1] == '_'
}
