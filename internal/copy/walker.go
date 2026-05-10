package copy

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/zp001/ncp/pkg/interfaces/progress"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

const batchThreshold = 1000

// WalkerStats holds point-in-time Walker statistics for progress reporting.
type WalkerStats struct {
	WalkComplete    bool
	DiscoveredCount int64
	DispatchedCount int64
	BacklogCount    int64
	ChannelFull     bool
}

// Walker traverses a source directory, writes progress to DB,
// and pushes discovered items to discoverCh.
type Walker struct {
	src         storage.Source
	store       progress.ProgressStore
	walkDone    atomic.Bool
	fileLog     FileLogger
	logInterval time.Duration

	discoveredCount atomic.Int64
	dispatchedCount atomic.Int64
	backlogCount    atomic.Int64
	channelFull     atomic.Bool
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

// Stats returns current Walker statistics.
func (w *Walker) Stats() WalkerStats {
	return WalkerStats{
		WalkComplete:    w.walkDone.Load(),
		DiscoveredCount: w.discoveredCount.Load(),
		DispatchedCount: w.dispatchedCount.Load(),
		BacklogCount:    w.backlogCount.Load(),
		ChannelFull:     w.channelFull.Load(),
	}
}

// Run traverses the source, writes progress, and pushes to discoverCh.
func (w *Walker) Run(ctx context.Context, discoverCh chan<- storage.DiscoverItem) error {
	defer func() {
		w.dispatchRemaining(ctx, discoverCh)
		close(discoverCh)
	}()

	pushEnabled := true
	batch := w.store.Batch()
	batchCount := 0

	err := w.src.Walk(ctx, func(_ context.Context, item storage.DiscoverItem) error {
		w.discoveredCount.Add(1)

		if pushEnabled {
			select {
			case discoverCh <- item:
				w.dispatchedCount.Add(1)
				batch.Set(item.RelPath, model.CopyDispatched, model.CksumNone)
			default:
				pushEnabled = false
				w.channelFull.Store(true)
				w.backlogCount.Add(1)
				batch.Set(item.RelPath, model.CopyDiscovered, model.CksumNone)
			}
		} else {
			w.backlogCount.Add(1)
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

		return nil
	})

	if batchCount > 0 {
		if cerr := batch.Commit(false); cerr != nil && err == nil {
			err = cerr
		}
	}
	batch.Close()

	if err != nil {
		return err
	}

	if err := w.store.SetWalkComplete(w.discoveredCount.Load()); err != nil {
		return err
	}

	w.walkDone.Store(true)
	return nil
}

// dispatchRemaining pushes discovered (not yet dispatched) items to discoverCh.
func (w *Walker) dispatchRemaining(ctx context.Context, discoverCh chan<- storage.DiscoverItem) {
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
		item, err := w.src.Stat(ctx, key)
		if err != nil {
			continue
		}
		discoverCh <- item
		w.dispatchedCount.Add(1)
		w.backlogCount.Add(-1)
	}
}

// ResumeFromDB restores discoverCh from DB for a completed walk.
func (w *Walker) ResumeFromDB(ctx context.Context, discoverCh chan<- storage.DiscoverItem) {
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
		item, err := w.src.Stat(ctx, key)
		if err != nil {
			continue
		}
		discoverCh <- item
	}
	close(discoverCh)
	w.walkDone.Store(true)
}

func shouldSkipForCopyResume(cs model.CopyStatus, cks model.CksumStatus) bool {
	if cks == model.CksumPass {
		return true
	}
	if cs == model.CopyDone && cks == model.CksumNone {
		return true
	}
	return false
}

// ResumeFromDBForCksum pushes files needing checksum verification to cksumCh.
func (w *Walker) ResumeFromDBForCksum(ctx context.Context, cksumCh chan<- storage.DiscoverItem) {
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
		if cs != model.CopyDone {
			continue
		}
		if cks == model.CksumPass {
			continue
		}
		item, err := w.src.Stat(ctx, key)
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
