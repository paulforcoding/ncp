package copy

import (
	"sync"
	"time"

	"github.com/zp001/ncp/pkg/interfaces/progress"
	"github.com/zp001/ncp/pkg/model"
)

const (
	defaultBatchSize    = 100
	defaultBatchTimeout = 5 * time.Second // NFR14: max 5s between Syncs
)

// DBWriter consumes FileResults from resultCh and batches writes to Pebble.
// It waits for walkComplete before writing to DB to avoid contention with Walker.
type DBWriter struct {
	store       progress.ProgressStore
	walker      *Walker
	fileLog     FileLogger
	batchSize   int
	batchTimeout time.Duration

	mu      sync.Mutex
	batch   []model.FileResult
	done    int64
	failed  int64
	total   int64
}

// NewDBWriter creates a DBWriter.
func NewDBWriter(store progress.ProgressStore, walker *Walker, fileLog FileLogger) *DBWriter {
	return &DBWriter{
		store:        store,
		walker:       walker,
		fileLog:      fileLog,
		batchSize:    defaultBatchSize,
		batchTimeout: defaultBatchTimeout,
	}
}

// Run processes results from resultCh until the channel is closed.
func (dw *DBWriter) Run(resultCh <-chan model.FileResult) {
	ticker := time.NewTicker(dw.batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case r, ok := <-resultCh:
			if !ok {
				// All replicators done, flush remaining
				dw.flush()
				return
			}
			dw.mu.Lock()
			dw.batch = append(dw.batch, r)
			dw.total++
			if r.CopyStatus == model.CopyDone {
				dw.done++
			} else if r.CopyStatus == model.CopyError {
				dw.failed++
			}
			if dw.walker.WalkComplete() && len(dw.batch) >= dw.batchSize {
				dw.flushLocked()
			}
			dw.mu.Unlock()

		case <-ticker.C:
			dw.mu.Lock()
			if dw.walker.WalkComplete() && len(dw.batch) > 0 {
				dw.flushLocked()
			}
			dw.mu.Unlock()
		}
	}
}

// Stats returns current progress counters.
func (dw *DBWriter) Stats() (done, failed, total int64) {
	dw.mu.Lock()
	defer dw.mu.Unlock()
	return dw.done, dw.failed, dw.total
}

func (dw *DBWriter) flush() {
	dw.mu.Lock()
	defer dw.mu.Unlock()
	dw.flushLocked()
}

func (dw *DBWriter) flushLocked() {
	if len(dw.batch) == 0 {
		return
	}

	batch := dw.store.Batch()
	for _, r := range dw.batch {
		batch.Set(r.RelPath, r.CopyStatus, r.CksumStatus)
	}
	if err := batch.Commit(true); err != nil {
		// Best-effort: log error but don't crash
		batch.Close()
		return
	}
	batch.Close()

	// Emit FileLog events
	if dw.fileLog != nil {
		for _, r := range dw.batch {
			switch r.CopyStatus {
			case model.CopyDone:
				dw.fileLog.Emit("file_complete", r)
			case model.CopyError:
				dw.fileLog.Emit("file_error", r)
			}
		}
	}

	dw.batch = dw.batch[:0]
}
