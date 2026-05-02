package cksum

import (
	"sync"
	"time"

	"github.com/zp001/ncp/internal/copy"
	"github.com/zp001/ncp/internal/filelog"
	"github.com/zp001/ncp/pkg/interfaces/progress"
	"github.com/zp001/ncp/pkg/model"
)

const (
	cksumDefaultBatchSize    = 100
	cksumDefaultBatchTimeout = 5 * time.Second
)

// CksumDBWriter consumes FileResults from resultCh and updates cksumStatus in DB.
// It preserves the existing copyStatus and only updates the cksumStatus byte.
type CksumDBWriter struct {
	store        progress.ProgressStore
	walker       *copy.Walker
	fileLog      copy.FileLogger
	batchSize    int
	batchTimeout time.Duration

	mu       sync.Mutex
	batch    []model.FileResult
	pass     int64
	mismatch int64
	failed   int64
	total    int64
}

// NewCksumDBWriter creates a CksumDBWriter.
func NewCksumDBWriter(store progress.ProgressStore, walker *copy.Walker, fileLog copy.FileLogger) *CksumDBWriter {
	return &CksumDBWriter{
		store:        store,
		walker:       walker,
		fileLog:      fileLog,
		batchSize:    cksumDefaultBatchSize,
		batchTimeout: cksumDefaultBatchTimeout,
	}
}

// Run processes results from resultCh until the channel is closed.
func (dw *CksumDBWriter) Run(resultCh <-chan model.FileResult) {
	ticker := time.NewTicker(dw.batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case r, ok := <-resultCh:
			if !ok {
				dw.flush()
				return
			}
			dw.mu.Lock()
			dw.batch = append(dw.batch, r)
			dw.total++
			switch r.CksumStatus {
			case model.CksumPass:
				dw.pass++
			case model.CksumMismatch:
				dw.mismatch++
			case model.CksumError:
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

// Stats returns current cksum progress counters.
func (dw *CksumDBWriter) Stats() (pass, mismatch, failed, total int64) {
	dw.mu.Lock()
	defer dw.mu.Unlock()
	return dw.pass, dw.mismatch, dw.failed, dw.total
}

func (dw *CksumDBWriter) flush() {
	dw.mu.Lock()
	defer dw.mu.Unlock()
	dw.flushLocked()
}

func (dw *CksumDBWriter) flushLocked() {
	if len(dw.batch) == 0 {
		return
	}

	batch := dw.store.Batch()
	for _, r := range dw.batch {
		// Preserve existing copyStatus, only update cksumStatus
		cs, _, err := dw.store.Get(r.RelPath)
		if err != nil {
			cs = model.CopyDone // best guess for cksum items
		}
		batch.Set(r.RelPath, cs, r.CksumStatus)
	}
	if err := batch.Commit(true); err != nil {
		batch.Close()
		return
	}
	batch.Close()

	// Emit FileLog events
	if dw.fileLog != nil {
		for _, r := range dw.batch {
			switch r.CksumStatus {
			case model.CksumPass:
				dw.fileLog.Emit(filelog.EventCksumPass, r)
			case model.CksumMismatch:
				dw.fileLog.Emit(filelog.EventCksumMismatch, r)
			case model.CksumError:
				dw.fileLog.Emit(filelog.EventCksumError, r)
			}
		}
	}

	dw.batch = dw.batch[:0]
}
