package copy

import (
	"sync"
	"time"

	"github.com/zp001/ncp/internal/filelog"
	"github.com/zp001/ncp/pkg/interfaces/progress"
	"github.com/zp001/ncp/pkg/model"
)

const (
	defaultBatchSize    = 100
	defaultBatchTimeout = 5 * time.Second
)

// DBWriter consumes FileResults from resultCh and batches writes to Pebble.
type DBWriter struct {
	store        progress.ProgressStore
	walker       *Walker
	fileLog      FileLogger
	batchSize    int
	batchTimeout time.Duration
	metrics      *ThroughputMeter
	logInterval  time.Duration

	mu      sync.Mutex
	batch   []model.FileResult
	done    int64
	failed  int64
	total   int64
}

// NewDBWriter creates a DBWriter.
func NewDBWriter(store progress.ProgressStore, walker *Walker, fileLog FileLogger, metrics *ThroughputMeter, logInterval time.Duration) *DBWriter {
	return &DBWriter{
		store:        store,
		walker:       walker,
		fileLog:      fileLog,
		metrics:      metrics,
		logInterval:  logInterval,
		batchSize:    defaultBatchSize,
		batchTimeout: defaultBatchTimeout,
	}
}

// Run processes results from resultCh until the channel is closed.
func (dw *DBWriter) Run(resultCh <-chan model.FileResult) {
	ticker := time.NewTicker(dw.batchTimeout)
	defer ticker.Stop()

	var lastProgressTime time.Time

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
			// Emit progress_summary
			if dw.fileLog != nil && dw.logInterval > 0 && time.Since(lastProgressTime) >= dw.logInterval {
				dw.emitProgressSummary(false, 0)
				lastProgressTime = time.Now()
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

// PendingCount returns the number of results waiting in the current batch.
func (dw *DBWriter) PendingCount() int {
	dw.mu.Lock()
	defer dw.mu.Unlock()
	return len(dw.batch)
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
		batch.Close()
		return
	}
	batch.Close()

	// Emit file_complete events
	if dw.fileLog != nil {
		for _, r := range dw.batch {
			dw.emitFileComplete(r)
		}
	}

	dw.batch = dw.batch[:0]
}

func (dw *DBWriter) emitFileComplete(r model.FileResult) {
	result := "done"
	errorCode := ""
	if r.CopyStatus == model.CopyError {
		result = "error"
		if r.Err != nil {
			errorCode = r.Err.Error()
		}
	}

	data := map[string]any{
		"action":    "copy",
		"result":    result,
		"errorCode": errorCode,
		"relPath":   r.RelPath,
		"fileType":  r.FileType.String(),
		"fileSize":  r.FileSize,
		"algorithm": r.Algorithm,
		"checksum":  r.Checksum,
		"srcHash":   r.SrcHash,
		"dstHash":   r.DstHash,
	}
	dw.fileLog.Emit(filelog.EventFileComplete, data)
}

func (dw *DBWriter) emitProgressSummary(finished bool, exitCode int) {
	walkerStats := dw.walker.Stats()

	var filesPerSec, bytesPerSec float64
	var filesCopied, bytesCopied int64
	if dw.metrics != nil {
		filesPerSec, bytesPerSec = dw.metrics.Rate()
		filesCopied, bytesCopied = dw.metrics.Totals()
	}

	data := map[string]any{
		"phase":    "copy",
		"finished": finished,
		"exitCode": exitCode,
		"walker": map[string]any{
			"walkComplete":    walkerStats.WalkComplete,
			"discoveredCount": walkerStats.DiscoveredCount,
			"dispatchedCount": walkerStats.DispatchedCount,
			"backlogCount":    walkerStats.BacklogCount,
			"channelFull":     walkerStats.ChannelFull,
		},
		"replicator": map[string]any{
			"filesCopied": filesCopied,
			"bytesCopied": bytesCopied,
			"filesPerSec": filesPerSec,
			"bytesPerSec": bytesPerSec,
		},
		"dbWriter": map[string]any{
			"pendingCount":   dw.PendingCount(),
			"totalDone":      dw.done,
			"totalFailed":    dw.failed,
			"totalProcessed": dw.total,
		},
	}
	dw.fileLog.Emit(filelog.EventProgressSummary, data)
}
