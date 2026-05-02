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
type CksumDBWriter struct {
	store        progress.ProgressStore
	walker       *copy.Walker
	fileLog      copy.FileLogger
	batchSize    int
	batchTimeout time.Duration
	metrics      *copy.ThroughputMeter
	logInterval  time.Duration

	mu       sync.Mutex
	batch    []model.FileResult
	pass     int64
	mismatch int64
	failed   int64
	total    int64
}

// NewCksumDBWriter creates a CksumDBWriter.
func NewCksumDBWriter(store progress.ProgressStore, walker *copy.Walker, fileLog copy.FileLogger, metrics *copy.ThroughputMeter, logInterval time.Duration) *CksumDBWriter {
	return &CksumDBWriter{
		store:        store,
		walker:       walker,
		fileLog:      fileLog,
		metrics:      metrics,
		logInterval:  logInterval,
		batchSize:    cksumDefaultBatchSize,
		batchTimeout: cksumDefaultBatchTimeout,
	}
}

// Run processes results from resultCh until the channel is closed.
func (dw *CksumDBWriter) Run(resultCh <-chan model.FileResult) {
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
			switch r.CksumStatus {
			case model.CksumPass:
				dw.pass++
				if !r.Skipped && dw.metrics != nil {
					dw.metrics.AddFile(r.FileSize)
				}
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
			// Emit progress_summary
			if dw.fileLog != nil && dw.logInterval > 0 && time.Since(lastProgressTime) >= dw.logInterval {
				dw.emitProgressSummary(false, 0)
				lastProgressTime = time.Now()
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

// PendingCount returns the number of results waiting in the current batch.
func (dw *CksumDBWriter) PendingCount() int {
	dw.mu.Lock()
	defer dw.mu.Unlock()
	return len(dw.batch)
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
		cs, _, err := dw.store.Get(r.RelPath)
		if err != nil {
			cs = model.CopyDone
		}
		batch.Set(r.RelPath, cs, r.CksumStatus)
	}
	if err := batch.Commit(true); err != nil {
		batch.Close()
		return
	}
	batch.Close()

	// Emit unified file_complete events
	if dw.fileLog != nil {
		for _, r := range dw.batch {
			dw.emitFileComplete(r)
		}
	}

	dw.batch = dw.batch[:0]
}

func (dw *CksumDBWriter) emitFileComplete(r model.FileResult) {
	result := "done"
	errorCode := ""
	if r.CksumStatus == model.CksumMismatch || r.CksumStatus == model.CksumError {
		result = "error"
		if r.Err != nil {
			errorCode = r.Err.Error()
		}
	}

	data := map[string]any{
		"action":    "cksum",
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
	if r.Skipped {
		data["skipped"] = true
	}
	dw.fileLog.Emit(filelog.EventFileComplete, data)
}

func (dw *CksumDBWriter) emitProgressSummary(finished bool, exitCode int) {
	walkerStats := dw.walker.Stats()

	var filesPerSec, bytesPerSec float64
	var filesCopied, bytesCopied int64
	if dw.metrics != nil {
		filesPerSec, bytesPerSec = dw.metrics.Rate()
		filesCopied, bytesCopied = dw.metrics.Totals()
	}

	data := map[string]any{
		"phase":    "cksum",
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
			"totalDone":      dw.pass,
			"totalFailed":    dw.mismatch + dw.failed,
			"totalProcessed": dw.total,
		},
	}
	dw.fileLog.Emit(filelog.EventProgressSummary, data)
}
