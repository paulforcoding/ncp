package copy

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/zp001/ncp/internal/progress/pebble"
	"github.com/zp001/ncp/pkg/storage"
	"github.com/zp001/ncp/pkg/model"
)

// Job orchestrates Walker, Replicators, and DBWriter for a copy task.
type Job struct {
	src            storage.Source
	dst            storage.Destination
	dstBase        string // for EnsureDirMtime
	store          *pebble.Store
	taskID         string
	parallelism    int
	fileLog        FileLogger
	logInterval    int
	ioSize         int
	discoverBuf    int
	resultBuf      int
	ensureDirMtime bool
}

// NewJob creates a copy Job.
func NewJob(src storage.Source, dst storage.Destination, store *pebble.Store, opts ...JobOption) *Job {
	j := &Job{
		src:            src,
		dst:            dst,
		store:          store,
		parallelism:    1,
		discoverBuf:    100000,
		resultBuf:      100000,
		ensureDirMtime: true,
	}
	for _, o := range opts {
		o(j)
	}
	return j
}

// JobOption configures a Job.
type JobOption func(*Job)

func WithParallelism(n int) JobOption          { return func(j *Job) { j.parallelism = n } }
func WithFileLog(fl FileLogger, sec int) JobOption { return func(j *Job) { j.fileLog = fl; j.logInterval = sec } }
func WithIOSize(size int) JobOption            { return func(j *Job) { j.ioSize = size } }
func WithBufferSizes(d, r int) JobOption       { return func(j *Job) { j.discoverBuf = d; j.resultBuf = r } }
func WithTaskID(id string) JobOption           { return func(j *Job) { j.taskID = id } }
func WithDstBase(base string) JobOption        { return func(j *Job) { j.dstBase = base } }
func WithEnsureDirMtime(v bool) JobOption      { return func(j *Job) { j.ensureDirMtime = v } }

// Run executes the copy job and blocks until completion.
// Returns exit code: 0=success, 2=partial failure.
func (j *Job) Run(ctx context.Context) (int, error) {
	discoverCh := make(chan model.DiscoverItem, j.discoverBuf)
	resultCh := make(chan model.FileResult, j.resultBuf)

	walker := NewWalker(j.src, j.store, j.fileLog, durationFromSec(j.logInterval))
	dbWriter := NewDBWriter(j.store, walker, j.fileLog)

	var wg sync.WaitGroup

	// 1. Start DBWriter — exits when resultCh is closed and flushed
	wg.Add(1)
	go func() {
		defer wg.Done()
		dbWriter.Run(resultCh)
	}()

	// 2. Start Replicators — each exits when discoverCh is closed
	var replWg sync.WaitGroup
	for i := 0; i < j.parallelism; i++ {
		replWg.Add(1)
		go func(id int) {
			defer replWg.Done()
			r := NewReplicator(id, j.src, j.dst, j.fileLog, j.ioSize)
			r.Run(discoverCh, resultCh)
		}(i)
	}

	// 3. Close resultCh after all Replicators exit
	go func() {
		replWg.Wait()
		close(resultCh)
	}()

	// 4. Run Walker — closes discoverCh when done
	walkErr := walker.Run(ctx, discoverCh)

	// 5. Wait for DBWriter to finish
	wg.Wait()

	done, failed, total := dbWriter.Stats()

	exitCode := 0
	var runErr error

	if walkErr != nil {
		exitCode = 2
		runErr = fmt.Errorf("walk failed: %w (completed %d/%d)", walkErr, done, total)
	} else if failed > 0 {
		exitCode = 2
		runErr = fmt.Errorf("%d of %d files failed", failed, total)
	}

	// 6. EnsureDirMtime (only if walk completed and destination is local filesystem)
	if walkErr == nil && j.ensureDirMtime && j.dstBase != "" {
		if err := EnsureDirMtime(j.store, j.src, j.dstBase); err != nil {
			// Non-fatal: log but don't change exit code
			_ = err
		}
	}

	// 7. Generate and write completion report
	if j.taskID != "" {
		report, _ := GenerateReport(j.taskID, j.store, done, failed, exitCode)
		if report != nil && j.fileLog != nil {
			j.fileLog.Emit("copy_complete", report)
		}
	}

	return exitCode, runErr
}

func durationFromSec(sec int) time.Duration {
	if sec <= 0 {
		return 5 * time.Second
	}
	return time.Duration(sec) * time.Second
}
