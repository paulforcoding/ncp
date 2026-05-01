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
	resume         bool
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

func WithParallelism(n int) JobOption             { return func(j *Job) { j.parallelism = n } }
func WithFileLog(fl FileLogger, sec int) JobOption { return func(j *Job) { j.fileLog = fl; j.logInterval = sec } }
func WithIOSize(size int) JobOption               { return func(j *Job) { j.ioSize = size } }
func WithBufferSizes(d, r int) JobOption          { return func(j *Job) { j.discoverBuf = d; j.resultBuf = r } }
func WithTaskID(id string) JobOption              { return func(j *Job) { j.taskID = id } }
func WithDstBase(base string) JobOption           { return func(j *Job) { j.dstBase = base } }
func WithEnsureDirMtime(v bool) JobOption         { return func(j *Job) { j.ensureDirMtime = v } }
func WithResume(v bool) JobOption                 { return func(j *Job) { j.resume = v } }

// Run executes the copy job and blocks until completion.
// Returns exit code: 0=success, 2=partial failure.
func (j *Job) Run(ctx context.Context) (int, error) {
	discoverCh := make(chan model.DiscoverItem, j.discoverBuf)
	resultCh := make(chan model.FileResult, j.resultBuf)

	walker := NewWalker(j.src, j.store, j.fileLog, durationFromSec(j.logInterval))
	dbWriter := NewDBWriter(j.store, walker, j.fileLog)

	// Start the pipeline
	replWg := j.startReplicators(discoverCh, resultCh)
	dbWg := j.startDBWriter(dbWriter, resultCh)

	// Close resultCh after all Replicators exit
	go func() {
		replWg.Wait()
		close(resultCh)
	}()

	// Populate discoverCh
	var walkErr error
	if j.resume {
		walkErr = j.populateFromResume(ctx, walker, discoverCh)
	} else {
		walkErr = walker.Run(ctx, discoverCh)
	}

	// Wait for pipeline to drain
	dbWg.Wait()

	return j.finalize(walker, dbWriter, walkErr)
}

// populateFromResume handles resume logic:
// - walk_complete exists: ResumeFromDB (push non-done items)
// - walk_complete absent: destroy DB, reopen, run fresh walk
func (j *Job) populateFromResume(ctx context.Context, walker *Walker, discoverCh chan<- model.DiscoverItem) error {
	hasWalkComplete, err := j.store.HasWalkComplete()
	if err != nil {
		return fmt.Errorf("check walk_complete: %w", err)
	}

	if hasWalkComplete {
		// Resume from DB — push non-done items, close discoverCh, set walkComplete
		walker.ResumeFromDB(discoverCh)
		return nil
	}

	// Walk was incomplete — destroy DB and start fresh
	if err := j.store.Destroy(); err != nil {
		return fmt.Errorf("destroy store for fresh start: %w", err)
	}
	// Re-open the store at the same directory
	if err := j.store.Reopen(); err != nil {
		return fmt.Errorf("reopen store: %w", err)
	}
	// Re-create walker since store was recreated
	freshWalker := NewWalker(j.src, j.store, j.fileLog, durationFromSec(j.logInterval))
	return freshWalker.Run(ctx, discoverCh)
}

// startReplicators launches N Replicator goroutines sharing discoverCh.
func (j *Job) startReplicators(discoverCh <-chan model.DiscoverItem, resultCh chan<- model.FileResult) *sync.WaitGroup {
	var wg sync.WaitGroup
	for i := 0; i < j.parallelism; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := NewReplicator(id, j.src, j.dst, j.fileLog, j.ioSize)
			r.Run(discoverCh, resultCh)
		}(i)
	}
	return &wg
}

// startDBWriter launches the DBWriter goroutine.
func (j *Job) startDBWriter(dbWriter *DBWriter, resultCh <-chan model.FileResult) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		dbWriter.Run(resultCh)
	}()
	return &wg
}

// finalize computes exit code, runs EnsureDirMtime, and emits completion report.
func (j *Job) finalize(walker *Walker, dbWriter *DBWriter, walkErr error) (int, error) {
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

	// EnsureDirMtime (only if walk completed and destination is local filesystem)
	if walkErr == nil && j.ensureDirMtime && j.dstBase != "" {
		if err := EnsureDirMtime(j.store, j.src, j.dstBase); err != nil {
			_ = err
		}
	}

	// Generate and write completion report
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
