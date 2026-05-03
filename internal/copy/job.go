package copy

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/zp001/ncp/internal/filelog"
	"github.com/zp001/ncp/pkg/interfaces/progress"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// Job orchestrates Walker, Replicators, and DBWriter for a copy task.
type Job struct {
	src            storage.Source
	dst            storage.Destination
	dstFactory     func(id int) (storage.Destination, error)
	dstBase        string
	store          progress.ProgressStore
	taskID         string
	parallelism    int
	fileLog        FileLogger
	logInterval    int
	ioSize         int
	cksumAlgo      model.CksumAlgorithm
	channelBuf     int
	ensureDirMtime bool
	resume         bool
	skipByMtime    bool
	metrics        *ThroughputMeter
}

// NewJob creates a copy Job.
func NewJob(src storage.Source, dst storage.Destination, store progress.ProgressStore, opts ...JobOption) *Job {
	j := &Job{
		src:            src,
		dst:            dst,
		store:          store,
		parallelism:    1,
		channelBuf:     100000,
		ensureDirMtime: true,
		metrics:        &ThroughputMeter{},
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
func WithChannelBuf(n int) JobOption              { return func(j *Job) { j.channelBuf = n } }
func WithTaskID(id string) JobOption              { return func(j *Job) { j.taskID = id } }
func WithDstBase(base string) JobOption           { return func(j *Job) { j.dstBase = base } }
func WithEnsureDirMtime(v bool) JobOption         { return func(j *Job) { j.ensureDirMtime = v } }
func WithResume(v bool) JobOption                 { return func(j *Job) { j.resume = v } }
func WithSkipByMtime(v bool) JobOption            { return func(j *Job) { j.skipByMtime = v } }
func WithCksumAlgo(algo model.CksumAlgorithm) JobOption { return func(j *Job) { j.cksumAlgo = algo } }
func WithDstFactory(f func(id int) (storage.Destination, error)) JobOption {
	return func(j *Job) { j.dstFactory = f }
}

// Run executes the copy job and blocks until completion.
func (j *Job) Run(ctx context.Context) (int, error) {
	discoverCh := make(chan model.DiscoverItem, j.channelBuf)
	resultCh := make(chan model.FileResult, j.channelBuf)

	logDuration := durationFromSec(j.logInterval)
	walker := NewWalker(j.src, j.store, j.fileLog, logDuration)
	dbWriter := NewDBWriter(j.store, walker, j.fileLog, j.metrics, logDuration)

	// Start the pipeline
	replWg := j.startReplicators(discoverCh, resultCh)
	dbWg := j.startDBWriter(dbWriter, resultCh)

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

	dbWg.Wait()

	return j.finalize(walker, dbWriter, walkErr)
}

func (j *Job) populateFromResume(ctx context.Context, walker *Walker, discoverCh chan<- model.DiscoverItem) error {
	hasWalkComplete, err := j.store.HasWalkComplete()
	if err != nil {
		return fmt.Errorf("check walk_complete: %w", err)
	}

	if hasWalkComplete {
		walker.ResumeFromDB(discoverCh)
		return nil
	}

	if err := j.store.Destroy(); err != nil {
		return fmt.Errorf("destroy store for fresh start: %w", err)
	}
	if err := j.store.Reopen(); err != nil {
		return fmt.Errorf("reopen store: %w", err)
	}
	freshWalker := NewWalker(j.src, j.store, j.fileLog, durationFromSec(j.logInterval))
	return freshWalker.Run(ctx, discoverCh)
}

func (j *Job) startReplicators(discoverCh <-chan model.DiscoverItem, resultCh chan<- model.FileResult) *sync.WaitGroup {
	var wg sync.WaitGroup
	for i := 0; i < j.parallelism; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			dst := j.dst
			if j.dstFactory != nil {
				var err error
				dst, err = j.dstFactory(id)
				if err != nil {
					for item := range discoverCh {
						resultCh <- model.FileResult{
							RelPath:    item.RelPath,
							FileType:   item.FileType,
							FileSize:   item.FileSize,
							CopyStatus: model.CopyError,
							Algorithm:  string(j.cksumAlgo),
							Err:        fmt.Errorf("create destination for replicator %d: %w", id, err),
						}
					}
					return
				}
			}
			r := NewReplicator(id, j.src, dst, j.fileLog, j.ioSize, j.cksumAlgo, j.metrics, j.skipByMtime)
			r.Run(discoverCh, resultCh)
		}(i)
	}
	return &wg
}

func (j *Job) startDBWriter(dbWriter *DBWriter, resultCh <-chan model.FileResult) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		dbWriter.Run(resultCh)
	}()
	return &wg
}

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

	// EnsureDirMtime
	if walkErr == nil && j.ensureDirMtime && j.dstBase != "" {
		if err := EnsureDirMtime(j.store, j.src, j.dstBase); err != nil {
			_ = err
		}
	}

	// Emit final progress_summary
	if j.fileLog != nil {
		dbWriter.EmitFinalSummary(exitCode)
	}

	// Generate internal report (still used for report file, but no longer emits copy_complete)
	if j.taskID != "" {
		_, _ = GenerateReport(j.taskID, j.store, done, failed, exitCode)
	}

	return exitCode, runErr
}

func durationFromSec(sec int) time.Duration {
	if sec <= 0 {
		return 5 * time.Second
	}
	return time.Duration(sec) * time.Second
}

// EmitCopyPlan emits a copy_plan event if fileLog is available.
func EmitCopyPlan(fileLog FileLogger, taskID string, src []string, dst string, algo model.CksumAlgorithm) {
	if fileLog == nil {
		return
	}
	fileLog.Emit(filelog.EventCopyPlan, map[string]any{
		"taskId":    taskID,
		"sources":   src,
		"dest":      dst,
		"algorithm": string(algo),
	})
}
