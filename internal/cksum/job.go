package cksum

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/zp001/ncp/internal/copy"
	"github.com/zp001/ncp/internal/task"
	"github.com/zp001/ncp/pkg/interfaces/progress"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// CksumJob orchestrates Walker, CksumWorkers, and CksumDBWriter for a checksum task.
type CksumJob struct {
	src         storage.Source
	dst         storage.Source
	srcFactory  func(id int) (storage.Source, error)
	dstFactory  func(id int) (storage.Source, error)
	store       progress.ProgressStore
	taskID      string
	parallelism int
	fileLog     copy.FileLogger
	logInterval int
	ioSize      int
	cksumAlgo   model.CksumAlgorithm
	channelBuf  int
	resume      bool
	skipByMtime bool
	metrics     *copy.ThroughputMeter
}

// NewCksumJob creates a cksum job.
func NewCksumJob(src, dst storage.Source, store progress.ProgressStore, opts ...CksumJobOption) *CksumJob {
	j := &CksumJob{
		src:         src,
		dst:         dst,
		store:       store,
		parallelism: 1,
		cksumAlgo:   model.DefaultCksumAlgorithm,
		channelBuf:  100000,
		metrics:     &copy.ThroughputMeter{},
	}
	for _, o := range opts {
		o(j)
	}
	if j.taskID == "" {
		j.taskID = task.GenerateTaskID()
	}
	return j
}

// CksumJobOption configures a CksumJob.
type CksumJobOption func(*CksumJob)

func WithCksumParallelism(n int) CksumJobOption { return func(j *CksumJob) { j.parallelism = n } }
func WithCksumFileLog(fl copy.FileLogger, sec int) CksumJobOption {
	return func(j *CksumJob) { j.fileLog = fl; j.logInterval = sec }
}
func WithCksumIOSize(size int) CksumJobOption    { return func(j *CksumJob) { j.ioSize = size } }
func WithCksumTaskID(id string) CksumJobOption   { return func(j *CksumJob) { j.taskID = id } }
func WithCksumResume(v bool) CksumJobOption      { return func(j *CksumJob) { j.resume = v } }
func WithCksumSkipByMtime(v bool) CksumJobOption { return func(j *CksumJob) { j.skipByMtime = v } }
func WithCksumAlgo(algo model.CksumAlgorithm) CksumJobOption {
	return func(j *CksumJob) { j.cksumAlgo = algo }
}
func WithCksumChannelBuf(n int) CksumJobOption { return func(j *CksumJob) { j.channelBuf = n } }
func WithCksumSrcFactory(f func(id int) (storage.Source, error)) CksumJobOption {
	return func(j *CksumJob) { j.srcFactory = f }
}
func WithCksumDstFactory(f func(id int) (storage.Source, error)) CksumJobOption {
	return func(j *CksumJob) { j.dstFactory = f }
}

// TaskID returns the job's task ID.
func (j *CksumJob) TaskID() string { return j.taskID }

// Run executes the checksum job and blocks until completion.
func (j *CksumJob) Run(ctx context.Context) (int, error) {
	cksumCh := make(chan storage.DiscoverItem, j.channelBuf)
	resultCh := make(chan model.FileResult, j.channelBuf)

	logDuration := durationFromSec(j.logInterval)
	walker := copy.NewWalker(j.src, j.store, j.fileLog, logDuration)
	dbWriter := NewCksumDBWriter(j.store, walker, j.fileLog, j.metrics, logDuration)

	workerWg := j.startCksumWorkers(ctx, cksumCh, resultCh)
	dbWg := j.startCksumDBWriter(dbWriter, resultCh)

	go func() {
		workerWg.Wait()
		close(resultCh)
	}()

	var walkErr error
	if j.resume {
		walkErr = j.populateFromResume(ctx, walker, cksumCh)
	} else {
		walkErr = walker.Run(ctx, cksumCh)
	}

	dbWg.Wait()

	return j.finalize(walker, dbWriter, walkErr)
}

func (j *CksumJob) populateFromResume(ctx context.Context, walker *copy.Walker, cksumCh chan<- storage.DiscoverItem) error {
	hasWalkComplete, err := j.store.HasWalkComplete()
	if err != nil {
		return fmt.Errorf("check walk_complete: %w", err)
	}

	if hasWalkComplete {
		walker.ResumeFromDBForCksum(ctx, cksumCh)
		return nil
	}

	if err := j.store.Destroy(); err != nil {
		return fmt.Errorf("destroy store for fresh start: %w", err)
	}
	if err := j.store.Reopen(); err != nil {
		return fmt.Errorf("reopen store: %w", err)
	}
	freshWalker := copy.NewWalker(j.src, j.store, j.fileLog, durationFromSec(j.logInterval))
	return freshWalker.Run(ctx, cksumCh)
}

func (j *CksumJob) startCksumWorkers(ctx context.Context, cksumCh <-chan storage.DiscoverItem, resultCh chan<- model.FileResult) *sync.WaitGroup {
	var wg sync.WaitGroup
	for i := 0; i < j.parallelism; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			src := j.src
			if j.srcFactory != nil {
				var err error
				src, err = j.srcFactory(id)
				if err != nil {
					return
				}
				if err := src.BeginTask(ctx, j.taskID); err != nil {
					return
				}
				defer func() { _ = src.EndTask(ctx, storage.TaskSummary{}) }()
			}

			dst := j.dst
			if j.dstFactory != nil {
				var err error
				dst, err = j.dstFactory(id)
				if err != nil {
					return
				}
				if err := dst.BeginTask(ctx, j.taskID); err != nil {
					return
				}
				defer func() { _ = dst.EndTask(ctx, storage.TaskSummary{}) }()
			}

			w := NewCksumWorker(id, src, dst, j.fileLog, j.ioSize, j.cksumAlgo, j.skipByMtime)
			w.Run(ctx, cksumCh, resultCh)
		}(i)
	}
	return &wg
}

func (j *CksumJob) startCksumDBWriter(dbWriter *CksumDBWriter, resultCh <-chan model.FileResult) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		dbWriter.Run(resultCh)
	}()
	return &wg
}

func (j *CksumJob) finalize(walker *copy.Walker, dbWriter *CksumDBWriter, walkErr error) (int, error) {
	pass, mismatch, failed, total := dbWriter.Stats()

	exitCode := 0
	var runErr error

	if walkErr != nil {
		exitCode = 2
		runErr = fmt.Errorf("walk failed: %w (checked %d/%d)", walkErr, pass, total)
	} else if mismatch > 0 || failed > 0 {
		exitCode = 2
		runErr = fmt.Errorf("%d mismatch, %d error of %d files", mismatch, failed, total)
	}

	// Emit final progress_summary
	if j.fileLog != nil {
		dbWriter.EmitFinalSummary(exitCode)
	}

	// Generate internal report (still used for report file, but no longer emits cksum_complete)
	if j.taskID != "" {
		_, _ = GenerateCksumReport(j.taskID, j.store, pass, mismatch, failed, exitCode)
	}

	return exitCode, runErr
}

func durationFromSec(sec int) time.Duration {
	if sec <= 0 {
		return 5 * time.Second
	}
	return time.Duration(sec) * time.Second
}
