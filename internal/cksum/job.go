package cksum

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/zp001/ncp/internal/copy"
	"github.com/zp001/ncp/internal/filelog"
	"github.com/zp001/ncp/pkg/interfaces/progress"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// CksumJob orchestrates Walker, CksumWorkers, and CksumDBWriter for a checksum task.
type CksumJob struct {
	src         storage.Source
	dst         storage.Source
	store       progress.ProgressStore
	taskID      string
	parallelism int
	fileLog     copy.FileLogger
	logInterval int
	ioSize      int
	cksumAlgo   model.CksumAlgorithm
	discoverBuf int
	resultBuf   int
	resume      bool
}

// NewCksumJob creates a cksum job.
func NewCksumJob(src, dst storage.Source, store progress.ProgressStore, opts ...CksumJobOption) *CksumJob {
	j := &CksumJob{
		src:         src,
		dst:         dst,
		store:       store,
		parallelism: 1,
		cksumAlgo:   model.DefaultCksumAlgorithm,
		discoverBuf: 100000,
		resultBuf:   100000,
	}
	for _, o := range opts {
		o(j)
	}
	return j
}

// CksumJobOption configures a CksumJob.
type CksumJobOption func(*CksumJob)

func WithCksumParallelism(n int) CksumJobOption              { return func(j *CksumJob) { j.parallelism = n } }
func WithCksumFileLog(fl copy.FileLogger, sec int) CksumJobOption {
	return func(j *CksumJob) { j.fileLog = fl; j.logInterval = sec }
}
func WithCksumIOSize(size int) CksumJobOption                { return func(j *CksumJob) { j.ioSize = size } }
func WithCksumTaskID(id string) CksumJobOption               { return func(j *CksumJob) { j.taskID = id } }
func WithCksumResume(v bool) CksumJobOption                  { return func(j *CksumJob) { j.resume = v } }
func WithCksumAlgo(algo model.CksumAlgorithm) CksumJobOption { return func(j *CksumJob) { j.cksumAlgo = algo } }

// Run executes the checksum job and blocks until completion.
// Returns exit code: 0=all pass, 2=mismatch or error.
func (j *CksumJob) Run(ctx context.Context) (int, error) {
	cksumCh := make(chan model.DiscoverItem, j.discoverBuf)
	resultCh := make(chan model.FileResult, j.resultBuf)

	walker := copy.NewWalker(j.src, j.store, j.fileLog, durationFromSec(j.logInterval))
	dbWriter := NewCksumDBWriter(j.store, walker, j.fileLog)

	// Start the pipeline
	workerWg := j.startCksumWorkers(cksumCh, resultCh)
	dbWg := j.startCksumDBWriter(dbWriter, resultCh)

	// Close resultCh after all workers exit
	go func() {
		workerWg.Wait()
		close(resultCh)
	}()

	// Populate cksumCh
	var walkErr error
	if j.resume {
		walkErr = j.populateFromResume(ctx, walker, cksumCh)
	} else {
		walkErr = walker.Run(ctx, cksumCh)
	}

	// Wait for pipeline to drain
	dbWg.Wait()

	return j.finalize(walker, dbWriter, walkErr)
}

// populateFromResume handles cksum resume logic:
// - walk_complete exists: push files needing cksum from DB
// - walk_complete absent: destroy DB, reopen, run fresh walk
func (j *CksumJob) populateFromResume(ctx context.Context, walker *copy.Walker, cksumCh chan<- model.DiscoverItem) error {
	hasWalkComplete, err := j.store.HasWalkComplete()
	if err != nil {
		return fmt.Errorf("check walk_complete: %w", err)
	}

	if hasWalkComplete {
		walker.ResumeFromDBForCksum(cksumCh)
		return nil
	}

	// Walk was incomplete — destroy DB and start fresh
	if err := j.store.Destroy(); err != nil {
		return fmt.Errorf("destroy store for fresh start: %w", err)
	}
	if err := j.store.Reopen(); err != nil {
		return fmt.Errorf("reopen store: %w", err)
	}
	freshWalker := copy.NewWalker(j.src, j.store, j.fileLog, durationFromSec(j.logInterval))
	return freshWalker.Run(ctx, cksumCh)
}

func (j *CksumJob) startCksumWorkers(cksumCh <-chan model.DiscoverItem, resultCh chan<- model.FileResult) *sync.WaitGroup {
	var wg sync.WaitGroup
	for i := 0; i < j.parallelism; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w := NewCksumWorker(id, j.src, j.dst, j.fileLog, j.ioSize, j.cksumAlgo)
			w.Run(cksumCh, resultCh)
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

	// Generate and write completion report
	if j.taskID != "" {
		report, _ := GenerateCksumReport(j.taskID, j.store, pass, mismatch, failed, exitCode)
		if report != nil && j.fileLog != nil {
			j.fileLog.Emit(filelog.EventCksumComplete, report)
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
