package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/zp001/ncp/internal/cksum"
	"github.com/zp001/ncp/internal/config"
	"github.com/zp001/ncp/internal/copy"
	"github.com/zp001/ncp/internal/di"
	"github.com/zp001/ncp/internal/filelog"
	"github.com/zp001/ncp/internal/task"
	"github.com/zp001/ncp/pkg/interfaces/storage"
)

// runResume handles `ncp resume <taskID>` — determines jobType from last run.
func runResume(cmd *cobra.Command, args []string) error {
	resolveBoolFlag(cmd, "SkipByMtime", "skip-by-mtime", "no-skip-by-mtime")
	taskID := args[0]
	progressDir, _ := cmd.Flags().GetString("ProgressStorePath")

	cfg, err := loadResumeConfig(cmd)
	if err != nil {
		return err
	}

	// --dry-run: print effective config and exit
	if dryRun, _ := cmd.Flags().GetBool("dry-run"); dryRun {
		meta, err := task.ReadMeta(progressDir, taskID)
		if err != nil {
			return fmt.Errorf("read task meta: %w", err)
		}
		urls := strings.Split(meta.SrcBase, ",")
		urls = append(urls, meta.DstBase)
		usedProfiles, err := config.ExtractUsedProfiles(urls, cfg.Profiles)
		if err != nil {
			return err
		}
		fmt.Print(config.FormatConfig(cfg, usedProfiles))
		return nil
	}

	// Check concurrency
	meta, lock, err := task.CheckTaskNotRunning(progressDir, taskID)
	if err != nil {
		return err
	}
	if lock != nil {
		defer func() { _ = lock.Release() }()
	}

	jobType := task.LastJobType(meta)

	if err := filelog.SetupProgramLog(cfg.ProgramLogOutput, cfg.ProgramLogLevel); err != nil {
		return fmt.Errorf("setup program log: %w", err)
	}

	// Append a new run record with detected jobType
	if err := task.AppendRun(meta, jobType, progressDir); err != nil {
		return fmt.Errorf("append run: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	fl, err := setupFileLog(cfg, taskID, progressDir)
	if err != nil {
		return err
	}
	defer fl.Close()

	var exitCode int
	var runErr error

	switch jobType {
	case task.JobTypeCksum:
		exitCode, runErr = runResumeCksum(cfg, meta, fl, taskID, progressDir, ctx)
	default:
		exitCode, runErr = runResumeCopy(cfg, meta, fl, taskID, progressDir, ctx)
	}

	_ = task.UpdateRunFinished(meta, exitCode, progressDir)

	if runErr != nil {
		slog.Error("resume failed", "taskId", taskID, "error", runErr, "exitCode", exitCode)
	}
	os.Exit(exitCode)
	return nil
}

func runResumeCopy(cfg *config.Config, meta *task.Meta, fl *filelog.Emitter, taskID, progressDir string, ctx context.Context) (int, error) {
	srcPaths := strings.Split(meta.SrcBase, ",")

	dbDir := filepath.Join(progressDir, taskID, "db")
	store, err := di.NewProgressStore(dbDir)
	if err != nil {
		return 1, fmt.Errorf("open progress store: %w", err)
	}
	defer store.Close()

	srcMode := resolveRemoteSourceMode(store)

	configJSON := buildConfigJSON(cfg)
	var src storage.Source
	var dst storage.Destination
	var extraOpts []copy.JobOption
	if !meta.BasenamePrefix {
		src, dst, extraOpts, err = setupCopyDepsPlain(cfg, meta.SrcBase, meta.DstBase, srcMode, configJSON)
	} else {
		src, dst, extraOpts, err = setupCopyDepsMulti(cfg, srcPaths, meta.DstBase, srcMode, configJSON)
	}
	if err != nil {
		return 1, err
	}

	if err := src.BeginTask(ctx, taskID); err != nil {
		return 1, fmt.Errorf("begin task on source: %w", err)
	}
	if dst != nil {
		if err := dst.BeginTask(ctx, taskID); err != nil {
			return 1, fmt.Errorf("begin task on destination: %w", err)
		}
	}

	jobOpts := []copy.JobOption{
		copy.WithParallelism(cfg.CopyParallelism),
		copy.WithFileLog(fl, cfg.FileLogInterval),
		copy.WithIOSize(cfg.IOSize),
		copy.WithTaskID(taskID),
		copy.WithDstBase(meta.DstBase),
		copy.WithEnsureDirMtime(cfg.EnsureDirMtime),
		copy.WithCksumAlgo(resolveCksumAlgo(cfg)),
		copy.WithResume(true),
		copy.WithSkipByMtime(cfg.SkipByMtime),
		copy.WithChannelBuf(cfg.ChannelBuf),
	}
	jobOpts = append(jobOpts, extraOpts...)

	job := copy.NewJob(src, dst, store, jobOpts...)
	exitCode, err := job.Run(ctx)

	_ = src.EndTask(ctx, storage.TaskSummary{})
	if dst != nil {
		_ = dst.EndTask(ctx, storage.TaskSummary{})
	}

	_ = notifyRemoteTaskDone(meta.SrcBase, meta.DstBase, taskID, srcMode, configJSON)

	return exitCode, err
}

func runResumeCksum(cfg *config.Config, meta *task.Meta, fl *filelog.Emitter, taskID, progressDir string, ctx context.Context) (int, error) {
	dbDir := filepath.Join(progressDir, taskID, "db")
	store, err := di.NewProgressStore(dbDir)
	if err != nil {
		return 1, fmt.Errorf("open progress store: %w", err)
	}
	defer store.Close()

	srcMode := resolveRemoteSourceMode(store)

	// Select source setup matching DB relPath format:
	// - Original cksum task: relPaths have no prefix → use setupCksumDeps
	// - Original copy task: relPaths have basename prefix → src needs BasenamePrefixedSource wrapping
	configJSON := buildConfigJSON(cfg)
	var src storage.Source
	var dst storage.Source
	var extraOpts []cksum.CksumJobOption
	if meta.BasenamePrefix {
		src, dst, extraOpts, err = setupCksumDepsFromCopy(cfg, meta, srcMode, configJSON)
	} else {
		src, dst, extraOpts, err = setupCksumDeps(cfg, meta.SrcBase, meta.DstBase, srcMode, srcMode, configJSON)
	}
	if err != nil {
		return 1, err
	}

	for _, s := range []storage.Source{src, dst} {
		if err := s.BeginTask(ctx, taskID); err != nil {
			return 1, fmt.Errorf("begin task on source: %w", err)
		}
	}

	jobOpts := []cksum.CksumJobOption{
		cksum.WithCksumParallelism(cfg.CopyParallelism),
		cksum.WithCksumFileLog(fl, cfg.FileLogInterval),
		cksum.WithCksumIOSize(cfg.IOSize),
		cksum.WithCksumTaskID(taskID),
		cksum.WithCksumAlgo(resolveCksumAlgo(cfg)),
		cksum.WithCksumResume(true),
		cksum.WithCksumSkipByMtime(cfg.SkipByMtime),
		cksum.WithCksumChannelBuf(cfg.ChannelBuf),
	}
	jobOpts = append(jobOpts, extraOpts...)

	job := cksum.NewCksumJob(src, dst, store, jobOpts...)
	exitCode, err := job.Run(ctx)

	for _, s := range []storage.Source{src, dst} {
		_ = s.EndTask(ctx, storage.TaskSummary{})
	}

	// Notify remote serve to exit
	_ = notifyRemoteTaskDone(meta.SrcBase, meta.DstBase, taskID, srcMode, configJSON)

	return exitCode, err
}

// setupCksumDepsFromCopy creates cksum deps when resuming from a copy-origin task.
// The DB has basename-prefixed relPaths, so src needs BasenamePrefixedSource wrapping.
func setupCksumDepsFromCopy(cfg *config.Config, meta *task.Meta, srcMode uint8, configJSON string) (storage.Source, storage.Source, []cksum.CksumJobOption, error) {
	var extraOpts []cksum.CksumJobOption

	srcPaths := strings.Split(meta.SrcBase, ",")
	copySrc, _, copyExtra, err := setupCopyDepsMulti(cfg, srcPaths, meta.DstBase, srcMode, configJSON)
	if err != nil {
		return nil, nil, nil, err
	}
	// Convert copy.JobOption src/dst factories to cksum.CksumJobOption
	for _, opt := range copyExtra {
		_ = opt
	}

	src := copySrc

	// dst is a Source (for reading), not a Destination
	dst, err := di.NewSourceWithRemoteMode(meta.DstBase, cfg.Profiles, srcMode, configJSON)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create destination source: %w", err)
	}
	du, _ := di.ParsePath(meta.DstBase)
	if du.Scheme == "ncp" {
		dstFactory := func(id int) (storage.Source, error) {
			return di.NewSourceWithRemoteMode(meta.DstBase, cfg.Profiles, srcMode, configJSON)
		}
		extraOpts = append(extraOpts, cksum.WithCksumDstFactory(dstFactory))
	}
	// Src factory for remote sources
	for _, sp := range srcPaths {
		su, _ := di.ParsePath(sp)
		if su.Scheme == "ncp" {
			srcFactory := func(id int) (storage.Source, error) {
				return di.NewSourceWithRemoteMode(sp, cfg.Profiles, srcMode, configJSON)
			}
			extraOpts = append(extraOpts, cksum.WithCksumSrcFactory(srcFactory))
			break
		}
	}

	return src, dst, extraOpts, nil
}
