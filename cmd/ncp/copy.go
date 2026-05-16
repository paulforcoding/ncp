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
	"github.com/zp001/ncp/internal/config"
	"github.com/zp001/ncp/internal/copy"
	"github.com/zp001/ncp/internal/di"
	"github.com/zp001/ncp/internal/filelog"
	"github.com/zp001/ncp/internal/task"
	"github.com/zp001/ncp/pkg/interfaces/storage"
)

// runCopy is the Composition Root for the copy command.
func runCopy(cmd *cobra.Command, args []string) error {
	resolveBoolFlag(cmd, "FileLogEnabled", "enable-FileLog", "disable-FileLog")
	resolveBoolFlag(cmd, "DirectIO", "enable-DirectIO", "disable-DirectIO")
	resolveBoolFlag(cmd, "SyncWrites", "enable-SyncWrites", "disable-SyncWrites")
	resolveBoolFlag(cmd, "EnsureDirMtime", "enable-EnsureDirMtime", "disable-EnsureDirMtime")
	resolveBoolFlag(cmd, "SkipByMtime", "skip-by-mtime", "no-skip-by-mtime")

	cfg, err := config.LoadFromViper(v)
	if err != nil {
		return err
	}

	// Reject if config files contain plain credentials with overly permissive modes.
	if err := config.CheckCredentialFilePermissions(cfg.Profiles); err != nil {
		return err
	}

	// --dry-run: print effective config and exit
	if dryRun, _ := cmd.Flags().GetBool("dry-run"); dryRun {
		return handleDryRun(cmd, cfg, args)
	}

	// --task flag: resume existing task
	taskID, _ := cmd.Flags().GetString("task")
	if taskID != "" {
		return runCopyResume(cmd, cfg, taskID)
	}

	// New copy: require src and dst
	if len(args) < 2 {
		return fmt.Errorf("copy requires <src> and <dst> arguments when not using --task")
	}

	srcPaths := args[:len(args)-1]
	dstPath := args[len(args)-1]

	taskID = task.GenerateTaskID()
	progressDir := cfg.ProgressStorePath

	// Setup ProgramLog
	if err := filelog.SetupProgramLog(cfg.ProgramLogOutput, cfg.ProgramLogLevel); err != nil {
		return fmt.Errorf("setup program log: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Write meta.json
	meta := task.NewMeta(taskID, strings.Join(srcPaths, ","), dstPath, os.Args[1:], task.JobTypeCopy)
	if err := task.WriteMetaTo(meta, progressDir); err != nil {
		return fmt.Errorf("write meta: %w", err)
	}

	// Setup FileLog
	fl, err := setupFileLog(cfg, taskID, progressDir)
	if err != nil {
		return err
	}
	defer fl.Close()

	// Validate cksum algorithm for destination
	if err := validateCksumAlgoForOSS(resolveCksumAlgo(cfg), dstPath); err != nil {
		return err
	}

	// Open progress store first to determine remote source mode
	dbDir := filepath.Join(progressDir, taskID, "db")
	store, err := di.NewProgressStore(dbDir)
	if err != nil {
		return fmt.Errorf("open progress store: %w", err)
	}
	defer store.Close()

	srcMode := resolveRemoteSourceMode(store)

	// Dependency injection
	src, dst, extraOpts, err := setupCopyDepsMulti(cfg, srcPaths, dstPath, srcMode)
	if err != nil {
		return err
	}

	// Source lifecycle
	if err := src.BeginTask(ctx, taskID); err != nil {
		return fmt.Errorf("begin task on source: %w", err)
	}

	// Destination lifecycle (non-factory)
	if dst != nil {
		if err := dst.BeginTask(ctx, taskID); err != nil {
			return fmt.Errorf("begin task on destination: %w", err)
		}
	}

	jobOpts := []copy.JobOption{
		copy.WithParallelism(cfg.CopyParallelism),
		copy.WithFileLog(fl, cfg.FileLogInterval),
		copy.WithIOSize(cfg.IOSize),
		copy.WithTaskID(taskID),
		copy.WithDstBase(dstPath),
		copy.WithEnsureDirMtime(cfg.EnsureDirMtime),
		copy.WithCksumAlgo(resolveCksumAlgo(cfg)),
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

	// Notify remote serve to exit
	_ = notifyRemoteTaskDone(strings.Join(srcPaths, ","), dstPath, taskID, srcMode)

	// Update meta.json
	_ = task.UpdateRunFinished(meta, exitCode, progressDir)

	if err != nil {
		slog.Error("copy job failed", "taskId", taskID, "error", err, "exitCode", exitCode)
	}
	os.Exit(exitCode)
	return nil
}

// runCopyResume handles `ncp copy --task <taskID>` — resume an existing copy task.
func runCopyResume(cmd *cobra.Command, cfg *config.Config, taskID string) error {
	progressDir := cfg.ProgressStorePath

	// Check concurrency
	meta, lock, err := task.CheckTaskNotRunning(progressDir, taskID)
	if err != nil {
		return err
	}
	if lock != nil {
		defer func() { _ = lock.Release() }()
	}

	if err := filelog.SetupProgramLog(cfg.ProgramLogOutput, cfg.ProgramLogLevel); err != nil {
		return fmt.Errorf("setup program log: %w", err)
	}

	// Append a new run record
	if err := task.AppendRun(meta, task.JobTypeCopy, progressDir); err != nil {
		return fmt.Errorf("append run: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	fl, err := setupFileLog(cfg, taskID, progressDir)
	if err != nil {
		return err
	}
	defer fl.Close()

	srcPaths := strings.Split(meta.SrcBase, ",")

	dbDir := filepath.Join(progressDir, taskID, "db")
	store, err := di.NewProgressStore(dbDir)
	if err != nil {
		return fmt.Errorf("open progress store: %w", err)
	}
	defer store.Close()

	srcMode := resolveRemoteSourceMode(store)

	// Select source setup matching DB relPath format:
	// - Original copy task: relPaths have basename prefix → use setupCopyDepsMulti
	// - Original cksum task: relPaths have no prefix → use setupCopyDepsPlain
	var src storage.Source
	var dst storage.Destination
	var extraOpts []copy.JobOption
	if firstRunJobType(meta) == task.JobTypeCksum {
		src, dst, extraOpts, err = setupCopyDepsPlain(cfg, meta.SrcBase, meta.DstBase, srcMode)
	} else {
		src, dst, extraOpts, err = setupCopyDepsMulti(cfg, srcPaths, meta.DstBase, srcMode)
	}
	if err != nil {
		return err
	}

	if err := src.BeginTask(ctx, taskID); err != nil {
		return fmt.Errorf("begin task on source: %w", err)
	}
	if dst != nil {
		if err := dst.BeginTask(ctx, taskID); err != nil {
			return fmt.Errorf("begin task on destination: %w", err)
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

	// Notify remote serve to exit
	_ = notifyRemoteTaskDone(meta.SrcBase, meta.DstBase, taskID, srcMode)

	_ = task.UpdateRunFinished(meta, exitCode, progressDir)

	if err != nil {
		slog.Error("resume copy failed", "taskId", taskID, "error", err, "exitCode", exitCode)
	}
	os.Exit(exitCode)
	return nil
}

// setupCopyDepsMulti creates source/destination deps for copy.
// All sources are wrapped in BasenamePrefixedSource so that every source
// (single or multiple) gets its basename as a subdirectory under dst.
// srcMode is the protocol mode for remote sources (ModeSource or ModeSourceNoWalker).
func setupCopyDepsMulti(cfg *config.Config, srcPaths []string, dstPath string, srcMode uint8) (storage.Source, storage.Destination, []copy.JobOption, error) {
	var src storage.Source
	var err error

	if len(srcPaths) > 1 {
		for _, sp := range srcPaths {
			u, _ := di.ParsePath(sp)
			if u.Scheme != "" && u.Scheme != "file" {
				return nil, nil, nil, fmt.Errorf("multi-source is only supported for local paths; %q has scheme %q", sp, u.Scheme)
			}
		}
	}

	sources := make([]storage.Source, len(srcPaths))
	basenames := make([]string, len(srcPaths))
	for i, sp := range srcPaths {
		sources[i], err = di.NewSourceWithRemoteMode(sp, cfg.Profiles, srcMode)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("create source %s: %w", sp, err)
		}
		basenames[i] = di.SourceBasename(sources[i], sp)
	}
	src, err = di.NewBasenamePrefixedSource(sources, basenames)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create basename-prefixed source: %w", err)
	}

	var extraOpts []copy.JobOption
	var dst storage.Destination

	u, _ := di.ParsePath(dstPath)
	switch u.Scheme {
	case "ncp":
		dstFactory := func(id int) (storage.Destination, error) {
			return di.NewRemoteDestination(u.Host, u.Path)
		}
		extraOpts = append(extraOpts, copy.WithDstFactory(dstFactory))
	case "oss", "cos", "obs":
		if _, vErr := di.NewDestination(dstPath, di.DestConfig{}, cfg.Profiles); vErr != nil {
			return nil, nil, nil, fmt.Errorf("create destination: %w", vErr)
		}
		dstFactory := func(id int) (storage.Destination, error) {
			return di.NewDestination(dstPath, di.DestConfig{}, cfg.Profiles)
		}
		extraOpts = append(extraOpts, copy.WithDstFactory(dstFactory))
	default:
		dstCfg := di.DestConfig{
			DirectIO:    cfg.DirectIO,
			SyncWrites:  cfg.SyncWrites,
			IOSize:      cfg.IOSize,
			IOSizeTiers: cfg.IOSizeTiers,
		}
		dst, err = di.NewDestination(dstPath, dstCfg, cfg.Profiles)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("create destination: %w", err)
		}
	}

	// Set srcFactory for remote sources.
	// Must wrap in BasenamePrefixedSource so that the replicator-side
	// source strips the basename prefix from walker-produced relPaths,
	// matching the walker-side j.src wrapping.
	for _, sp := range srcPaths {
		su, _ := di.ParsePath(sp)
		if su.Scheme == "ncp" {
			srcFactory := func(id int) (storage.Source, error) {
				rawSrc, err := di.NewSourceWithRemoteMode(sp, cfg.Profiles, srcMode)
				if err != nil {
					return nil, err
				}
				basename := di.SourceBasename(rawSrc, sp)
				return di.NewBasenamePrefixedSource([]storage.Source{rawSrc}, []string{basename})
			}
			extraOpts = append(extraOpts, copy.WithSrcFactory(srcFactory))
			break
		}
	}

	return src, dst, extraOpts, nil
}

// setupCopyDepsPlain creates source/destination deps for copy without
// BasenamePrefixedSource wrapping. Used when resuming a copy from a cksum
// task whose DB relPaths do not include the basename prefix.
func setupCopyDepsPlain(cfg *config.Config, srcPath, dstPath string, srcMode uint8) (storage.Source, storage.Destination, []copy.JobOption, error) {
	src, err := di.NewSourceWithRemoteMode(srcPath, cfg.Profiles, srcMode)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create source %s: %w", srcPath, err)
	}

	var extraOpts []copy.JobOption
	var dst storage.Destination

	u, _ := di.ParsePath(dstPath)
	switch u.Scheme {
	case "ncp":
		dstFactory := func(id int) (storage.Destination, error) {
			return di.NewRemoteDestination(u.Host, u.Path)
		}
		extraOpts = append(extraOpts, copy.WithDstFactory(dstFactory))
	case "oss", "cos", "obs":
		if _, vErr := di.NewDestination(dstPath, di.DestConfig{}, cfg.Profiles); vErr != nil {
			return nil, nil, nil, fmt.Errorf("create destination: %w", vErr)
		}
		dstFactory := func(id int) (storage.Destination, error) {
			return di.NewDestination(dstPath, di.DestConfig{}, cfg.Profiles)
		}
		extraOpts = append(extraOpts, copy.WithDstFactory(dstFactory))
	default:
		dstCfg := di.DestConfig{
			DirectIO:    cfg.DirectIO,
			SyncWrites:  cfg.SyncWrites,
			IOSize:      cfg.IOSize,
			IOSizeTiers: cfg.IOSizeTiers,
		}
		dst, err = di.NewDestination(dstPath, dstCfg, cfg.Profiles)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("create destination: %w", err)
		}
	}

	// Set srcFactory for remote sources
	su, _ := di.ParsePath(srcPath)
	if su.Scheme == "ncp" {
		srcFactory := func(id int) (storage.Source, error) {
			return di.NewSourceWithRemoteMode(srcPath, cfg.Profiles, srcMode)
		}
		extraOpts = append(extraOpts, copy.WithSrcFactory(srcFactory))
	}

	return src, dst, extraOpts, nil
}
