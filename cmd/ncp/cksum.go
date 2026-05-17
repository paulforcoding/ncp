package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/zp001/ncp/internal/cksum"
	"github.com/zp001/ncp/internal/config"
	"github.com/zp001/ncp/internal/di"
	"github.com/zp001/ncp/internal/filelog"
	"github.com/zp001/ncp/internal/task"
	"github.com/zp001/ncp/pkg/interfaces/storage"
)

// runCksum is the Composition Root for the cksum command.
func runCksum(cmd *cobra.Command, args []string) error {
	resolveBoolFlag(cmd, "FileLogEnabled", "enable-FileLog", "disable-FileLog")
	resolveBoolFlag(cmd, "SkipByMtime", "skip-by-mtime", "no-skip-by-mtime")

	// Bind cksum flags to Viper
	_ = v.BindPFlag("CopyParallelism", cmd.Flags().Lookup("CopyParallelism"))
	_ = v.BindPFlag("ProgramLogLevel", cmd.Flags().Lookup("ProgramLogLevel"))
	_ = v.BindPFlag("ProgramLogOutput", cmd.Flags().Lookup("ProgramLogOutput"))
	_ = v.BindPFlag("FileLogEnabled", cmd.Flags().Lookup("enable-FileLog"))
	_ = v.BindPFlag("FileLogOutput", cmd.Flags().Lookup("FileLogOutput"))
	_ = v.BindPFlag("FileLogInterval", cmd.Flags().Lookup("FileLogInterval"))
	_ = v.BindPFlag("ProgressStorePath", cmd.Flags().Lookup("ProgressStorePath"))
	_ = v.BindPFlag("CksumAlgorithm", cmd.Flags().Lookup("cksum-algorithm"))
	_ = v.BindPFlag("SkipByMtime", cmd.Flags().Lookup("skip-by-mtime"))

	cfg, err := config.LoadFromViper(v)
	if err != nil {
		return err
	}

	// --dry-run: print effective config and exit
	if dryRun, _ := cmd.Flags().GetBool("dry-run"); dryRun {
		if len(args) < 2 {
			return fmt.Errorf("cksum --dry-run requires <src> and <dst> arguments")
		}
		usedProfiles, err := config.ExtractUsedProfiles(args[:2], cfg.Profiles)
		if err != nil {
			return err
		}
		fmt.Print(config.FormatConfig(cfg, usedProfiles))
		return nil
	}

	// --task flag: resume existing cksum task
	taskID, _ := cmd.Flags().GetString("task")
	if taskID != "" {
		return runCksumResume(cmd, cfg, taskID)
	}

	// New cksum: require src and dst
	if len(args) < 2 {
		return fmt.Errorf("cksum requires <src> and <dst> arguments when not using --task")
	}

	srcPath := args[0]
	dstPath := args[1]

	// Validate cksum algorithm for OSS
	if err := validateCksumAlgoForOSS(resolveCksumAlgo(cfg), srcPath, dstPath); err != nil {
		return err
	}

	taskID = task.GenerateTaskID()
	progressDir := cfg.ProgressStorePath

	if err := filelog.SetupProgramLog(cfg.ProgramLogOutput, cfg.ProgramLogLevel); err != nil {
		return fmt.Errorf("setup program log: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Write meta.json
	meta := task.NewMeta(taskID, srcPath, dstPath, os.Args[1:], task.JobTypeCksum)
	if err := task.WriteMetaTo(meta, progressDir); err != nil {
		return fmt.Errorf("write meta: %w", err)
	}

	// Setup FileLog
	fl, err := setupFileLog(cfg, taskID, progressDir)
	if err != nil {
		return err
	}
	defer fl.Close()

	// Open progress store first to determine remote source mode
	dbDir := filepath.Join(progressDir, taskID, "db")
	store, err := di.NewProgressStore(dbDir)
	if err != nil {
		return fmt.Errorf("open progress store: %w", err)
	}
	defer store.Close()

	srcMode := resolveRemoteSourceMode(store)

	// Dependency injection
	configJSON := buildConfigJSON(cfg)
	src, dst, extraOpts, err := setupCksumDeps(cfg, srcPath, dstPath, srcMode, srcMode, configJSON)
	if err != nil {
		return err
	}

	for _, s := range []storage.Source{src, dst} {
		if err := s.BeginTask(ctx, taskID); err != nil {
			return fmt.Errorf("begin task on source: %w", err)
		}
	}

	jobOpts := []cksum.CksumJobOption{
		cksum.WithCksumParallelism(cfg.CopyParallelism),
		cksum.WithCksumFileLog(fl, cfg.FileLogInterval),
		cksum.WithCksumIOSize(cfg.IOSize),
		cksum.WithCksumTaskID(taskID),
		cksum.WithCksumAlgo(resolveCksumAlgo(cfg)),
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
	_ = notifyRemoteTaskDone(srcPath, dstPath, taskID, srcMode, configJSON)

	_ = task.UpdateRunFinished(meta, exitCode, progressDir)

	if err != nil {
		slog.Error("cksum job failed", "taskId", taskID, "error", err, "exitCode", exitCode)
	}
	os.Exit(exitCode)
	return nil
}

// runCksumResume handles `ncp cksum --task <taskID>`.
func runCksumResume(cmd *cobra.Command, cfg *config.Config, taskID string) error {
	progressDir := cfg.ProgressStorePath

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

	if err := task.AppendRun(meta, task.JobTypeCksum, progressDir); err != nil {
		return fmt.Errorf("append run: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	fl, err := setupFileLog(cfg, taskID, progressDir)
	if err != nil {
		return err
	}
	defer fl.Close()

	dbDir := filepath.Join(progressDir, taskID, "db")
	store, err := di.NewProgressStore(dbDir)
	if err != nil {
		return fmt.Errorf("open progress store: %w", err)
	}
	defer store.Close()

	srcMode := resolveRemoteSourceMode(store)

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
		return err
	}

	for _, s := range []storage.Source{src, dst} {
		if err := s.BeginTask(ctx, taskID); err != nil {
			return fmt.Errorf("begin task on source: %w", err)
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

	_ = task.UpdateRunFinished(meta, exitCode, progressDir)

	if err != nil {
		slog.Error("resume cksum failed", "taskId", taskID, "error", err, "exitCode", exitCode)
	}
	os.Exit(exitCode)
	return nil
}

// setupCksumDeps creates src Source, dst Source, and cksum options.
// Both src and dst are Sources (readable) for checksum comparison.
// srcMode and dstMode are the protocol modes for remote sources.
func setupCksumDeps(cfg *config.Config, srcPath, dstPath string, srcMode, dstMode uint8, configJSON string) (storage.Source, storage.Source, []cksum.CksumJobOption, error) {
	var extraOpts []cksum.CksumJobOption

	src, err := di.NewSourceWithRemoteMode(srcPath, cfg.Profiles, srcMode, configJSON)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create source: %w", err)
	}

	su, _ := di.ParsePath(srcPath)
	if su.Scheme == "ncp" {
		srcFactory := func(id int) (storage.Source, error) {
			return di.NewSourceWithRemoteMode(srcPath, cfg.Profiles, srcMode, configJSON)
		}
		extraOpts = append(extraOpts, cksum.WithCksumSrcFactory(srcFactory))
	}

	dst, err := di.NewSourceWithRemoteMode(dstPath, cfg.Profiles, dstMode, configJSON)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create destination source: %w", err)
	}

	du, _ := di.ParsePath(dstPath)
	if du.Scheme == "ncp" {
		dstFactory := func(id int) (storage.Source, error) {
			return di.NewSourceWithRemoteMode(dstPath, cfg.Profiles, dstMode, configJSON)
		}
		extraOpts = append(extraOpts, cksum.WithCksumDstFactory(dstFactory))
	}

	return src, dst, extraOpts, nil
}
