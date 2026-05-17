package main

import (
	"context"
	"encoding/json"
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
	"github.com/zp001/ncp/pkg/model"
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

	// Resolve basename prefix early (needed for meta.json)
	needsPrefix, err := resolveBasenamePrefix(dstPath, srcPaths, cfg.Profiles)
	if err != nil {
		return err
	}

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
	meta.BasenamePrefix = needsPrefix
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
	configJSON := buildConfigJSON(cfg)
	src, dst, extraOpts, err := setupCopyDepsMulti(cfg, srcPaths, dstPath, srcMode, configJSON)
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
	_ = notifyRemoteTaskDone(strings.Join(srcPaths, ","), dstPath, taskID, srcMode, configJSON)

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
	configJSON := buildConfigJSON(cfg)
	if !meta.BasenamePrefix {
		src, dst, extraOpts, err = setupCopyDepsPlain(cfg, meta.SrcBase, meta.DstBase, srcMode, configJSON)
	} else {
		src, dst, extraOpts, err = setupCopyDepsMulti(cfg, srcPaths, meta.DstBase, srcMode, configJSON)
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
	_ = notifyRemoteTaskDone(meta.SrcBase, meta.DstBase, taskID, srcMode, configJSON)

	_ = task.UpdateRunFinished(meta, exitCode, progressDir)

	if err != nil {
		slog.Error("resume copy failed", "taskId", taskID, "error", err, "exitCode", exitCode)
	}
	os.Exit(exitCode)
	return nil
}

// setupCopyDepsMulti creates source/destination deps for copy.
// Path semantics align with cp:
//   - Single source + dst doesn't exist: copy AS dst (no basename prefix)
//   - Single source + dst exists as directory: copy INTO dst (basename prefix)
//   - Multiple sources + dst exists as directory: copy INTO dst (basename prefix)
//   - Multiple sources + dst doesn't exist: error
//
// srcMode is the protocol mode for remote sources (ModeSource or ModeSourceNoWalker).
func setupCopyDepsMulti(cfg *config.Config, srcPaths []string, dstPath string, srcMode uint8, configJSON string) (storage.Source, storage.Destination, []copy.JobOption, error) {
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

	// Resolve whether basename prefix is needed (cp semantics)
	needsPrefix, err := resolveBasenamePrefix(dstPath, srcPaths, cfg.Profiles)
	if err != nil {
		return nil, nil, nil, err
	}

	sources := make([]storage.Source, len(srcPaths))
	basenames := make([]string, len(srcPaths))
	for i, sp := range srcPaths {
		sources[i], err = di.NewSourceWithRemoteMode(sp, cfg.Profiles, srcMode, configJSON)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("create source %s: %w", sp, err)
		}
		basenames[i] = di.SourceBasename(sources[i], sp)
	}

	if needsPrefix {
		src, err = di.NewBasenamePrefixedSource(sources, basenames)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("create basename-prefixed source: %w", err)
		}
	} else {
		// Single source, dst doesn't exist: use source directly (copy AS dst)
		src = sources[0]
	}

	var extraOpts []copy.JobOption
	var dst storage.Destination

	dst, err = createDestination(cfg, dstPath, configJSON)
	if err != nil {
		return nil, nil, nil, err
	}
	if dst == nil {
		// Factory-based destination (ncp/oss/cos/obs) — add factory option
		dst, extraOpts, err = createDstFactory(cfg, dstPath, configJSON)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	// Set srcFactory for remote sources.
	for _, sp := range srcPaths {
		su, _ := di.ParsePath(sp)
		if su.Scheme == "ncp" {
			srcFactory := makeRemoteSrcFactory(sp, cfg, srcMode, configJSON, needsPrefix)
			extraOpts = append(extraOpts, copy.WithSrcFactory(srcFactory))
			break
		}
	}

	return src, dst, extraOpts, nil
}

// setupCopyDepsPlain creates source/destination deps for copy without
// BasenamePrefixedSource wrapping. Used when resuming a copy from a cksum
// task whose DB relPaths do not include the basename prefix.
func setupCopyDepsPlain(cfg *config.Config, srcPath, dstPath string, srcMode uint8, configJSON string) (storage.Source, storage.Destination, []copy.JobOption, error) {
	src, err := di.NewSourceWithRemoteMode(srcPath, cfg.Profiles, srcMode, configJSON)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create source %s: %w", srcPath, err)
	}

	var extraOpts []copy.JobOption
	var dst storage.Destination

	dst, err = createDestination(cfg, dstPath, configJSON)
	if err != nil {
		return nil, nil, nil, err
	}
	if dst == nil {
		dst, extraOpts, err = createDstFactory(cfg, dstPath, configJSON)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	// Set srcFactory for remote sources
	su, _ := di.ParsePath(srcPath)
	if su.Scheme == "ncp" {
		srcFactory := func(id int) (storage.Source, error) {
			return di.NewSourceWithRemoteMode(srcPath, cfg.Profiles, srcMode, configJSON)
		}
		extraOpts = append(extraOpts, copy.WithSrcFactory(srcFactory))
	}

	return src, dst, extraOpts, nil
}

// resolveBasenamePrefix decides whether sources need basename prefix, aligned with cp semantics.
// Returns (needsPrefix bool, error).
func resolveBasenamePrefix(dstPath string, srcPaths []string, profiles map[string]model.Profile) (bool, error) {
	exists, isDir, err := dstExistsAsDir(dstPath, profiles)
	if err != nil {
		return false, fmt.Errorf("check destination: %w", err)
	}

	if len(srcPaths) > 1 {
		if !exists || !isDir {
			return false, fmt.Errorf("destination %q is not a directory (multi-source requires existing directory)", dstPath)
		}
		return true, nil
	}

	return exists && isDir, nil
}

// dstExistsAsDir checks whether dst exists and is a directory, for all backend types.
func dstExistsAsDir(dstPath string, profiles map[string]model.Profile) (exists bool, isDir bool, err error) {
	u, _ := di.ParsePath(dstPath)
	switch u.Scheme {
	case "", "file":
		info, statErr := os.Stat(u.Path)
		if os.IsNotExist(statErr) {
			return false, false, nil
		}
		if statErr != nil {
			return false, false, statErr
		}
		return true, info.IsDir(), nil
	case "ncp":
		// Remote destination: cannot check without connecting.
		// For now, assume doesn't exist — will be verified on actual connection.
		return false, false, nil
	case "oss", "cos", "obs":
		// Create a lightweight destination to check ExistsDir
		dst, dstErr := di.NewDestination(dstPath, di.DestConfig{}, profiles)
		if dstErr != nil {
			return false, false, nil // Can't create dest → assume doesn't exist
		}
		ctx := context.Background()
		isDir, checkErr := dst.ExistsDir(ctx)
		if checkErr != nil {
			return false, false, nil // Can't check → assume doesn't exist
		}
		return isDir, isDir, nil
	default:
		return false, false, fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}
}

// createDestination creates a local Destination, or returns (nil, nil) for factory-based backends.
func createDestination(cfg *config.Config, dstPath, configJSON string) (storage.Destination, error) {
	u, _ := di.ParsePath(dstPath)
	switch u.Scheme {
	case "", "file":
		dstCfg := di.DestConfig{
			DirectIO:    cfg.DirectIO,
			SyncWrites:  cfg.SyncWrites,
			IOSize:      cfg.IOSize,
			IOSizeTiers: cfg.IOSizeTiers,
		}
		return di.NewDestination(dstPath, dstCfg, cfg.Profiles)
	default:
		return nil, nil // factory-based
	}
}

// createDstFactory creates factory-based destination options for ncp/oss/cos/obs.
func createDstFactory(cfg *config.Config, dstPath, configJSON string) (storage.Destination, []copy.JobOption, error) {
	u, _ := di.ParsePath(dstPath)
	switch u.Scheme {
	case "ncp":
		dstFactory := func(id int) (storage.Destination, error) {
			return di.NewRemoteDestination(u.Host, u.Path, configJSON)
		}
		return nil, []copy.JobOption{copy.WithDstFactory(dstFactory)}, nil
	case "oss", "cos", "obs":
		if _, vErr := di.NewDestination(dstPath, di.DestConfig{}, cfg.Profiles); vErr != nil {
			return nil, nil, fmt.Errorf("create destination: %w", vErr)
		}
		dstFactory := func(id int) (storage.Destination, error) {
			return di.NewDestination(dstPath, di.DestConfig{}, cfg.Profiles)
		}
		return nil, []copy.JobOption{copy.WithDstFactory(dstFactory)}, nil
	default:
		dstCfg := di.DestConfig{
			DirectIO:    cfg.DirectIO,
			SyncWrites:  cfg.SyncWrites,
			IOSize:      cfg.IOSize,
			IOSizeTiers: cfg.IOSizeTiers,
		}
		dst, err := di.NewDestination(dstPath, dstCfg, cfg.Profiles)
		if err != nil {
			return nil, nil, err
		}
		return dst, nil, nil
	}
}

// makeRemoteSrcFactory creates a source factory for remote ncp sources.
func makeRemoteSrcFactory(srcPath string, cfg *config.Config, srcMode uint8, configJSON string, needsPrefix bool) func(int) (storage.Source, error) {
	return func(id int) (storage.Source, error) {
		rawSrc, err := di.NewSourceWithRemoteMode(srcPath, cfg.Profiles, srcMode, configJSON)
		if err != nil {
			return nil, err
		}
		if needsPrefix {
			basename := di.SourceBasename(rawSrc, srcPath)
			return di.NewBasenamePrefixedSource([]storage.Source{rawSrc}, []string{basename})
		}
		return rawSrc, nil
	}
}

// buildConfigJSON serializes ServerConfig from the client config for transport in InitMsg.
func buildConfigJSON(cfg *config.Config) string {
	serverCfg := model.ServerConfig{
		ProgramLogLevel:   cfg.ProgramLogLevel,
		ProgramLogOutput:  cfg.ProgramLogOutput,
		FileLogEnabled:    cfg.FileLogEnabled,
		FileLogOutput:     cfg.FileLogOutput,
		FileLogInterval:   cfg.FileLogInterval,
		ProgressStorePath: cfg.ProgressStorePath,
		CksumAlgorithm:    cfg.CksumAlgorithm,
	}
	data, err := json.Marshal(serverCfg)
	if err != nil {
		return "{}"
	}
	return string(data)
}
