package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/zp001/ncp/internal/cksum"
	"github.com/zp001/ncp/internal/config"
	"github.com/zp001/ncp/internal/copy"
	"github.com/zp001/ncp/internal/di"
	"github.com/zp001/ncp/internal/filelog"
	"github.com/zp001/ncp/internal/mkfiles"
	"github.com/zp001/ncp/internal/ncpserver"
	"github.com/zp001/ncp/internal/protocol"
	"github.com/zp001/ncp/internal/task"
	"github.com/zp001/ncp/pkg/interfaces/progress"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

var v *viper.Viper

func main() {
	v = viper.New()

	rootCmd := &cobra.Command{
		Use:           "ncp",
		Short:         "ncp — Agent-First file copy tool for massive-scale data migration",
		Long:          `ncp copies files to remote servers and cloud object storage with DB-backed progress tracking and Agent-First structured output.`,
		Version:       version,
		SilenceErrors: true,
		SilenceUsage:  true,
	}

	copyCmd := &cobra.Command{
		Use:   "copy <src>... <dst>",
		Short: "Copy files from source to destination",
		Long:  "Copy files from source to destination. Supports local→local, local→remote (ncp://), and local→cloud (oss://). Each source is placed under its basename as a subdirectory of dst. For example, 'ncp copy /data/dir /tmp/' creates /tmp/dir/... .\n\nObject storage URLs (oss://) require a <profile>@ prefix referring to a profile defined in ncp_config.json (key: Profiles). Example: oss://prod@bucket/path/.",
		Args: func(cmd *cobra.Command, args []string) error {
			taskID, _ := cmd.Flags().GetString("task")
			if taskID != "" {
				return nil
			}
			if len(args) < 2 {
				return fmt.Errorf("copy requires <src> and <dst> arguments when not using --task")
			}
			return nil
		},
		RunE: runCopy,
	}

	// Config flags (all fields overridable via CLI)
	copyCmd.Flags().Int("CopyParallelism", 1, "Number of parallel copy workers")
	copyCmd.Flags().String("ProgramLogLevel", "info", "Log level: trace/debug/info/warn/error/critical")
	copyCmd.Flags().String("ProgramLogOutput", "console", "ProgramLog output: console or file path")
	copyCmd.Flags().Bool("enable-FileLog", true, "Enable FileLog output")
	copyCmd.Flags().Bool("disable-FileLog", false, "Disable FileLog output")
	copyCmd.Flags().String("FileLogOutput", "console", "FileLog output: console or file path")
	copyCmd.Flags().Int("FileLogInterval", 5, "FileLog output interval (seconds)")
	copyCmd.Flags().Bool("enable-DirectIO", false, "Enable Direct IO (mutually exclusive with SyncWrites)")
	copyCmd.Flags().Bool("disable-DirectIO", false, "Disable Direct IO")
	copyCmd.Flags().Bool("enable-SyncWrites", true, "Enable fsync on write (mutually exclusive with DirectIO)")
	copyCmd.Flags().Bool("disable-SyncWrites", false, "Disable fsync on write")
	copyCmd.Flags().Int("IOSize", 0, "IO size in bytes (0 = use tiered default)")
	copyCmd.Flags().Bool("enable-EnsureDirMtime", true, "Restore directory mtime after copy")
	copyCmd.Flags().Bool("disable-EnsureDirMtime", false, "Do not restore directory mtime")
	copyCmd.Flags().String("ProgressStorePath", "/tmp/ncp_progress_store", "Progress storage directory")
	copyCmd.Flags().Bool("dry-run", false, "Preview effective config without executing")
	copyCmd.Flags().String("task", "", "Resume an existing task by taskID")
	copyCmd.Flags().String("cksum-algorithm", "md5", "Checksum algorithm: md5 or xxh64")
	copyCmd.Flags().Bool("skip-by-mtime", true, "Skip files with matching mtime+size (and ETag for OSS)")
	copyCmd.Flags().Bool("no-skip-by-mtime", false, "Disable skip-by-mtime, copy/verify all files")
	copyCmd.Flags().Int("ChannelBuf", 100000, "Channel buffer size for discover/result queues")

	// Bind all flags to Viper
	_ = v.BindPFlag("CopyParallelism", copyCmd.Flags().Lookup("CopyParallelism"))
	_ = v.BindPFlag("ProgramLogLevel", copyCmd.Flags().Lookup("ProgramLogLevel"))
	_ = v.BindPFlag("ProgramLogOutput", copyCmd.Flags().Lookup("ProgramLogOutput"))
	_ = v.BindPFlag("FileLogEnabled", copyCmd.Flags().Lookup("enable-FileLog"))
	_ = v.BindPFlag("FileLogOutput", copyCmd.Flags().Lookup("FileLogOutput"))
	_ = v.BindPFlag("FileLogInterval", copyCmd.Flags().Lookup("FileLogInterval"))
	_ = v.BindPFlag("DirectIO", copyCmd.Flags().Lookup("enable-DirectIO"))
	_ = v.BindPFlag("SyncWrites", copyCmd.Flags().Lookup("enable-SyncWrites"))
	_ = v.BindPFlag("IOSize", copyCmd.Flags().Lookup("IOSize"))
	_ = v.BindPFlag("EnsureDirMtime", copyCmd.Flags().Lookup("enable-EnsureDirMtime"))
	_ = v.BindPFlag("ProgressStorePath", copyCmd.Flags().Lookup("ProgressStorePath"))
	_ = v.BindPFlag("CksumAlgorithm", copyCmd.Flags().Lookup("cksum-algorithm"))
	_ = v.BindPFlag("SkipByMtime", copyCmd.Flags().Lookup("skip-by-mtime"))
	_ = v.BindPFlag("ChannelBuf", copyCmd.Flags().Lookup("ChannelBuf"))

	// resume command
	resumeCmd := &cobra.Command{
		Use:   "resume <taskID>",
		Short: "Resume an interrupted task from its last checkpoint",
		Long:  "Resume an interrupted copy or checksum task. Reads the last run's jobType from meta.json and continues accordingly.",
		Args:  cobra.ExactArgs(1),
		RunE:  runResume,
	}
	resumeCmd.Flags().Int("CopyParallelism", 1, "Number of parallel copy workers")
	resumeCmd.Flags().String("ProgramLogLevel", "info", "Log level")
	resumeCmd.Flags().String("ProgramLogOutput", "console", "ProgramLog output")
	resumeCmd.Flags().Bool("enable-FileLog", true, "Enable FileLog output")
	resumeCmd.Flags().Bool("disable-FileLog", false, "Disable FileLog output")
	resumeCmd.Flags().String("FileLogOutput", "console", "FileLog output")
	resumeCmd.Flags().Int("FileLogInterval", 5, "FileLog output interval (seconds)")
	resumeCmd.Flags().String("ProgressStorePath", "/tmp/ncp_progress_store", "Progress storage directory")
	resumeCmd.Flags().Bool("dry-run", false, "Preview effective config without executing")
	resumeCmd.Flags().Bool("skip-by-mtime", true, "Skip files with matching mtime+size (and ETag for OSS)")
	resumeCmd.Flags().Bool("no-skip-by-mtime", false, "Disable skip-by-mtime, copy/verify all files")

	// task command group
	taskCmd := &cobra.Command{
		Use:   "task",
		Short: "Manage copy/checksum tasks",
		Long:  "List, show, and delete ncp tasks.",
	}

	taskListCmd := &cobra.Command{
		Use:   "list",
		Short: "List all tasks",
		Long:  "List all tasks with their status. Output is JSON Lines.",
		Args:  cobra.NoArgs,
		RunE:  runTaskList,
	}
	taskListCmd.Flags().String("ProgressStorePath", "/tmp/ncp_progress_store", "Progress storage directory")

	taskShowCmd := &cobra.Command{
		Use:   "show <taskID>",
		Short: "Show task details",
		Long:  "Show detailed information about a task. Output is JSON.",
		Args:  cobra.ExactArgs(1),
		RunE:  runTaskShow,
	}
	taskShowCmd.Flags().String("ProgressStorePath", "/tmp/ncp_progress_store", "Progress storage directory")

	taskDeleteCmd := &cobra.Command{
		Use:   "delete <taskID>",
		Short: "Delete a task and its progress data",
		Long:  "Delete a task's progress data. Refuses if the task is currently running.",
		Args:  cobra.ExactArgs(1),
		RunE:  runTaskDelete,
	}
	taskDeleteCmd.Flags().String("ProgressStorePath", "/tmp/ncp_progress_store", "Progress storage directory")

	taskCmd.AddCommand(taskListCmd, taskShowCmd, taskDeleteCmd)

	// serve command
	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Start ncp server to receive file pushes",
		Long:  "Start ncp server to receive file pushes over the ncp protocol.\n\nWARNING: MVP uses cleartext — only use on internal networks or VPN.",
		Args:  cobra.NoArgs,
		RunE:  runServe,
	}
	serveCmd.Flags().String("listen", ":9900", "Listen address (host:port)")
	serveCmd.Flags().String("serve-temp-dir", "/tmp/ncpserve", "Temporary directory for walker DB")

	// cksum command
	cksumCmd := &cobra.Command{
		Use:   "cksum <src> <dst>",
		Short: "Verify data consistency between source and destination",
		Long:  "Verify source and destination data consistency by comparing checksums. Both paths are explicit base directories; no automatic basename joining is performed. Supports any combination of local, OSS, and ncp:// (remote) endpoints.\n\nObject storage URLs (oss://) require a <profile>@ prefix referring to a profile defined in ncp_config.json (key: Profiles). Example: oss://prod@bucket/path/.",
		Args:  cobra.MaximumNArgs(2),
		RunE:  runCksum,
	}
	cksumCmd.Flags().Int("CopyParallelism", 1, "Number of parallel checksum workers")
	cksumCmd.Flags().String("ProgramLogLevel", "info", "Log level")
	cksumCmd.Flags().String("ProgramLogOutput", "console", "ProgramLog output")
	cksumCmd.Flags().Bool("enable-FileLog", true, "Enable FileLog output")
	cksumCmd.Flags().Bool("disable-FileLog", false, "Disable FileLog output")
	cksumCmd.Flags().String("FileLogOutput", "console", "FileLog output")
	cksumCmd.Flags().Int("FileLogInterval", 5, "FileLog output interval (seconds)")
	cksumCmd.Flags().String("ProgressStorePath", "/tmp/ncp_progress_store", "Progress storage directory")
	cksumCmd.Flags().String("task", "", "Resume an existing task by taskID")
	cksumCmd.Flags().String("cksum-algorithm", "md5", "Checksum algorithm: md5 or xxh64")
	cksumCmd.Flags().Bool("skip-by-mtime", true, "Skip files with matching mtime+size (and ETag for OSS)")
	cksumCmd.Flags().Bool("dry-run", false, "Preview effective config without executing")
	cksumCmd.Flags().Bool("no-skip-by-mtime", false, "Disable skip-by-mtime, verify all files")

	// mkfiles command
	mkfilesCmd := &cobra.Command{
		Use:   "mkfiles <dir>",
		Short: "Generate random test files in a directory",
		Long:  "Generate random test files with random names and content in the specified directory. Useful for testing ncp copy/cksum. Directories are nested up to --maxdirdepth levels, with 2^(depth-1) directories at each level. Files are randomly distributed across all directories.",
		Args:  cobra.ExactArgs(1),
		RunE:  runMkfiles,
	}
	mkfilesCmd.Flags().Int("num", 100, "Number of files to generate")
	mkfilesCmd.Flags().Int64("minsize", 0, "Minimum file size in bytes")
	mkfilesCmd.Flags().Int64("maxsize", 1024, "Maximum file size in bytes")
	mkfilesCmd.Flags().Int("maxdirdepth", 0, "Maximum directory nesting depth (0 = flat, no subdirectories)")

	rootCmd.AddCommand(copyCmd, resumeCmd, taskCmd, serveCmd, cksumCmd, mkfilesCmd, newConfigCmd())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

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

// handleDryRun prints the effective configuration for copy/cksum/resume --dry-run.
// For copy, it handles both --task resume and fresh copy paths.
func handleDryRun(cmd *cobra.Command, cfg *config.Config, args []string) error {
	taskID, _ := cmd.Flags().GetString("task")
	var urls []string
	if taskID != "" {
		meta, err := task.ReadMeta(cfg.ProgressStorePath, taskID)
		if err != nil {
			return fmt.Errorf("read task meta: %w", err)
		}
		urls = strings.Split(meta.SrcBase, ",")
		urls = append(urls, meta.DstBase)
	} else {
		if len(args) < 2 {
			return fmt.Errorf("dry-run requires <src> and <dst> arguments when not using --task")
		}
		urls = append([]string(nil), args[:len(args)-1]...)
		urls = append(urls, args[len(args)-1])
	}
	usedProfiles, err := config.ExtractUsedProfiles(urls, cfg.Profiles)
	if err != nil {
		return err
	}
	fmt.Print(config.FormatConfig(cfg, usedProfiles))
	return nil
}

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

	var src storage.Source
	var dst storage.Destination
	var extraOpts []copy.JobOption
	if firstRunJobType(meta) == task.JobTypeCksum {
		src, dst, extraOpts, err = setupCopyDepsPlain(cfg, meta.SrcBase, meta.DstBase, srcMode)
	} else {
		src, dst, extraOpts, err = setupCopyDepsMulti(cfg, srcPaths, meta.DstBase, srcMode)
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

	_ = notifyRemoteTaskDone(meta.SrcBase, meta.DstBase, taskID, srcMode)

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
	var src storage.Source
	var dst storage.Source
	var extraOpts []cksum.CksumJobOption
	if firstRunJobType(meta) == task.JobTypeCopy {
		// Copy-task DB has basename-prefixed relPaths (e.g. "project/file1").
		// Source must use BasenamePrefixedSource to route correctly.
		srcPaths := strings.Split(meta.SrcBase, ",")
		var copySrc storage.Source
		var copyExtra []copy.JobOption
		copySrc, _, copyExtra, err = setupCopyDepsMulti(cfg, srcPaths, meta.DstBase, srcMode)
		if err != nil {
			return 1, err
		}
		src = copySrc
		// Convert copy.JobOption src/dst factories to cksum.CksumJobOption
		for _, opt := range copyExtra {
			_ = opt
		}

		// dst is a Source (for reading), not a Destination
		dst, err = di.NewSourceWithRemoteMode(meta.DstBase, cfg.Profiles, srcMode)
		if err != nil {
			return 1, fmt.Errorf("create destination source: %w", err)
		}
		du, _ := di.ParsePath(meta.DstBase)
		if du.Scheme == "ncp" {
			dstFactory := func(id int) (storage.Source, error) {
				return di.NewSourceWithRemoteMode(meta.DstBase, cfg.Profiles, srcMode)
			}
			extraOpts = append(extraOpts, cksum.WithCksumDstFactory(dstFactory))
		}
		// Src factory for remote sources
		for _, sp := range srcPaths {
			su, _ := di.ParsePath(sp)
			if su.Scheme == "ncp" {
				srcFactory := func(id int) (storage.Source, error) {
					return di.NewSourceWithRemoteMode(sp, cfg.Profiles, srcMode)
				}
				extraOpts = append(extraOpts, cksum.WithCksumSrcFactory(srcFactory))
				break
			}
		}
	} else {
		src, dst, extraOpts, err = setupCksumDeps(cfg, meta.SrcBase, meta.DstBase, srcMode, srcMode)
		if err != nil {
			return 1, err
		}
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
	_ = notifyRemoteTaskDone(meta.SrcBase, meta.DstBase, taskID, srcMode)

	return exitCode, err
}

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
	src, dst, extraOpts, err := setupCksumDeps(cfg, srcPath, dstPath, srcMode, srcMode)
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
	_ = notifyRemoteTaskDone(srcPath, dstPath, taskID, srcMode)

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

	var src storage.Source
	var dst storage.Source
	var extraOpts []cksum.CksumJobOption
	if firstRunJobType(meta) == task.JobTypeCopy {
		srcPaths := strings.Split(meta.SrcBase, ",")
		var copySrc storage.Source
		var copyExtra []copy.JobOption
		copySrc, _, copyExtra, err = setupCopyDepsMulti(cfg, srcPaths, meta.DstBase, srcMode)
		if err != nil {
			return err
		}
		src = copySrc
		for _, opt := range copyExtra {
			_ = opt
		}

		dst, err = di.NewSourceWithRemoteMode(meta.DstBase, cfg.Profiles, srcMode)
		if err != nil {
			return fmt.Errorf("create destination source: %w", err)
		}
		du, _ := di.ParsePath(meta.DstBase)
		if du.Scheme == "ncp" {
			dstFactory := func(id int) (storage.Source, error) {
				return di.NewSourceWithRemoteMode(meta.DstBase, cfg.Profiles, srcMode)
			}
			extraOpts = append(extraOpts, cksum.WithCksumDstFactory(dstFactory))
		}
		for _, sp := range srcPaths {
			su, _ := di.ParsePath(sp)
			if su.Scheme == "ncp" {
				srcFactory := func(id int) (storage.Source, error) {
					return di.NewSourceWithRemoteMode(sp, cfg.Profiles, srcMode)
				}
				extraOpts = append(extraOpts, cksum.WithCksumSrcFactory(srcFactory))
				break
			}
		}
	} else {
		src, dst, extraOpts, err = setupCksumDeps(cfg, meta.SrcBase, meta.DstBase, srcMode, srcMode)
		if err != nil {
			return err
		}
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
	_ = notifyRemoteTaskDone(meta.SrcBase, meta.DstBase, taskID, srcMode)

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
func setupCksumDeps(cfg *config.Config, srcPath, dstPath string, srcMode, dstMode uint8) (storage.Source, storage.Source, []cksum.CksumJobOption, error) {
	var extraOpts []cksum.CksumJobOption

	src, err := di.NewSourceWithRemoteMode(srcPath, cfg.Profiles, srcMode)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create source: %w", err)
	}

	su, _ := di.ParsePath(srcPath)
	if su.Scheme == "ncp" {
		srcFactory := func(id int) (storage.Source, error) {
			return di.NewSourceWithRemoteMode(srcPath, cfg.Profiles, srcMode)
		}
		extraOpts = append(extraOpts, cksum.WithCksumSrcFactory(srcFactory))
	}

	dst, err := di.NewSourceWithRemoteMode(dstPath, cfg.Profiles, dstMode)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create destination source: %w", err)
	}

	du, _ := di.ParsePath(dstPath)
	if du.Scheme == "ncp" {
		dstFactory := func(id int) (storage.Source, error) {
			return di.NewSourceWithRemoteMode(dstPath, cfg.Profiles, dstMode)
		}
		extraOpts = append(extraOpts, cksum.WithCksumDstFactory(dstFactory))
	}

	return src, dst, extraOpts, nil
}

// runServe handles `ncp serve` — starts the ncp protocol server.
func runServe(cmd *cobra.Command, args []string) error {
	listenAddr, _ := cmd.Flags().GetString("listen")
	tempDir, _ := cmd.Flags().GetString("serve-temp-dir")

	if err := ncpserver.CleanupTempDir(tempDir); err != nil {
		return fmt.Errorf("cleanup temp dir: %w", err)
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", listenAddr, err)
	}

	srv := ncpserver.NewServer(listener, tempDir)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		<-ctx.Done()
		srv.Shutdown()
	}()

	slog.Info("ncp serve started", "listen", listenAddr)

	err = srv.Serve()
	if err != nil {
		return err
	}

	// Serve returned (Shutdown was called), cleanup walker DB
	if cErr := srv.Cleanup(); cErr != nil {
		slog.Warn("server cleanup failed", "error", cErr)
	}
	return nil
}

// runTaskList handles `ncp task list`.
// notifyRemoteTaskDone dials the remote ncp serve and sends MsgTaskDone
// after the task has completed. No-op if neither src nor dst is ncp://.
// srcMode is the mode used for remote sources (ModeSource or ModeSourceNoWalker).
func notifyRemoteTaskDone(srcBase, dstBase, taskID string, srcMode uint8) error {
	var addr, basePath string
	var mode uint8

	if u, err := di.ParsePath(srcBase); err == nil && u.Scheme == "ncp" {
		addr = u.Host
		basePath = u.Path
		mode = srcMode
	} else if u, err := di.ParsePath(dstBase); err == nil && u.Scheme == "ncp" {
		addr = u.Host
		basePath = u.Path
		mode = protocol.ModeDestination
	} else {
		return nil
	}

	conn, err := protocol.Dial(addr)
	if err != nil {
		return fmt.Errorf("dial remote for task done: %w", err)
	}
	defer conn.Close()

	initMsg := &protocol.InitMsg{BasePath: basePath, Mode: mode, TaskID: taskID}
	if _, err := conn.SendMsgRecvAck(protocol.MsgInit, initMsg.Encode()); err != nil {
		return fmt.Errorf("send init for task done: %w", err)
	}

	if _, err := conn.SendMsgRecvAck(protocol.MsgTaskDone, (&protocol.TaskDoneMsg{}).Encode()); err != nil {
		return fmt.Errorf("send task done: %w", err)
	}
	return nil
}

func runTaskList(cmd *cobra.Command, args []string) error {
	progressDir, _ := cmd.Flags().GetString("ProgressStorePath")

	entries, err := os.ReadDir(progressDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read progress dir: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		meta, err := task.ReadMeta(progressDir, entry.Name())
		if err != nil {
			continue
		}
		line, _ := json.Marshal(meta)
		fmt.Println(string(line))
	}
	return nil
}

// runTaskShow handles `ncp task show <taskID>`.
func runTaskShow(cmd *cobra.Command, args []string) error {
	progressDir, _ := cmd.Flags().GetString("ProgressStorePath")
	taskID := args[0]

	meta, err := task.ReadMeta(progressDir, taskID)
	if err != nil {
		return fmt.Errorf("task %s not found: %w", taskID, err)
	}

	out, _ := json.MarshalIndent(meta, "", "  ")
	fmt.Println(string(out))
	return nil
}

// runTaskDelete handles `ncp task delete <taskID>`.
func runTaskDelete(cmd *cobra.Command, args []string) error {
	progressDir, _ := cmd.Flags().GetString("ProgressStorePath")
	taskID := args[0]

	// Check if task is running
	_, lock, err := task.CheckTaskNotRunning(progressDir, taskID)
	if err != nil {
		return fmt.Errorf("cannot delete: %w", err)
	}
	if lock != nil {
		_ = lock.Release()
	}

	dir := task.TaskDir(progressDir, taskID)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("delete task %s: %w", taskID, err)
	}

	fmt.Printf("{\"taskId\":%q,\"action\":\"deleted\"}\n", taskID)
	return nil
}

// setupFileLog creates a FileLog emitter.
func setupFileLog(cfg *config.Config, taskID, progressDir string) (*filelog.Emitter, error) {
	flOutput := cfg.FileLogOutput
	if flOutput == "" || flOutput == "progress" {
		flOutput = filepath.Join(progressDir, taskID, "file.log")
	}
	fl, err := filelog.NewEmitter(taskID, flOutput, cfg.FileLogEnabled)
	if err != nil {
		return nil, fmt.Errorf("create filelog: %w", err)
	}
	return fl, nil
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

// firstRunJobType returns the job type of the first run in the task's meta.
func firstRunJobType(meta *task.Meta) task.JobType {
	if len(meta.Runs) == 0 {
		return task.JobTypeCopy
	}
	return meta.Runs[0].JobType
}

// loadResumeConfig loads config from a resume/task command's Viper flags.
func loadResumeConfig(cmd *cobra.Command) (*config.Config, error) {
	// Bind resume-specific flags
	_ = v.BindPFlag("CopyParallelism", cmd.Flags().Lookup("CopyParallelism"))
	_ = v.BindPFlag("ProgramLogLevel", cmd.Flags().Lookup("ProgramLogLevel"))
	_ = v.BindPFlag("ProgramLogOutput", cmd.Flags().Lookup("ProgramLogOutput"))
	_ = v.BindPFlag("FileLogEnabled", cmd.Flags().Lookup("enable-FileLog"))
	_ = v.BindPFlag("FileLogOutput", cmd.Flags().Lookup("FileLogOutput"))
	_ = v.BindPFlag("FileLogInterval", cmd.Flags().Lookup("FileLogInterval"))
	_ = v.BindPFlag("ProgressStorePath", cmd.Flags().Lookup("ProgressStorePath"))
	_ = v.BindPFlag("SkipByMtime", cmd.Flags().Lookup("skip-by-mtime"))

	return config.LoadFromViper(v)
}

// resolveBoolFlag handles --enable-*/--disable-* paired flags.
// If --disable-* is set, it takes precedence.
func resolveBoolFlag(cmd *cobra.Command, viperKey, enableFlag, disableFlag string) {
	if disabled, _ := cmd.Flags().GetBool(disableFlag); disabled {
		v.Set(viperKey, false)
	} else if enabled, _ := cmd.Flags().GetBool(enableFlag); enabled {
		v.Set(viperKey, true)
	}
}

// resolveCksumAlgo parses the CksumAlgorithm from config, returning the model value.
func resolveCksumAlgo(cfg *config.Config) model.CksumAlgorithm {
	algo, err := model.ParseCksumAlgorithm(cfg.CksumAlgorithm)
	if err != nil {
		return model.DefaultCksumAlgorithm
	}
	return algo
}

// validateCksumAlgoForOSS checks that the chosen checksum algorithm is
// compatible when object storage is involved. OSS uses Content-MD5
// for integrity verification and only supports md5.
func validateCksumAlgoForOSS(algo model.CksumAlgorithm, urls ...string) error {
	for _, u := range urls {
		if strings.HasPrefix(u, "oss://") && algo != model.CksumMD5 {
			return fmt.Errorf("cksum-algorithm %q is not supported for OSS; only md5 is supported", algo)
		}
	}
	return nil
}

// resolveRemoteSourceMode determines the mode for a remote source connection.
// If the progress DB has a complete walk, returns ModeSourceNoWalker (no server-side walk).
// Otherwise returns ModeSource (server creates a walker).
func resolveRemoteSourceMode(store progress.ProgressStore) uint8 {
	if hasComplete, err := store.HasWalkComplete(); err == nil && hasComplete {
		return protocol.ModeSourceNoWalker
	}
	return protocol.ModeSource
}

// runMkfiles handles `ncp mkfiles <dir>`.
func runMkfiles(cmd *cobra.Command, args []string) error {
	num, _ := cmd.Flags().GetInt("num")
	minSize, _ := cmd.Flags().GetInt64("minsize")
	maxSize, _ := cmd.Flags().GetInt64("maxsize")
	maxDirDepth, _ := cmd.Flags().GetInt("maxdirdepth")
	dir := args[0]

	gen, err := mkfiles.NewGenerator(mkfiles.Config{
		Dir:         dir,
		NumFiles:    num,
		MinSize:     minSize,
		MaxSize:     maxSize,
		MaxDirDepth: maxDirDepth,
	})
	if err != nil {
		return err
	}

	return gen.Run()
}
