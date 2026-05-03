package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/zp001/ncp/internal/protocol"
	"github.com/zp001/ncp/internal/serve"
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
		Long:  "Copy files from source to destination. Supports local→local, local→remote (ncp://), and local→cloud (oss://). Multiple sources are copied into subdirectories of dst named after each source's basename.",
		Args:  cobra.MinimumNArgs(2),
		RunE:  runCopy,
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
	copyCmd.Flags().String("ProgressStorePath", "./progress", "Progress storage directory")
	copyCmd.Flags().Bool("dry-run", false, "Preview effective config without executing")
	copyCmd.Flags().String("task", "", "Resume an existing task by taskID")
	copyCmd.Flags().String("endpoint", "", "OSS endpoint (e.g. oss-cn-shenzhen.aliyuncs.com)")
	copyCmd.Flags().String("region", "", "OSS region (e.g. cn-shenzhen)")
	copyCmd.Flags().String("access-key-id", "", "OSS AccessKey ID")
	copyCmd.Flags().String("access-key-secret", "", "OSS AccessKey Secret")
	copyCmd.Flags().String("cksum-algorithm", "md5", "Checksum algorithm: md5 or xxh64")
	copyCmd.Flags().Bool("skip-by-mtime", true, "Skip files with matching mtime+size (and ETag for OSS)")
	copyCmd.Flags().Bool("no-skip-by-mtime", false, "Disable skip-by-mtime, copy/verify all files")
	copyCmd.Flags().Int("ChannelBuf", 100000, "Channel buffer size for discover/result queues")

	// Bind all flags to Viper
	v.BindPFlag("CopyParallelism", copyCmd.Flags().Lookup("CopyParallelism"))
	v.BindPFlag("ProgramLogLevel", copyCmd.Flags().Lookup("ProgramLogLevel"))
	v.BindPFlag("ProgramLogOutput", copyCmd.Flags().Lookup("ProgramLogOutput"))
	v.BindPFlag("FileLogEnabled", copyCmd.Flags().Lookup("enable-FileLog"))
	v.BindPFlag("FileLogOutput", copyCmd.Flags().Lookup("FileLogOutput"))
	v.BindPFlag("FileLogInterval", copyCmd.Flags().Lookup("FileLogInterval"))
	v.BindPFlag("DirectIO", copyCmd.Flags().Lookup("enable-DirectIO"))
	v.BindPFlag("SyncWrites", copyCmd.Flags().Lookup("enable-SyncWrites"))
	v.BindPFlag("IOSize", copyCmd.Flags().Lookup("IOSize"))
	v.BindPFlag("EnsureDirMtime", copyCmd.Flags().Lookup("enable-EnsureDirMtime"))
	v.BindPFlag("ProgressStorePath", copyCmd.Flags().Lookup("ProgressStorePath"))
	v.BindPFlag("OSSEndpoint", copyCmd.Flags().Lookup("endpoint"))
	v.BindPFlag("OSSRegion", copyCmd.Flags().Lookup("region"))
	v.BindPFlag("OSSAK", copyCmd.Flags().Lookup("access-key-id"))
	v.BindPFlag("OSSSK", copyCmd.Flags().Lookup("access-key-secret"))
	v.BindPFlag("CksumAlgorithm", copyCmd.Flags().Lookup("cksum-algorithm"))
	v.BindPFlag("SkipByMtime", copyCmd.Flags().Lookup("skip-by-mtime"))
	v.BindPFlag("ChannelBuf", copyCmd.Flags().Lookup("ChannelBuf"))

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
	resumeCmd.Flags().String("ProgressStorePath", "./progress", "Progress storage directory")
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
	taskListCmd.Flags().String("ProgressStorePath", "./progress", "Progress storage directory")

	taskShowCmd := &cobra.Command{
		Use:   "show <taskID>",
		Short: "Show task details",
		Long:  "Show detailed information about a task. Output is JSON.",
		Args:  cobra.ExactArgs(1),
		RunE:  runTaskShow,
	}
	taskShowCmd.Flags().String("ProgressStorePath", "./progress", "Progress storage directory")

	taskDeleteCmd := &cobra.Command{
		Use:   "delete <taskID>",
		Short: "Delete a task and its progress data",
		Long:  "Delete a task's progress data. Refuses if the task is currently running.",
		Args:  cobra.ExactArgs(1),
		RunE:  runTaskDelete,
	}
	taskDeleteCmd.Flags().String("ProgressStorePath", "./progress", "Progress storage directory")

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

	// cksum command
	cksumCmd := &cobra.Command{
		Use:   "cksum <src> <dst>",
		Short: "Verify data consistency between source and destination",
		Long:  "Verify source and destination data consistency by comparing MD5 checksums. Supports local↔local, local↔OSS, and OSS↔OSS.",
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
	cksumCmd.Flags().String("ProgressStorePath", "./progress", "Progress storage directory")
	cksumCmd.Flags().String("task", "", "Resume an existing task by taskID")
	cksumCmd.Flags().String("endpoint", "", "OSS endpoint")
	cksumCmd.Flags().String("region", "", "OSS region")
	cksumCmd.Flags().String("access-key-id", "", "OSS AccessKey ID")
	cksumCmd.Flags().String("access-key-secret", "", "OSS AccessKey Secret")
	cksumCmd.Flags().String("cksum-algorithm", "md5", "Checksum algorithm: md5 or xxh64")
	cksumCmd.Flags().Bool("skip-by-mtime", true, "Skip files with matching mtime+size (and ETag for OSS)")
	cksumCmd.Flags().Bool("no-skip-by-mtime", false, "Disable skip-by-mtime, verify all files")

	rootCmd.AddCommand(copyCmd, resumeCmd, taskCmd, serveCmd, cksumCmd)

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

	// Warn about permissive config files if OSS credentials are in use
	if cfg.OSSAK != "" || cfg.OSSSK != "" {
		config.CheckCredentialFilePermissions()
	}

	// --dry-run: print effective config as JSON and exit
	if dryRun, _ := cmd.Flags().GetBool("dry-run"); dryRun {
		out, _ := json.MarshalIndent(cfg, "", "  ")
		fmt.Println(string(out))
		return nil
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

	// Dependency injection
	src, dst, store, extraOpts, err := setupCopyDepsMulti(cfg, srcPaths, dstPath, progressDir, taskID)
	if err != nil {
		return err
	}
	defer store.Close()

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

	// Update meta.json
	task.UpdateRunFinished(meta, exitCode, progressDir)

	if err != nil {
		fmt.Fprintf(os.Stderr, "{\"error\":%q,\"taskId\":%q,\"code\":%d}\n", err.Error(), taskID, exitCode)
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
		defer lock.Release()
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
	src, dst, store, extraOpts, err := setupCopyDepsMulti(cfg, srcPaths, meta.DstBase, progressDir, taskID)
	if err != nil {
		return err
	}
	defer store.Close()

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

	task.UpdateRunFinished(meta, exitCode, progressDir)

	if err != nil {
		fmt.Fprintf(os.Stderr, "{\"error\":%q,\"taskId\":%q,\"code\":%d}\n", err.Error(), taskID, exitCode)
	}
	os.Exit(exitCode)
	return nil
}

// runResume handles `ncp resume <taskID>` — determines jobType from last run.
func runResume(cmd *cobra.Command, args []string) error {
	resolveBoolFlag(cmd, "SkipByMtime", "skip-by-mtime", "no-skip-by-mtime")
	taskID := args[0]
	progressDir, _ := cmd.Flags().GetString("ProgressStorePath")

	// Check concurrency
	meta, lock, err := task.CheckTaskNotRunning(progressDir, taskID)
	if err != nil {
		return err
	}
	if lock != nil {
		defer lock.Release()
	}

	jobType := task.LastJobType(meta)

	cfg, err := loadResumeConfig(cmd)
	if err != nil {
		return err
	}

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

	task.UpdateRunFinished(meta, exitCode, progressDir)

	if runErr != nil {
		fmt.Fprintf(os.Stderr, "{\"error\":%q,\"taskId\":%q,\"code\":%d}\n", runErr.Error(), taskID, exitCode)
	}
	os.Exit(exitCode)
	return nil
}

func runResumeCopy(cfg *config.Config, meta *task.Meta, fl *filelog.Emitter, taskID, progressDir string, ctx context.Context) (int, error) {
	srcPaths := strings.Split(meta.SrcBase, ",")
	src, dst, store, extraOpts, err := setupCopyDepsMulti(cfg, srcPaths, meta.DstBase, progressDir, taskID)
	if err != nil {
		return 1, err
	}
	defer store.Close()

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
	return job.Run(ctx)
}

func runResumeCksum(cfg *config.Config, meta *task.Meta, fl *filelog.Emitter, taskID, progressDir string, ctx context.Context) (int, error) {
	src, dst, store, err := setupCksumDeps(cfg, meta.SrcBase, meta.DstBase, progressDir, taskID)
	if err != nil {
		return 1, err
	}
	defer store.Close()

	job := cksum.NewCksumJob(src, dst, store,
		cksum.WithCksumParallelism(cfg.CopyParallelism),
		cksum.WithCksumFileLog(fl, cfg.FileLogInterval),
		cksum.WithCksumIOSize(cfg.IOSize),
		cksum.WithCksumTaskID(taskID),
		cksum.WithCksumAlgo(resolveCksumAlgo(cfg)),
		cksum.WithCksumResume(true),
		cksum.WithCksumSkipByMtime(cfg.SkipByMtime),
		cksum.WithCksumChannelBuf(cfg.ChannelBuf),
	)
	return job.Run(ctx)
}

// runCksum is the Composition Root for the cksum command.
func runCksum(cmd *cobra.Command, args []string) error {
	resolveBoolFlag(cmd, "FileLogEnabled", "enable-FileLog", "disable-FileLog")

	// Bind cksum flags to Viper
	v.BindPFlag("CopyParallelism", cmd.Flags().Lookup("CopyParallelism"))
	v.BindPFlag("ProgramLogLevel", cmd.Flags().Lookup("ProgramLogLevel"))
	v.BindPFlag("ProgramLogOutput", cmd.Flags().Lookup("ProgramLogOutput"))
	v.BindPFlag("FileLogEnabled", cmd.Flags().Lookup("enable-FileLog"))
	v.BindPFlag("FileLogOutput", cmd.Flags().Lookup("FileLogOutput"))
	v.BindPFlag("FileLogInterval", cmd.Flags().Lookup("FileLogInterval"))
	v.BindPFlag("ProgressStorePath", cmd.Flags().Lookup("ProgressStorePath"))
	v.BindPFlag("OSSEndpoint", cmd.Flags().Lookup("endpoint"))
	v.BindPFlag("OSSRegion", cmd.Flags().Lookup("region"))
	v.BindPFlag("OSSAK", cmd.Flags().Lookup("access-key-id"))
	v.BindPFlag("OSSSK", cmd.Flags().Lookup("access-key-secret"))
	v.BindPFlag("CksumAlgorithm", cmd.Flags().Lookup("cksum-algorithm"))
	v.BindPFlag("SkipByMtime", cmd.Flags().Lookup("skip-by-mtime"))

	cfg, err := config.LoadFromViper(v)
	if err != nil {
		return err
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

	// Dependency injection
	src, dst, store, err := setupCksumDeps(cfg, srcPath, dstPath, progressDir, taskID)
	if err != nil {
		return err
	}
	defer store.Close()

	job := cksum.NewCksumJob(src, dst, store,
		cksum.WithCksumParallelism(cfg.CopyParallelism),
		cksum.WithCksumFileLog(fl, cfg.FileLogInterval),
		cksum.WithCksumIOSize(cfg.IOSize),
		cksum.WithCksumTaskID(taskID),
		cksum.WithCksumAlgo(resolveCksumAlgo(cfg)),
		cksum.WithCksumSkipByMtime(cfg.SkipByMtime),
		cksum.WithCksumChannelBuf(cfg.ChannelBuf),
	)

	exitCode, err := job.Run(ctx)

	task.UpdateRunFinished(meta, exitCode, progressDir)

	if err != nil {
		fmt.Fprintf(os.Stderr, "{\"error\":%q,\"taskId\":%q,\"code\":%d}\n", err.Error(), taskID, exitCode)
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
		defer lock.Release()
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

	src, dst, store, err := setupCksumDeps(cfg, meta.SrcBase, meta.DstBase, progressDir, taskID)
	if err != nil {
		return err
	}
	defer store.Close()

	job := cksum.NewCksumJob(src, dst, store,
		cksum.WithCksumParallelism(cfg.CopyParallelism),
		cksum.WithCksumFileLog(fl, cfg.FileLogInterval),
		cksum.WithCksumIOSize(cfg.IOSize),
		cksum.WithCksumTaskID(taskID),
		cksum.WithCksumAlgo(resolveCksumAlgo(cfg)),
		cksum.WithCksumResume(true),
		cksum.WithCksumSkipByMtime(cfg.SkipByMtime),
		cksum.WithCksumChannelBuf(cfg.ChannelBuf),
	)

	exitCode, err := job.Run(ctx)

	task.UpdateRunFinished(meta, exitCode, progressDir)

	if err != nil {
		fmt.Fprintf(os.Stderr, "{\"error\":%q,\"taskId\":%q,\"code\":%d}\n", err.Error(), taskID, exitCode)
	}
	os.Exit(exitCode)
	return nil
}

// setupCksumDeps creates src Source, dst Source, and opens the Pebble store.
// Both src and dst are Sources (readable) for checksum comparison.
func setupCksumDeps(cfg *config.Config, srcPath, dstPath, progressDir, taskID string) (storage.Source, storage.Source, progress.ProgressStore, error) {
	ossCfg := di.OSSConfig{
		Endpoint: cfg.OSSEndpoint,
		Region:   cfg.OSSRegion,
		AK:       cfg.OSSAK,
		SK:       cfg.OSSSK,
	}

	src, err := di.NewSourceWithOSS(srcPath, ossCfg)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create source: %w", err)
	}

	// For cksum, dst must also be a Source (readable).
	// ncp:// as dst does not support cksum because the remote server
	// doesn't expose a Restatter interface for the cksum workflow.
	u, _ := di.ParsePath(dstPath)
	if u.Scheme == "ncp" {
		return nil, nil, nil, fmt.Errorf("ncp:// destinations do not support cksum (protocol has built-in MD5 verification)")
	}

	dst, err := di.NewSourceWithOSS(dstPath, ossCfg)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create destination source: %w", err)
	}

	dbDir := filepath.Join(progressDir, taskID, "db")
	store, err := di.NewProgressStore(dbDir)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open progress store: %w", err)
	}

	return src, dst, store, nil
}

// runServe handles `ncp serve` — starts the ncp protocol server.
func runServe(cmd *cobra.Command, args []string) error {
	listenAddr, _ := cmd.Flags().GetString("listen")

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", listenAddr, err)
	}

	handlerFactory := func() protocol.ConnHandler {
		return serve.NewConnHandler()
	}

	srv := protocol.NewServer(listener, handlerFactory)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		<-ctx.Done()
		srv.Close()
	}()

	fmt.Fprintf(os.Stderr, "{\"event\":\"serve\",\"listen\":%q}\n", listenAddr)

	return srv.Serve()
}

// runTaskList handles `ncp task list`.
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
		lock.Release()
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

// setupCopyDeps creates Source, Destination, and opens the Pebble store.
// For ncp:// destinations, returns a dstFactory via extraOpts instead of a shared dst.
func setupCopyDeps(cfg *config.Config, srcPath, dstPath, progressDir, taskID string) (storage.Source, storage.Destination, progress.ProgressStore, []copy.JobOption, error) {
	return setupCopyDepsMulti(cfg, []string{srcPath}, dstPath, progressDir, taskID)
}

// setupCopyDepsMulti supports multiple source paths. Single source falls through
// to the same path; multiple sources create a di.MultiSource.
func setupCopyDepsMulti(cfg *config.Config, srcPaths []string, dstPath, progressDir, taskID string) (storage.Source, storage.Destination, progress.ProgressStore, []copy.JobOption, error) {
	ossCfg := di.OSSConfig{
		Endpoint: cfg.OSSEndpoint,
		Region:   cfg.OSSRegion,
		AK:       cfg.OSSAK,
		SK:       cfg.OSSSK,
	}

	var src storage.Source
	var err error

	if len(srcPaths) > 1 {
		for _, sp := range srcPaths {
			u, _ := di.ParsePath(sp)
			if u.Scheme != "" && u.Scheme != "file" {
				return nil, nil, nil, nil, fmt.Errorf("multi-source is only supported for local paths; %q has scheme %q", sp, u.Scheme)
			}
		}
	}

	if len(srcPaths) == 1 {
		src, err = di.NewSourceWithOSS(srcPaths[0], ossCfg)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("create source: %w", err)
		}
	} else {
		sources := make([]storage.Source, len(srcPaths))
		for i, sp := range srcPaths {
			sources[i], err = di.NewSourceWithOSS(sp, ossCfg)
			if err != nil {
				return nil, nil, nil, nil, fmt.Errorf("create source %s: %w", sp, err)
			}
		}
		src, err = di.NewMultiSource(sources)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("create multi-source: %w", err)
		}
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
		extraOpts = append(extraOpts, copy.WithEnsureDirMtime(false))
	case "oss":
		dstFactory := func(id int) (storage.Destination, error) {
			return di.NewDestinationWithConfig(dstPath, di.DestConfig{}, ossCfg)
		}
		extraOpts = append(extraOpts, copy.WithDstFactory(dstFactory))
		extraOpts = append(extraOpts, copy.WithEnsureDirMtime(false))
	default:
		dstCfg := di.DestConfig{
			DirectIO:    cfg.DirectIO,
			SyncWrites:  cfg.SyncWrites,
			IOSize:      cfg.IOSize,
			IOSizeTiers: cfg.IOSizeTiers,
		}
		dst, err = di.NewDestinationWithConfig(dstPath, dstCfg)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("create destination: %w", err)
		}
	}

	dbDir := filepath.Join(progressDir, taskID, "db")
	store, err := di.NewProgressStore(dbDir)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("open progress store: %w", err)
	}

	return src, dst, store, extraOpts, nil
}

// loadResumeConfig loads config from a resume/task command's Viper flags.
func loadResumeConfig(cmd *cobra.Command) (*config.Config, error) {
	// Bind resume-specific flags
	v.BindPFlag("CopyParallelism", cmd.Flags().Lookup("CopyParallelism"))
	v.BindPFlag("ProgramLogLevel", cmd.Flags().Lookup("ProgramLogLevel"))
	v.BindPFlag("ProgramLogOutput", cmd.Flags().Lookup("ProgramLogOutput"))
	v.BindPFlag("FileLogEnabled", cmd.Flags().Lookup("enable-FileLog"))
	v.BindPFlag("FileLogOutput", cmd.Flags().Lookup("FileLogOutput"))
	v.BindPFlag("FileLogInterval", cmd.Flags().Lookup("FileLogInterval"))
	v.BindPFlag("ProgressStorePath", cmd.Flags().Lookup("ProgressStorePath"))
	v.BindPFlag("SkipByMtime", cmd.Flags().Lookup("skip-by-mtime"))

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
