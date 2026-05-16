package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
