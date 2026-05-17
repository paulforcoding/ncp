package main

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/zp001/ncp/internal/config"
	"github.com/zp001/ncp/internal/di"
	"github.com/zp001/ncp/internal/filelog"
	"github.com/zp001/ncp/internal/protocol"
	"github.com/zp001/ncp/internal/task"
	"github.com/zp001/ncp/pkg/interfaces/progress"
	"github.com/zp001/ncp/pkg/model"
)

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

// notifyRemoteTaskDone dials the remote ncp serve and sends MsgTaskDone
// after the task has completed. No-op if neither src nor dst is ncp://.
// srcMode is the mode used for remote sources (ModeSource or ModeSourceNoWalker).
func notifyRemoteTaskDone(srcBase, dstBase, taskID string, srcMode uint8, configJSON string) error {
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

	initMsg := &protocol.InitMsg{BasePath: basePath, Mode: mode, TaskID: taskID, ConfigJSON: configJSON}
	if _, err := conn.SendMsgRecvAck(protocol.MsgInit, initMsg.Encode()); err != nil {
		return fmt.Errorf("send init for task done: %w", err)
	}

	if _, err := conn.SendMsgRecvAck(protocol.MsgTaskDone, (&protocol.TaskDoneMsg{}).Encode()); err != nil {
		return fmt.Errorf("send task done: %w", err)
	}
	return nil
}
