package main

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/spf13/cobra"

	"github.com/zp001/ncp/internal/task"
)

func newTaskMigrateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate-profile <taskID>",
		Short: "Inject profile reference into a pre-profile task's meta.json URLs",
		Long:  "Rewrite the src/dst URLs in a task's meta.json to include a <profile>@ prefix. Local paths and ncp:// URLs are left unchanged.",
		Args:  cobra.ExactArgs(1),
		RunE:  runTaskMigrate,
	}
	cmd.Flags().String("profile", "", "Default profile applied to both src and dst cloud URLs")
	cmd.Flags().String("src-profile", "", "Profile for src URL (overrides --profile)")
	cmd.Flags().String("dst-profile", "", "Profile for dst URL (overrides --profile)")
	cmd.Flags().String("ProgressStorePath", "./progress", "Progress storage directory")
	return cmd
}

func runTaskMigrate(cmd *cobra.Command, args []string) error {
	progressDir, _ := cmd.Flags().GetString("ProgressStorePath")
	taskID := args[0]
	defaultProf, _ := cmd.Flags().GetString("profile")
	srcProf, _ := cmd.Flags().GetString("src-profile")
	dstProf, _ := cmd.Flags().GetString("dst-profile")
	if srcProf == "" {
		srcProf = defaultProf
	}
	if dstProf == "" {
		dstProf = defaultProf
	}

	// Refuse to mutate meta.json while another process holds the task lock.
	meta, lock, err := task.CheckTaskNotRunning(progressDir, taskID)
	if err != nil {
		return fmt.Errorf("cannot migrate: %w", err)
	}
	if lock != nil {
		defer lock.Release()
	}

	srcURLs := strings.Split(meta.SrcBase, ",")
	for i, s := range srcURLs {
		injected, err := injectProfile(s, srcProf)
		if err != nil {
			return fmt.Errorf("src[%d]: %w", i, err)
		}
		srcURLs[i] = injected
	}
	meta.SrcBase = strings.Join(srcURLs, ",")

	injectedDst, err := injectProfile(meta.DstBase, dstProf)
	if err != nil {
		return fmt.Errorf("dst: %w", err)
	}
	meta.DstBase = injectedDst

	if err := task.WriteMetaTo(meta, progressDir); err != nil {
		return fmt.Errorf("write meta: %w", err)
	}
	fmt.Printf("{\"taskId\":%q,\"srcBase\":%q,\"dstBase\":%q,\"action\":\"migrated\"}\n",
		taskID, meta.SrcBase, meta.DstBase)
	return nil
}

// injectProfile rewrites a URL string to embed `profile` as userinfo.
//   - Local paths (no "://") are returned unchanged.
//   - ncp:// and file:// URLs are returned unchanged (they MUST NOT carry profiles).
//   - Cloud URLs without userinfo get `profile` injected; with existing userinfo, this errors.
//   - A cloud URL passed without a profile name produces an error.
func injectProfile(rawURL, profile string) (string, error) {
	if !strings.Contains(rawURL, "://") {
		return rawURL, nil
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse %q: %w", rawURL, err)
	}
	if u.Scheme == "ncp" || u.Scheme == "file" || u.Scheme == "" {
		return rawURL, nil
	}
	if u.User != nil {
		return "", fmt.Errorf("URL %q already has userinfo", rawURL)
	}
	if profile == "" {
		return "", fmt.Errorf("URL %q is a cloud URL but no profile provided", rawURL)
	}
	u.User = url.User(profile)
	return u.String(), nil
}
