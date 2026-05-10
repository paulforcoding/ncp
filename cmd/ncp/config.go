package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/zp001/ncp/internal/config"
	"github.com/zp001/ncp/pkg/model"
)

func newConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Inspect effective configuration",
		Long:  "Show the merged effective configuration across all sources (defaults, config files, environment variables, CLI flags). AK/SK are automatically masked.",
	}

	showCmd := &cobra.Command{
		Use:   "show",
		Short: "Show effective configuration",
		Long:  "Show all effective configuration values with AK/SK masked. Config sources are also listed.",
		Args:  cobra.NoArgs,
		RunE:  runConfigShow,
	}
	showCmd.Flags().String("profile", "", "Filter: show only the specified profile")

	cmd.AddCommand(showCmd)
	return cmd
}

func runConfigShow(cmd *cobra.Command, args []string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	// If --profile is specified, filter to only that profile
	filterProfile, _ := cmd.Flags().GetString("profile")
	if filterProfile != "" {
		if p, ok := cfg.Profiles[filterProfile]; ok {
			cfg.Profiles = map[string]model.Profile{filterProfile: p}
		} else {
			return fmt.Errorf("profile %q not found", filterProfile)
		}
	}

	fmt.Print(config.FormatConfig(cfg, nil))
	fmt.Print(config.FormatConfigSources())
	return nil
}
