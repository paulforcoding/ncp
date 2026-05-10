package main

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/spf13/cobra"

	"github.com/zp001/ncp/internal/config"
	"github.com/zp001/ncp/pkg/model"
)

func newProfileCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "profile",
		Short: "Manage cloud credential profiles",
		Long:  "Inspect profiles defined in ncp_config.json across the layered config search path. Profiles are referenced from URLs via userinfo (e.g. oss://prod@bucket/path).",
	}
	cmd.AddCommand(
		&cobra.Command{
			Use:   "list",
			Short: "List effective profile names (one per line)",
			Args:  cobra.NoArgs,
			RunE:  runProfileList,
		},
		&cobra.Command{
			Use:   "show <name>",
			Short: "Show a single profile with AK/SK masked",
			Args:  cobra.ExactArgs(1),
			RunE:  runProfileShow,
		},
	)
	return cmd
}

func runProfileList(cmd *cobra.Command, args []string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	names := make([]string, 0, len(cfg.Profiles))
	for name := range cfg.Profiles {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		p := cfg.Profiles[name]
		fmt.Printf("%s\t%s\t%s\n", name, p.Provider, p.Region)
	}
	return nil
}

func runProfileShow(cmd *cobra.Command, args []string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	p, ok := cfg.Profiles[args[0]]
	if !ok {
		return fmt.Errorf("profile %q not found", args[0])
	}
	masked := model.Profile{
		Provider: p.Provider,
		Endpoint: p.Endpoint,
		Region:   p.Region,
		AK:       maskSecret(p.AK),
		SK:       maskSecret(p.SK),
	}
	out, err := json.MarshalIndent(masked, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

// maskSecret reveals only the first and last 4 characters of a non-empty secret.
// Short or empty secrets are fully masked to avoid leaking length-correlated info.
func maskSecret(s string) string {
	if s == "" {
		return ""
	}
	if len(s) <= 8 {
		return "****"
	}
	return s[:4] + "****" + s[len(s)-4:]
}
