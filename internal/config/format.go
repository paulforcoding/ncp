package config

import (
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"

	"github.com/zp001/ncp/pkg/model"
)

// MaskSecret masks a secret, revealing only first and last 4 chars.
// Short or empty secrets are fully masked.
func MaskSecret(s string) string {
	if s == "" {
		return ""
	}
	if len(s) <= 8 {
		return "****"
	}
	return s[:4] + "****" + s[len(s)-4:]
}

// ExtractUsedProfiles parses a list of URLs and returns the unique profile
// names referenced by cloud-scheme URLs. Returns an error if any cloud URL
// lacks a profile, embeds a password, or references an undefined profile.
func ExtractUsedProfiles(urls []string, profiles map[string]model.Profile) ([]string, error) {
	seen := make(map[string]struct{})
	var result []string

	for _, raw := range urls {
		if !strings.Contains(raw, "://") {
			continue // local path
		}
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("parse URL %q: %w", raw, err)
		}
		if !model.IsCloudScheme(u.Scheme) {
			continue
		}
		if u.User == nil {
			return nil, fmt.Errorf("URL %q requires a profile; use form: %s://<profile>@<bucket>/<path>", raw, u.Scheme)
		}
		if _, hasPwd := u.User.Password(); hasPwd {
			return nil, fmt.Errorf("URL %q: embedding password in URL is not allowed; reference a profile name only", raw)
		}
		name := u.User.Username()
		if name == "" {
			return nil, fmt.Errorf("URL %q: empty profile name", raw)
		}
		if _, ok := profiles[name]; !ok {
			return nil, fmt.Errorf("profile %q referenced in URL %q is not defined in config", name, raw)
		}
		if _, ok := seen[name]; !ok {
			seen[name] = struct{}{}
			result = append(result, name)
		}
	}
	sort.Strings(result)
	return result, nil
}

// FormatConfig formats the effective configuration as a structured string.
// If usedProfiles is non-nil, marks which profiles are used by the operation.
func FormatConfig(cfg *Config, usedProfiles []string) string {
	var b strings.Builder

	b.WriteString("=== General ===\n")
	fmt.Fprintf(&b, "CopyParallelism:   %d\n", cfg.CopyParallelism)
	fmt.Fprintf(&b, "ProgramLogLevel:   %s\n", cfg.ProgramLogLevel)
	fmt.Fprintf(&b, "ProgramLogOutput:  %s\n", cfg.ProgramLogOutput)
	fmt.Fprintf(&b, "FileLogEnabled:    %t\n", cfg.FileLogEnabled)
	fmt.Fprintf(&b, "FileLogOutput:     %s\n", cfg.FileLogOutput)
	fmt.Fprintf(&b, "FileLogInterval:   %d\n", cfg.FileLogInterval)
	fmt.Fprintf(&b, "DirectIO:          %t\n", cfg.DirectIO)
	fmt.Fprintf(&b, "SyncWrites:        %t\n", cfg.SyncWrites)
	fmt.Fprintf(&b, "IOSize:            %d\n", cfg.IOSize)
	fmt.Fprintf(&b, "EnsureDirMtime:    %t\n", cfg.EnsureDirMtime)
	fmt.Fprintf(&b, "ProgressStorePath: %s\n", cfg.ProgressStorePath)
	fmt.Fprintf(&b, "ServerListenAddr:  %s\n", cfg.ServerListenAddr)
	fmt.Fprintf(&b, "CksumAlgorithm:    %s\n", cfg.CksumAlgorithm)
	fmt.Fprintf(&b, "SkipByMtime:       %t\n", cfg.SkipByMtime)
	fmt.Fprintf(&b, "ChannelBuf:        %d\n", cfg.ChannelBuf)
	b.WriteString("\n")

	b.WriteString("=== Profiles ===\n")
	if len(cfg.Profiles) == 0 {
		b.WriteString("(none defined)\n")
	} else {
		usedSet := make(map[string]struct{})
		for _, name := range usedProfiles {
			usedSet[name] = struct{}{}
		}

		names := make([]string, 0, len(cfg.Profiles))
		for name := range cfg.Profiles {
			names = append(names, name)
		}
		sort.Strings(names)

		for _, name := range names {
			p := cfg.Profiles[name]
			fmt.Fprintf(&b, "%s:\n", name)
			fmt.Fprintf(&b, "  Provider: %s\n", p.Provider)
			if p.Endpoint != "" {
				fmt.Fprintf(&b, "  Endpoint: %s\n", p.Endpoint)
			}
			if p.Region != "" {
				fmt.Fprintf(&b, "  Region:   %s\n", p.Region)
			}
			fmt.Fprintf(&b, "  AK:       %s\n", MaskSecret(p.AK))
			fmt.Fprintf(&b, "  SK:       %s\n", MaskSecret(p.SK))
			if _, ok := usedSet[name]; ok {
				b.WriteString("  [used by this operation]\n")
			}
			b.WriteString("\n")
		}
	}

	return b.String()
}

// FormatConfigSources shows which config files exist.
func FormatConfigSources() string {
	var b strings.Builder
	b.WriteString("=== Config Sources ===\n")
	for _, path := range ConfigPaths() {
		_, err := os.Stat(path)
		status := "not found"
		if err == nil {
			status = "found"
		}
		fmt.Fprintf(&b, "%-26s %s\n", path, status)
	}
	return b.String()
}
