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
	b.WriteString(fmt.Sprintf("CopyParallelism:   %d\n", cfg.CopyParallelism))
	b.WriteString(fmt.Sprintf("ProgramLogLevel:   %s\n", cfg.ProgramLogLevel))
	b.WriteString(fmt.Sprintf("ProgramLogOutput:  %s\n", cfg.ProgramLogOutput))
	b.WriteString(fmt.Sprintf("FileLogEnabled:    %t\n", cfg.FileLogEnabled))
	b.WriteString(fmt.Sprintf("FileLogOutput:     %s\n", cfg.FileLogOutput))
	b.WriteString(fmt.Sprintf("FileLogInterval:   %d\n", cfg.FileLogInterval))
	b.WriteString(fmt.Sprintf("DirectIO:          %t\n", cfg.DirectIO))
	b.WriteString(fmt.Sprintf("SyncWrites:        %t\n", cfg.SyncWrites))
	b.WriteString(fmt.Sprintf("IOSize:            %d\n", cfg.IOSize))
	b.WriteString(fmt.Sprintf("EnsureDirMtime:    %t\n", cfg.EnsureDirMtime))
	b.WriteString(fmt.Sprintf("ProgressStorePath: %s\n", cfg.ProgressStorePath))
	b.WriteString(fmt.Sprintf("ServerListenAddr:  %s\n", cfg.ServerListenAddr))
	b.WriteString(fmt.Sprintf("CksumAlgorithm:    %s\n", cfg.CksumAlgorithm))
	b.WriteString(fmt.Sprintf("SkipByMtime:       %t\n", cfg.SkipByMtime))
	b.WriteString(fmt.Sprintf("ChannelBuf:        %d\n", cfg.ChannelBuf))
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
			b.WriteString(fmt.Sprintf("%s:\n", name))
			b.WriteString(fmt.Sprintf("  Provider: %s\n", p.Provider))
			if p.Endpoint != "" {
				b.WriteString(fmt.Sprintf("  Endpoint: %s\n", p.Endpoint))
			}
			if p.Region != "" {
				b.WriteString(fmt.Sprintf("  Region:   %s\n", p.Region))
			}
			b.WriteString(fmt.Sprintf("  AK:       %s\n", MaskSecret(p.AK)))
			b.WriteString(fmt.Sprintf("  SK:       %s\n", MaskSecret(p.SK)))
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
		b.WriteString(fmt.Sprintf("%-26s %s\n", path, status))
	}
	return b.String()
}
