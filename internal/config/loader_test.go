package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/zp001/ncp/pkg/model"
)

func TestExpandProfileEnv_NilMap(t *testing.T) {
	if got := expandProfileEnv(nil); got != nil {
		t.Fatalf("expected nil for nil input, got %+v", got)
	}
}

func TestExpandProfileEnv_PlaceholdersExpanded(t *testing.T) {
	t.Setenv("MYCO_AK", "ak-from-env")
	t.Setenv("MYCO_SK", "sk-from-env")
	in := map[string]model.Profile{
		"prod": {
			Provider: "oss",
			Endpoint: "host",
			Region:   "r",
			AK:       "${env:MYCO_AK}",
			SK:       "${env:MYCO_SK}",
		},
	}
	out := expandProfileEnv(in)
	if out["prod"].AK != "ak-from-env" {
		t.Fatalf("AK not expanded: %q", out["prod"].AK)
	}
	if out["prod"].SK != "sk-from-env" {
		t.Fatalf("SK not expanded: %q", out["prod"].SK)
	}
}

func TestAnyPlainSecret(t *testing.T) {
	cases := []struct {
		name string
		in   map[string]model.Profile
		want bool
	}{
		{"all env refs", map[string]model.Profile{
			"a": {AK: "${env:X}", SK: "${env:Y}"},
		}, false},
		{"one plain", map[string]model.Profile{
			"a": {AK: "${env:X}", SK: "${env:Y}"},
			"b": {AK: "real", SK: "${env:Y}"},
		}, true},
		{"empty map", map[string]model.Profile{}, false},
	}
	for _, tc := range cases {
		if got := anyPlainSecret(tc.in); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

func TestCheckCredentialFilePermissions_NoPlainSecret(t *testing.T) {
	// All env refs → permission check skipped, never errors.
	profiles := map[string]model.Profile{
		"prod": {AK: "${env:X}", SK: "${env:Y}"},
	}
	if err := CheckCredentialFilePermissions(profiles); err != nil {
		t.Fatalf("expected nil for env-only profiles, got %v", err)
	}
}

func TestCheckCredentialFilePermissions_PlainSecretBadPerms(t *testing.T) {
	if !systemAllowsCustomConfigPaths(t) {
		t.Skip("/etc/ncp_config.json present, skipping path-mocking test")
	}
	tmpHome := t.TempDir()
	t.Setenv("HOME", tmpHome)

	cfgPath := filepath.Join(tmpHome, "ncp_config.json")
	if err := os.WriteFile(cfgPath, []byte("{}"), 0o644); err != nil { // bad perms
		t.Fatalf("write: %v", err)
	}

	profiles := map[string]model.Profile{
		"prod": {AK: "real-ak", SK: "real-sk"}, // plain
	}
	err := CheckCredentialFilePermissions(profiles)
	if err == nil {
		t.Fatal("expected error for plain creds + bad perms")
	}
}

func TestCheckCredentialFilePermissions_PlainSecretGoodPerms(t *testing.T) {
	if !systemAllowsCustomConfigPaths(t) {
		t.Skip("/etc/ncp_config.json present, skipping path-mocking test")
	}
	tmpHome := t.TempDir()
	t.Setenv("HOME", tmpHome)

	cfgPath := filepath.Join(tmpHome, "ncp_config.json")
	if err := os.WriteFile(cfgPath, []byte("{}"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	profiles := map[string]model.Profile{
		"prod": {AK: "real-ak", SK: "real-sk"},
	}
	if err := CheckCredentialFilePermissions(profiles); err != nil {
		t.Fatalf("expected nil for plain creds + good perms, got %v", err)
	}
}

// systemAllowsCustomConfigPaths returns true when the system's
// /etc/ncp_config.json doesn't exist, so HOME-based test paths can
// dominate without contamination.
func systemAllowsCustomConfigPaths(t *testing.T) bool {
	t.Helper()
	_, err := os.Stat("/etc/ncp_config.json")
	return os.IsNotExist(err)
}

// withChdir saves and restores the working directory across a test.
func withChdir(t *testing.T, dir string) {
	t.Helper()
	orig, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(orig) })
}

func TestLoad_LayeredProfileWholeReplace(t *testing.T) {
	if !systemAllowsCustomConfigPaths(t) {
		t.Skip("/etc/ncp_config.json present; skipping layered test")
	}

	tmpHome := t.TempDir()
	tmpCwd := t.TempDir()
	t.Setenv("HOME", tmpHome)
	withChdir(t, tmpCwd)

	homeCfg := `{
		"Profiles": {
			"prod": {
				"Provider": "oss",
				"Endpoint": "host-old",
				"Region":   "r-old",
				"AK":       "ak-old",
				"SK":       "sk-old"
			}
		}
	}`
	cwdCfg := `{
		"Profiles": {
			"prod": {
				"Provider": "oss",
				"Endpoint": "host-new",
				"Region":   "r-new",
				"AK":       "ak-new"
			}
		}
	}`
	if err := os.WriteFile(filepath.Join(tmpHome, "ncp_config.json"), []byte(homeCfg), 0o600); err != nil {
		t.Fatalf("write home cfg: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpCwd, "ncp_config.json"), []byte(cwdCfg), 0o600); err != nil {
		t.Fatalf("write cwd cfg: %v", err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	prod, ok := cfg.Profiles["prod"]
	if !ok {
		t.Fatalf("profile 'prod' missing")
	}
	if prod.AK != "ak-new" {
		t.Errorf("AK: got %q want ak-new", prod.AK)
	}
	if prod.Endpoint != "host-new" {
		t.Errorf("Endpoint: got %q want host-new", prod.Endpoint)
	}
	if prod.Region != "r-new" {
		t.Errorf("Region: got %q want r-new", prod.Region)
	}
	// Whole-profile replacement: SK from older layer must NOT leak through.
	if prod.SK != "" {
		t.Errorf("SK should be empty after whole-profile replacement, got %q", prod.SK)
	}
}

func TestLoad_EnvExpansionInProfile(t *testing.T) {
	if !systemAllowsCustomConfigPaths(t) {
		t.Skip("/etc/ncp_config.json present; skipping")
	}

	t.Setenv("MY_AK", "ak-from-env")
	t.Setenv("MY_SK", "sk-from-env")

	tmpHome := t.TempDir()
	tmpCwd := t.TempDir()
	t.Setenv("HOME", tmpHome)
	withChdir(t, tmpCwd)

	cfg := `{
		"Profiles": {
			"prod": {
				"Provider": "oss",
				"Endpoint": "host",
				"Region":   "r",
				"AK":       "${env:MY_AK}",
				"SK":       "${env:MY_SK}"
			}
		}
	}`
	if err := os.WriteFile(filepath.Join(tmpHome, "ncp_config.json"), []byte(cfg), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	loaded, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	prod := loaded.Profiles["prod"]
	if prod.AK != "ak-from-env" || prod.SK != "sk-from-env" {
		t.Errorf("env not expanded: %+v", prod)
	}
}
