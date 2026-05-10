package config

import (
	"testing"

	"github.com/zp001/ncp/pkg/model"
)

func TestMaskSecret(t *testing.T) {
	cases := map[string]string{
		"":                   "",
		"short":              "****",
		"12345678":           "****",
		"123456789":          "1234****6789",
		"abcdefghijklmnop":   "abcd****mnop",
		"AKIAVERYLONGSECRET": "AKIA****CRET",
	}
	for in, want := range cases {
		if got := MaskSecret(in); got != want {
			t.Errorf("MaskSecret(%q)=%q, want %q", in, got, want)
		}
	}
}

func TestExtractUsedProfiles_LocalOnly(t *testing.T) {
	profiles := map[string]model.Profile{"prod": {Provider: "oss"}}
	got, err := ExtractUsedProfiles([]string{"/data/dir", "/tmp/dir"}, profiles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no profiles, got %v", got)
	}
}

func TestExtractUsedProfiles_OSS(t *testing.T) {
	profiles := map[string]model.Profile{"prod": {Provider: "oss"}}
	got, err := ExtractUsedProfiles([]string{"oss://prod@bucket/path"}, profiles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || got[0] != "prod" {
		t.Fatalf("expected [prod], got %v", got)
	}
}

func TestExtractUsedProfiles_Deduplicates(t *testing.T) {
	profiles := map[string]model.Profile{"prod": {Provider: "oss"}}
	got, err := ExtractUsedProfiles([]string{"oss://prod@b1/p1", "oss://prod@b2/p2"}, profiles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || got[0] != "prod" {
		t.Fatalf("expected [prod], got %v", got)
	}
}

func TestExtractUsedProfiles_MultipleProfiles(t *testing.T) {
	profiles := map[string]model.Profile{
		"src": {Provider: "oss"},
		"dst": {Provider: "cos"},
	}
	got, err := ExtractUsedProfiles([]string{"oss://src@b1/p", "cos://dst@b2/p"}, profiles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 || got[0] != "dst" || got[1] != "src" {
		t.Fatalf("expected [dst src] (sorted), got %v", got)
	}
}

func TestExtractUsedProfiles_MissingProfile(t *testing.T) {
	profiles := map[string]model.Profile{}
	_, err := ExtractUsedProfiles([]string{"oss://missing@bucket/path"}, profiles)
	if err == nil {
		t.Fatal("expected error for undefined profile")
	}
}

func TestExtractUsedProfiles_NoProfile(t *testing.T) {
	profiles := map[string]model.Profile{}
	_, err := ExtractUsedProfiles([]string{"oss://bucket/path"}, profiles)
	if err == nil {
		t.Fatal("expected error for missing profile")
	}
}

func TestExtractUsedProfiles_PasswordInURL(t *testing.T) {
	profiles := map[string]model.Profile{"prod": {Provider: "oss"}}
	_, err := ExtractUsedProfiles([]string{"oss://prod:secret@bucket/path"}, profiles)
	if err == nil {
		t.Fatal("expected error for password in URL")
	}
}

func TestFormatConfig_NoProfiles(t *testing.T) {
	cfg := &Config{
		CopyParallelism: 4,
		ProgramLogLevel: "info",
	}
	out := FormatConfig(cfg, nil)
	if !contains(out, "CopyParallelism:   4") {
		t.Errorf("expected CopyParallelism in output, got:\n%s", out)
	}
	if !contains(out, "(none defined)") {
		t.Errorf("expected '(none defined)' for profiles, got:\n%s", out)
	}
}

func TestFormatConfig_WithProfiles(t *testing.T) {
	cfg := &Config{
		CopyParallelism: 4,
		Profiles: map[string]model.Profile{
			"prod": {
				Provider: "oss",
				Endpoint: "oss-cn-shenzhen.aliyuncs.com",
				Region:   "cn-shenzhen",
				AK:       "AKIAVERYLONGSECRET",
				SK:       "sk-even-longer-secret-key",
			},
		},
	}
	out := FormatConfig(cfg, []string{"prod"})
	if !contains(out, "AK:       AKIA****CRET") {
		t.Errorf("expected masked AK, got:\n%s", out)
	}
	if !contains(out, "[used by this operation]") {
		t.Errorf("expected usage marker, got:\n%s", out)
	}
}

func contains(s, substr string) bool {
	return len(substr) <= len(s) && findSubstr(s, substr)
}

func findSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
