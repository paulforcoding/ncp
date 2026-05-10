package model

import (
	"strings"
	"testing"
)

func TestIsCloudScheme(t *testing.T) {
	cases := map[string]bool{
		"oss":  true,
		"file": false,
		"":     false,
		"ncp":  false,
		"cos":  true,
		"obs":  true,
	}
	for scheme, want := range cases {
		if got := IsCloudScheme(scheme); got != want {
			t.Errorf("IsCloudScheme(%q)=%v, want %v", scheme, got, want)
		}
	}
}

func TestExpandEnv(t *testing.T) {
	t.Setenv("MY_AK", "real-ak")
	t.Setenv("MY_SK", "real-sk")
	t.Setenv("MY_ENDPOINT", "host.example.com")

	p := Profile{
		Provider: "oss",
		Endpoint: "${env:MY_ENDPOINT}",
		Region:   "cn-shenzhen",
		AK:       "${env:MY_AK}",
		SK:       "${env:MY_SK}",
	}
	p.ExpandEnv()

	if p.AK != "real-ak" {
		t.Errorf("AK not expanded: %q", p.AK)
	}
	if p.SK != "real-sk" {
		t.Errorf("SK not expanded: %q", p.SK)
	}
	if p.Endpoint != "host.example.com" {
		t.Errorf("Endpoint not expanded: %q", p.Endpoint)
	}
	if p.Region != "cn-shenzhen" {
		t.Errorf("Region should not change: %q", p.Region)
	}
}

func TestExpandEnv_UnsetEnvVar(t *testing.T) {
	p := Profile{AK: "${env:DEFINITELY_NOT_SET_XYZ}"}
	p.ExpandEnv()
	if p.AK != "" {
		t.Errorf("expected empty for unset env var, got %q", p.AK)
	}
}

func TestExpandEnv_NotPlaceholder(t *testing.T) {
	cases := []string{
		"plain-value",
		"prefix-${env:X}-suffix", // partial match — should NOT expand
		"${notenv:X}",
		"",
		"${env:}",
	}
	for _, in := range cases {
		p := Profile{AK: in}
		p.ExpandEnv()
		if p.AK != in {
			t.Errorf("input %q should not be touched, got %q", in, p.AK)
		}
	}
}

func TestValidate_OK(t *testing.T) {
	p := Profile{
		Provider: "oss",
		Endpoint: "host",
		Region:   "r",
		AK:       "a",
		SK:       "b",
	}
	if err := p.Validate("prod", "oss"); err != nil {
		t.Fatalf("expected ok, got %v", err)
	}
}

func TestValidate_ProviderEmpty(t *testing.T) {
	err := Profile{}.Validate("prod", "oss")
	if err == nil || !strings.Contains(err.Error(), "Provider is required") {
		t.Fatalf("got %v", err)
	}
}

func TestValidate_ProviderMismatch(t *testing.T) {
	p := Profile{Provider: "cos", AK: "a", SK: "b", Endpoint: "e", Region: "r"}
	err := p.Validate("prod", "oss")
	if err == nil || !strings.Contains(err.Error(), "does not match URL scheme") {
		t.Fatalf("got %v", err)
	}
}

func TestValidate_MissingFields(t *testing.T) {
	p := Profile{Provider: "oss"}
	err := p.Validate("prod", "oss")
	if err == nil || !strings.Contains(err.Error(), "all required") {
		t.Fatalf("got %v", err)
	}
}

func TestValidate_UnknownProvider(t *testing.T) {
	p := Profile{Provider: "weird"}
	err := p.Validate("prod", "weird")
	if err == nil || !strings.Contains(err.Error(), "unknown provider") {
		t.Fatalf("got %v", err)
	}
}

func TestValidate_OBS_OK(t *testing.T) {
	p := Profile{
		Provider: "obs",
		Endpoint: "obs.cn-north-4.myhuaweicloud.com",
		Region:   "cn-north-4",
		AK:       "a",
		SK:       "b",
	}
	if err := p.Validate("prod", "obs"); err != nil {
		t.Fatalf("expected ok, got %v", err)
	}
}

func TestValidate_OBS_MissingEndpoint(t *testing.T) {
	p := Profile{Provider: "obs", AK: "a", SK: "b", Region: "r"}
	err := p.Validate("prod", "obs")
	if err == nil || !strings.Contains(err.Error(), "all required") {
		t.Fatalf("got %v", err)
	}
}

func TestHasPlainSecret(t *testing.T) {
	cases := []struct {
		name string
		p    Profile
		want bool
	}{
		{"both env refs", Profile{AK: "${env:A}", SK: "${env:B}"}, false},
		{"AK plain", Profile{AK: "real", SK: "${env:B}"}, true},
		{"SK plain", Profile{AK: "${env:A}", SK: "real"}, true},
		{"both plain", Profile{AK: "x", SK: "y"}, true},
		{"both empty", Profile{}, false},
	}
	for _, tc := range cases {
		if got := tc.p.HasPlainSecret(); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}
