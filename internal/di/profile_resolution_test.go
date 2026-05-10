package di

import (
	"net/url"
	"strings"
	"testing"

	"github.com/zp001/ncp/pkg/model"
)

func TestResolveProfile_LocalNoUserinfo_OK(t *testing.T) {
	u, _ := ParsePath("/tmp/data")
	prof, err := resolveProfile(u, nil)
	if err != nil {
		t.Fatalf("expected nil error for local path, got %v", err)
	}
	if prof != nil {
		t.Fatalf("expected nil profile for local path, got %+v", prof)
	}
}

func TestResolveProfile_LocalWithUserinfo_Rejected(t *testing.T) {
	u := &url.URL{Scheme: "file", User: url.User("prod"), Path: "/tmp"}
	_, err := resolveProfile(u, nil)
	if err == nil || !strings.Contains(err.Error(), "does not accept a profile") {
		t.Fatalf("expected rejection, got %v", err)
	}
}

func TestResolveProfile_NCPWithUserinfo_Rejected(t *testing.T) {
	u, _ := ParsePath("ncp://prod@host:9900/data")
	_, err := resolveProfile(u, nil)
	if err == nil || !strings.Contains(err.Error(), "does not accept a profile") {
		t.Fatalf("expected rejection, got %v", err)
	}
}

func TestResolveProfile_OSSWithoutUserinfo_Error(t *testing.T) {
	u, _ := ParsePath("oss://bkt/path")
	_, err := resolveProfile(u, nil)
	if err == nil || !strings.Contains(err.Error(), "requires a profile") {
		t.Fatalf("expected 'requires a profile' error, got %v", err)
	}
}

func TestResolveProfile_OSSProfileNotFound(t *testing.T) {
	u, _ := ParsePath("oss://prod@bkt/path")
	_, err := resolveProfile(u, map[string]model.Profile{})
	if err == nil || !strings.Contains(err.Error(), "not defined") {
		t.Fatalf("expected 'not defined' error, got %v", err)
	}
}

func TestResolveProfile_ProviderMismatch(t *testing.T) {
	u, _ := ParsePath("oss://tx@bkt/path")
	profiles := map[string]model.Profile{
		"tx": {Provider: "cos", Endpoint: "host", Region: "ap-shanghai", AK: "a", SK: "b"},
	}
	_, err := resolveProfile(u, profiles)
	if err == nil || !strings.Contains(err.Error(), "does not match URL scheme") {
		t.Fatalf("expected mismatch error, got %v", err)
	}
}

func TestResolveProfile_PasswordEmbedded_Rejected(t *testing.T) {
	u, _ := ParsePath("oss://prod:secret@bkt/path")
	_, err := resolveProfile(u, map[string]model.Profile{})
	if err == nil || !strings.Contains(err.Error(), "password in URL is not allowed") {
		t.Fatalf("expected password rejection, got %v", err)
	}
}

func TestResolveProfile_EmptyProfileName(t *testing.T) {
	u := &url.URL{Scheme: "oss", User: url.User(""), Host: "bkt", Path: "/path"}
	_, err := resolveProfile(u, map[string]model.Profile{})
	if err == nil || !strings.Contains(err.Error(), "empty profile name") {
		t.Fatalf("expected empty-name error, got %v", err)
	}
}

func TestResolveProfile_Success(t *testing.T) {
	u, _ := ParsePath("oss://prod@bkt/path")
	profiles := map[string]model.Profile{
		"prod": {Provider: "oss", Endpoint: "oss-cn-shenzhen.aliyuncs.com",
			Region: "cn-shenzhen", AK: "a", SK: "b"},
	}
	p, err := resolveProfile(u, profiles)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if p == nil || p.AK != "a" || p.SK != "b" {
		t.Fatalf("unexpected profile: %+v", p)
	}
}

func TestNewSource_OSSWithoutProfile_FailsFast(t *testing.T) {
	_, err := NewSource("oss://bkt/path", nil)
	if err == nil || !strings.Contains(err.Error(), "requires a profile") {
		t.Fatalf("expected fail-fast on missing profile, got %v", err)
	}
}

func TestNewDestination_OSSWithoutProfile_FailsFast(t *testing.T) {
	_, err := NewDestination("oss://bkt/path", DestConfig{}, nil)
	if err == nil || !strings.Contains(err.Error(), "requires a profile") {
		t.Fatalf("expected fail-fast on missing profile, got %v", err)
	}
}

func TestResolveProfile_OBSSuccess(t *testing.T) {
	u, _ := ParsePath("obs://prod@bkt/path")
	profiles := map[string]model.Profile{
		"prod": {Provider: "obs", Endpoint: "obs.cn-north-4.myhuaweicloud.com",
			Region: "cn-north-4", AK: "a", SK: "b"},
	}
	p, err := resolveProfile(u, profiles)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if p == nil || p.AK != "a" || p.SK != "b" {
		t.Fatalf("unexpected profile: %+v", p)
	}
}

func TestResolveProfile_OBSWithoutUserinfo(t *testing.T) {
	u, _ := ParsePath("obs://bkt/path")
	_, err := resolveProfile(u, nil)
	if err == nil || !strings.Contains(err.Error(), "requires a profile") {
		t.Fatalf("expected 'requires a profile' error, got %v", err)
	}
}

func TestNewSource_OBSWithoutProfile_FailsFast(t *testing.T) {
	_, err := NewSource("obs://bkt/path", nil)
	if err == nil || !strings.Contains(err.Error(), "requires a profile") {
		t.Fatalf("expected fail-fast on missing profile, got %v", err)
	}
}

func TestNewDestination_OBSWithoutProfile_FailsFast(t *testing.T) {
	_, err := NewDestination("obs://bkt/path", DestConfig{}, nil)
	if err == nil || !strings.Contains(err.Error(), "requires a profile") {
		t.Fatalf("expected fail-fast on missing profile, got %v", err)
	}
}
