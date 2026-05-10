package main

import (
	"strings"
	"testing"
)

func TestInjectProfile_LocalUnchanged(t *testing.T) {
	cases := []string{
		"/data/dir",
		"./relative",
		"relative/path",
	}
	for _, in := range cases {
		got, err := injectProfile(in, "prod")
		if err != nil {
			t.Errorf("input %q: unexpected error %v", in, err)
		}
		if got != in {
			t.Errorf("input %q: should be unchanged, got %q", in, got)
		}
	}
}

func TestInjectProfile_NCPUnchanged(t *testing.T) {
	in := "ncp://host:9900/path"
	got, err := injectProfile(in, "prod")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != in {
		t.Fatalf("ncp:// should be untouched, got %q", got)
	}
}

func TestInjectProfile_OSSInject(t *testing.T) {
	got, err := injectProfile("oss://bkt/path", "prod")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != "oss://prod@bkt/path" {
		t.Fatalf("got %q, want oss://prod@bkt/path", got)
	}
}

func TestInjectProfile_AlreadyHasUserinfo_Errors(t *testing.T) {
	_, err := injectProfile("oss://existing@bkt/path", "prod")
	if err == nil || !strings.Contains(err.Error(), "already has userinfo") {
		t.Fatalf("expected userinfo error, got %v", err)
	}
}

func TestInjectProfile_CloudWithoutProfile_Errors(t *testing.T) {
	_, err := injectProfile("oss://bkt/path", "")
	if err == nil || !strings.Contains(err.Error(), "no profile provided") {
		t.Fatalf("expected error, got %v", err)
	}
}
