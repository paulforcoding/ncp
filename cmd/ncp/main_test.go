package main

import (
	"testing"

	"github.com/zp001/ncp/internal/config"
	"github.com/zp001/ncp/pkg/model"
)

func TestResolveCksumAlgo(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.Config
		want model.CksumAlgorithm
	}{
		{"md5", config.Config{CksumAlgorithm: "md5"}, model.CksumMD5},
		{"xxh64", config.Config{CksumAlgorithm: "xxh64"}, model.CksumXXH64},
		{"empty", config.Config{CksumAlgorithm: ""}, model.DefaultCksumAlgorithm},
		{"invalid", config.Config{CksumAlgorithm: "invalid"}, model.DefaultCksumAlgorithm},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveCksumAlgo(&tt.cfg)
			if got != tt.want {
				t.Errorf("resolveCksumAlgo() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateCksumAlgoForOSS(t *testing.T) {
	tests := []struct {
		name    string
		algo    model.CksumAlgorithm
		urls    []string
		wantErr bool
	}{
		{"md5 no oss", model.CksumMD5, []string{"/local/path"}, false},
		{"md5 with oss", model.CksumMD5, []string{"oss://bucket/prefix"}, false},
		{"xxh64 no oss", model.CksumXXH64, []string{"/local/path"}, false},
		{"xxh64 with oss", model.CksumXXH64, []string{"oss://bucket/prefix"}, true},
		{"xxh64 mixed", model.CksumXXH64, []string{"/local/path", "oss://bucket/prefix"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCksumAlgoForOSS(tt.algo, tt.urls...)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestVersionNotEmpty(t *testing.T) {
	if version == "" {
		t.Error("version should not be empty at link time")
	}
}
