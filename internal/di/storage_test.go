package di

import (
	"fmt"
	"testing"
)

func TestParsePath_LocalAbsolute(t *testing.T) {
	u, err := ParsePath("/tmp/data")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if u.Scheme != "" {
		t.Fatalf("expected empty scheme, got %q", u.Scheme)
	}
	if u.Path == "" {
		t.Fatal("expected non-empty path")
	}
}

func TestParsePath_LocalRelative(t *testing.T) {
	u, err := ParsePath("some/relative/path")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if u.Scheme != "" {
		t.Fatalf("expected empty scheme, got %q", u.Scheme)
	}
}

func TestParsePath_NCP(t *testing.T) {
	u, err := ParsePath("ncp://host:9900/data/backup")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if u.Scheme != "ncp" {
		t.Fatalf("scheme: got %q, want %q", u.Scheme, "ncp")
	}
	if u.Host != "host:9900" {
		t.Fatalf("host: got %q, want %q", u.Host, "host:9900")
	}
	if u.Path != "/data/backup" {
		t.Fatalf("path: got %q, want %q", u.Path, "/data/backup")
	}
}

func TestParsePath_OSS(t *testing.T) {
	u, err := ParsePath("oss://mybucket/path/to/dir")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if u.Scheme != "oss" {
		t.Fatalf("scheme: got %q, want %q", u.Scheme, "oss")
	}
	if u.Host != "mybucket" {
		t.Fatalf("host: got %q, want %q", u.Host, "mybucket")
	}
}

func TestNewSource_LocalScheme(t *testing.T) {
	src, err := NewSource("/tmp", nil)
	if err != nil {
		t.Fatalf("new source: %v", err)
	}
	gotType := fmt.Sprintf("%T", src)
	if gotType != "*local.Source" {
		t.Fatalf("type: got %q, want *local.Source", gotType)
	}
}

func TestNewSource_UnsupportedScheme(t *testing.T) {
	_, err := NewSource("s3://bucket/path", nil)
	if err == nil {
		t.Fatal("expected error for unsupported scheme")
	}
}

func TestNewDestination_LocalScheme(t *testing.T) {
	dst, err := NewDestination("/tmp", DestConfig{}, nil)
	if err != nil {
		t.Fatalf("new destination: %v", err)
	}
	gotType := fmt.Sprintf("%T", dst)
	if gotType != "*local.Destination" {
		t.Fatalf("type: got %q, want *local.Destination", gotType)
	}
}

func TestNewDestination_UnsupportedScheme(t *testing.T) {
	_, err := NewDestination("s3://bucket/path", DestConfig{}, nil)
	if err == nil {
		t.Fatal("expected error for unsupported scheme")
	}
}

func TestParseOSSURL(t *testing.T) {
	tests := []struct {
		raw        string
		wantBucket string
		wantPrefix string
	}{
		{"oss://mybucket/path/to/dir", "mybucket", "path/to/dir/"},
		{"oss://mybucket/", "mybucket", ""},
		{"oss://mybucket", "mybucket", ""},
		{"oss://mybucket/deep/nested/path", "mybucket", "deep/nested/path/"},
	}

	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			u, err := ParsePath(tt.raw)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			bucket, prefix := parseOSSURL(u)
			if bucket != tt.wantBucket {
				t.Fatalf("bucket: got %q, want %q", bucket, tt.wantBucket)
			}
			if prefix != tt.wantPrefix {
				t.Fatalf("prefix: got %q, want %q", prefix, tt.wantPrefix)
			}
		})
	}
}

func TestParsePath_OBS(t *testing.T) {
	u, err := ParsePath("obs://mybucket/path/to/dir")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if u.Scheme != "obs" {
		t.Fatalf("scheme: got %q, want %q", u.Scheme, "obs")
	}
	if u.Host != "mybucket" {
		t.Fatalf("host: got %q, want %q", u.Host, "mybucket")
	}
}

func TestParseCOSURL(t *testing.T) {
	tests := []struct {
		raw        string
		wantBucket string
		wantPrefix string
	}{
		{"cos://mybucket-1250000000/path/to/dir", "mybucket-1250000000", "path/to/dir/"},
		{"cos://mybucket-1250000000/", "mybucket-1250000000", ""},
		{"cos://mybucket-1250000000", "mybucket-1250000000", ""},
		{"cos://mybucket-1250000000/deep/nested/path", "mybucket-1250000000", "deep/nested/path/"},
	}

	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			u, err := ParsePath(tt.raw)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			bucket, prefix := parseCOSURL(u)
			if bucket != tt.wantBucket {
				t.Fatalf("bucket: got %q, want %q", bucket, tt.wantBucket)
			}
			if prefix != tt.wantPrefix {
				t.Fatalf("prefix: got %q, want %q", prefix, tt.wantPrefix)
			}
		})
	}
}

func TestNewSourceWithRemoteMode_NCP(t *testing.T) {
	// ncp:// URLs should create remote.Source with the given mode.
	src, err := NewSourceWithRemoteMode("ncp://host:9900/data", nil, 3) // ModeSourceNoWalker
	if err != nil {
		t.Fatalf("NewSourceWithRemoteMode: %v", err)
	}
	gotType := fmt.Sprintf("%T", src)
	if gotType != "*remote.Source" {
		t.Fatalf("type: got %q, want *remote.Source", gotType)
	}
}

func TestNewSourceWithRemoteMode_NonNCP(t *testing.T) {
	// Non-ncp URLs should fall through to NewSource.
	src, err := NewSourceWithRemoteMode(t.TempDir(), nil, 3)
	if err != nil {
		t.Fatalf("NewSourceWithRemoteMode: %v", err)
	}
	gotType := fmt.Sprintf("%T", src)
	if gotType != "*local.Source" {
		t.Fatalf("type: got %q, want *local.Source", gotType)
	}
}

func TestParseOBSURL(t *testing.T) {
	tests := []struct {
		raw        string
		wantBucket string
		wantPrefix string
	}{
		{"obs://mybucket/path/to/dir", "mybucket", "path/to/dir/"},
		{"obs://mybucket/", "mybucket", ""},
		{"obs://mybucket", "mybucket", ""},
		{"obs://mybucket/deep/nested/path", "mybucket", "deep/nested/path/"},
	}

	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			u, err := ParsePath(tt.raw)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			bucket, prefix := parseOBSURL(u)
			if bucket != tt.wantBucket {
				t.Fatalf("bucket: got %q, want %q", bucket, tt.wantBucket)
			}
			if prefix != tt.wantPrefix {
				t.Fatalf("prefix: got %q, want %q", prefix, tt.wantPrefix)
			}
		})
	}
}
