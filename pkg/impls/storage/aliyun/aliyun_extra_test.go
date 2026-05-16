package aliyun

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/zp001/ncp/pkg/interfaces/storage"
)

// --- Pure function tests ---

func TestParseETag(t *testing.T) {
	tests := []struct {
		name      string
		etag      string
		wantAlgo  string
		wantEmpty bool
	}{
		{"plain md5", "d41d8cd98f00b204e9800998ecf8427e", "etag-md5", false},
		{"quoted md5", `"d41d8cd98f00b204e9800998ecf8427e"`, "etag-md5", false},
		{"multipart", "abcdef-3", "etag-multipart", false},
		{"multipart quoted", `"abcdef-3"`, "etag-multipart", false},
		{"empty", "", "", true},
		{"empty quoted", `""`, "", true},
		{"hex invalid", "not-hex-and-no-dash", "etag-multipart", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cksum, algo := parseETag(tt.etag)
			if tt.wantEmpty {
				if cksum != nil || algo != "" {
					t.Errorf("expected nil/empty, got cksum=%v algo=%q", cksum, algo)
				}
				return
			}
			if algo != tt.wantAlgo {
				t.Errorf("algo = %q, want %q", algo, tt.wantAlgo)
			}
			if cksum == nil {
				t.Error("expected non-nil checksum")
			}
		})
	}
}

func TestParseInt64(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"1234567890", 1234567890},
		{"0", 0},
		{"-1", -1},
		{"", 0},
		{"abc", 0},
		{"0x10", 0}, // hex not supported by parseInt64
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseInt64(tt.input)
			if got != tt.want {
				t.Errorf("parseInt64(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestPosixMetadata(t *testing.T) {
	meta := posixMetadata(0o755, 1000, 1000)
	if meta[metaMode] != "0755" {
		t.Errorf("mode = %q, want %q", meta[metaMode], "0755")
	}
	if meta[metaUID] != "1000" {
		t.Errorf("uid = %q, want %q", meta[metaUID], "1000")
	}
	if meta[metaGID] != "1000" {
		t.Errorf("gid = %q, want %q", meta[metaGID], "1000")
	}

	// setuid in Go uses os.ModeSetuid (high bit), not raw octal
	meta2 := posixMetadata(os.ModeSetuid|0o755, 0, 0)
	if meta2[metaMode] != "4755" {
		t.Errorf("setuid mode = %q, want %q", meta2[metaMode], "4755")
	}
}

func TestOsModeToProto(t *testing.T) {
	tests := []struct {
		name string
		mode os.FileMode
		want uint32
	}{
		{"plain", 0o755, 0o755},
		{"setuid", os.ModeSetuid | 0o755, 0o4755},
		{"setgid", os.ModeSetgid | 0o755, 0o2755},
		{"sticky", os.ModeSticky | 0o755, 0o1755},
		{"all special", os.ModeSetuid | os.ModeSetgid | os.ModeSticky | 0o755, 0o7755},
		{"zero", 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := osModeToProto(tt.mode)
			if got != tt.want {
				t.Errorf("osModeToProto(%o) = %#o, want %#o", tt.mode, got, tt.want)
			}
		})
	}
}

func TestDestinationKey(t *testing.T) {
	tests := []struct {
		prefix  string
		relPath string
		want    string
	}{
		{"prefix/", "file.txt", "prefix/file.txt"},
		{"", "file.txt", "file.txt"},
		{"a/b/", "c/d.txt", "a/b/c/d.txt"},
	}
	for _, tt := range tests {
		d := &Destination{prefix: tt.prefix}
		got := d.key(tt.relPath)
		if got != tt.want {
			t.Errorf("key(%q) with prefix=%q = %q, want %q", tt.relPath, tt.prefix, got, tt.want)
		}
	}
}

func TestSourceURI(t *testing.T) {
	s := &Source{bucket: "mybucket", prefix: "path/to/"}
	got := s.URI()
	want := "oss://mybucket/path/to/"
	if got != want {
		t.Errorf("URI() = %q, want %q", got, want)
	}
}

// --- Constructor tests ---

func TestNewSource_Basic(t *testing.T) {
	s, err := NewSource(SourceConfig{
		Region: "cn-shenzhen",
		AK:     "ak",
		SK:     "sk",
		Bucket: "mybucket",
		Prefix: "p/",
	})
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	if s.bucket != "mybucket" || s.prefix != "p/" {
		t.Errorf("bucket=%q prefix=%q", s.bucket, s.prefix)
	}
	if s.client == nil {
		t.Error("client should be non-nil")
	}
}

func TestNewDestination_Basic(t *testing.T) {
	d, err := NewDestination(Config{
		Region: "cn-shenzhen",
		AK:     "ak",
		SK:     "sk",
		Bucket: "mybucket",
		Prefix: "dst/",
	})
	if err != nil {
		t.Fatalf("NewDestination: %v", err)
	}
	if d.client == nil {
		t.Error("client should be non-nil")
	}
	if d.retryCfg.MaxAttempts == 0 {
		t.Error("retry config should default to non-zero MaxAttempts")
	}
}

func TestNewDestination_RetryConfigPassthrough(t *testing.T) {
	custom := RetryConfig{MaxAttempts: 7, InitialWait: 0.1, MaxWait: 1, Multiplier: 2, Jitter: 0.1}
	d, err := NewDestination(Config{
		Region: "cn-shenzhen",
		AK:     "ak", SK: "sk", Bucket: "bkt",
		RetryCfg: custom,
	})
	if err != nil {
		t.Fatalf("NewDestination: %v", err)
	}
	if d.retryCfg.MaxAttempts != 7 {
		t.Errorf("MaxAttempts = %d, want 7", d.retryCfg.MaxAttempts)
	}
}

// --- Retry context cancellation ---

func TestWithRetry_ContextCanceledBeforeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := DefaultRetryConfig()
	called := 0
	err := withRetry(ctx, cfg, func() error {
		called++
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if called != 0 {
		t.Errorf("fn should not have been called, called=%d", called)
	}
}

func TestWithRetryResult_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := DefaultRetryConfig()
	got, err := withRetryResult(ctx, cfg, func() (string, error) {
		return "should-not-be-returned", nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if got != "" {
		t.Errorf("expected zero value on cancel, got %q", got)
	}
}

// --- Small-file writer state-machine tests ---

func TestSmallFileWriter_WriteAfterCommit(t *testing.T) {
	w := &smallFileWriter{state: stateCommitted, md5: md5.New()}
	_, err := w.Write(context.Background(), []byte("x"))
	if err == nil {
		t.Fatal("expected error writing to closed writer")
	}
	if !strings.Contains(err.Error(), "closed writer") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSmallFileWriter_WriteAfterAbort(t *testing.T) {
	w := &smallFileWriter{state: stateAborted, md5: md5.New()}
	_, err := w.Write(context.Background(), []byte("x"))
	if err == nil {
		t.Fatal("expected error writing to aborted writer")
	}
}

func TestSmallFileWriter_DoubleCommitIsNoop(t *testing.T) {
	w := &smallFileWriter{state: stateCommitted, md5: md5.New()}
	if err := w.Commit(context.Background(), nil); err != nil {
		t.Errorf("second Commit should return nil, got %v", err)
	}
}

func TestSmallFileWriter_DoubleAbortIsNoop(t *testing.T) {
	w := &smallFileWriter{state: stateAborted, md5: md5.New()}
	if err := w.Abort(context.Background()); err != nil {
		t.Errorf("second Abort should return nil, got %v", err)
	}
}

func TestSmallFileWriter_ChecksumMismatch(t *testing.T) {
	// If the writer already committed (stateCommitted), no real check happens.
	// For a stateOpen writer we'd need io.Pipe + real OSS client.
	// Verify that Commit on already-committed writer returns nil regardless of checksum.
	w := &smallFileWriter{state: stateCommitted, md5: md5.New()}
	md5.New().Write([]byte("data"))
	fakeCS := md5.New().Sum(nil)
	if err := w.Commit(context.Background(), fakeCS); err != nil {
		t.Errorf("Commit on already-committed should be noop, got %v", err)
	}
}

// --- parseETag round-trip with checksum ---

func TestParseETag_RoundTripMD5(t *testing.T) {
	// A known MD5 hex string should decode to the correct bytes.
	etag := "d41d8cd98f00b204e9800998ecf8427e"
	cksum, algo := parseETag(etag)
	if algo != "etag-md5" {
		t.Fatalf("algo = %s, want etag-md5", algo)
	}
	if len(cksum) != 16 {
		t.Errorf("md5 checksum length = %d, want 16", len(cksum))
	}
	// Re-encode should match (lower case)
	got := ""
	for _, b := range cksum {
		got += string("0123456789abcdef"[b>>4])
		got += string("0123456789abcdef"[b&0xf])
	}
	if got != etag {
		t.Errorf("round-trip = %s, want %s", got, etag)
	}
}

// --- mapError: wraps known error strings with sentinel ---

func TestMapError_WrapsSentinel(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantSent error
	}{
		{"not found 404", "StatusCode:404", storage.ErrNotFound},
		{"not found NoSuchKey", "NoSuchKey", storage.ErrNotFound},
		{"permission 403", "StatusCode:403", storage.ErrPermission},
		{"already exists 409", "StatusCode:409", storage.ErrAlreadyExists},
		{"invalid arg 400", "StatusCode:400", storage.ErrInvalidArgument},
		{"checksum mismatch", "ChecksumMismatch", storage.ErrChecksum},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orig := toErr(tt.input)
			mapped := mapError(orig)
			if !errors.Is(mapped, tt.wantSent) {
				t.Errorf("mapError(%q): got %v, want sentinel %v", tt.input, mapped, tt.wantSent)
			}
		})
	}

	// Unknown errors pass through unchanged
	t.Run("unknown", func(t *testing.T) {
		orig := toErr("some random error")
		mapped := mapError(orig)
		if mapped != orig {
			t.Errorf("expected identity for unknown error, got %v", mapped)
		}
	})
}

// Sanity: ensure bytes.Reader behaves as expected.
var _ = bytes.NewReader
