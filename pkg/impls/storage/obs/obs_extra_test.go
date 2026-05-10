package obs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// --- parseInt64 (mirrors COS test for parity) ---

func TestParseInt64(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"1000", 1000},
		{"0", 0},
		{"", 0},
		{"abc", 0},
		{"-42", -42},
	}
	for _, tt := range tests {
		got := parseInt64(tt.input)
		if got != tt.want {
			t.Errorf("parseInt64(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

// --- newOBSClient validation ---

func TestNewOBSClient_RegionInferred(t *testing.T) {
	cli, err := newOBSClient("ak", "sk", "", "cn-east-3")
	if err != nil {
		t.Fatalf("newOBSClient: %v", err)
	}
	if cli == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestNewOBSClient_ExplicitEndpoint(t *testing.T) {
	cli, err := newOBSClient("ak", "sk", "https://obs.cn-east-3.myhuaweicloud.com", "")
	if err != nil {
		t.Fatalf("newOBSClient: %v", err)
	}
	if cli == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestNewOBSClient_BothEmpty(t *testing.T) {
	_, err := newOBSClient("ak", "sk", "", "")
	if err == nil {
		t.Fatal("expected error when both Endpoint and Region are empty")
	}
	if !strings.Contains(err.Error(), "Endpoint or Region is required") {
		t.Errorf("unexpected error: %v", err)
	}
}

// --- Constructor success/failure ---

func TestNewSource_Success(t *testing.T) {
	s, err := NewSource(SourceConfig{
		Endpoint: "https://obs.cn-east-3.myhuaweicloud.com",
		AK:       "ak",
		SK:       "sk",
		Bucket:   "my-bucket",
		Prefix:   "data/",
	})
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	if s.bucket != "my-bucket" || s.prefix != "data/" {
		t.Errorf("source state mismatch: bucket=%q prefix=%q", s.bucket, s.prefix)
	}
}

func TestNewSource_MissingEndpointAndRegion(t *testing.T) {
	_, err := NewSource(SourceConfig{
		AK: "ak", SK: "sk", Bucket: "bkt",
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewDestination_Success(t *testing.T) {
	d, err := NewDestination(Config{
		Endpoint: "https://obs.cn-east-3.myhuaweicloud.com",
		AK:       "ak", SK: "sk",
		Bucket: "bkt", Prefix: "p/",
	})
	if err != nil {
		t.Fatalf("NewDestination: %v", err)
	}
	if d.retryCfg.MaxAttempts == 0 {
		t.Error("retry config should default to non-zero MaxAttempts")
	}
}

func TestNewDestination_RetryPassthrough(t *testing.T) {
	custom := RetryConfig{MaxAttempts: 9, InitialWait: 0.1, MaxWait: 1, Multiplier: 2, Jitter: 0.1}
	d, err := NewDestination(Config{
		Endpoint: "https://obs.cn-east-3.myhuaweicloud.com",
		AK:       "ak", SK: "sk", Bucket: "bkt",
		RetryCfg: custom,
	})
	if err != nil {
		t.Fatalf("NewDestination: %v", err)
	}
	if d.retryCfg.MaxAttempts != 9 {
		t.Errorf("MaxAttempts = %d, want 9", d.retryCfg.MaxAttempts)
	}
}

func TestNewDestination_MissingEndpointAndRegion(t *testing.T) {
	_, err := NewDestination(Config{
		AK: "ak", SK: "sk", Bucket: "bkt",
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- Source.Base / Destination.key ---

func TestSource_Base(t *testing.T) {
	s := &Source{bucket: "my-bucket", prefix: "data/"}
	want := "obs://my-bucket/data/"
	if got := s.Base(); got != want {
		t.Errorf("Base() = %q, want %q", got, want)
	}
}

func TestDestination_key(t *testing.T) {
	d := &Destination{bucket: "bkt", prefix: "prefix/"}
	if got := d.key("file.txt"); got != "prefix/file.txt" {
		t.Errorf("key(file.txt) = %q, want prefix/file.txt", got)
	}
	if got := d.key("subdir/file.txt"); got != "prefix/subdir/file.txt" {
		t.Errorf("key(subdir/file.txt) = %q, want prefix/subdir/file.txt", got)
	}
	if got := d.key(""); got != "prefix/" {
		t.Errorf("key() = %q, want prefix/", got)
	}
}

// --- posixMetadata ---

func TestPosixMetadata(t *testing.T) {
	m := posixMetadata(0o755, 1000, 1000)
	if m[metaMode] != "0755" {
		t.Errorf("mode = %q, want 0755", m[metaMode])
	}
	if m[metaUID] != "1000" {
		t.Errorf("uid = %q, want 1000", m[metaUID])
	}
	if m[metaGID] != "1000" {
		t.Errorf("gid = %q, want 1000", m[metaGID])
	}

	m2 := posixMetadata(0o644, 0, 0)
	if m2[metaMode] != "0644" {
		t.Errorf("mode = %q, want 0644", m2[metaMode])
	}
}

// --- Retry: context cancellation paths ---

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

func TestWithRetry_ContextCanceledDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := RetryConfig{MaxAttempts: 5, InitialWait: 1, MaxWait: 1, Multiplier: 1, Jitter: 0}

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := withRetry(ctx, cfg, func() error {
		return toErr("Status=503 Service Unavailable")
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
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

func TestWithRetryResult_NonRetryable(t *testing.T) {
	cfg := RetryConfig{MaxAttempts: 3, InitialWait: 0.001, MaxWait: 0.001, Multiplier: 1, Jitter: 0}
	got, err := withRetryResult(context.Background(), cfg, func() (int, error) {
		return 0, toErr("Status=403 Forbidden")
	})
	if err == nil {
		t.Fatal("expected non-retryable error")
	}
	if got != 0 {
		t.Errorf("expected zero on non-retryable, got %d", got)
	}
}

func TestWithRetryResult_RetryableThenSuccess(t *testing.T) {
	cfg := RetryConfig{MaxAttempts: 3, InitialWait: 0.001, MaxWait: 0.001, Multiplier: 1, Jitter: 0}
	count := 0
	got, err := withRetryResult(context.Background(), cfg, func() (int, error) {
		count++
		if count < 3 {
			return 0, toErr("Status=500 Internal Server Error")
		}
		return 42, nil
	})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if got != 42 || count != 3 {
		t.Errorf("got=%d count=%d, want 42/3", got, count)
	}
}

func TestWithRetry_RetryExhausted(t *testing.T) {
	cfg := RetryConfig{MaxAttempts: 2, InitialWait: 0.001, MaxWait: 0.001, Multiplier: 1, Jitter: 0}
	err := withRetry(context.Background(), cfg, func() error {
		return toErr("Status=503")
	})
	if err == nil {
		t.Fatal("expected exhaustion error")
	}
	if !strings.HasPrefix(err.Error(), "obs:") {
		t.Errorf("expected error prefix 'obs:', got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "retry exhausted") {
		t.Errorf("expected 'retry exhausted', got %q", err.Error())
	}
}

// --- Backoff edge cases ---

func TestBackoffJitterRange(t *testing.T) {
	cfg := DefaultRetryConfig()
	for i := 0; i < 100; i++ {
		d := backoffDuration(cfg, 0)
		if d < 0 {
			t.Errorf("backoffDuration produced negative duration: %v", d)
			break
		}
	}
}

// --- Small-file writer state machine (no HTTP) ---

func TestSmallFileWriter_BufferAccumulates(t *testing.T) {
	w := newSmallFileWriter(context.Background(), nil, "bkt", "k", nil, DefaultRetryConfig())

	if _, err := w.WriteAt([]byte("hello "), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if _, err := w.WriteAt([]byte("world"), 6); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if got := w.buf.String(); got != "hello world" {
		t.Errorf("buf = %q, want %q", got, "hello world")
	}
}

func TestSmallFileWriter_WriteAfterClose(t *testing.T) {
	w := newSmallFileWriter(context.Background(), nil, "bkt", "k", nil, DefaultRetryConfig())
	w.closed = true

	_, err := w.WriteAt([]byte("x"), 0)
	if err == nil {
		t.Fatal("expected error writing to closed writer")
	}
	if !strings.Contains(err.Error(), "closed writer") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSmallFileWriter_SyncIsNoop(t *testing.T) {
	w := newSmallFileWriter(context.Background(), nil, "bkt", "k", nil, DefaultRetryConfig())
	if err := w.Sync(); err != nil {
		t.Errorf("Sync should be no-op, got %v", err)
	}
}

func TestSmallFileWriter_DoubleCloseIsNoop(t *testing.T) {
	w := newSmallFileWriter(context.Background(), nil, "bkt", "k", nil, DefaultRetryConfig())
	w.closed = true
	if err := w.Close(context.Background(), nil); err != nil {
		t.Errorf("second Close should return nil, got %v", err)
	}
}

// --- multipart writer state-machine (no HTTP) ---

func TestMultipartWriter_WriteAfterClose(t *testing.T) {
	w := &multipartFileWriter{
		closed:   true,
		retryCfg: DefaultRetryConfig(),
	}
	_, err := w.WriteAt([]byte("x"), 0)
	if err == nil {
		t.Fatal("expected error writing to closed writer")
	}
}

func TestMultipartWriter_SyncIsNoop(t *testing.T) {
	w := &multipartFileWriter{retryCfg: DefaultRetryConfig()}
	if err := w.Sync(); err != nil {
		t.Errorf("Sync should be no-op, got %v", err)
	}
}

func TestMultipartWriter_FlushEmptyIsNoop(t *testing.T) {
	w := &multipartFileWriter{retryCfg: DefaultRetryConfig()}
	// Empty partBuf → flushPart returns nil without invoking SDK.
	if err := w.flushPart(); err != nil {
		t.Errorf("flushPart on empty buffer should be nil, got %v", err)
	}
}
