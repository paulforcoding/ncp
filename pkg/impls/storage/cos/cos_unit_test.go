package cos

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestParseMode(t *testing.T) {
	tests := []struct {
		input string
		want  uint32
	}{
		{"0755", 0755},
		{"0644", 0644},
		{"0600", 0600},
		{"", 0},
		{"invalid", 0},
	}
	for _, tt := range tests {
		got := parseMode(tt.input)
		if got != tt.want {
			t.Errorf("parseMode(%q) = %o, want %o", tt.input, got, tt.want)
		}
	}
}

func TestParseInt(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"1000", 1000},
		{"0", 0},
		{"", 0},
		{"abc", 0},
	}
	for _, tt := range tests {
		got := parseInt(tt.input)
		if got != tt.want {
			t.Errorf("parseInt(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestParseInt64(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"1000", 1000},
		{"0", 0},
		{"", 0},
		{"abc", 0},
	}
	for _, tt := range tests {
		got := parseInt64(tt.input)
		if got != tt.want {
			t.Errorf("parseInt64(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestRetryable(t *testing.T) {
	tests := []struct {
		errMsg string
		want   bool
	}{
		{"StatusCode:429", true},
		{"StatusCode:503", true},
		{"StatusCode:500", true},
		{"StatusCode:403", false},
		{"StatusCode:404", false},
		{"StatusCode:400", false},
		{"AccessDenied", false},
		{"NoSuchBucket", false},
		{"RequestTimeout", true},
		{"InternalError", true},
		{"SlowDown", true},
		{"ChecksumMismatch", true},
		{"connection reset", true},
		{"i/o timeout", true},
		{"SignatureDoesNotMatch", false},
		{"some unknown error", false},
	}
	for _, tt := range tests {
		got := retryable(toErr(tt.errMsg))
		if got != tt.want {
			t.Errorf("retryable(%q) = %v, want %v", tt.errMsg, got, tt.want)
		}
	}
}

func TestNonRetryable(t *testing.T) {
	tests := []struct {
		errMsg string
		want   bool
	}{
		{"StatusCode:403", true},
		{"StatusCode:404", true},
		{"StatusCode:400", true},
		{"AccessDenied", true},
		{"NoSuchBucket", true},
		{"NoSuchKey", true},
		{"InvalidArgument", true},
		{"SignatureDoesNotMatch", true},
		{"StatusCode:500", false},
		{"SlowDown", false},
	}
	for _, tt := range tests {
		got := nonRetryable(toErr(tt.errMsg))
		if got != tt.want {
			t.Errorf("nonRetryable(%q) = %v, want %v", tt.errMsg, got, tt.want)
		}
	}
}

func TestBackoffDuration(t *testing.T) {
	cfg := DefaultRetryConfig()

	d0 := backoffDuration(cfg, 0)
	if d0 < time.Duration(cfg.InitialWait*0.5*float64(time.Second)) ||
		d0 > time.Duration(cfg.InitialWait*2*float64(time.Second)) {
		t.Errorf("backoffDuration(0) = %v, expected ~%v", d0, time.Duration(cfg.InitialWait)*time.Second)
	}

	d3 := backoffDuration(cfg, 3)
	if d3 <= d0 {
		t.Errorf("backoffDuration(3) = %v should be > backoffDuration(0) = %v", d3, d0)
	}

	d10 := backoffDuration(cfg, 10)
	if d10 > time.Duration(cfg.MaxWait*1.5*float64(time.Second)) {
		t.Errorf("backoffDuration(10) = %v should be capped near MaxWait %v", d10, time.Duration(cfg.MaxWait)*time.Second)
	}
}

func TestWithRetry(t *testing.T) {
	cfg := RetryConfig{MaxAttempts: 3, InitialWait: 0.01, MaxWait: 0.01, Multiplier: 1, Jitter: 0}

	err := withRetry(context.Background(), cfg, func() error { return nil })
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}

	err = withRetry(context.Background(), cfg, func() error {
		return toErr("StatusCode:403")
	})
	if err == nil {
		t.Error("expected error for non-retryable")
	}

	count := 0
	err = withRetry(context.Background(), cfg, func() error {
		count++
		if count < 3 {
			return toErr("StatusCode:500")
		}
		return nil
	})
	if err != nil {
		t.Errorf("expected nil after retries, got %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 attempts, got %d", count)
	}
}

func TestExtractMetadata(t *testing.T) {
	h := make(http.Header)
	h.Set("x-cos-meta-ncp-mode", "0644")
	h.Set("x-cos-meta-ncp-uid", "1000")
	h.Set("x-cos-meta-ncp-gid", "1000")
	h.Set("Content-Type", "application/octet-stream")

	meta := extractMetadata(h)
	if meta["ncp-mode"] != "0644" {
		t.Errorf("expected ncp-mode=0644, got %q", meta["ncp-mode"])
	}
	if meta["ncp-uid"] != "1000" {
		t.Errorf("expected ncp-uid=1000, got %q", meta["ncp-uid"])
	}
	if meta["ncp-gid"] != "1000" {
		t.Errorf("expected ncp-gid=1000, got %q", meta["ncp-gid"])
	}
	if meta["Content-Type"] != "" {
		t.Errorf("expected Content-Type not in metadata, got %q", meta["Content-Type"])
	}
}

func TestBuildMetaHeader(t *testing.T) {
	meta := map[string]string{
		"ncp-mode": "0644",
		"ncp-uid":  "1000",
	}
	h := buildMetaHeader(meta)
	if h == nil {
		t.Fatal("expected non-nil header")
	}
	if h.Get("ncp-mode") != "0644" {
		t.Errorf("expected ncp-mode=0644, got %q", h.Get("ncp-mode"))
	}
	if h.Get("ncp-uid") != "1000" {
		t.Errorf("expected ncp-uid=1000, got %q", h.Get("ncp-uid"))
	}
}

type testErr struct{ msg string }

func (e *testErr) Error() string { return e.msg }

func toErr(msg string) error { return &testErr{msg: msg} }

func TestSource_Base(t *testing.T) {
	s := &Source{bucket: "my-bucket-1250000000", prefix: "backup/"}
	want := "cos://my-bucket-1250000000/backup/"
	if got := s.Base(); got != want {
		t.Errorf("Base()=%q, want %q", got, want)
	}
}

func TestDestination_key(t *testing.T) {
	d := &Destination{bucket: "bkt", prefix: "prefix/"}
	if got := d.key("file.txt"); got != "prefix/file.txt" {
		t.Errorf("key(file.txt)=%q, want prefix/file.txt", got)
	}
	if got := d.key("subdir/file.txt"); got != "prefix/subdir/file.txt" {
		t.Errorf("key(subdir/file.txt)=%q, want prefix/subdir/file.txt", got)
	}
}

func TestPosixMetadata(t *testing.T) {
	meta := posixMetadata(0o755, 1000, 1000)
	if meta["ncp-mode"] != "0755" {
		t.Errorf("expected mode=0755, got %q", meta["ncp-mode"])
	}
	if meta["ncp-uid"] != "1000" {
		t.Errorf("expected uid=1000, got %q", meta["ncp-uid"])
	}
	if meta["ncp-gid"] != "1000" {
		t.Errorf("expected gid=1000, got %q", meta["ncp-gid"])
	}

	meta2 := posixMetadata(0o644, 0, 0)
	if meta2["ncp-mode"] != "0644" {
		t.Errorf("expected mode=0644, got %q", meta2["ncp-mode"])
	}
}

func TestWithRetryResult_Success(t *testing.T) {
	cfg := RetryConfig{MaxAttempts: 1, InitialWait: 0.01, MaxWait: 0.01, Multiplier: 1, Jitter: 0}
	got, err := withRetryResult(context.Background(), cfg, func() (int, error) { return 42, nil })
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got != 42 {
		t.Errorf("expected 42, got %d", got)
	}
}

func TestWithRetryResult_NonRetryable(t *testing.T) {
	cfg := RetryConfig{MaxAttempts: 3, InitialWait: 0.01, MaxWait: 0.01, Multiplier: 1, Jitter: 0}
	_, err := withRetryResult(context.Background(), cfg, func() (string, error) {
		return "", toErr("StatusCode:403")
	})
	if err == nil {
		t.Fatal("expected error for non-retryable")
	}
	if !strings.Contains(err.Error(), "StatusCode:403") {
		t.Errorf("expected original error, got %v", err)
	}
}

func TestWithRetryResult_RetryableThenSuccess(t *testing.T) {
	cfg := RetryConfig{MaxAttempts: 3, InitialWait: 0.01, MaxWait: 0.01, Multiplier: 1, Jitter: 0}
	count := 0
	got, err := withRetryResult(context.Background(), cfg, func() (int, error) {
		count++
		if count < 3 {
			return 0, toErr("StatusCode:500")
		}
		return 99, nil
	})
	if err != nil {
		t.Fatalf("expected nil after retries, got %v", err)
	}
	if got != 99 {
		t.Errorf("expected 99, got %d", got)
	}
	if count != 3 {
		t.Errorf("expected 3 attempts, got %d", count)
	}
}

func TestRetryExhausted(t *testing.T) {
	cfg := RetryConfig{MaxAttempts: 2, InitialWait: 0.001, MaxWait: 0.001, Multiplier: 1, Jitter: 0}
	err := withRetry(context.Background(), cfg, func() error {
		return toErr("StatusCode:503")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.HasPrefix(err.Error(), "cos:") {
		t.Errorf("expected error to start with 'cos:', got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "retry exhausted") {
		t.Errorf("expected 'retry exhausted' in error, got %q", err.Error())
	}
}

func TestBackoffJitterRange(t *testing.T) {
	cfg := DefaultRetryConfig()
	base := cfg.InitialWait
	for i := 0; i < 100; i++ {
		d := backoffDuration(cfg, 0)
		secs := d.Seconds()
		minVal := base * (1 - cfg.Jitter)
		maxVal := base * (1 + cfg.Jitter)
		if secs < minVal*0.5 || secs > maxVal*2 {
			t.Errorf("backoffDuration(0) = %v, expected roughly in [%v, %v]", d, minVal, maxVal)
			break
		}
	}
}

func TestExtractMetadata_CaseInsensitive(t *testing.T) {
	h := make(http.Header)
	h.Set("X-COS-META-NCP-Mode", "0755")
	h.Set("x-cos-meta-ncp-uid", "1000")

	meta := extractMetadata(h)
	if meta["ncp-mode"] != "0755" {
		t.Errorf("expected ncp-mode=0755 (case insensitive), got %q", meta["ncp-mode"])
	}
	if meta["ncp-uid"] != "1000" {
		t.Errorf("expected ncp-uid=1000, got %q", meta["ncp-uid"])
	}
}

func TestExtractMetadata_EmptyValue(t *testing.T) {
	h := make(http.Header)
	h.Set("x-cos-meta-ncp-mode", "0644")
	h.Add("x-cos-meta-ncp-empty", "")

	meta := extractMetadata(h)
	if meta["ncp-mode"] != "0644" {
		t.Errorf("expected ncp-mode=0644, got %q", meta["ncp-mode"])
	}
	// Empty value headers may or may not be present depending on http.Header behavior
	// The important thing is non-empty values are extracted correctly
}
