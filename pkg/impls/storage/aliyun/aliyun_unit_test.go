package aliyun

import (
	"context"
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

	// First attempt should be close to InitialWait
	d0 := backoffDuration(cfg, 0)
	if d0 < time.Duration(cfg.InitialWait*0.5*float64(time.Second)) ||
		d0 > time.Duration(cfg.InitialWait*2*float64(time.Second)) {
		t.Errorf("backoffDuration(0) = %v, expected ~%v", d0, time.Duration(cfg.InitialWait)*time.Second)
	}

	// Should increase exponentially
	d3 := backoffDuration(cfg, 3)
	if d3 <= d0 {
		t.Errorf("backoffDuration(3) = %v should be > backoffDuration(0) = %v", d3, d0)
	}

	// Should cap at MaxWait
	d10 := backoffDuration(cfg, 10)
	if d10 > time.Duration(cfg.MaxWait*1.5*float64(time.Second)) {
		t.Errorf("backoffDuration(10) = %v should be capped near MaxWait %v", d10, time.Duration(cfg.MaxWait)*time.Second)
	}
}

func TestWithRetry(t *testing.T) {
	cfg := RetryConfig{MaxAttempts: 3, InitialWait: 0.01, MaxWait: 0.01, Multiplier: 1, Jitter: 0}

	// Success on first try
	err := withRetry(context.Background(), cfg, func() error { return nil })
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}

	// Non-retryable error fails immediately
	err = withRetry(context.Background(), cfg, func() error {
		return toErr("StatusCode:403")
	})
	if err == nil {
		t.Error("expected error for non-retryable")
	}

	// Retryable error eventually succeeds
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

type testErr struct{ msg string }

func (e *testErr) Error() string { return e.msg }

func toErr(msg string) error { return &testErr{msg: msg} }
