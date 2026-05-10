package obs

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
		// OBS-native error format: "Status=NNN ..."
		{"obs: service returned error: Status=429 Too Many Requests, Code=", true},
		{"Status=503 Service Unavailable", true},
		{"Status=500 Internal Server Error", true},
		{"Status=502 Bad Gateway", true},
		{"Status=403 Forbidden", false},
		{"Status=404 Not Found", false},
		{"Status=400 Bad Request", false},
		// Cross-backend StatusCode:NNN format
		{"StatusCode:429", true},
		{"StatusCode:503", true},
		{"StatusCode:500", true},
		{"StatusCode:403", false},
		{"StatusCode:404", false},
		{"StatusCode:400", false},
		// OBS error codes (same names as OSS)
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
		// OBS-native format
		{"Status=403 Forbidden", true},
		{"Status=404 Not Found", true},
		{"Status=400 Bad Request", true},
		{"Status=409 Conflict", true},
		// Cross-backend format
		{"StatusCode:403", true},
		{"StatusCode:404", true},
		{"StatusCode:400", true},
		{"StatusCode:409", true},
		// Error codes
		{"AccessDenied", true},
		{"NoSuchBucket", true},
		{"NoSuchKey", true},
		{"InvalidArgument", true},
		{"SignatureDoesNotMatch", true},
		// Retryable should not be flagged here
		{"Status=500 Internal Server Error", false},
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

	// Non-retryable in OBS-native format
	err = withRetry(context.Background(), cfg, func() error {
		return toErr("Status=403 Forbidden")
	})
	if err == nil {
		t.Error("expected error for non-retryable")
	}

	// Retryable in OBS-native format
	count := 0
	err = withRetry(context.Background(), cfg, func() error {
		count++
		if count < 3 {
			return toErr("Status=500 Internal Server Error")
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
