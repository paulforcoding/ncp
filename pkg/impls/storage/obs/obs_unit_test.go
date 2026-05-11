package obs

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/zp001/ncp/pkg/interfaces/storage"
)

func TestParseMode(t *testing.T) {
	tests := []struct {
		input string
		want  os.FileMode
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

func TestMapError(t *testing.T) {
	tests := []struct {
		input string
		want  error
	}{
		{"Status=404 Not Found", storage.ErrNotFound},
		{"StatusCode:404", storage.ErrNotFound},
		{"NoSuchKey", storage.ErrNotFound},
		{"NoSuchBucket", storage.ErrNotFound},
		{"Status=403 Forbidden", storage.ErrPermission},
		{"StatusCode:403", storage.ErrPermission},
		{"AccessDenied", storage.ErrPermission},
		{"SignatureDoesNotMatch", storage.ErrPermission},
		{"Status=409 Conflict", storage.ErrAlreadyExists},
		{"StatusCode:409", storage.ErrAlreadyExists},
		{"Status=400 Bad Request", storage.ErrInvalidArgument},
		{"StatusCode:400", storage.ErrInvalidArgument},
		{"InvalidArgument", storage.ErrInvalidArgument},
		{"ChecksumMismatch", storage.ErrChecksum},
		{"md5 mismatch", storage.ErrChecksum},
		{"Status=500 Internal Server Error", nil},
		{"some unknown", nil},
	}
	for _, tt := range tests {
		orig := toErr(tt.input)
		got := mapError(orig)
		if tt.want == nil {
			if got != orig {
				t.Errorf("mapError(%q) expected no wrap, got %v", tt.input, got)
			}
			continue
		}
		if !errors.Is(got, tt.want) {
			t.Errorf("mapError(%q) should wrap %v, got %v", tt.input, tt.want, got)
		}
	}
}

func TestRetryable(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{toErr("Status=429 Too Many Requests"), true},
		{toErr("Status=503 Service Unavailable"), true},
		{toErr("Status=500 Internal Server Error"), true},
		{toErr("Status=502 Bad Gateway"), true},
		{toErr("StatusCode:429"), true},
		{toErr("StatusCode:503"), true},
		{toErr("StatusCode:500"), true},
		{toErr("StatusCode:502"), true},
		{storage.ErrPermission, false},
		{storage.ErrNotFound, false},
		{storage.ErrAlreadyExists, false},
		{storage.ErrInvalidArgument, false},
		{toErr("RequestTimeout"), true},
		{toErr("InternalError"), true},
		{toErr("ServiceUnavailable"), true},
		{toErr("SlowDown"), true},
		{storage.ErrChecksum, true},
		{toErr("connection reset"), true},
		{toErr("i/o timeout"), true},
		{toErr("TLS handshake"), true},
		{toErr("some unknown error"), false},
	}
	for _, tt := range tests {
		got := retryable(tt.err)
		if got != tt.want {
			t.Errorf("retryable(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

func TestNonRetryable(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{storage.ErrPermission, true},
		{storage.ErrNotFound, true},
		{storage.ErrAlreadyExists, true},
		{storage.ErrInvalidArgument, true},
		{toErr("Status=500 Internal Server Error"), false},
		{toErr("StatusCode:500"), false},
		{toErr("SlowDown"), false},
	}
	for _, tt := range tests {
		got := nonRetryable(tt.err)
		if got != tt.want {
			t.Errorf("nonRetryable(%v) = %v, want %v", tt.err, got, tt.want)
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
	if !errors.Is(err, storage.ErrPermission) {
		t.Errorf("expected wrapped ErrPermission, got %v", err)
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
