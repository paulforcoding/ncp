package aliyun

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"strings"
	"time"
)

// RetryConfig controls retry behavior for OSS operations.
type RetryConfig struct {
	MaxAttempts int     // maximum retry attempts (default: 5)
	InitialWait float64 // initial backoff in seconds (default: 1)
	MaxWait     float64 // maximum backoff in seconds (default: 60)
	Multiplier  float64 // backoff multiplier (default: 2)
	Jitter      float64 // jitter fraction ± (default: 0.2)
}

// DefaultRetryConfig returns the default retry configuration.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts: 5,
		InitialWait: 1,
		MaxWait:     60,
		Multiplier:  2,
		Jitter:      0.2,
	}
}

// retryable checks if an error is retryable based on OSS error semantics.
func retryable(err error) bool {
	if err == nil {
		return false
	}

	// Network timeouts
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}

	errMsg := err.Error()

	// HTTP status codes
	switch {
	case strings.Contains(errMsg, "StatusCode:429"),
		strings.Contains(errMsg, "StatusCode:503"),
		strings.Contains(errMsg, "StatusCode:500"),
		strings.Contains(errMsg, "StatusCode:502"):
		return true
	}

	// OSS error codes
	switch {
	case strings.Contains(errMsg, "RequestTimeout"),
		strings.Contains(errMsg, "InternalError"),
		strings.Contains(errMsg, "ServiceUnavailable"),
		strings.Contains(errMsg, "SlowDown"),
		strings.Contains(errMsg, "RequestTimeTooSkewed"):
		return true
	}

	// ChecksumMismatch is retryable
	if strings.Contains(errMsg, "ChecksumMismatch") ||
		strings.Contains(errMsg, "md5 mismatch") {
		return true
	}

	// Connection errors
	if strings.Contains(errMsg, "connection reset") ||
		strings.Contains(errMsg, "connection refused") ||
		strings.Contains(errMsg, "i/o timeout") ||
		strings.Contains(errMsg, "TLS handshake") {
		return true
	}

	return false
}

// nonRetryable checks if an error is definitely not retryable.
func nonRetryable(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()

	switch {
	case strings.Contains(errMsg, "StatusCode:403"),
		strings.Contains(errMsg, "StatusCode:404"),
		strings.Contains(errMsg, "StatusCode:400"),
		strings.Contains(errMsg, "StatusCode:409"):
		return true
	case strings.Contains(errMsg, "AccessDenied"),
		strings.Contains(errMsg, "NoSuchBucket"),
		strings.Contains(errMsg, "NoSuchKey"),
		strings.Contains(errMsg, "InvalidArgument"),
		strings.Contains(errMsg, "SignatureDoesNotMatch"):
		return true
	}
	return false
}

// withRetry executes fn with exponential backoff retry on retryable errors.
func withRetry(ctx context.Context, cfg RetryConfig, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt <= cfg.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		// Don't retry non-retryable errors
		if nonRetryable(lastErr) {
			return lastErr
		}

		// Don't retry if not retryable
		if !retryable(lastErr) {
			return lastErr
		}

		// Last attempt — return the error
		if attempt == cfg.MaxAttempts {
			break
		}

		wait := backoffDuration(cfg, attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return fmt.Errorf("aliyun: retry exhausted after %d attempts: %w", cfg.MaxAttempts, lastErr)
}

// withRetryResult executes fn with exponential backoff retry, returning a result.
func withRetryResult[T any](ctx context.Context, cfg RetryConfig, fn func() (T, error)) (T, error) {
	var lastErr error
	var result T
	for attempt := 0; attempt <= cfg.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			var zero T
			return zero, err
		}

		result, lastErr = fn()
		if lastErr == nil {
			return result, nil
		}

		if nonRetryable(lastErr) {
			var zero T
			return zero, lastErr
		}

		if !retryable(lastErr) {
			var zero T
			return zero, lastErr
		}

		if attempt == cfg.MaxAttempts {
			break
		}

		wait := backoffDuration(cfg, attempt)
		select {
		case <-ctx.Done():
			var zero T
			return zero, ctx.Err()
		case <-time.After(wait):
		}
	}
	var zero T
	return zero, fmt.Errorf("aliyun: retry exhausted after %d attempts: %w", cfg.MaxAttempts, lastErr)
}

func backoffDuration(cfg RetryConfig, attempt int) time.Duration {
	wait := cfg.InitialWait * math.Pow(cfg.Multiplier, float64(attempt))
	if wait > cfg.MaxWait {
		wait = cfg.MaxWait
	}
	// Apply jitter: ±Jitter fraction
	jitter := wait * cfg.Jitter * (2*rand.Float64() - 1)
	wait += jitter
	if wait < 0 {
		wait = cfg.InitialWait
	}
	return time.Duration(wait * float64(time.Second))
}
