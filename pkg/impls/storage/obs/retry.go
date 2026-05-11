package obs

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"strings"
	"time"

	"github.com/zp001/ncp/pkg/interfaces/storage"
)

// RetryConfig controls retry behavior for OBS operations.
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

// mapError converts SDK string errors into sentinel errors so that callers
// can use errors.Is instead of string matching.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, storage.ErrNotFound) ||
		errors.Is(err, storage.ErrPermission) ||
		errors.Is(err, storage.ErrAlreadyExists) ||
		errors.Is(err, storage.ErrInvalidArgument) ||
		errors.Is(err, storage.ErrChecksum) {
		return err
	}

	errMsg := err.Error()

	switch {
	case strings.Contains(errMsg, "Status=404"),
		strings.Contains(errMsg, "StatusCode:404"),
		strings.Contains(errMsg, "NoSuchKey"),
		strings.Contains(errMsg, "NoSuchBucket"):
		return fmt.Errorf("%w: %s", storage.ErrNotFound, errMsg)
	case strings.Contains(errMsg, "Status=403"),
		strings.Contains(errMsg, "StatusCode:403"),
		strings.Contains(errMsg, "AccessDenied"),
		strings.Contains(errMsg, "SignatureDoesNotMatch"):
		return fmt.Errorf("%w: %s", storage.ErrPermission, errMsg)
	case strings.Contains(errMsg, "Status=409"),
		strings.Contains(errMsg, "StatusCode:409"):
		return fmt.Errorf("%w: %s", storage.ErrAlreadyExists, errMsg)
	case strings.Contains(errMsg, "Status=400"),
		strings.Contains(errMsg, "StatusCode:400"),
		strings.Contains(errMsg, "InvalidArgument"):
		return fmt.Errorf("%w: %s", storage.ErrInvalidArgument, errMsg)
	}

	if strings.Contains(errMsg, "ChecksumMismatch") ||
		strings.Contains(errMsg, "md5 mismatch") {
		return fmt.Errorf("%w: %s", storage.ErrChecksum, errMsg)
	}

	return err
}

// retryable checks if an error is retryable based on OBS error semantics.
func retryable(err error) bool {
	if err == nil {
		return false
	}

	// Network timeouts
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}

	// Checksum mismatch is retryable
	if errors.Is(err, storage.ErrChecksum) {
		return true
	}

	// If already classified as non-retryable, don't retry
	if nonRetryable(err) {
		return false
	}

	errMsg := err.Error()

	// HTTP status codes — OBS uses "Status=NNN ..."; keep StatusCode:NNN for parity
	// with cross-backend error wrapping.
	switch {
	case strings.Contains(errMsg, "Status=429"),
		strings.Contains(errMsg, "Status=500"),
		strings.Contains(errMsg, "Status=502"),
		strings.Contains(errMsg, "Status=503"),
		strings.Contains(errMsg, "StatusCode:429"),
		strings.Contains(errMsg, "StatusCode:500"),
		strings.Contains(errMsg, "StatusCode:502"),
		strings.Contains(errMsg, "StatusCode:503"):
		return true
	}

	// OBS error codes (same names as OSS)
	switch {
	case strings.Contains(errMsg, "RequestTimeout"),
		strings.Contains(errMsg, "InternalError"),
		strings.Contains(errMsg, "ServiceUnavailable"),
		strings.Contains(errMsg, "SlowDown"),
		strings.Contains(errMsg, "RequestTimeTooSkewed"):
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
	return errors.Is(err, storage.ErrPermission) ||
		errors.Is(err, storage.ErrNotFound) ||
		errors.Is(err, storage.ErrAlreadyExists) ||
		errors.Is(err, storage.ErrInvalidArgument)
}

// withRetry executes fn with exponential backoff retry on retryable errors.
func withRetry(ctx context.Context, cfg RetryConfig, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt <= cfg.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		lastErr = mapError(fn())
		if lastErr == nil {
			return nil
		}

		if nonRetryable(lastErr) {
			return lastErr
		}

		if !retryable(lastErr) {
			return lastErr
		}

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
	return fmt.Errorf("obs: retry exhausted after %d attempts: %w", cfg.MaxAttempts, lastErr)
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
		lastErr = mapError(lastErr)
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
	return zero, fmt.Errorf("obs: retry exhausted after %d attempts: %w", cfg.MaxAttempts, lastErr)
}

func backoffDuration(cfg RetryConfig, attempt int) time.Duration {
	wait := cfg.InitialWait * math.Pow(cfg.Multiplier, float64(attempt))
	if wait > cfg.MaxWait {
		wait = cfg.MaxWait
	}
	jitter := wait * cfg.Jitter * (2*rand.Float64() - 1)
	wait += jitter
	if wait < 0 {
		wait = cfg.InitialWait
	}
	return time.Duration(wait * float64(time.Second))
}
