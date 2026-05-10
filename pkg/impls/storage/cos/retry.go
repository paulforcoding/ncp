package cos

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

// RetryConfig controls retry behavior for COS operations.
type RetryConfig struct {
	MaxAttempts int
	InitialWait float64
	MaxWait     float64
	Multiplier  float64
	Jitter      float64
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
	case strings.Contains(errMsg, "StatusCode:404"),
		strings.Contains(errMsg, "NoSuchKey"),
		strings.Contains(errMsg, "NoSuchBucket"):
		return fmt.Errorf("%w: %s", storage.ErrNotFound, errMsg)
	case strings.Contains(errMsg, "StatusCode:403"),
		strings.Contains(errMsg, "AccessDenied"),
		strings.Contains(errMsg, "SignatureDoesNotMatch"):
		return fmt.Errorf("%w: %s", storage.ErrPermission, errMsg)
	case strings.Contains(errMsg, "StatusCode:409"):
		return fmt.Errorf("%w: %s", storage.ErrAlreadyExists, errMsg)
	case strings.Contains(errMsg, "StatusCode:400"),
		strings.Contains(errMsg, "InvalidArgument"):
		return fmt.Errorf("%w: %s", storage.ErrInvalidArgument, errMsg)
	}

	if strings.Contains(errMsg, "ChecksumMismatch") ||
		strings.Contains(errMsg, "md5 mismatch") {
		return fmt.Errorf("%w: %s", storage.ErrChecksum, errMsg)
	}

	return err
}

func retryable(err error) bool {
	if err == nil {
		return false
	}
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}
	if errors.Is(err, storage.ErrChecksum) {
		return true
	}
	if nonRetryable(err) {
		return false
	}

	errMsg := err.Error()
	switch {
	case strings.Contains(errMsg, "StatusCode:429"),
		strings.Contains(errMsg, "StatusCode:503"),
		strings.Contains(errMsg, "StatusCode:500"),
		strings.Contains(errMsg, "StatusCode:502"):
		return true
	}
	switch {
	case strings.Contains(errMsg, "RequestTimeout"),
		strings.Contains(errMsg, "InternalError"),
		strings.Contains(errMsg, "ServiceUnavailable"),
		strings.Contains(errMsg, "SlowDown"),
		strings.Contains(errMsg, "RequestTimeTooSkewed"):
		return true
	}
	if strings.Contains(errMsg, "connection reset") ||
		strings.Contains(errMsg, "connection refused") ||
		strings.Contains(errMsg, "i/o timeout") ||
		strings.Contains(errMsg, "TLS handshake") {
		return true
	}
	return false
}

func nonRetryable(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, storage.ErrPermission) ||
		errors.Is(err, storage.ErrNotFound) ||
		errors.Is(err, storage.ErrAlreadyExists) ||
		errors.Is(err, storage.ErrInvalidArgument)
}

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
	return fmt.Errorf("cos: retry exhausted after %d attempts: %w", cfg.MaxAttempts, lastErr)
}

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
	return zero, fmt.Errorf("cos: retry exhausted after %d attempts: %w", cfg.MaxAttempts, lastErr)
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
