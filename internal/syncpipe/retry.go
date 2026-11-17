// Projection retries are bounded and respect context cancellation.
package syncpipe

import (
	"context"
	"errors"
	"time"

	linkerrors "ticketpulse/internal/syncpipe/linkerrors"
)

type RetryPolicy struct {
	MaxAttempts int
	BaseBackoff time.Duration
}

func (p RetryPolicy) withDefaults() RetryPolicy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 4
	}
	if p.BaseBackoff <= 0 {
		p.BaseBackoff = 10 * time.Millisecond
	}
	return p
}

// Retry 执行 fn 并按 policy 重试。onRetry 在每次重试（非首次）前调用，
// 用于记录重试次数；onRetry 可为 nil。
func Retry(ctx context.Context, policy RetryPolicy, fn func() error, onRetry func(attempt int)) error {
	policy = policy.withDefaults()
	var last error
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		last = fn()
		if last == nil {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if errors.Is(last, context.Canceled) || errors.Is(last, context.DeadlineExceeded) {
			return last
		}
		if !linkerrors.IsTransient(last) && !linkerrors.IsRetryable(last) {
			return last
		}
		if attempt == policy.MaxAttempts {
			break
		}
		if onRetry != nil {
			onRetry(attempt + 1)
		}
		wait := policy.BaseBackoff * time.Duration(1<<(attempt-1))
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return last
}
