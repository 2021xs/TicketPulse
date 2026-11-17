package syncpipe

import (
	"context"
	"errors"
	"testing"
	"time"

	"ticketpulse/internal/syncpipe/linkerrors"
)

func TestRetryClassificationAndCancellation(t *testing.T) {
	attempts := 0
	err := Retry(context.Background(), RetryPolicy{MaxAttempts: 3, BaseBackoff: time.Millisecond}, func() error {
		attempts++
		if attempts < 3 {
			return linkerrors.Transient("temporary", errors.New("offline"))
		}
		return nil
	}, nil)
	if err != nil || attempts != 3 {
		t.Fatalf("retry=%d %v", attempts, err)
	}
	attempts = 0
	err = Retry(context.Background(), RetryPolicy{MaxAttempts: 3, BaseBackoff: time.Millisecond}, func() error { attempts++; return linkerrors.Permanent("mapping", errors.New("invalid")) }, nil)
	if err == nil || attempts != 1 {
		t.Fatalf("permanent=%d %v", attempts, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Retry(ctx, RetryPolicy{}, func() error { t.Fatal("executed after cancellation"); return nil }, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
