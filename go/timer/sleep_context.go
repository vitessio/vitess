package timer

import (
	"context"
	"time"
)

// SleepContext sleeps for the specified time period.
// If the context expires early, it returns an error.
func SleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
