package tabletthrottler

import (
	"context"
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

// ThrottleClientWrapper defines the methods we use from a Throttler client.
// This is used to make the code testable.
type ThrottleClientWrapper interface {
	ThrottleCheckOK(ctx context.Context, overrideAppName throttlerapp.Name) (checkResult *throttle.CheckResult, throttleCheckOK bool)
}

// assert that throttle.Client implements ThrottleClientWrapper.
var _ ThrottleClientWrapper = (*throttle.Client)(nil)

// assert that FakeThrottleClientWrapper implements ThrottleClientWrapper.
var _ ThrottleClientWrapper = (*FakeThrottleClientWrapper)(nil)

// FakeThrottleClientWrapper is a test fake that implements ThrottleClientWrapper.
// It is thread-safe to prevent race conditions during testing.
type FakeThrottleClientWrapper struct {
	mu              sync.RWMutex
	checkResult     *throttle.CheckResult
	throttleCheckOK bool
	callCount       atomic.Int64
}

// NewFakeThrottleClientWrapper creates a fake throttle client wrapper
// with a fully constructed CheckResult and success/failure flag.
func NewFakeThrottleClientWrapper(checkResult *throttle.CheckResult, ok bool) *FakeThrottleClientWrapper {
	return &FakeThrottleClientWrapper{
		checkResult:     checkResult,
		throttleCheckOK: ok,
	}
}

// ThrottleCheckOK implements the ThrottleClientWrapper interface.
// Thread-safe method that uses read lock for concurrent access.
func (f *FakeThrottleClientWrapper) ThrottleCheckOK(ctx context.Context, overrideAppName throttlerapp.Name) (*throttle.CheckResult, bool) {
	f.callCount.Add(1)
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.checkResult, f.throttleCheckOK
}

// GetCallCount returns the number of times ThrottleCheckOK was called (for testing).
func (f *FakeThrottleClientWrapper) GetCallCount() int {
	return int(f.callCount.Load())
}

// ResetCallCount resets the call counter (for testing).
func (f *FakeThrottleClientWrapper) ResetCallCount() {
	f.callCount.Store(0)
}

// SetCheckResult safely updates the checkResult and throttleCheckOK using write lock.
// This method should be used by tests instead of directly modifying fields.
func (f *FakeThrottleClientWrapper) SetCheckResult(result *throttle.CheckResult, ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checkResult = result
	f.throttleCheckOK = ok
}

// GetCheckResult safely reads the current checkResult and throttleCheckOK using read lock.
// This method is provided for testing purposes.
func (f *FakeThrottleClientWrapper) GetCheckResult() (*throttle.CheckResult, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.checkResult, f.throttleCheckOK
}
