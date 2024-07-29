/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package throttle

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

const (
	throttleCheckDuration = 250 * time.Millisecond
)

var (
	throttleTicks    int64
	throttleInit     sync.Once
	emptyCheckResult = &CheckResult{}
)

func initThrottleTicker() {
	throttleInit.Do(func() {
		throttleTicks = 1 // above zero so that `lastSuccessfulThrottle` is initially stale
		go func() {
			tick := time.NewTicker(throttleCheckDuration)
			defer tick.Stop()
			for range tick.C {
				atomic.AddInt64(&throttleTicks, 1)
			}
		}()
	})
}

// Client construct is used by apps who wish to consult with a throttler. It encapsulates the check/throttling/backoff logic
type Client struct {
	throttler *Throttler
	appName   throttlerapp.Name
	flags     CheckFlags

	lastSuccessfulThrottleMu sync.Mutex
	// lastSuccessfulThrottle records the latest tick (value of `throttleTicks`) when the throttler was
	// satisfied for a given metric. This makes it possible to potentially skip a throttler check.
	// key is the app name.
	lastSuccessfulThrottle map[string]int64
}

// NewBackgroundClient creates a client suitable for background jobs, which have low priority over production traffic,
// e.g. migration, table pruning, vreplication
func NewBackgroundClient(throttler *Throttler, appName throttlerapp.Name, scope base.Scope) *Client {
	initThrottleTicker()
	return &Client{
		throttler: throttler,
		appName:   appName,
		flags: CheckFlags{
			Scope:               scope,
			MultiMetricsEnabled: true,
		},
		lastSuccessfulThrottle: make(map[string]int64),
	}
}

// clearSuccessfulResultsCache clears lastSuccessfulThrottleMu, so that the next check will be fresh.
func (c *Client) clearSuccessfulResultsCache() {
	c.lastSuccessfulThrottleMu.Lock()
	defer c.lastSuccessfulThrottleMu.Unlock()
	c.lastSuccessfulThrottle = make(map[string]int64)
}

// ThrottleCheckOK checks the throttler, and returns 'true' when the throttler is satisfied.
// It does not sleep.
// The function caches results for a brief amount of time, hence it's safe and efficient to
// be called very frequently.
// The function is not thread safe.
func (c *Client) ThrottleCheckOK(ctx context.Context, overrideAppName throttlerapp.Name) (checkResult *CheckResult, throttleCheckOK bool) {
	if c == nil {
		// no client
		return emptyCheckResult, true
	}
	if c.throttler == nil {
		// no throttler
		return emptyCheckResult, true
	}
	checkApp := c.appName
	if overrideAppName != "" {
		checkApp = overrideAppName
	}
	c.lastSuccessfulThrottleMu.Lock()
	defer c.lastSuccessfulThrottleMu.Unlock()
	if c.lastSuccessfulThrottle[checkApp.String()] >= atomic.LoadInt64(&throttleTicks) {
		// if last check was OK just very recently there is no need to check again
		return emptyCheckResult, true
	}
	// It's time to run a throttler check
	checkResult = c.throttler.Check(ctx, checkApp.String(), nil, &c.flags)
	if !checkResult.IsOK() {
		return checkResult, false
	}
	for _, metricResult := range checkResult.Metrics {
		if !metricResult.IsOK() {
			return checkResult, false
		}
	}
	c.lastSuccessfulThrottle[checkApp.String()] = atomic.LoadInt64(&throttleTicks)
	return checkResult, true

}

// ThrottleCheckOKOrWait checks the throttler; if throttler is satisfied, the function returns 'true' immediately,
// otherwise it briefly sleeps and returns 'false'.
// Non-empty appName overrides the default appName.
// The function is not thread safe.
func (c *Client) ThrottleCheckOKOrWaitAppName(ctx context.Context, appName throttlerapp.Name) (checkResult *CheckResult, throttleCheckOK bool) {
	checkResult, throttleCheckOK = c.ThrottleCheckOK(ctx, appName)
	if throttleCheckOK {
		return checkResult, true
	}
	if ctx.Err() != nil {
		// context expired, skip sleeping
		return checkResult, false
	}
	time.Sleep(throttleCheckDuration)
	return checkResult, false
}

// ThrottleCheckOKOrWait checks the throttler; if throttler is satisfied, the function returns 'true' immediately,
// otherwise it briefly sleeps and returns 'false'.
// The function is not thread safe.
func (c *Client) ThrottleCheckOKOrWait(ctx context.Context) (checkResult *CheckResult, throttleCheckOK bool) {
	return c.ThrottleCheckOKOrWaitAppName(ctx, "")
}

// Throttle throttles until the throttler is satisfied, or until context is cancelled.
// The function sleeps between throttle checks.
// The function is not thread safe.
func (c *Client) Throttle(ctx context.Context) {
	for {
		if _, ok := c.ThrottleCheckOKOrWait(ctx); ok {
			return
		}
		// The function incorporates a bit of sleep so this is not a busy wait.
	}
}
