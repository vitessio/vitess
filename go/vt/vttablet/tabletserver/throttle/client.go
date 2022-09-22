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
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	throttleCheckDuration = 250 * time.Millisecond
)

var throttleTicks int64
var throttleInit sync.Once

func initThrottleTicker() {
	throttleInit.Do(func() {
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
	appName   string
	checkType ThrottleCheckType
	flags     CheckFlags

	lastSuccessfulThrottle int64
}

// NewProductionClient creates a client suitable for foreground/production jobs, which have normal priority.
func NewProductionClient(throttler *Throttler, appName string, checkType ThrottleCheckType) *Client {
	initThrottleTicker()
	return &Client{
		throttler: throttler,
		appName:   appName,
		checkType: checkType,
		flags: CheckFlags{
			LowPriority: false,
		},
	}
}

// NewBackgroundClient creates a client suitable for background jobs, which have low priority over productio ntraffic,
// e.g. migration, table pruning, vreplication
func NewBackgroundClient(throttler *Throttler, appName string, checkType ThrottleCheckType) *Client {
	initThrottleTicker()
	return &Client{
		throttler: throttler,
		appName:   appName,
		checkType: checkType,
		flags: CheckFlags{
			LowPriority: true,
		},
	}
}

// ThrottleCheckOK checks the throttler, and returns 'true' when the throttler is satisfied.
// It does not sleep.
// The function caches results for a brief amount of time, hence it's safe and efficient to
// be called very frequenty.
// The function is not thread safe.
func (c *Client) ThrottleCheckOK(ctx context.Context, overrideAppName string) (throttleCheckOK bool) {
	if c == nil {
		// no client
		return true
	}
	if c.throttler == nil {
		// no throttler
		return true
	}
	if c.lastSuccessfulThrottle >= atomic.LoadInt64(&throttleTicks) {
		// if last check was OK just very recently there is no need to check again
		return true
	}
	// It's time to run a throttler check
	checkApp := c.appName
	if overrideAppName != "" {
		checkApp = overrideAppName
	}
	checkResult := c.throttler.CheckByType(ctx, checkApp, "", &c.flags, c.checkType)
	if checkResult.StatusCode != http.StatusOK {
		return false
	}
	c.lastSuccessfulThrottle = atomic.LoadInt64(&throttleTicks)
	return true

}

// ThrottleCheckOKOrWait checks the throttler; if throttler is satisfied, the function returns 'true' mmediately,
// otherwise it briefly sleeps and returns 'false'.
// Non-empty appName overrides the default appName.
// The function is not thread safe.
func (c *Client) ThrottleCheckOKOrWaitAppName(ctx context.Context, appName string) bool {
	ok := c.ThrottleCheckOK(ctx, appName)
	if !ok {
		time.Sleep(throttleCheckDuration)
	}
	return ok
}

// ThrottleCheckOKOrWait checks the throttler; if throttler is satisfied, the function returns 'true' mmediately,
// otherwise it briefly sleeps and returns 'false'.
// The function is not thread safe.
func (c *Client) ThrottleCheckOKOrWait(ctx context.Context) bool {
	return c.ThrottleCheckOKOrWaitAppName(ctx, "")
}

// Throttle throttles until the throttler is satisfied, or until context is cancelled.
// The function sleeps between throttle checks.
// The function is not thread safe.
func (c *Client) Throttle(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if c.ThrottleCheckOKOrWait(ctx) {
			break
		}
	}
}
