/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
*/

package throttle

import (
	"testing"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/stretchr/testify/assert"
)

type FakeHeartbeatWriter struct {
}

func (w FakeHeartbeatWriter) RequestHeartbeats() {
}

func TestIsAppThrottled(t *testing.T) {
	throttler := Throttler{
		throttledApps:   cache.New(cache.NoExpiration, 0),
		heartbeatWriter: FakeHeartbeatWriter{},
	}
	assert.False(t, throttler.IsAppThrottled("app1"))
	assert.False(t, throttler.IsAppThrottled("app2"))
	assert.False(t, throttler.IsAppThrottled("app3"))
	assert.False(t, throttler.IsAppThrottled("app4"))
	//
	throttler.ThrottleApp("app1", time.Now().Add(time.Hour), DefaultThrottleRatio, true)
	throttler.ThrottleApp("app2", time.Now(), DefaultThrottleRatio, false)
	throttler.ThrottleApp("app3", time.Now().Add(time.Hour), DefaultThrottleRatio, false)
	throttler.ThrottleApp("app4", time.Now().Add(time.Hour), 0, false)
	assert.False(t, throttler.IsAppThrottled("app1")) // exempted
	assert.False(t, throttler.IsAppThrottled("app2")) // expired
	assert.True(t, throttler.IsAppThrottled("app3"))
	assert.False(t, throttler.IsAppThrottled("app4")) // ratio is zero
	//
	throttler.UnthrottleApp("app1")
	throttler.UnthrottleApp("app2")
	throttler.UnthrottleApp("app3")
	throttler.UnthrottleApp("app4")
	assert.False(t, throttler.IsAppThrottled("app1"))
	assert.False(t, throttler.IsAppThrottled("app2"))
	assert.False(t, throttler.IsAppThrottled("app3"))
	assert.False(t, throttler.IsAppThrottled("app4"))
}

func TestIsAppExempted(t *testing.T) {
	throttler := Throttler{
		throttledApps:   cache.New(cache.NoExpiration, 0),
		heartbeatWriter: FakeHeartbeatWriter{},
	}
	assert.False(t, throttler.IsAppExempted("app1"))
	assert.False(t, throttler.IsAppExempted("app2"))
	assert.False(t, throttler.IsAppExempted("app3"))
	//
	throttler.ThrottleApp("app1", time.Now().Add(time.Hour), DefaultThrottleRatio, true)
	throttler.ThrottleApp("app2", time.Now(), DefaultThrottleRatio, true) // instantly expire
	assert.True(t, throttler.IsAppExempted("app1"))
	assert.True(t, throttler.IsAppExempted("app1:other-tag"))
	assert.False(t, throttler.IsAppExempted("app2")) // expired
	assert.False(t, throttler.IsAppExempted("app3"))
	//
	throttler.UnthrottleApp("app1")
	throttler.ThrottleApp("app2", time.Now().Add(time.Hour), DefaultThrottleRatio, false)
	assert.False(t, throttler.IsAppExempted("app1"))
	assert.False(t, throttler.IsAppExempted("app2"))
	assert.False(t, throttler.IsAppExempted("app3"))
	//
	assert.True(t, throttler.IsAppExempted("schema-tracker"))
	throttler.UnthrottleApp("schema-tracker") // meaningless. App is statically exempted
	assert.True(t, throttler.IsAppExempted("schema-tracker"))
}
