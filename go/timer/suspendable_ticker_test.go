/*
Copyright 2023 The Vitess Authors.

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

package timer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	fastTickerInterval = 10 * time.Millisecond
)

func TestInitiallySuspended(t *testing.T) {
	ctx := context.Background()
	t.Run("true", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		ticker := NewSuspendableTicker(fastTickerInterval, true)
		defer ticker.Stop()
		select {
		case <-ticker.C:
			assert.Fail(t, "unexpected tick. Was supposed to be suspended")
		case <-ctx.Done():
			return
		}
	})
	t.Run("false", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		ticker := NewSuspendableTicker(fastTickerInterval, false)
		defer ticker.Stop()
		select {
		case <-ticker.C:
			return
		case <-ctx.Done():
			assert.Fail(t, "unexpected timeout. Expected tick")
		}
	})
}

func TestSuspendableTicker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ticker := NewSuspendableTicker(fastTickerInterval, false)
	defer ticker.Stop()

	var ticks atomic.Int64
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				ticks.Add(1)
			}
		}
	}()
	t.Run("ticks running", func(t *testing.T) {
		time.Sleep(time.Second)
		after := ticks.Load()
		assert.Greater(t, after, int64(10)) // should be about 100
	})
	t.Run("ticks suspended", func(t *testing.T) {
		ticker.Suspend()
		before := ticks.Load()
		time.Sleep(time.Second)
		after := ticks.Load()
		assert.Less(t, after-before, int64(10))
	})
	t.Run("ticks resumed", func(t *testing.T) {
		ticker.Resume()
		before := ticks.Load()
		time.Sleep(time.Second)
		after := ticks.Load()
		assert.Greater(t, after-before, int64(10))
	})
	t.Run("ticker stopped", func(t *testing.T) {
		ticker.Stop()
		before := ticks.Load()
		time.Sleep(time.Second)
		after := ticks.Load()
		assert.Less(t, after-before, int64(10))
	})
}

func TestSuspendableTickerTick(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ticker := NewSuspendableTicker(time.Hour, false)
	defer ticker.Stop()

	var ticks atomic.Int64
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				ticks.Add(1)
			}
		}
	}()
	t.Run("nothing going on", func(t *testing.T) {
		time.Sleep(time.Second)
		after := ticks.Load()
		assert.Zero(t, after)
	})
	t.Run("tick now", func(t *testing.T) {
		before := ticks.Load()
		ticker.TickNow()
		time.Sleep(time.Second)
		after := ticks.Load()
		assert.Equal(t, int64(1), after-before)
	})
	t.Run("tick after", func(t *testing.T) {
		before := ticks.Load()
		ticker.TickAfter(2 * time.Second)
		time.Sleep(time.Second)
		after := ticks.Load()
		assert.Zero(t, after-before)
		time.Sleep(3 * time.Second)
		after = ticks.Load()
		assert.Equal(t, int64(1), after-before)
	})
}
