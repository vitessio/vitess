/*
Copyright 2022 The Vitess Authors.

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

package cache_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/vtadmin/cache"
)

type testkey string

func (k testkey) Key() string { return string(k) }

func TestBackfillDuplicates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		cfg               cache.Config
		enqueueInterval   time.Duration
		enqueueCount      int
		expectedCallCount int
		assertionMsg      string
	}{
		{
			name: "duplicates too close together",
			cfg: cache.Config{
				BackfillRequestTTL:               time.Hour,
				BackfillRequestDuplicateInterval: time.Second,
			},
			enqueueInterval:   0, // no sleep between enqueues
			enqueueCount:      2,
			expectedCallCount: 1,
			assertionMsg:      "fillFunc should only have been called once for rapid duplicate requests",
		},
		{
			name: "duplicates with enough time passed",
			cfg: cache.Config{
				BackfillRequestTTL:               time.Hour,
				BackfillRequestDuplicateInterval: time.Millisecond,
			},
			enqueueInterval:   time.Millisecond * 5, // sleep longer than the duplicate interval
			enqueueCount:      2,
			expectedCallCount: 2,
			assertionMsg:      "fillFunc should have been called for duplicate requests with enough time between them",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var callcount int
			c := cache.New(func(ctx context.Context, req testkey) (any, error) {
				callcount++
				return nil, nil
			}, tt.cfg)

			key := testkey("testkey")
			for i := 0; i < tt.enqueueCount; i++ {
				if !c.EnqueueBackfill(key) {
					assert.Fail(t, "failed to enqueue backfill for key %s", key)
				}

				time.Sleep(tt.enqueueInterval)
			}

			c.Close()

			assert.Equal(t, tt.expectedCallCount, callcount, tt.assertionMsg)
		})
	}
}

func TestBackfillTTL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		cfg               cache.Config
		fillSleep         time.Duration
		enqueueCount      int
		expectedCallCount int
		assertionMsg      string
	}{
		{
			name: "within backfill ttl",
			cfg: cache.Config{
				BackfillRequestTTL:               time.Millisecond * 50,
				BackfillRequestDuplicateInterval: time.Second,
				BackfillEnqueueWaitTime:          time.Second,
			},
			fillSleep:         time.Millisecond * 10,
			enqueueCount:      2,
			expectedCallCount: 1,
			assertionMsg:      "both requests are within TTL and should have been filled",
		},
		{
			name: "backfill ttl exceeded",
			cfg: cache.Config{
				BackfillRequestTTL:               time.Millisecond * 10,
				BackfillRequestDuplicateInterval: time.Millisecond,
				BackfillEnqueueWaitTime:          time.Second,
			},
			fillSleep:         time.Millisecond * 50,
			enqueueCount:      2,
			expectedCallCount: 1,
			assertionMsg:      "second request exceeds TTL and should not have been filled",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var callcount int
			c := cache.New(func(ctx context.Context, req testkey) (any, error) {
				time.Sleep(tt.fillSleep) // make fills take time so that requests in the queue can exceed ttl
				callcount++
				return nil, nil
			}, tt.cfg)

			key := testkey("testkey")
			for i := 0; i < tt.enqueueCount; i++ {
				if !c.EnqueueBackfill(key) {
					assert.Fail(t, "failed to enqueue backfill for key %s", key)
				}
			}

			time.Sleep(tt.fillSleep * time.Duration(tt.enqueueCount))
			c.Close()

			assert.Equal(t, tt.expectedCallCount, callcount, tt.assertionMsg)
		})
	}
}

type intkey int

func (k intkey) Key() string { return fmt.Sprintf("%d", int(k)) }

func TestEnqueueBackfillTimeout(t *testing.T) {
	t.Parallel()

	var callcount int
	c := cache.New(func(ctx context.Context, req intkey) (any, error) {
		time.Sleep(time.Millisecond * 50) // make fills take time so that the second enqueue exceeds WaitTimeout
		callcount++
		return nil, nil
	}, cache.Config{
		BackfillEnqueueWaitTime: time.Millisecond * 10,
	})

	var enqueues = []struct {
		shouldFail bool
		msg        string
	}{
		{
			shouldFail: false,
			msg:        "not exceed",
		},
		{
			shouldFail: true,
			msg:        "exceed",
		},
	}
	for i, q := range enqueues {
		ok := c.EnqueueBackfill(intkey(i))
		assert.Equal(t, q.shouldFail, !ok, "enqueue should %s wait timeout", q.msg)
	}
}
