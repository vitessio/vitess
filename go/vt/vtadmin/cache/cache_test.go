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
