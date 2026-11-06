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

package schemacache

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/vtadmin/cache"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func TestLogs(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)

	fill := func(ctx context.Context, key Key) ([]*vtadminpb.Schema, error) { return nil, nil }
	assertKey := func(t *testing.T, record map[string]any, keyspace string) {
		assert.Equal(t, "c1", record["cluster_id"])
		assert.Equal(t, keyspace, record["keyspace"])
		assert.Equal(t, true, record["include_non_serving_shards"])
	}

	c := cache.New(fill, cache.Config{})
	defer c.Close()
	allKey := Key{ClusterID: "c1", IncludeNonServingShards: true}
	require.NoError(t, c.Add(allKey, []*vtadminpb.Schema{{Keyspace: "ks1"}, {Keyspace: "ks2"}}, cache.DefaultExpiration))
	_, found, err := LoadOne(c, allKey, LoadOptions{BaseRequest: &vtctldatapb.GetSchemaRequest{}})
	require.Error(t, err)
	assert.True(t, found)
	record := logs.Record(t, "impossible: cache should have exactly 1 schema for request")
	assert.Equal(t, "ERROR", record["level"])
	assertKey(t, record, "")
	assert.InDelta(t, 2, record["schema_count"], 0)

	key := Key{ClusterID: "c1", Keyspace: "ks1", IncludeNonServingShards: true}

	// A closed cache with no queue can't take a backfill request.
	closed := cache.New(fill, cache.Config{BackfillQueueSize: 0, BackfillEnqueueWaitTime: time.Millisecond})
	require.NoError(t, closed.Close())
	AddOrBackfill(closed, nil, key, cache.DefaultExpiration, LoadOptions{BaseRequest: &vtctldatapb.GetSchemaRequest{}})
	record = logs.Record(t, "failed to enqueue backfill for schema cache")
	assert.Equal(t, "WARN", record["level"])
	assertKey(t, record, "ks1")

	// Adding fails only when the key is added, replaced or expires between the
	// cache's existence check and its write.
	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Go(func() {
		for !stop.Load() {
			_ = c.Add(key, nil, time.Hour)
			_ = c.Add(key, nil, time.Nanosecond)
		}
	})
	t.Cleanup(func() {
		stop.Store(true)
		wg.Wait()
	})
	full := LoadOptions{BaseRequest: &vtctldatapb.GetSchemaRequest{IncludeViews: true}, AggregateSizes: true}
	require.Eventually(t, func() bool {
		for range 100 {
			AddOrBackfill(c, nil, key, time.Hour, full)
		}
		return strings.Contains(logs.String(), "failed to add schema to cache")
	}, 30*time.Second, time.Millisecond)
	for _, record := range logs.Records(t) {
		if record["msg"] == "failed to add schema to cache" {
			assert.Equal(t, "WARN", record["level"])
			assertKey(t, record, "ks1")
			assert.NotEmpty(t, record["error"])
			return
		}
	}
	assert.Fail(t, "no record of the failed add")
}

func TestLoadOptions(t *testing.T) {
	t.Parallel()

	t.Run("isFullPayload", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name          string
			opts          LoadOptions
			isFullPayload bool
		}{
			{
				name: "full payload",
				opts: LoadOptions{
					BaseRequest: &vtctldatapb.GetSchemaRequest{
						IncludeViews: true,
					},
					AggregateSizes: true,
				},
				isFullPayload: true,
			},
			{
				name: "tables",
				opts: LoadOptions{
					BaseRequest: &vtctldatapb.GetSchemaRequest{
						Tables:       []string{"t1"},
						IncludeViews: true,
					},
					AggregateSizes: true,
				},
				isFullPayload: false,
			},
			{
				name: "exclude tables",
				opts: LoadOptions{
					BaseRequest: &vtctldatapb.GetSchemaRequest{
						ExcludeTables: []string{"t1"},
						IncludeViews:  true,
					},
					AggregateSizes: true,
				},
				isFullPayload: false,
			},
			{
				name: "no views",
				opts: LoadOptions{
					BaseRequest: &vtctldatapb.GetSchemaRequest{
						IncludeViews: false,
					},
					AggregateSizes: true,
				},
				isFullPayload: false,
			},
			{
				name: "names only",
				opts: LoadOptions{
					BaseRequest: &vtctldatapb.GetSchemaRequest{
						IncludeViews:   true,
						TableNamesOnly: true,
					},
					AggregateSizes: true,
				},
				isFullPayload: false,
			},
			{
				name: "sizes only",
				opts: LoadOptions{
					BaseRequest: &vtctldatapb.GetSchemaRequest{
						IncludeViews:   true,
						TableSizesOnly: true,
					},
					AggregateSizes: true,
				},
				isFullPayload: false,
			},
			{
				name: "schema only",
				opts: LoadOptions{
					BaseRequest: &vtctldatapb.GetSchemaRequest{
						IncludeViews:    true,
						TableSchemaOnly: true,
					},
					AggregateSizes: true,
				},
				isFullPayload: false,
			},
			{
				name: "no size aggregation",
				opts: LoadOptions{
					BaseRequest: &vtctldatapb.GetSchemaRequest{
						IncludeViews: true,
					},
					AggregateSizes: false,
				},
				isFullPayload: false,
			},
			{
				name: "missing request",
				opts: LoadOptions{
					BaseRequest:    nil,
					AggregateSizes: true,
				},
				isFullPayload: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				assert.Equal(t, tt.isFullPayload, tt.opts.isFullPayload(), "%+v", tt.opts)
			})
		}
	})
}
