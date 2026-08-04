/*
Copyright 2019 The Vitess Authors.

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

package topotools

import (
	"math/rand/v2"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

// TestCreateShard tests a few cases for topo.CreateShard
func TestCreateShard(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "test_cell")
	defer ts.Close()

	keyspace := "test_keyspace"
	shard := "0"

	// create shard in a non-existing keyspace
	err := ts.CreateShard(ctx, keyspace, shard)
	require.Error(t, err, "CreateShard(invalid keyspace) didn't fail")

	// create keyspace
	if err := ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}); err != nil {
		require.NoError(t, err)
	}

	// create shard should now work
	if err := ts.CreateShard(ctx, keyspace, shard); err != nil {
		require.NoError(t, err)
	}
}

// TestCreateShardMultiUnsharded checks ServedTypes is set
// only for the first created shard.
// TODO(sougou): we should eventually disallow multiple shards
// for unsharded keyspaces.
func TestCreateShardMultiUnsharded(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "test_cell")
	defer ts.Close()

	// create keyspace
	keyspace := "test_keyspace"
	if err := ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}); err != nil {
		require.NoError(t, err)
	}

	// create first shard in keyspace
	shard0 := "0"
	if err := ts.CreateShard(ctx, keyspace, shard0); err != nil {
		require.NoError(t, err)
	}
	if si, err := ts.GetShard(ctx, keyspace, shard0); err != nil {
		require.NoError(t, err)
	} else {
		require.True(t, si.IsPrimaryServing, "shard0 should have all 3 served types")
	}

	// create second shard in keyspace
	shard1 := "1"
	if err := ts.CreateShard(ctx, keyspace, shard1); err != nil {
		require.NoError(t, err)
	}
	if si, err := ts.GetShard(ctx, keyspace, shard1); err != nil {
		require.NoError(t, err)
	} else {
		require.False(t, si.IsPrimaryServing, "shard1 should have all 3 served types")
	}
}

// TestGetOrCreateShard will create / get 100 shards in a keyspace
// for a long time in parallel, making sure the locking and everything
// works correctly.
func TestGetOrCreateShard(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "test_cell")
	defer ts.Close()

	// and do massive parallel GetOrCreateShard
	keyspace := "test_keyspace"
	wg := sync.WaitGroup{}
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			for range 100 {
				index := rand.IntN(10)
				shard := strconv.Itoa(index)
				si, err := ts.GetOrCreateShard(ctx, keyspace, shard)
				assert.NoErrorf(t, err, "GetOrCreateShard(%v, %v) failed", i, shard)
				if err == nil {
					assert.Equalf(t, shard, si.ShardName(), "si.ShardName() is wrong")
				}
			}
		}(i)
	}
	wg.Wait()
}
