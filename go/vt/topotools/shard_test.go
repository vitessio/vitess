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
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"context"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

// TestCreateShard tests a few cases for topo.CreateShard
func TestCreateShard(t *testing.T) {
	ctx := context.Background()

	// Set up topology.
	ts := memorytopo.NewServer("test_cell")

	keyspace := "test_keyspace"
	shard := "0"

	// create shard in a non-existing keyspace
	if err := ts.CreateShard(ctx, keyspace, shard); err == nil {
		t.Fatalf("CreateShard(invalid keyspace) didn't fail")
	}

	// create keyspace
	if err := ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}

	// create shard should now work
	if err := ts.CreateShard(ctx, keyspace, shard); err != nil {
		t.Fatalf("CreateShard failed: %v", err)
	}
}

// TestCreateShardMultiUnsharded checks ServedTypes is set
// only for the first created shard.
// TODO(sougou): we should eventually disallow multiple shards
// for unsharded keyspaces.
func TestCreateShardMultiUnsharded(t *testing.T) {
	ctx := context.Background()

	// Set up topology.
	ts := memorytopo.NewServer("test_cell")

	// create keyspace
	keyspace := "test_keyspace"
	if err := ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}

	// create first shard in keyspace
	shard0 := "0"
	if err := ts.CreateShard(ctx, keyspace, shard0); err != nil {
		t.Fatalf("CreateShard(shard0) failed: %v", err)
	}
	if si, err := ts.GetShard(ctx, keyspace, shard0); err != nil {
		t.Fatalf("GetShard(shard0) failed: %v", err)
	} else {
		if !si.IsMasterServing {
			t.Fatalf("shard0 should have all 3 served types")
		}
	}

	// create second shard in keyspace
	shard1 := "1"
	if err := ts.CreateShard(ctx, keyspace, shard1); err != nil {
		t.Fatalf("CreateShard(shard1) failed: %v", err)
	}
	if si, err := ts.GetShard(ctx, keyspace, shard1); err != nil {
		t.Fatalf("GetShard(shard1) failed: %v", err)
	} else {
		if si.IsMasterServing {
			t.Fatalf("shard1 should have all 3 served types")
		}
	}
}

// TestGetOrCreateShard will create / get 100 shards in a keyspace
// for a long time in parallel, making sure the locking and everything
// works correctly.
func TestGetOrCreateShard(t *testing.T) {
	ctx := context.Background()

	// Set up topology.
	cell := "test_cell"
	ts := memorytopo.NewServer(cell)

	// and do massive parallel GetOrCreateShard
	keyspace := "test_keyspace"
	wg := sync.WaitGroup{}
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			for j := 0; j < 100; j++ {
				index := rand.Intn(10)
				shard := fmt.Sprintf("%v", index)
				si, err := ts.GetOrCreateShard(ctx, keyspace, shard)
				if err != nil {
					t.Errorf("GetOrCreateShard(%v, %v) failed: %v", i, shard, err)
				}
				if si.ShardName() != shard {
					t.Errorf("si.ShardName() is wrong, got %v expected %v", si.ShardName(), shard)
				}
			}
		}(i)
	}
	wg.Wait()
}
