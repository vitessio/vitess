// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
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

// TestCreateShardCustomSharding checks ServedTypes is set correctly
// when creating multiple custom sharding shards
func TestCreateShardCustomSharding(t *testing.T) {
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
		if len(si.ServedTypes) != 3 {
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
		if len(si.ServedTypes) != 3 {
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
	ts := memorytopo.NewServer("test_cell")

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
