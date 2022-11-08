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

package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// checkShard verifies the Shard operations work correctly
func checkShard(t *testing.T, ts *topo.Server) {
	ctx := context.Background()
	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	// Check GetShardNames returns [], nil for existing keyspace with no shards.
	if names, err := ts.GetShardNames(ctx, "test_keyspace"); err != nil || len(names) != 0 {
		t.Errorf("GetShardNames(keyspace with no shards) didn't return [] nil: %v %v", names, err)
	}

	if err := ts.CreateShard(ctx, "test_keyspace", "b0-c0"); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}
	if err := ts.CreateShard(ctx, "test_keyspace", "b0-c0"); !topo.IsErrType(err, topo.NodeExists) {
		t.Errorf("CreateShard called second time, got: %v", err)
	}

	// Delete shard and see if we can re-create it.
	if err := ts.DeleteShard(ctx, "test_keyspace", "b0-c0"); err != nil {
		t.Fatalf("DeleteShard: %v", err)
	}
	if err := ts.DeleteShard(ctx, "test_keyspace", "b0-c0"); !topo.IsErrType(err, topo.NoNode) {
		t.Errorf("DeleteShard(again): %v", err)
	}
	if err := ts.CreateShard(ctx, "test_keyspace", "b0-c0"); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}

	// Test getting an invalid shard returns ErrNoNode.
	if _, err := ts.GetShard(ctx, "test_keyspace", "666"); !topo.IsErrType(err, topo.NoNode) {
		t.Errorf("GetShard(666): %v", err)
	}

	// Test UpdateShardFields works.
	other := &topodatapb.TabletAlias{Cell: "ny", Uid: 82873}
	_, err := ts.UpdateShardFields(ctx, "test_keyspace", "b0-c0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = other
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateShardFields error: %v", err)
	}

	si, err := ts.GetShard(ctx, "test_keyspace", "b0-c0")
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	if !proto.Equal(si.Shard.PrimaryAlias, other) {
		t.Fatalf("shard.PrimaryAlias = %v, want %v", si.Shard.PrimaryAlias, other)
	}

	// test GetShardNames
	shards, err := ts.GetShardNames(ctx, "test_keyspace")
	if err != nil {
		t.Errorf("GetShardNames: %v", err)
	}
	if len(shards) != 1 || shards[0] != "b0-c0" {
		t.Errorf(`GetShardNames: want [ "b0-c0" ], got %v`, shards)
	}

	if _, err := ts.GetShardNames(ctx, "test_keyspace666"); !topo.IsErrType(err, topo.NoNode) {
		t.Errorf("GetShardNames(666): %v", err)
	}
}

// checkShardWithLock verifies the Shard operations work correctly With Lock
func checkShardWithLock(t *testing.T, ts *topo.Server) {
	ctx := context.Background()
	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}

	unblock := make(chan struct{})
	finished := make(chan struct{})

	// Check GetShardNames returns [], nil for existing keyspace with no shards.
	if names, err := ts.GetShardNames(ctx, "test_keyspace"); err != nil || len(names) != 0 {
		t.Errorf("GetShardNames(keyspace with no shards) didn't return [] nil: %v %v", names, err)
	}

	if err := ts.CreateShard(ctx, "test_keyspace", "b0-c0"); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}

	_, unlock1, err := ts.LockShard(ctx, "test_keyspace", "b0-c0", "lock")
	if err != nil {
		t.Errorf("CreateShard called second time, got: %v", err)
	}

	// As soon as we're unblocked, we try to lock the keyspace.
	go func() {
		<-unblock
		waitUntil := time.Now().Add(10 * time.Second)
		var isUnLocked1 = false
		for time.Now().Before(waitUntil) {
			_, unlock2, err := ts.TryLockShard(ctx, "test_keyspace", "b0-c0", "lock")
			if err != nil {
				if !topo.IsErrType(err, topo.NodeExists) {
					require.Fail(t, "expected node exists during trylock", err.Error())
				}
				time.Sleep(1 * time.Second)
				var finalErr error
				unlock1(&finalErr)
				isUnLocked1 = true
				if finalErr != nil {
					require.Fail(t, "Unlock(test_keyspace) failed", finalErr.Error())
				}
			} else {
				unlock2(&err)
				if err != nil {
					require.Fail(t, "Unlock(test_keyspace) failed", err.Error())
				}
				if isUnLocked1 {
					close(finished)
				}
				break
			}
		}
	}()

	// sleep for a while so we're sure the go routine is blocking
	time.Sleep(2 * time.Second)

	// unblock the go routine so it starts waiting
	close(unblock)

	timeout := time.After(15 * time.Second)
	select {
	case <-finished:
	case <-timeout:
		t.Fatalf("unlocking timed out")
	}
}
