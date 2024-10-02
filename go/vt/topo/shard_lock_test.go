/*
Copyright 2024 The Vitess Authors.

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

package topo_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

// TestTopoShardLock tests shard lock operations.
func TestTopoShardLock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()

	currentTopoLockTimeout := topo.LockTimeout
	topo.LockTimeout = testLockTimeout
	defer func() {
		topo.LockTimeout = currentTopoLockTimeout
	}()

	ks := "ks"
	shard1 := "80-"
	shard2 := "-80"
	_, err := ts.GetOrCreateShard(ctx, ks, shard1)
	require.NoError(t, err)
	_, err = ts.GetOrCreateShard(ctx, ks, shard2)
	require.NoError(t, err)

	origCtx := ctx
	ctx, unlock, err := ts.LockShard(origCtx, ks, shard1, "ks80-")
	require.NoError(t, err)

	// locking the same key again, without unlocking, should return an error
	_, _, err2 := ts.LockShard(ctx, ks, shard1, "ks80-")
	require.ErrorContains(t, err2, "already held")

	// Check that we have the shard lock shouldn't return an error
	err = topo.CheckShardLocked(ctx, ks, shard1)
	require.NoError(t, err)

	// Check that we have the shard lock for the other shard should return an error
	err = topo.CheckShardLocked(ctx, ks, shard2)
	require.ErrorContains(t, err, "shard ks/-80 is not locked")

	// Check we can acquire a shard lock for the other shard
	ctx2, unlock2, err := ts.LockShard(ctx, ks, shard2, "ks-80")
	require.NoError(t, err)
	defer unlock2(&err)

	// Unlock the first shard
	unlock(&err)

	// Check shard locked output for both shards
	err = topo.CheckShardLocked(ctx2, ks, shard1)
	require.ErrorContains(t, err, "shard ks/80- is not locked")
	err = topo.CheckShardLocked(ctx2, ks, shard2)
	require.NoError(t, err)

	// confirm that the lock can be re-acquired after unlocking
	_, unlock, err = ts.TryLockShard(origCtx, ks, shard1, "ks80-")
	require.NoError(t, err)
	defer unlock(&err)
}
