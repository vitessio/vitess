/*
Copyright 2026 The Vitess Authors.

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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// barrierRefreshTMC holds every RefreshState call until all of the expected
// calls have started, or until the call's context is done.
type barrierRefreshTMC struct {
	tmclient.TabletManagerClient

	expected int
	mu       sync.Mutex
	started  int
	allIn    chan struct{}
}

func (c *barrierRefreshTMC) RefreshState(ctx context.Context, tablet *topodatapb.Tablet) error {
	c.mu.Lock()
	c.started++
	if c.started == c.expected {
		close(c.allIn)
	}
	c.mu.Unlock()

	select {
	case <-c.allIn:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TestUpdateShardRecordsRefreshesShardsConcurrently checks that UpdateShardRecords
// refreshes the tablets of all the 'to' shards concurrently. Each shard has one
// tablet whose RefreshState returns only once every shard's RefreshState has
// started, so refreshing the shards one at a time runs into the context deadline.
func TestUpdateShardRecordsRefreshesShardsConcurrently(t *testing.T) {
	const (
		cell      = "cell1"
		keyspace  = "ks"
		numShards = 16
	)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	ts := memorytopo.NewServer(ctx, cell)
	t.Cleanup(ts.Close)

	require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}))
	shardNames, err := key.GenerateShardRanges(numShards, 2)
	require.NoError(t, err)
	shards := make([]*topo.ShardInfo, 0, numShards)
	for i, shardName := range shardNames {
		require.NoError(t, ts.CreateShard(ctx, keyspace, shardName))
		require.NoError(t, ts.CreateTablet(ctx, &topodatapb.Tablet{
			Alias:    &topodatapb.TabletAlias{Cell: cell, Uid: uint32(i + 1)},
			Hostname: "host",
			Keyspace: keyspace,
			Shard:    shardName,
			Type:     topodatapb.TabletType_REPLICA,
		}))
		si, err := ts.GetShard(ctx, keyspace, shardName)
		require.NoError(t, err)
		shards = append(shards, si)
	}

	lockCtx, unlock, err := ts.LockKeyspace(ctx, keyspace, "TestUpdateShardRecordsRefreshesShardsConcurrently")
	require.NoError(t, err)
	t.Cleanup(func() {
		var unlockErr error
		unlock(&unlockErr)
		require.NoError(t, unlockErr)
	})

	tmc := &barrierRefreshTMC{expected: numShards, allIn: make(chan struct{})}
	err = UpdateShardRecords(lockCtx, ts, tmc, keyspace, shards, []string{cell}, topodatapb.TabletType_REPLICA,
		false /* isFrom */, false /* clearSourceShards */, logutil.NewMemoryLogger())
	require.NoError(t, err)

	tmc.mu.Lock()
	started := tmc.started
	tmc.mu.Unlock()
	require.Equal(t, numShards, started)
}
