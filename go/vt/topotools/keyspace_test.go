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
	"errors"
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
// calls have started, or until the call's context is done. It records the
// longest time any call had left before its context deadline.
type barrierRefreshTMC struct {
	tmclient.TabletManagerClient

	expected int
	mu       sync.Mutex
	started  int
	maxLeft  time.Duration
	allIn    chan struct{}
}

func (c *barrierRefreshTMC) RefreshState(ctx context.Context, tablet *topodatapb.Tablet) error {
	c.mu.Lock()
	c.started++
	if c.started == c.expected {
		close(c.allIn)
	}
	if deadline, ok := ctx.Deadline(); ok {
		c.maxLeft = max(c.maxLeft, time.Until(deadline))
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
// refreshes the tablets of all the 'to' shards concurrently, within refreshTimeout.
// Each shard has one tablet whose RefreshState returns only once every shard's
// RefreshState has started, so refreshing the shards one at a time runs into the
// refresh deadline.
func TestUpdateShardRecordsRefreshesShardsConcurrently(t *testing.T) {
	const (
		cell           = "cell1"
		keyspace       = "ks"
		numShards      = 16
		refreshTimeout = 30 * time.Second
	)
	ctx, cancel := context.WithTimeout(t.Context(), 2*refreshTimeout)
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
		false /* isFrom */, refreshTimeout, logutil.NewMemoryLogger())
	require.NoError(t, err)

	tmc.mu.Lock()
	defer tmc.mu.Unlock()
	require.Equal(t, numShards, tmc.started)
	require.LessOrEqual(t, tmc.maxLeft, refreshTimeout)
}

// leaseFactory creates memorytopo connections whose locks taken with a TTL
// expire when they are not checked for longer than the TTL, as with the etcd
// topo, where checking a lock renews its lease.
type leaseFactory struct {
	*memorytopo.Factory
}

func (f *leaseFactory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	conn, err := f.Factory.Create(cell, serverAddr, root)
	if err != nil {
		return nil, err
	}
	return &leaseConn{Conn: conn}, nil
}

type leaseConn struct {
	topo.Conn
}

func (c *leaseConn) LockWithTTL(ctx context.Context, dirPath, contents string, ttl time.Duration) (topo.LockDescriptor, error) {
	ld, err := c.Conn.LockWithTTL(ctx, dirPath, contents, ttl)
	if err != nil {
		return nil, err
	}
	return &leaseLock{LockDescriptor: ld, ttl: ttl, renewed: time.Now()}, nil
}

type leaseLock struct {
	topo.LockDescriptor
	ttl time.Duration

	mu      sync.Mutex
	renewed time.Time
}

func (l *leaseLock) Check(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if time.Since(l.renewed) > l.ttl {
		return errors.New("lease expired")
	}
	l.renewed = time.Now()
	return nil
}

// slowRefreshTMC returns from RefreshState after delay, like a tablet whose
// action lock is held by another action, and counts the completed calls.
type slowRefreshTMC struct {
	tmclient.TabletManagerClient
	delay time.Duration

	mu        sync.Mutex
	refreshed int
}

func (c *slowRefreshTMC) RefreshState(ctx context.Context, tablet *topodatapb.Tablet) error {
	select {
	case <-time.After(c.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.refreshed++
	return nil
}

// TestUpdateShardRecordsChecksKeyspaceLockDuringRefresh checks that
// UpdateShardRecords keeps checking the keyspace lock while a 'to' shard's
// tablet refresh runs: a lock whose TTL is longer than the check interval is
// kept for a refresh that takes longer than the TTL, and a lock that is lost
// during the refresh fails UpdateShardRecords and cancels the refresh. The test
// topo expires a lock that is not checked within its TTL.
func TestUpdateShardRecordsChecksKeyspaceLockDuringRefresh(t *testing.T) {
	const (
		cell           = "cell1"
		keyspace       = "ks"
		shard          = "0"
		refreshDelay   = 5 * time.Second
		refreshTimeout = 30 * time.Second
	)
	tests := []struct {
		name          string
		lockTTL       time.Duration
		wantErr       string
		wantRefreshed int
	}{
		{
			name:          "lock kept",
			lockTTL:       3 * lockCheckInterval,
			wantRefreshed: 1,
		},
		{
			name:    "lock lost",
			lockTTL: lockCheckInterval / 2,
			wantErr: "ks keyspace lock was lost while refreshing the tablets: lease expired",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*refreshTimeout)
			t.Cleanup(cancel)
			memts, factory := memorytopo.NewServerAndFactory(ctx, cell)
			t.Cleanup(memts.Close)
			ts, err := topo.NewWithFactory(&leaseFactory{Factory: factory}, "" /* serverAddress */, "" /* root */)
			require.NoError(t, err)
			t.Cleanup(ts.Close)

			require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}))
			require.NoError(t, ts.CreateShard(ctx, keyspace, shard))
			require.NoError(t, ts.CreateTablet(ctx, &topodatapb.Tablet{
				Alias:    &topodatapb.TabletAlias{Cell: cell, Uid: 1},
				Hostname: "host",
				Keyspace: keyspace,
				Shard:    shard,
				Type:     topodatapb.TabletType_REPLICA,
			}))
			si, err := ts.GetShard(ctx, keyspace, shard)
			require.NoError(t, err)

			lockCtx, unlock, err := ts.LockKeyspace(ctx, keyspace, "TestUpdateShardRecordsChecksKeyspaceLockDuringRefresh", topo.WithTTL(tt.lockTTL))
			require.NoError(t, err)
			t.Cleanup(func() {
				var unlockErr error
				unlock(&unlockErr)
				require.NoError(t, unlockErr)
			})

			tmc := &slowRefreshTMC{delay: refreshDelay}
			err = UpdateShardRecords(lockCtx, ts, tmc, keyspace, []*topo.ShardInfo{si}, []string{cell}, topodatapb.TabletType_REPLICA,
				false /* isFrom */, refreshTimeout, logutil.NewMemoryLogger())
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				require.NoError(t, topo.CheckKeyspaceLocked(lockCtx, keyspace))
			}

			tmc.mu.Lock()
			defer tmc.mu.Unlock()
			require.Equal(t, tt.wantRefreshed, tmc.refreshed)
		})
	}
}
