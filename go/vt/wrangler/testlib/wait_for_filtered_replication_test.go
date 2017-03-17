// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/vttablet/grpcqueryservice"
	"github.com/youtube/vitess/go/vt/vttablet/tabletmanager"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"github.com/youtube/vitess/go/vt/vttablet/tmclient"
	"github.com/youtube/vitess/go/vt/wrangler"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

const keyspace = "ks"
const destShard = "-80"

// TestWaitForFilteredReplication tests the vtctl command "WaitForFilteredReplication".
// WaitForFilteredReplication ensures that the dest shard has caught up
// with the source shard up to a maximum replication delay (in seconds).
func TestWaitForFilteredReplication(t *testing.T) {
	// Replication is lagging behind.
	oneHourDelay := &querypb.RealtimeStats{
		BinlogPlayersCount:                     1,
		SecondsBehindMasterFilteredReplication: 3600,
	}

	// Replication caught up.
	oneSecondDelayFunc := func() *querypb.RealtimeStats {
		return &querypb.RealtimeStats{
			BinlogPlayersCount:                     1,
			SecondsBehindMasterFilteredReplication: 1,
		}
	}

	waitForFilteredReplication(t, "" /* expectedErr */, oneHourDelay, oneSecondDelayFunc)
}

// TestWaitForFilteredReplication_noFilteredReplication checks that
// vtctl WaitForFilteredReplication fails when no filtered replication is
// running (judging by the tablet's returned stream health record).
func TestWaitForFilteredReplication_noFilteredReplication(t *testing.T) {
	noFilteredReplication := &querypb.RealtimeStats{
		BinlogPlayersCount: 0,
	}
	noFilteredReplicationFunc := func() *querypb.RealtimeStats {
		return noFilteredReplication
	}

	waitForFilteredReplication(t, "no filtered replication running", noFilteredReplication, noFilteredReplicationFunc)
}

// TestWaitForFilteredReplication_unhealthy checks that
// vtctl WaitForFilteredReplication fails when a tablet is not healthy.
func TestWaitForFilteredReplication_unhealthy(t *testing.T) {
	unhealthy := &querypb.RealtimeStats{
		HealthError: "WaitForFilteredReplication: unhealthy test",
	}
	unhealthyFunc := func() *querypb.RealtimeStats {
		return unhealthy
	}

	waitForFilteredReplication(t, "tablet is not healthy", unhealthy, unhealthyFunc)
}

func waitForFilteredReplication(t *testing.T, expectedErr string, initialStats *querypb.RealtimeStats, broadcastStatsFunc func() *querypb.RealtimeStats) {
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// create keyspace
	if err := ts.CreateKeyspace(context.Background(), keyspace, &topodatapb.Keyspace{
		ShardingColumnName: "keyspace_id",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
	}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}

	// source of the filtered replication. We don't start its loop because we don't connect to it.
	source := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil,
		TabletKeyspaceShard(t, keyspace, "0"))
	// dest is the master of the dest shard which receives filtered replication events.
	dest := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_MASTER, nil,
		TabletKeyspaceShard(t, keyspace, destShard))
	dest.StartActionLoop(t, wr)
	defer dest.StopActionLoop(t)

	// Build topology state as we would expect it when filtered replication is enabled.
	ctx := context.Background()
	wr.SetSourceShards(ctx, keyspace, destShard, []*topodatapb.TabletAlias{source.Tablet.GetAlias()}, nil)

	// Set a BinlogPlayerMap to avoid a nil panic when the explicit RunHealthCheck
	// is called by WaitForFilteredReplication.
	// Note that for this test we don't mock the BinlogPlayerMap i.e. although
	// its state says no filtered replication is running, the code under test will
	// observe otherwise because we call TabletServer.BroadcastHealth() directly and
	// skip going through the tabletmanager's agent.
	dest.Agent.BinlogPlayerMap = tabletmanager.NewBinlogPlayerMap(ts, nil, nil)

	// Use real, but trimmed down QueryService.
	qs := tabletserver.NewTabletServerWithNilTopoServer(tabletenv.DefaultQsConfig)
	grpcqueryservice.Register(dest.RPCServer, qs)

	qs.BroadcastHealth(42, initialStats)

	// run vtctl WaitForFilteredReplication
	stopBroadcasting := make(chan struct{})
	go func() {
		defer close(stopBroadcasting)
		err := vp.Run([]string{"WaitForFilteredReplication", "-max_delay", "10s", dest.Tablet.Keyspace + "/" + dest.Tablet.Shard})
		if expectedErr == "" {
			if err != nil {
				t.Fatalf("WaitForFilteredReplication must not fail: %v", err)
			}
		} else {
			if err == nil || !strings.Contains(err.Error(), expectedErr) {
				t.Fatalf("WaitForFilteredReplication wrong error. got: %v want substring: %v", err, expectedErr)
			}
		}
	}()

	// Broadcast health record as long as vtctl is running.
	for {
		// Give vtctl a head start to consume the initial stats.
		// (We do this because there's unfortunately no way to explicitly
		//  synchronize with the point where conn.StreamHealth() has started.)
		// (Tests won't break if vtctl misses the initial stats. Only coverage
		//  will be impacted.)
		timer := time.NewTimer(1 * time.Millisecond)

		select {
		case <-stopBroadcasting:
			timer.Stop()
			return
		case <-timer.C:
			qs.BroadcastHealth(42, broadcastStatsFunc())
			// Pace the flooding broadcasting to waste less CPU.
			timer.Reset(1 * time.Millisecond)
		}
	}

	// vtctl WaitForFilteredReplication returned.
}
