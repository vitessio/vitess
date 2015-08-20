// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/grpcqueryservice"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/zktopo"

	pbq "github.com/youtube/vitess/go/vt/proto/query"
	pbt "github.com/youtube/vitess/go/vt/proto/topodata"
)

const keyspace = "ks"
const destShard = "-80"

// TestWaitForFilteredReplication tests the vtctl command "WaitForFilteredReplication".
// WaitForFilteredReplication ensures that the dest shard has caught up
// with the source shard up to a maximum replication delay (in seconds).
func TestWaitForFilteredReplication(t *testing.T) {
	// Replication is lagging behind.
	oneHourDelay := &pbq.RealtimeStats{
		BinlogPlayersCount:                     1,
		SecondsBehindMasterFilteredReplication: 3600,
	}

	// Replication caught up.
	oneSecondDelayFunc := func() *pbq.RealtimeStats {
		return &pbq.RealtimeStats{
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
	noFilteredReplication := &pbq.RealtimeStats{
		BinlogPlayersCount: 0,
	}
	noFilteredReplicationFunc := func() *pbq.RealtimeStats {
		return noFilteredReplication
	}

	waitForFilteredReplication(t, "no filtered replication running", noFilteredReplication, noFilteredReplicationFunc)
}

// TestWaitForFilteredReplication_unhealthy checks that
// vtctl WaitForFilteredReplication fails eventually when a tablet is not healthy.
func TestWaitForFilteredReplication_unhealthy(t *testing.T) {
	unhealthy := &pbq.RealtimeStats{
		HealthError: "WaitForFilteredReplication: unhealthy test",
	}
	unhealthyFunc := func() *pbq.RealtimeStats {
		return unhealthy
	}

	waitForFilteredReplication(t, "tablet is not healthy", unhealthy, unhealthyFunc)
}

func waitForFilteredReplication(t *testing.T, expectedErr string, initialStats *pbq.RealtimeStats, broadcastStatsFunc func() *pbq.RealtimeStats) {
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient(), time.Second)
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// source of the filtered replication. We don't start its loop because we don't connect to it.
	source := NewFakeTablet(t, wr, "cell1", 0, pbt.TabletType_MASTER,
		TabletKeyspaceShard(t, keyspace, "0"))
	// dest is the master of the dest shard which receives filtered replication events.
	dest := NewFakeTablet(t, wr, "cell1", 1, pbt.TabletType_MASTER,
		TabletKeyspaceShard(t, keyspace, destShard))
	dest.StartActionLoop(t, wr)
	defer dest.StopActionLoop(t)

	// Build topology state as we would expect it when filtered replication is enabled.
	ctx := context.Background()
	wr.SetSourceShards(ctx, keyspace, destShard, []*pbt.TabletAlias{source.Tablet.GetAlias()}, nil)

	// Use real, but trimmed down QueryService.
	testConfig := tabletserver.DefaultQsConfig
	testConfig.EnablePublishStats = false
	testConfig.DebugURLPrefix = fmt.Sprintf("TestWaitForFilteredReplication-%d-", rand.Int63())
	qs := tabletserver.NewSqlQuery(testConfig)
	grpcqueryservice.RegisterForTest(dest.RPCServer, qs)

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
