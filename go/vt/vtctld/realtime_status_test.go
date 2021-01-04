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

package vtctld

import (
	"fmt"
	"testing"
	"time"

	"context"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/grpcqueryservice"
	"vitess.io/vitess/go/vt/vttablet/queryservice/fakes"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"
	"vitess.io/vitess/go/vt/wrangler/testlib"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestRealtimeStatsWithQueryService uses fakeTablets and the fakeQueryService to
// copy the environment needed for the LegacyHealthCheck object.
func TestRealtimeStatsWithQueryService(t *testing.T) {
	// Set up testing keyspace with 2 tablets within 2 cells.
	keyspace := "ks"
	shard := "-80"
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	if err := ts.CreateKeyspace(context.Background(), keyspace, &topodatapb.Keyspace{
		ShardingColumnName: "keyspace_id",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
	}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}

	t1 := testlib.NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_REPLICA, nil,
		testlib.TabletKeyspaceShard(t, keyspace, shard))
	t2 := testlib.NewFakeTablet(t, wr, "cell2", 1, topodatapb.TabletType_REPLICA, nil,
		testlib.TabletKeyspaceShard(t, keyspace, shard))

	target := querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: topodatapb.TabletType_REPLICA,
	}
	fqs1 := fakes.NewStreamHealthQueryService(target)
	fqs2 := fakes.NewStreamHealthQueryService(target)
	grpcqueryservice.Register(t1.RPCServer, fqs1)
	grpcqueryservice.Register(t2.RPCServer, fqs2)

	for _, ft := range []*(testlib.FakeTablet){t1, t2} {
		ft.StartActionLoop(t, wr)
		defer ft.StopActionLoop(t)
	}

	fqs1.AddDefaultHealthResponse()

	realtimeStats, err := newRealtimeStats(ts)
	if err != nil {
		t.Fatalf("newRealtimeStats error: %v", err)
	}

	// Insert tablet1.
	want := &querypb.RealtimeStats{
		SecondsBehindMaster: 1,
	}
	if err := checkStats(realtimeStats, t1, want); err != nil {
		t.Errorf("%v", err)
	}

	// Update tablet1.
	fqs1.AddHealthResponseWithQPS(2.0)
	want2 := &querypb.RealtimeStats{
		SecondsBehindMaster: 1,
		Qps:                 2.0,
	}
	if err := checkStats(realtimeStats, t1, want2); err != nil {
		t.Errorf("%v", err)
	}

	// Insert tablet2.
	fqs2.AddHealthResponseWithQPS(3.0)
	want3 := &querypb.RealtimeStats{
		SecondsBehindMaster: 1,
		Qps:                 3.0,
	}
	if err := checkStats(realtimeStats, t2, want3); err != nil {
		t.Errorf("%v", err)
	}

	if err := checkStats(realtimeStats, t1, want2); err != nil {
		t.Errorf("%v", err)
	}
}

// checkStats ensures that the LegacyHealthCheck object received an update and passed
// that information to the correct tablet.
func checkStats(realtimeStats *realtimeStats, tablet *testlib.FakeTablet, want *querypb.RealtimeStats) error {
	deadline := time.Now().Add(time.Second * 5)
	for time.Now().Before(deadline) {
		result, err := realtimeStats.tabletStats(tablet.Tablet.Alias)
		if err != nil {
			continue
		}
		if result.DeepEqual(&discovery.LegacyTabletStats{}) {
			continue
		}
		got := result.Stats
		if proto.Equal(got, want) {
			return nil
		}
		time.Sleep(1 * time.Millisecond)
	}
	return fmt.Errorf("timeout error when getting tabletStatuses")
}

// newRealtimeStatsForTesting creates a new realtimeStats object without creating a LegacyHealthCheck object.
func newRealtimeStatsForTesting() *realtimeStats {
	tabletStatsCache := newTabletStatsCache()
	return &realtimeStats{
		tabletStatsCache: tabletStatsCache,
	}
}
