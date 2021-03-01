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

package vtgate

import (
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/log"

	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/srvtopo/srvtopotest"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestDiscoveryGatewayExecute(t *testing.T) {
	testDiscoveryGatewayGeneric(t, func(dg *DiscoveryGateway, target *querypb.Target) error {
		_, err := dg.Execute(context.Background(), target, "query", nil, 0, 0, nil)
		return err
	})
	testDiscoveryGatewayTransact(t, func(dg *DiscoveryGateway, target *querypb.Target) error {
		_, err := dg.Execute(context.Background(), target, "query", nil, 1, 0, nil)
		return err
	})
}

func TestDiscoveryGatewayExecuteBatch(t *testing.T) {
	testDiscoveryGatewayGeneric(t, func(dg *DiscoveryGateway, target *querypb.Target) error {
		queries := []*querypb.BoundQuery{{Sql: "query", BindVariables: nil}}
		_, err := dg.ExecuteBatch(context.Background(), target, queries, false, 0, nil)
		return err
	})
	testDiscoveryGatewayTransact(t, func(dg *DiscoveryGateway, target *querypb.Target) error {
		queries := []*querypb.BoundQuery{{Sql: "query", BindVariables: nil}}
		_, err := dg.ExecuteBatch(context.Background(), target, queries, false, 1, nil)
		return err
	})
}

func TestDiscoveryGatewayExecuteStream(t *testing.T) {
	testDiscoveryGatewayGeneric(t, func(dg *DiscoveryGateway, target *querypb.Target) error {
		err := dg.StreamExecute(context.Background(), target, "query", nil, 0, nil, func(qr *sqltypes.Result) error {
			return nil
		})
		return err
	})
}

func TestDiscoveryGatewayBegin(t *testing.T) {
	testDiscoveryGatewayGeneric(t, func(dg *DiscoveryGateway, target *querypb.Target) error {
		_, _, err := dg.Begin(context.Background(), target, nil)
		return err
	})
}

func TestDiscoveryGatewayCommit(t *testing.T) {
	testDiscoveryGatewayTransact(t, func(dg *DiscoveryGateway, target *querypb.Target) error {
		_, err := dg.Commit(context.Background(), target, 1)
		return err
	})
}

func TestDiscoveryGatewayRollback(t *testing.T) {
	testDiscoveryGatewayTransact(t, func(dg *DiscoveryGateway, target *querypb.Target) error {
		_, err := dg.Rollback(context.Background(), target, 1)
		return err
	})
}

func TestDiscoveryGatewayBeginExecute(t *testing.T) {
	testDiscoveryGatewayGeneric(t, func(dg *DiscoveryGateway, target *querypb.Target) error {
		_, _, _, err := dg.BeginExecute(context.Background(), target, nil, "query", nil, 0, nil)
		return err
	})
}

func TestDiscoveryGatewayBeginExecuteBatch(t *testing.T) {
	testDiscoveryGatewayGeneric(t, func(dg *DiscoveryGateway, target *querypb.Target) error {
		queries := []*querypb.BoundQuery{{Sql: "query", BindVariables: nil}}
		_, _, _, err := dg.BeginExecuteBatch(context.Background(), target, queries, false, nil)
		return err
	})
}

func TestDiscoveryGatewayGetTablets(t *testing.T) {
	keyspace := "ks"
	shard := "0"
	hc := discovery.NewFakeLegacyHealthCheck()
	dg := NewDiscoveryGateway(context.Background(), hc, nil, "local", 2)

	// replica should only use local ones
	hc.Reset()
	dg.tsc.ResetForTesting()
	hc.AddTestTablet("remote", "1.1.1.1", 1001, keyspace, shard, topodatapb.TabletType_REPLICA, true, 10, nil)
	ep1 := hc.AddTestTablet("local", "2.2.2.2", 1001, keyspace, shard, topodatapb.TabletType_REPLICA, true, 10, nil).Tablet()
	tsl := dg.tsc.GetHealthyTabletStats(keyspace, shard, topodatapb.TabletType_REPLICA)
	if len(tsl) != 1 || !topo.TabletEquality(tsl[0].Tablet, ep1) {
		t.Errorf("want %+v, got %+v", ep1, tsl)
	}

	// master should use the one with newer timestamp regardless of cell
	hc.Reset()
	dg.tsc.ResetForTesting()
	hc.AddTestTablet("remote", "1.1.1.1", 1001, keyspace, shard, topodatapb.TabletType_MASTER, true, 5, nil)
	ep1 = hc.AddTestTablet("remote", "2.2.2.2", 1001, keyspace, shard, topodatapb.TabletType_MASTER, true, 10, nil).Tablet()
	tsl = dg.tsc.GetHealthyTabletStats(keyspace, shard, topodatapb.TabletType_MASTER)
	if len(tsl) != 1 || !topo.TabletEquality(tsl[0].Tablet, ep1) {
		t.Errorf("want %+v, got %+v", ep1, tsl)
	}
}

func TestDiscoveryGatewayWaitForTablets(t *testing.T) {
	keyspace := "ks"
	shard := "0"
	cell := "local"
	hc := discovery.NewFakeLegacyHealthCheck()
	ts := memorytopo.NewServer("local")
	srvTopo := srvtopotest.NewPassthroughSrvTopoServer()
	srvTopo.TopoServer = ts
	srvTopo.SrvKeyspaceNames = []string{keyspace}
	srvTopo.SrvKeyspace = &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodata.TabletType_MASTER,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name: shard,
					},
				},
			},
			{
				ServedType: topodata.TabletType_REPLICA,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name: shard,
					},
				},
			},
			{
				ServedType: topodata.TabletType_RDONLY,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name: shard,
					},
				},
			},
		},
	}
	dg := NewDiscoveryGateway(context.Background(), hc, srvTopo, "local", 2)

	// replica should only use local ones
	hc.Reset()
	dg.tsc.ResetForTesting()
	hc.AddTestTablet(cell, "2.2.2.2", 1001, keyspace, shard, topodatapb.TabletType_REPLICA, true, 10, nil)
	hc.AddTestTablet(cell, "1.1.1.1", 1001, keyspace, shard, topodatapb.TabletType_MASTER, true, 5, nil)
	{
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second) //nolint
		defer cancel()
		err := dg.WaitForTablets(ctx, []topodatapb.TabletType{topodatapb.TabletType_REPLICA, topodatapb.TabletType_MASTER})
		if err != nil {
			t.Errorf("want %+v, got %+v", nil, err)
		}

		// fails if there are no available tablets for the desired TabletType
		err = dg.WaitForTablets(ctx, []topodatapb.TabletType{topodatapb.TabletType_RDONLY})
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	}
	{
		// errors because there is no primary on  ks2
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second) //nolint
		defer cancel()
		srvTopo.SrvKeyspaceNames = []string{keyspace, "ks2"}
		err := dg.WaitForTablets(ctx, []topodatapb.TabletType{topodatapb.TabletType_MASTER})
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	}
	discovery.KeyspacesToWatch = []string{keyspace}
	// does not wait for ks2 if it's not part of the filter
	{
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second) //nolint
		defer cancel()
		err := dg.WaitForTablets(ctx, []topodatapb.TabletType{topodatapb.TabletType_MASTER})
		if err != nil {
			t.Errorf("want %+v, got %+v", nil, err)
		}
	}
	discovery.KeyspacesToWatch = []string{}
}

func TestShuffleTablets(t *testing.T) {
	ts1 := discovery.LegacyTabletStats{
		Key:     "t1",
		Tablet:  topo.NewTablet(10, "cell1", "host1"),
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:      true,
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}

	ts2 := discovery.LegacyTabletStats{
		Key:     "t2",
		Tablet:  topo.NewTablet(10, "cell1", "host2"),
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:      true,
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}

	ts3 := discovery.LegacyTabletStats{
		Key:     "t3",
		Tablet:  topo.NewTablet(10, "cell2", "host3"),
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:      true,
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}

	ts4 := discovery.LegacyTabletStats{
		Key:     "t4",
		Tablet:  topo.NewTablet(10, "cell2", "host4"),
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:      true,
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}

	sameCellTablets := []discovery.LegacyTabletStats{ts1, ts2}
	diffCellTablets := []discovery.LegacyTabletStats{ts3, ts4}
	mixedTablets := []discovery.LegacyTabletStats{ts1, ts2, ts3, ts4}
	// repeat shuffling 10 times and every time the same cell tablets should be in the front
	for i := 0; i < 10; i++ {
		shuffleTablets("cell1", sameCellTablets)
		if (len(sameCellTablets) != 2) ||
			(sameCellTablets[0].Key != "t1" && sameCellTablets[0].Key != "t2") ||
			(sameCellTablets[1].Key != "t1" && sameCellTablets[1].Key != "t2") {
			t.Errorf("should shuffle in only same cell tablets, got %+v", sameCellTablets)
		}

		shuffleTablets("cell1", diffCellTablets)
		if (len(diffCellTablets) != 2) ||
			(diffCellTablets[0].Key != "t3" && diffCellTablets[0].Key != "t4") ||
			(diffCellTablets[1].Key != "t3" && diffCellTablets[1].Key != "t4") {
			t.Errorf("should shuffle in only diff cell tablets, got %+v", diffCellTablets)
		}

		shuffleTablets("cell1", mixedTablets)
		if len(mixedTablets) != 4 {
			t.Errorf("should have 4 tablets, got %+v", mixedTablets)
		}

		if (mixedTablets[0].Key != "t1" && mixedTablets[0].Key != "t2") ||
			(mixedTablets[1].Key != "t1" && mixedTablets[1].Key != "t2") {
			t.Errorf("should have same cell tablets in the front, got %+v", mixedTablets)
		}

		if (mixedTablets[2].Key != "t3" && mixedTablets[2].Key != "t4") ||
			(mixedTablets[3].Key != "t3" && mixedTablets[3].Key != "t4") {
			t.Errorf("should have diff cell tablets in the rear, got %+v", mixedTablets)
		}
	}
}

func TestDiscoveryGatewayGetTabletsInRegion(t *testing.T) {
	keyspace := "ks"
	shard := "0"
	hc := discovery.NewFakeLegacyHealthCheck()
	ts := memorytopo.NewServer("local-west", "local-east", "local", "remote")
	srvTopo := srvtopotest.NewPassthroughSrvTopoServer()
	srvTopo.TopoServer = ts

	cellsAlias := &topodatapb.CellsAlias{
		Cells: []string{"local-west", "local-east"},
	}

	dg := NewDiscoveryGateway(context.Background(), hc, srvTopo, "local-west", 2)

	ts.CreateCellsAlias(context.Background(), "local", cellsAlias)

	defer ts.DeleteCellsAlias(context.Background(), "local")

	// this is a test
	// replica should only use local ones
	hc.Reset()
	dg.tsc.ResetForTesting()
	hc.AddTestTablet("remote", "1.1.1.1", 1001, keyspace, shard, topodatapb.TabletType_REPLICA, true, 10, nil)
	ep1 := hc.AddTestTablet("local-west", "2.2.2.2", 1001, keyspace, shard, topodatapb.TabletType_REPLICA, true, 10, nil).Tablet()
	ep2 := hc.AddTestTablet("local-east", "3.3.3.3", 1001, keyspace, shard, topodatapb.TabletType_REPLICA, true, 10, nil).Tablet()
	tsl := dg.tsc.GetHealthyTabletStats(keyspace, shard, topodatapb.TabletType_REPLICA)
	if len(tsl) != 2 || (!topo.TabletEquality(tsl[0].Tablet, ep1) && !topo.TabletEquality(tsl[0].Tablet, ep2)) {
		t.Fatalf("want %+v or %+v, got %+v", ep1, ep2, tsl)
	}
}
func TestDiscoveryGatewayGetTabletsWithRegion(t *testing.T) {
	keyspace := "ks"
	shard := "0"
	hc := discovery.NewFakeLegacyHealthCheck()
	ts := memorytopo.NewServer("local-west", "local-east", "local", "remote")
	srvTopo := srvtopotest.NewPassthroughSrvTopoServer()
	srvTopo.TopoServer = ts

	cellsAlias := &topodatapb.CellsAlias{
		Cells: []string{"local-west", "local-east"},
	}

	dg := NewDiscoveryGateway(context.Background(), hc, srvTopo, "local", 2)

	if err := ts.CreateCellsAlias(context.Background(), "local", cellsAlias); err != nil {
		log.Errorf("ts.CreateCellsAlias(context.Background()... %v", err)
	}

	defer ts.DeleteCellsAlias(context.Background(), "local")

	// this is a test
	// replica should only use local ones
	hc.Reset()
	dg.tsc.ResetForTesting()
	hc.AddTestTablet("remote", "1.1.1.1", 1001, keyspace, shard, topodatapb.TabletType_REPLICA, true, 10, nil)
	ep1 := hc.AddTestTablet("local-west", "2.2.2.2", 1001, keyspace, shard, topodatapb.TabletType_REPLICA, true, 10, nil).Tablet()
	ep2 := hc.AddTestTablet("local-east", "3.3.3.3", 1001, keyspace, shard, topodatapb.TabletType_REPLICA, true, 10, nil).Tablet()
	tsl := dg.tsc.GetHealthyTabletStats(keyspace, shard, topodatapb.TabletType_REPLICA)
	if len(tsl) != 2 || (!topo.TabletEquality(tsl[0].Tablet, ep1) && !topo.TabletEquality(tsl[0].Tablet, ep2)) {
		t.Fatalf("want %+v or %+v, got %+v", ep1, ep2, tsl)
	}
}

func testDiscoveryGatewayGeneric(t *testing.T, f func(dg *DiscoveryGateway, target *querypb.Target) error) {
	keyspace := "ks"
	shard := "0"
	tabletType := topodatapb.TabletType_REPLICA
	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: tabletType,
	}
	hc := discovery.NewFakeLegacyHealthCheck()
	dg := NewDiscoveryGateway(context.Background(), hc, nil, "cell", 2)

	// no tablet
	hc.Reset()
	dg.tsc.ResetForTesting()
	want := []string{"target: ks.0.replica", `no healthy tablet available for 'keyspace:"ks" shard:"0" tablet_type:REPLICA`}
	err := f(dg, target)
	verifyShardErrors(t, err, want, vtrpcpb.Code_UNAVAILABLE)

	// tablet with error
	hc.Reset()
	dg.tsc.ResetForTesting()
	hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, false, 10, fmt.Errorf("no connection"))
	err = f(dg, target)
	verifyShardErrors(t, err, want, vtrpcpb.Code_UNAVAILABLE)

	// tablet without connection
	hc.Reset()
	dg.tsc.ResetForTesting()
	_ = hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, false, 10, nil).Tablet()
	err = f(dg, target)
	verifyShardErrors(t, err, want, vtrpcpb.Code_UNAVAILABLE)

	// retry error
	hc.Reset()
	dg.tsc.ResetForTesting()
	sc1 := hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil)
	sc2 := hc.AddTestTablet("cell", "1.1.1.1", 1002, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
	sc2.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1

	err = f(dg, target)
	verifyContainsError(t, err, "target: ks.0.replica", vtrpcpb.Code_FAILED_PRECONDITION)

	// fatal error
	hc.Reset()
	dg.tsc.ResetForTesting()
	sc1 = hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil)
	sc2 = hc.AddTestTablet("cell", "1.1.1.1", 1002, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
	sc2.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
	err = f(dg, target)
	verifyContainsError(t, err, "target: ks.0.replica", vtrpcpb.Code_FAILED_PRECONDITION)

	// server error - no retry
	hc.Reset()
	dg.tsc.ResetForTesting()
	sc1 = hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	err = f(dg, target)
	verifyContainsError(t, err, "target: ks.0.replica", vtrpcpb.Code_INVALID_ARGUMENT)

	// no failure
	hc.Reset()
	dg.tsc.ResetForTesting()
	hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil)
	err = f(dg, target)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
}

func testDiscoveryGatewayTransact(t *testing.T, f func(dg *DiscoveryGateway, target *querypb.Target) error) {
	keyspace := "ks"
	shard := "0"
	tabletType := topodatapb.TabletType_REPLICA
	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: tabletType,
	}
	hc := discovery.NewFakeLegacyHealthCheck()
	dg := NewDiscoveryGateway(context.Background(), hc, nil, "cell", 2)

	// retry error - no retry
	hc.Reset()
	dg.tsc.ResetForTesting()
	sc1 := hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil)
	sc2 := hc.AddTestTablet("cell", "1.1.1.1", 1002, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
	sc2.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1

	err := f(dg, target)
	verifyContainsError(t, err, "target: ks.0.replica", vtrpcpb.Code_FAILED_PRECONDITION)

	// server error - no retry
	hc.Reset()
	dg.tsc.ResetForTesting()
	sc1 = hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	err = f(dg, target)
	verifyContainsError(t, err, "target: ks.0.replica", vtrpcpb.Code_INVALID_ARGUMENT)
}
