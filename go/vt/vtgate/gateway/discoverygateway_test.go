/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gateway

import (
	"fmt"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/srvtopo/srvtopotest"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestDiscoveryGatewayExecute(t *testing.T) {
	testDiscoveryGatewayGeneric(t, func(dg Gateway, target *querypb.Target) error {
		_, err := dg.Execute(context.Background(), target, "query", nil, 0, nil)
		return err
	})
	testDiscoveryGatewayTransact(t, func(dg Gateway, target *querypb.Target) error {
		_, err := dg.Execute(context.Background(), target, "query", nil, 1, nil)
		return err
	})
}

func TestDiscoveryGatewayExecuteBatch(t *testing.T) {
	testDiscoveryGatewayGeneric(t, func(dg Gateway, target *querypb.Target) error {
		queries := []*querypb.BoundQuery{{Sql: "query", BindVariables: nil}}
		_, err := dg.ExecuteBatch(context.Background(), target, queries, false, 0, nil)
		return err
	})
	testDiscoveryGatewayTransact(t, func(dg Gateway, target *querypb.Target) error {
		queries := []*querypb.BoundQuery{{Sql: "query", BindVariables: nil}}
		_, err := dg.ExecuteBatch(context.Background(), target, queries, false, 1, nil)
		return err
	})
}

func TestDiscoveryGatewayExecuteStream(t *testing.T) {
	testDiscoveryGatewayGeneric(t, func(dg Gateway, target *querypb.Target) error {
		err := dg.StreamExecute(context.Background(), target, "query", nil, 0, nil, func(qr *sqltypes.Result) error {
			return nil
		})
		return err
	})
}

func TestDiscoveryGatewayBegin(t *testing.T) {
	testDiscoveryGatewayGeneric(t, func(dg Gateway, target *querypb.Target) error {
		_, err := dg.Begin(context.Background(), target, nil)
		return err
	})
}

func TestDiscoveryGatewayCommit(t *testing.T) {
	testDiscoveryGatewayTransact(t, func(dg Gateway, target *querypb.Target) error {
		return dg.Commit(context.Background(), target, 1)
	})
}

func TestDiscoveryGatewayRollback(t *testing.T) {
	testDiscoveryGatewayTransact(t, func(dg Gateway, target *querypb.Target) error {
		return dg.Rollback(context.Background(), target, 1)
	})
}

func TestDiscoveryGatewayBeginExecute(t *testing.T) {
	testDiscoveryGatewayGeneric(t, func(dg Gateway, target *querypb.Target) error {
		_, _, err := dg.BeginExecute(context.Background(), target, "query", nil, nil)
		return err
	})
}

func TestDiscoveryGatewayBeginExecuteBatch(t *testing.T) {
	testDiscoveryGatewayGeneric(t, func(dg Gateway, target *querypb.Target) error {
		queries := []*querypb.BoundQuery{{Sql: "query", BindVariables: nil}}
		_, _, err := dg.BeginExecuteBatch(context.Background(), target, queries, false, nil)
		return err
	})
}

func TestDiscoveryGatewayGetTablets(t *testing.T) {
	keyspace := "ks"
	shard := "0"
	hc := discovery.NewFakeHealthCheck()
	dg := createDiscoveryGateway(context.Background(), hc, nil, "local", 2).(*discoveryGateway)

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

func TestShuffleTablets(t *testing.T) {
	ts1 := discovery.TabletStats{
		Key:     "t1",
		Tablet:  topo.NewTablet(10, "cell1", "host1"),
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:      true,
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}

	ts2 := discovery.TabletStats{
		Key:     "t2",
		Tablet:  topo.NewTablet(10, "cell1", "host2"),
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:      true,
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}

	ts3 := discovery.TabletStats{
		Key:     "t3",
		Tablet:  topo.NewTablet(10, "cell2", "host3"),
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:      true,
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}

	ts4 := discovery.TabletStats{
		Key:     "t4",
		Tablet:  topo.NewTablet(10, "cell2", "host4"),
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:      true,
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}

	sameCellTablets := []discovery.TabletStats{ts1, ts2}
	diffCellTablets := []discovery.TabletStats{ts3, ts4}
	mixedTablets := []discovery.TabletStats{ts1, ts2, ts3, ts4}
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

func TestDiscoveryGatewayGetAggregateStats(t *testing.T) {
	keyspace := "ks"
	shard := "0"
	hc := discovery.NewFakeHealthCheck()
	dg := createDiscoveryGateway(context.Background(), hc, nil, "cell1", 2).(*discoveryGateway)

	// replica should only use local ones
	hc.Reset()
	dg.tsc.ResetForTesting()
	hc.AddTestTablet("cell1", "1.1.1.1", 1001, keyspace, shard, topodatapb.TabletType_REPLICA, true, 10, nil)
	hc.AddTestTablet("cell1", "2.2.2.2", 1001, keyspace, shard, topodatapb.TabletType_REPLICA, true, 10, nil)
	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: topodatapb.TabletType_REPLICA,
		Cell:       "cell1",
	}
	tsl, err := dg.tsc.GetAggregateStats(target)
	if err != nil {
		t.Error(err)
	}
	if tsl.HealthyTabletCount != 2 {
		t.Errorf("Expected 2 healthy replica tablets, got: %v", tsl.HealthyTabletCount)
	}
}

func TestDiscoveryGatewayGetAggregateStatsRegion(t *testing.T) {
	keyspace := "ks"
	shard := "0"
	hc := discovery.NewFakeHealthCheck()
	ts := memorytopo.NewServer("local-west", "local-east", "remote")
	srvTopo := srvtopotest.NewPassthroughSrvTopoServer()
	srvTopo.TopoServer = ts
	dg := createDiscoveryGateway(context.Background(), hc, srvTopo, "local-east", 2).(*discoveryGateway)

	cellsAlias := &topodatapb.CellsAlias{
		Cells: []string{"local-west", "local-east"},
	}

	ts.CreateCellsAlias(context.Background(), "local", cellsAlias)

	defer ts.DeleteCellsAlias(context.Background(), "local")

	hc.Reset()
	dg.tsc.ResetForTesting()
	hc.AddTestTablet("remote", "1.1.1.1", 1001, keyspace, shard, topodatapb.TabletType_REPLICA, true, 10, nil)
	hc.AddTestTablet("local-west", "2.2.2.2", 1001, keyspace, shard, topodatapb.TabletType_REPLICA, true, 10, nil)
	hc.AddTestTablet("local-east", "3.3.3.3", 1001, keyspace, shard, topodatapb.TabletType_REPLICA, true, 10, nil)

	// Non master targets in the same region as the gateway should be discoverable
	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: topodatapb.TabletType_REPLICA,
		Cell:       "local-west",
	}
	tsl, err := dg.tsc.GetAggregateStats(target)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if tsl.HealthyTabletCount != 2 {
		t.Errorf("Expected 2 healthy replica tablets, got: %v", tsl.HealthyTabletCount)
	}
}

func TestDiscoveryGatewayGetAggregateStatsMaster(t *testing.T) {
	keyspace := "ks"
	shard := "0"
	hc := discovery.NewFakeHealthCheck()
	dg := createDiscoveryGateway(context.Background(), hc, nil, "cell1", 2).(*discoveryGateway)

	// replica should only use local ones
	hc.Reset()
	dg.tsc.ResetForTesting()
	hc.AddTestTablet("cell1", "1.1.1.1", 1001, keyspace, shard, topodatapb.TabletType_MASTER, true, 10, nil)
	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: topodatapb.TabletType_MASTER,
		Cell:       "cell1",
	}
	tsl, err := dg.tsc.GetAggregateStats(target)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if tsl.HealthyTabletCount != 1 {
		t.Errorf("Expected one healthy master, got: %v", tsl.HealthyTabletCount)
	}

	// You can get aggregate regardless of the cell when requesting a master
	target = &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: topodatapb.TabletType_MASTER,
		Cell:       "cell2",
	}

	tsl, err = dg.tsc.GetAggregateStats(target)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if tsl.HealthyTabletCount != 1 {
		t.Errorf("Expected one healthy master, got: %v", tsl.HealthyTabletCount)
	}
}

func TestDiscoveryGatewayGetTabletsInRegion(t *testing.T) {
	keyspace := "ks"
	shard := "0"
	hc := discovery.NewFakeHealthCheck()
	ts := memorytopo.NewServer("local-west", "local-east", "local", "remote")
	srvTopo := srvtopotest.NewPassthroughSrvTopoServer()
	srvTopo.TopoServer = ts

	cellsAlias := &topodatapb.CellsAlias{
		Cells: []string{"local-west", "local-east"},
	}

	dg := createDiscoveryGateway(context.Background(), hc, srvTopo, "local-west", 2).(*discoveryGateway)

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
	hc := discovery.NewFakeHealthCheck()
	ts := memorytopo.NewServer("local-west", "local-east", "local", "remote")
	srvTopo := srvtopotest.NewPassthroughSrvTopoServer()
	srvTopo.TopoServer = ts

	cellsAlias := &topodatapb.CellsAlias{
		Cells: []string{"local-west", "local-east"},
	}

	dg := createDiscoveryGateway(context.Background(), hc, srvTopo, "local", 2).(*discoveryGateway)

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

func BenchmarkOneCellGetAggregateStats(b *testing.B) { benchmarkCellsGetAggregateStats(1, b) }

func BenchmarkTenCellGetAggregateStats(b *testing.B) { benchmarkCellsGetAggregateStats(10, b) }

func Benchmark100CellGetAggregateStats(b *testing.B) { benchmarkCellsGetAggregateStats(100, b) }

func Benchmark1000CellGetAggregateStats(b *testing.B) { benchmarkCellsGetAggregateStats(1000, b) }

func benchmarkCellsGetAggregateStats(i int, b *testing.B) {
	keyspace := "ks"
	shard := "0"
	hc := discovery.NewFakeHealthCheck()
	dg := createDiscoveryGateway(context.Background(), hc, nil, "cell0", 2).(*discoveryGateway)
	cellsToregions := make(map[string]string)
	for j := 0; j < i; j++ {
		cell := fmt.Sprintf("cell%v", j)
		cellsToregions[cell] = "local"
	}

	//topo.UpdateCellsToRegionsForTests(cellsToregions)
	hc.Reset()
	dg.tsc.ResetForTesting()

	for j := 0; j < i; j++ {
		cell := fmt.Sprintf("cell%v", j)
		ip := fmt.Sprintf("%v.%v.%v,%v", j, j, j, j)
		hc.AddTestTablet(cell, ip, 1001, keyspace, shard, topodatapb.TabletType_REPLICA, true, 10, nil)
	}

	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: topodatapb.TabletType_REPLICA,
		Cell:       "cell0",
	}

	for n := 0; n < b.N; n++ {
		_, err := dg.tsc.GetAggregateStats(target)
		if err != nil {
			b.Fatalf("Expected no error, got %v", err)
		}
	}
}

func testDiscoveryGatewayGeneric(t *testing.T, f func(dg Gateway, target *querypb.Target) error) {
	keyspace := "ks"
	shard := "0"
	tabletType := topodatapb.TabletType_REPLICA
	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: tabletType,
	}
	hc := discovery.NewFakeHealthCheck()
	dg := createDiscoveryGateway(context.Background(), hc, nil, "cell", 2).(*discoveryGateway)

	// no tablet
	hc.Reset()
	dg.tsc.ResetForTesting()
	want := []string{"target: ks.0.replica", "no valid tablet"}
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
	ep1 := sc1.Tablet()
	ep2 := sc2.Tablet()

	err = f(dg, target)
	verifyContainsError(t, err, "target: ks.0.replica", vtrpcpb.Code_FAILED_PRECONDITION)
	verifyShardErrorEither(t, err,
		fmt.Sprintf(`used tablet: %s`, topotools.TabletIdent(ep1)),
		fmt.Sprintf(`used tablet: %s`, topotools.TabletIdent(ep2)))

	// fatal error
	hc.Reset()
	dg.tsc.ResetForTesting()
	sc1 = hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil)
	sc2 = hc.AddTestTablet("cell", "1.1.1.1", 1002, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
	sc2.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
	ep1 = sc1.Tablet()
	ep2 = sc2.Tablet()
	err = f(dg, target)
	verifyContainsError(t, err, "target: ks.0.replica", vtrpcpb.Code_FAILED_PRECONDITION)
	verifyShardErrorEither(t, err,
		fmt.Sprintf(`used tablet: %s`, topotools.TabletIdent(ep1)),
		fmt.Sprintf(`used tablet: %s`, topotools.TabletIdent(ep2)))

	// server error - no retry
	hc.Reset()
	dg.tsc.ResetForTesting()
	sc1 = hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	ep1 = sc1.Tablet()
	err = f(dg, target)
	verifyContainsError(t, err, fmt.Sprintf(`used tablet: %s`, topotools.TabletIdent(ep1)), vtrpcpb.Code_INVALID_ARGUMENT)

	// no failure
	hc.Reset()
	dg.tsc.ResetForTesting()
	hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil)
	err = f(dg, target)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
}

func testDiscoveryGatewayTransact(t *testing.T, f func(dg Gateway, target *querypb.Target) error) {
	keyspace := "ks"
	shard := "0"
	tabletType := topodatapb.TabletType_REPLICA
	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: tabletType,
	}
	hc := discovery.NewFakeHealthCheck()
	dg := createDiscoveryGateway(context.Background(), hc, nil, "cell", 2).(*discoveryGateway)

	// retry error - no retry
	hc.Reset()
	dg.tsc.ResetForTesting()
	sc1 := hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil)
	sc2 := hc.AddTestTablet("cell", "1.1.1.1", 1002, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
	sc2.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
	ep1 := sc1.Tablet()
	ep2 := sc2.Tablet()

	err := f(dg, target)
	verifyContainsError(t, err, "target: ks.0.replica", vtrpcpb.Code_FAILED_PRECONDITION)
	format := `used tablet: %s`
	verifyShardErrorEither(t, err,
		fmt.Sprintf(format, topotools.TabletIdent(ep1)),
		fmt.Sprintf(format, topotools.TabletIdent(ep2)))

	// server error - no retry
	hc.Reset()
	dg.tsc.ResetForTesting()
	sc1 = hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	ep1 = sc1.Tablet()
	err = f(dg, target)
	verifyContainsError(t, err, "target: ks.0.replica", vtrpcpb.Code_INVALID_ARGUMENT)
	verifyContainsError(t, err, fmt.Sprintf(format, topotools.TabletIdent(ep1)), vtrpcpb.Code_INVALID_ARGUMENT)
}

func verifyContainsError(t *testing.T, err error, wantErr string, wantCode vtrpcpb.Code) {
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("wanted error: \n%s\n, got error: \n%v\n", wantErr, err)
	}
	if code := vterrors.Code(err); code != wantCode {
		t.Fatalf("wanted error code: %s, got: %v", wantCode, code)
	}
}

func verifyShardErrorEither(t *testing.T, err error, a, b string) {
	if err == nil || !strings.Contains(err.Error(), a) || !strings.Contains(err.Error(), b) {
		t.Fatalf("wanted error to contain: %v or %v\n, got error: %v", a, b, err)
	}
}

func verifyShardErrors(t *testing.T, err error, wantErrors []string, wantCode vtrpcpb.Code) {
	if err != nil {
		for _, wantErr := range wantErrors {
			if err == nil || !strings.Contains(err.Error(), wantErr) {
				t.Fatalf("wanted error: \n%s\n, got error: \n%v\n", wantErr, err)
			}
		}
	}
	if code := vterrors.Code(err); code != wantCode {
		t.Fatalf("wanted error code: %s, got: %v", wantCode, code)
	}
}
