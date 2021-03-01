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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vterrors"

	"github.com/stretchr/testify/assert"

	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo"
)

func TestTabletGatewayExecute(t *testing.T) {
	testTabletGatewayGeneric(t, func(tg *TabletGateway, target *querypb.Target) error {
		_, err := tg.Execute(context.Background(), target, "query", nil, 0, 0, nil)
		return err
	})
	testTabletGatewayTransact(t, func(tg *TabletGateway, target *querypb.Target) error {
		_, err := tg.Execute(context.Background(), target, "query", nil, 1, 0, nil)
		return err
	})
}

func TestTabletGatewayExecuteBatch(t *testing.T) {
	testTabletGatewayGeneric(t, func(tg *TabletGateway, target *querypb.Target) error {
		queries := []*querypb.BoundQuery{{Sql: "query", BindVariables: nil}}
		_, err := tg.ExecuteBatch(context.Background(), target, queries, false, 0, nil)
		return err
	})
	testTabletGatewayTransact(t, func(tg *TabletGateway, target *querypb.Target) error {
		queries := []*querypb.BoundQuery{{Sql: "query", BindVariables: nil}}
		_, err := tg.ExecuteBatch(context.Background(), target, queries, false, 1, nil)
		return err
	})
}

func TestTabletGatewayExecuteStream(t *testing.T) {
	testTabletGatewayGeneric(t, func(tg *TabletGateway, target *querypb.Target) error {
		err := tg.StreamExecute(context.Background(), target, "query", nil, 0, nil, func(qr *sqltypes.Result) error {
			return nil
		})
		return err
	})
}

func TestTabletGatewayBegin(t *testing.T) {
	testTabletGatewayGeneric(t, func(tg *TabletGateway, target *querypb.Target) error {
		_, _, err := tg.Begin(context.Background(), target, nil)
		return err
	})
}

func TestTabletGatewayCommit(t *testing.T) {
	testTabletGatewayTransact(t, func(tg *TabletGateway, target *querypb.Target) error {
		_, err := tg.Commit(context.Background(), target, 1)
		return err
	})
}

func TestTabletGatewayRollback(t *testing.T) {
	testTabletGatewayTransact(t, func(tg *TabletGateway, target *querypb.Target) error {
		_, err := tg.Rollback(context.Background(), target, 1)
		return err
	})
}

func TestTabletGatewayBeginExecute(t *testing.T) {
	testTabletGatewayGeneric(t, func(tg *TabletGateway, target *querypb.Target) error {
		_, _, _, err := tg.BeginExecute(context.Background(), target, nil, "query", nil, 0, nil)
		return err
	})
}

func TestTabletGatewayBeginExecuteBatch(t *testing.T) {
	testTabletGatewayGeneric(t, func(tg *TabletGateway, target *querypb.Target) error {
		queries := []*querypb.BoundQuery{{Sql: "query", BindVariables: nil}}
		_, _, _, err := tg.BeginExecuteBatch(context.Background(), target, queries, false, nil)
		return err
	})
}

func TestTabletGatewayShuffleTablets(t *testing.T) {
	tg := NewTabletGateway(context.Background(), nil, nil, "local")

	ts1 := &discovery.TabletHealth{
		Tablet:  topo.NewTablet(1, "cell1", "host1"),
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}

	ts2 := &discovery.TabletHealth{
		Tablet:  topo.NewTablet(2, "cell1", "host2"),
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}

	ts3 := &discovery.TabletHealth{
		Tablet:  topo.NewTablet(3, "cell2", "host3"),
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}

	ts4 := &discovery.TabletHealth{
		Tablet:  topo.NewTablet(4, "cell2", "host4"),
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}

	sameCellTablets := []*discovery.TabletHealth{ts1, ts2}
	diffCellTablets := []*discovery.TabletHealth{ts3, ts4}
	mixedTablets := []*discovery.TabletHealth{ts1, ts2, ts3, ts4}
	// repeat shuffling 10 times and every time the same cell tablets should be in the front
	for i := 0; i < 10; i++ {
		tg.shuffleTablets("cell1", sameCellTablets)
		assert.Len(t, sameCellTablets, 2, "Wrong number of TabletHealth")
		assert.Equal(t, sameCellTablets[0].Tablet.Alias.Cell, "cell1", "Wrong tablet cell")
		assert.Equal(t, sameCellTablets[1].Tablet.Alias.Cell, "cell1", "Wrong tablet cell")

		tg.shuffleTablets("cell1", diffCellTablets)
		assert.Len(t, diffCellTablets, 2, "should shuffle in only diff cell tablets")
		assert.Contains(t, diffCellTablets, ts3, "diffCellTablets should contain %v", ts3)
		assert.Contains(t, diffCellTablets, ts4, "diffCellTablets should contain %v", ts4)

		tg.shuffleTablets("cell1", mixedTablets)
		assert.Len(t, mixedTablets, 4, "should have 4 tablets, got %+v", mixedTablets)

		assert.Contains(t, mixedTablets[0:2], ts1, "should have same cell tablets in the front, got %+v", mixedTablets)
		assert.Contains(t, mixedTablets[0:2], ts2, "should have same cell tablets in the front, got %+v", mixedTablets)

		assert.Contains(t, mixedTablets[2:4], ts3, "should have diff cell tablets in the rear, got %+v", mixedTablets)
		assert.Contains(t, mixedTablets[2:4], ts4, "should have diff cell tablets in the rear, got %+v", mixedTablets)
	}
}

func TestTabletGatewayReplicaTransactionError(t *testing.T) {
	keyspace := "ks"
	shard := "0"
	// transactions on REPLICA are not allowed from tabletgateway
	// they have to be executed directly on tabletserver
	tabletType := topodatapb.TabletType_REPLICA
	host := "1.1.1.1"
	port := int32(1001)
	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: tabletType,
	}
	hc := discovery.NewFakeHealthCheck()
	tg := NewTabletGateway(context.Background(), hc, nil, "cell")

	_ = hc.AddTestTablet("cell", host, port, keyspace, shard, tabletType, true, 10, nil)
	_, err := tg.Execute(context.Background(), target, "query", nil, 1, 0, nil)
	verifyContainsError(t, err, "query service can only be used for non-transactional queries on replicas", vtrpcpb.Code_INTERNAL)
}

func testTabletGatewayGeneric(t *testing.T, f func(tg *TabletGateway, target *querypb.Target) error) {
	t.Helper()
	keyspace := "ks"
	shard := "0"
	tabletType := topodatapb.TabletType_REPLICA
	host := "1.1.1.1"
	port := int32(1001)
	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: tabletType,
	}
	hc := discovery.NewFakeHealthCheck()
	tg := NewTabletGateway(context.Background(), hc, nil, "cell")

	// no tablet
	want := []string{"target: ks.0.replica", `no healthy tablet available for 'keyspace:"ks" shard:"0" tablet_type:REPLICA`}
	err := f(tg, target)
	verifyShardErrors(t, err, want, vtrpcpb.Code_UNAVAILABLE)

	// tablet with error
	hc.Reset()
	hc.AddTestTablet("cell", host, port, keyspace, shard, tabletType, false, 10, fmt.Errorf("no connection"))
	err = f(tg, target)
	verifyShardErrors(t, err, want, vtrpcpb.Code_UNAVAILABLE)

	// tablet without connection
	hc.Reset()
	_ = hc.AddTestTablet("cell", host, port, keyspace, shard, tabletType, false, 10, nil).Tablet()
	err = f(tg, target)
	verifyShardErrors(t, err, want, vtrpcpb.Code_UNAVAILABLE)

	// retry error
	hc.Reset()
	sc1 := hc.AddTestTablet("cell", host, port, keyspace, shard, tabletType, true, 10, nil)
	sc2 := hc.AddTestTablet("cell", host, port+1, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
	sc2.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1

	err = f(tg, target)
	verifyContainsError(t, err, "target: ks.0.replica", vtrpcpb.Code_FAILED_PRECONDITION)

	// fatal error
	hc.Reset()
	sc1 = hc.AddTestTablet("cell", host, port, keyspace, shard, tabletType, true, 10, nil)
	sc2 = hc.AddTestTablet("cell", host, port+1, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
	sc2.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
	err = f(tg, target)
	verifyContainsError(t, err, "target: ks.0.replica", vtrpcpb.Code_FAILED_PRECONDITION)

	// server error - no retry
	hc.Reset()
	sc1 = hc.AddTestTablet("cell", host, port, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	err = f(tg, target)
	assert.Equal(t, vtrpcpb.Code_INVALID_ARGUMENT, vterrors.Code(err))

	// no failure
	hc.Reset()
	hc.AddTestTablet("cell", host, port, keyspace, shard, tabletType, true, 10, nil)
	err = f(tg, target)
	assert.NoError(t, err)
}

func testTabletGatewayTransact(t *testing.T, f func(tg *TabletGateway, target *querypb.Target) error) {
	t.Helper()
	keyspace := "ks"
	shard := "0"
	// test with MASTER because replica transactions don't use gateway's queryservice
	// they are executed directly on tabletserver
	tabletType := topodatapb.TabletType_MASTER
	host := "1.1.1.1"
	port := int32(1001)
	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: tabletType,
	}
	hc := discovery.NewFakeHealthCheck()
	tg := NewTabletGateway(context.Background(), hc, nil, "cell")

	// retry error - no retry
	sc1 := hc.AddTestTablet("cell", host, port, keyspace, shard, tabletType, true, 10, nil)
	sc2 := hc.AddTestTablet("cell", host, port+1, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
	sc2.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1

	err := f(tg, target)
	verifyContainsError(t, err, "target: ks.0.master", vtrpcpb.Code_FAILED_PRECONDITION)

	// server error - no retry
	hc.Reset()
	sc1 = hc.AddTestTablet("cell", host, port, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	err = f(tg, target)
	verifyContainsError(t, err, "target: ks.0.master", vtrpcpb.Code_INVALID_ARGUMENT)
}

func verifyContainsError(t *testing.T, err error, wantErr string, wantCode vtrpcpb.Code) {
	require.Error(t, err)
	if !strings.Contains(err.Error(), wantErr) {
		assert.Failf(t, "", "wanted error: \n%s\n, got error: \n%v\n", wantErr, err)
	}
	if code := vterrors.Code(err); code != wantCode {
		assert.Failf(t, "", "wanted error code: %v, got: %v", wantCode, code)
	}
}

func verifyShardErrors(t *testing.T, err error, wantErrors []string, wantCode vtrpcpb.Code) {
	require.Error(t, err)
	for _, wantErr := range wantErrors {
		require.Contains(t, err.Error(), wantErr, "wanted error: \n%s\n, got error: \n%v\n", wantErr, err)
	}
	require.Equal(t, vterrors.Code(err), wantCode, "wanted error code: %s, got: %v", wantCode, vterrors.Code(err))
}
