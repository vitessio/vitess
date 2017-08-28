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
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/vterrors"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestDiscoveryGatewayExecute(t *testing.T) {
	testDiscoveryGatewayGeneric(t, false, func(dg Gateway, target *querypb.Target) error {
		_, err := dg.Execute(context.Background(), target, "query", nil, 0, nil)
		return err
	})
	testDiscoveryGatewayTransact(t, false, func(dg Gateway, target *querypb.Target) error {
		_, err := dg.Execute(context.Background(), target, "query", nil, 1, nil)
		return err
	})
}

func TestDiscoveryGatewayExecuteBatch(t *testing.T) {
	testDiscoveryGatewayGeneric(t, false, func(dg Gateway, target *querypb.Target) error {
		queries := []*querypb.BoundQuery{{Sql: "query", BindVariables: nil}}
		_, err := dg.ExecuteBatch(context.Background(), target, queries, false, 0, nil)
		return err
	})
	testDiscoveryGatewayTransact(t, false, func(dg Gateway, target *querypb.Target) error {
		queries := []*querypb.BoundQuery{{Sql: "query", BindVariables: nil}}
		_, err := dg.ExecuteBatch(context.Background(), target, queries, false, 1, nil)
		return err
	})
}

func TestDiscoveryGatewayExecuteStream(t *testing.T) {
	testDiscoveryGatewayGeneric(t, true, func(dg Gateway, target *querypb.Target) error {
		err := dg.StreamExecute(context.Background(), target, "query", nil, nil, func(qr *sqltypes.Result) error {
			return nil
		})
		return err
	})
}

func TestDiscoveryGatewayBegin(t *testing.T) {
	testDiscoveryGatewayGeneric(t, false, func(dg Gateway, target *querypb.Target) error {
		_, err := dg.Begin(context.Background(), target, nil)
		return err
	})
}

func TestDiscoveryGatewayCommit(t *testing.T) {
	testDiscoveryGatewayTransact(t, false, func(dg Gateway, target *querypb.Target) error {
		return dg.Commit(context.Background(), target, 1)
	})
}

func TestDiscoveryGatewayRollback(t *testing.T) {
	testDiscoveryGatewayTransact(t, false, func(dg Gateway, target *querypb.Target) error {
		return dg.Rollback(context.Background(), target, 1)
	})
}

func TestDiscoveryGatewayBeginExecute(t *testing.T) {
	testDiscoveryGatewayGeneric(t, false, func(dg Gateway, target *querypb.Target) error {
		_, _, err := dg.BeginExecute(context.Background(), target, "query", nil, nil)
		return err
	})
}

func TestDiscoveryGatewayBeginExecuteBatch(t *testing.T) {
	testDiscoveryGatewayGeneric(t, false, func(dg Gateway, target *querypb.Target) error {
		queries := []*querypb.BoundQuery{{Sql: "query", BindVariables: nil}}
		_, _, err := dg.BeginExecuteBatch(context.Background(), target, queries, false, nil)
		return err
	})
}

func TestDiscoveryGatewayGetTablets(t *testing.T) {
	keyspace := "ks"
	shard := "0"
	hc := discovery.NewFakeHealthCheck()
	dg := createDiscoveryGateway(hc, topo.Server{}, nil, "local", 2).(*discoveryGateway)

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

func testDiscoveryGatewayGeneric(t *testing.T, streaming bool, f func(dg Gateway, target *querypb.Target) error) {
	keyspace := "ks"
	shard := "0"
	tabletType := topodatapb.TabletType_REPLICA
	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: tabletType,
	}
	hc := discovery.NewFakeHealthCheck()
	dg := createDiscoveryGateway(hc, topo.Server{}, nil, "cell", 2).(*discoveryGateway)

	// no tablet
	hc.Reset()
	dg.tsc.ResetForTesting()
	want := "target: ks.0.replica, no valid tablet"
	err := f(dg, target)
	verifyShardError(t, err, want, vtrpcpb.Code_UNAVAILABLE)

	// tablet with error
	hc.Reset()
	dg.tsc.ResetForTesting()
	hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, false, 10, fmt.Errorf("no connection"))
	want = "target: ks.0.replica, no valid tablet"
	err = f(dg, target)
	verifyShardError(t, err, want, vtrpcpb.Code_UNAVAILABLE)

	// tablet without connection
	hc.Reset()
	dg.tsc.ResetForTesting()
	ep1 := hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, false, 10, nil).Tablet()
	want = fmt.Sprintf(`target: ks.0.replica, no valid tablet`)
	err = f(dg, target)
	verifyShardError(t, err, want, vtrpcpb.Code_UNAVAILABLE)

	// retry error
	hc.Reset()
	dg.tsc.ResetForTesting()
	sc1 := hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil)
	sc2 := hc.AddTestTablet("cell", "1.1.1.1", 1002, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
	sc2.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
	ep1 = sc1.Tablet()
	ep2 := sc2.Tablet()
	wants := map[string]int{
		fmt.Sprintf(`target: ks.0.replica, used tablet: %s, FAILED_PRECONDITION error`, topotools.TabletIdent(ep1)): 0,
		fmt.Sprintf(`target: ks.0.replica, used tablet: %s, FAILED_PRECONDITION error`, topotools.TabletIdent(ep2)): 0,
	}
	err = f(dg, target)
	if _, ok := wants[fmt.Sprintf("%v", err)]; !ok {
		t.Errorf("wanted error: %+v, got error: %v", wants, err)
	}

	// fatal error
	hc.Reset()
	dg.tsc.ResetForTesting()
	sc1 = hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil)
	sc2 = hc.AddTestTablet("cell", "1.1.1.1", 1002, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
	sc2.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
	ep1 = sc1.Tablet()
	ep2 = sc2.Tablet()
	wants = map[string]int{
		fmt.Sprintf(`target: ks.0.replica, used tablet: %s, FAILED_PRECONDITION error`, topotools.TabletIdent(ep1)): 0,
		fmt.Sprintf(`target: ks.0.replica, used tablet: %s, FAILED_PRECONDITION error`, topotools.TabletIdent(ep2)): 0,
	}
	err = f(dg, target)
	if _, ok := wants[fmt.Sprintf("%v", err)]; !ok {
		t.Errorf("wanted error: %+v, got error: %v", wants, err)
	}

	// server error - no retry
	hc.Reset()
	dg.tsc.ResetForTesting()
	sc1 = hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	ep1 = sc1.Tablet()
	want = fmt.Sprintf(`target: ks.0.replica, used tablet: %s, INVALID_ARGUMENT error`, topotools.TabletIdent(ep1))
	err = f(dg, target)
	verifyShardError(t, err, want, vtrpcpb.Code_INVALID_ARGUMENT)

	// no failure
	hc.Reset()
	dg.tsc.ResetForTesting()
	hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil)
	err = f(dg, target)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
}

func testDiscoveryGatewayTransact(t *testing.T, streaming bool, f func(dg Gateway, target *querypb.Target) error) {
	keyspace := "ks"
	shard := "0"
	tabletType := topodatapb.TabletType_REPLICA
	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: tabletType,
	}
	hc := discovery.NewFakeHealthCheck()
	dg := createDiscoveryGateway(hc, topo.Server{}, nil, "cell", 2).(*discoveryGateway)

	// retry error - no retry
	hc.Reset()
	dg.tsc.ResetForTesting()
	sc1 := hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil)
	sc2 := hc.AddTestTablet("cell", "1.1.1.1", 1002, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
	sc2.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
	ep1 := sc1.Tablet()
	ep2 := sc2.Tablet()
	wants := map[string]int{
		fmt.Sprintf(`target: ks.0.replica, used tablet: %s, FAILED_PRECONDITION error`, topotools.TabletIdent(ep1)): 0,
		fmt.Sprintf(`target: ks.0.replica, used tablet: %s, FAILED_PRECONDITION error`, topotools.TabletIdent(ep2)): 0,
	}
	err := f(dg, target)
	if _, ok := wants[fmt.Sprintf("%v", err)]; !ok {
		t.Errorf("wanted error: %+v, got error: %v", wants, err)
	}

	// server error - no retry
	hc.Reset()
	dg.tsc.ResetForTesting()
	sc1 = hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	ep1 = sc1.Tablet()
	want := fmt.Sprintf(`target: ks.0.replica, used tablet: %s, INVALID_ARGUMENT error`, topotools.TabletIdent(ep1))
	err = f(dg, target)
	verifyShardError(t, err, want, vtrpcpb.Code_INVALID_ARGUMENT)
}

func verifyShardError(t *testing.T, err error, wantErr string, wantCode vtrpcpb.Code) {
	if err == nil || err.Error() != wantErr {
		t.Errorf("wanted error: %s, got error: %v", wantErr, err)
	}
	if code := vterrors.Code(err); code != wantCode {
		t.Errorf("wanted error code: %s, got: %v", wantCode, code)
	}
}
