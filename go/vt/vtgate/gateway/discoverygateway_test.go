package gateway

import (
	"fmt"
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/topo"
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
		queries := []querytypes.BoundQuery{{Sql: "query", BindVariables: nil}}
		_, err := dg.ExecuteBatch(context.Background(), target, queries, false, 0, nil)
		return err
	})
	testDiscoveryGatewayTransact(t, false, func(dg Gateway, target *querypb.Target) error {
		queries := []querytypes.BoundQuery{{Sql: "query", BindVariables: nil}}
		_, err := dg.ExecuteBatch(context.Background(), target, queries, false, 1, nil)
		return err
	})
}

func TestDiscoveryGatewayExecuteStream(t *testing.T) {
	testDiscoveryGatewayGeneric(t, true, func(dg Gateway, target *querypb.Target) error {
		_, err := dg.StreamExecute(context.Background(), target, "query", nil, nil)
		return err
	})
}

func TestDiscoveryGatewayBegin(t *testing.T) {
	testDiscoveryGatewayGeneric(t, false, func(dg Gateway, target *querypb.Target) error {
		_, err := dg.Begin(context.Background(), target)
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
		queries := []querytypes.BoundQuery{{Sql: "query", BindVariables: nil}}
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
	verifyShardError(t, err, want, vtrpcpb.ErrorCode_INTERNAL_ERROR)

	// tablet with error
	hc.Reset()
	dg.tsc.ResetForTesting()
	hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, false, 10, fmt.Errorf("no connection"))
	want = "target: ks.0.replica, no valid tablet"
	err = f(dg, target)
	verifyShardError(t, err, want, vtrpcpb.ErrorCode_INTERNAL_ERROR)

	// tablet without connection
	hc.Reset()
	dg.tsc.ResetForTesting()
	ep1 := hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, false, 10, nil).Tablet()
	want = fmt.Sprintf(`target: ks.0.replica, no valid tablet`)
	err = f(dg, target)
	verifyShardError(t, err, want, vtrpcpb.ErrorCode_INTERNAL_ERROR)

	// retry error
	hc.Reset()
	dg.tsc.ResetForTesting()
	sc1 := hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil)
	sc2 := hc.AddTestTablet("cell", "1.1.1.1", 1002, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailRetry = 1
	sc2.MustFailRetry = 1
	ep1 = sc1.Tablet()
	ep2 := sc2.Tablet()
	wants := map[string]int{
		fmt.Sprintf(`target: ks.0.replica, used tablet: (%+v), retry: err`, ep1): 0,
		fmt.Sprintf(`target: ks.0.replica, used tablet: (%+v), retry: err`, ep2): 0,
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
	sc1.MustFailFatal = 1
	sc2.MustFailFatal = 1
	ep1 = sc1.Tablet()
	ep2 = sc2.Tablet()
	wants = map[string]int{
		fmt.Sprintf(`target: ks.0.replica, used tablet: (%+v), fatal: err`, ep1): 0,
		fmt.Sprintf(`target: ks.0.replica, used tablet: (%+v), fatal: err`, ep2): 0,
	}
	err = f(dg, target)
	if _, ok := wants[fmt.Sprintf("%v", err)]; !ok {
		t.Errorf("wanted error: %+v, got error: %v", wants, err)
	}

	// server error - no retry
	hc.Reset()
	dg.tsc.ResetForTesting()
	sc1 = hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailServer = 1
	ep1 = sc1.Tablet()
	want = fmt.Sprintf(`target: ks.0.replica, used tablet: (%+v), error: err`, ep1)
	err = f(dg, target)
	verifyShardError(t, err, want, vtrpcpb.ErrorCode_BAD_INPUT)

	// conn error - no retry
	hc.Reset()
	dg.tsc.ResetForTesting()
	sc1 = hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailConn = 1
	ep1 = sc1.Tablet()
	want = fmt.Sprintf(`target: ks.0.replica, used tablet: (%+v), error: conn`, ep1)
	err = f(dg, target)
	verifyShardError(t, err, want, vtrpcpb.ErrorCode_UNKNOWN_ERROR)

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
	sc1.MustFailRetry = 1
	sc2.MustFailRetry = 1
	ep1 := sc1.Tablet()
	ep2 := sc2.Tablet()
	wants := map[string]int{
		fmt.Sprintf(`target: ks.0.replica, used tablet: (%+v), retry: err`, ep1): 0,
		fmt.Sprintf(`target: ks.0.replica, used tablet: (%+v), retry: err`, ep2): 0,
	}
	err := f(dg, target)
	if _, ok := wants[fmt.Sprintf("%v", err)]; !ok {
		t.Errorf("wanted error: %+v, got error: %v", wants, err)
	}

	// conn error - no retry
	hc.Reset()
	dg.tsc.ResetForTesting()
	sc1 = hc.AddTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil)
	sc1.MustFailConn = 1
	ep1 = sc1.Tablet()
	want := fmt.Sprintf(`target: ks.0.replica, used tablet: (%+v), error: conn`, ep1)
	err = f(dg, target)
	verifyShardError(t, err, want, vtrpcpb.ErrorCode_UNKNOWN_ERROR)
}

func verifyShardError(t *testing.T, err error, wantErr string, wantCode vtrpcpb.ErrorCode) {
	if err == nil || err.Error() != wantErr {
		t.Errorf("wanted error: %s, got error: %v", wantErr, err)
	}
	if _, ok := err.(*ShardError); !ok {
		t.Errorf("wanted error type *ShardConnError, got error type: %v", reflect.TypeOf(err))
	}
	code := vterrors.RecoverVtErrorCode(err)
	if code != wantCode {
		t.Errorf("wanted error code: %s, got: %v", wantCode, code)
	}
}
