package vtgate

import (
	"fmt"
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vterrors"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestDiscoveryGatewayExecute(t *testing.T) {
	testDiscoveryGatewayGeneric(t, false, func(dg Gateway, keyspace, shard string, tabletType topodatapb.TabletType) error {
		_, err := dg.Execute(context.Background(), keyspace, shard, tabletType, "query", nil, 0)
		return err
	})
	testDiscoveryGatewayTransact(t, false, func(dg Gateway, keyspace, shard string, tabletType topodatapb.TabletType) error {
		_, err := dg.Execute(context.Background(), keyspace, shard, tabletType, "query", nil, 1)
		return err
	})
}

func TestDiscoveryGatewayExecuteBatch(t *testing.T) {
	testDiscoveryGatewayGeneric(t, false, func(dg Gateway, keyspace, shard string, tabletType topodatapb.TabletType) error {
		queries := []querytypes.BoundQuery{{"query", nil}}
		_, err := dg.ExecuteBatch(context.Background(), keyspace, shard, tabletType, queries, false, 0)
		return err
	})
	testDiscoveryGatewayTransact(t, false, func(dg Gateway, keyspace, shard string, tabletType topodatapb.TabletType) error {
		queries := []querytypes.BoundQuery{{"query", nil}}
		_, err := dg.ExecuteBatch(context.Background(), keyspace, shard, tabletType, queries, false, 1)
		return err
	})
}

func TestDiscoveryGatewayExecuteStream(t *testing.T) {
	testDiscoveryGatewayGeneric(t, true, func(dg Gateway, keyspace, shard string, tabletType topodatapb.TabletType) error {
		_, err := dg.StreamExecute(context.Background(), keyspace, shard, tabletType, "query", nil)
		return err
	})
}

func TestDiscoveryGatewayBegin(t *testing.T) {
	testDiscoveryGatewayGeneric(t, false, func(dg Gateway, keyspace, shard string, tabletType topodatapb.TabletType) error {
		_, err := dg.Begin(context.Background(), keyspace, shard, tabletType)
		return err
	})
}

func TestDiscoveryGatewayCommit(t *testing.T) {
	testDiscoveryGatewayTransact(t, false, func(dg Gateway, keyspace, shard string, tabletType topodatapb.TabletType) error {
		return dg.Commit(context.Background(), keyspace, shard, tabletType, 1)
	})
}

func TestDiscoveryGatewayRollback(t *testing.T) {
	testDiscoveryGatewayTransact(t, false, func(dg Gateway, keyspace, shard string, tabletType topodatapb.TabletType) error {
		return dg.Rollback(context.Background(), keyspace, shard, tabletType, 1)
	})
}

func TestDiscoveryGatewayBeginExecute(t *testing.T) {
	testDiscoveryGatewayGeneric(t, false, func(dg Gateway, keyspace, shard string, tabletType topodatapb.TabletType) error {
		_, _, err := dg.BeginExecute(context.Background(), keyspace, shard, tabletType, "query", nil)
		return err
	})
}

func TestDiscoveryGatewayBeginExecuteBatch(t *testing.T) {
	testDiscoveryGatewayGeneric(t, false, func(dg Gateway, keyspace, shard string, tabletType topodatapb.TabletType) error {
		queries := []querytypes.BoundQuery{{"query", nil}}
		_, _, err := dg.BeginExecuteBatch(context.Background(), keyspace, shard, tabletType, queries, false)
		return err
	})
}

func TestDiscoveryGatewayGetTablets(t *testing.T) {
	keyspace := "ks"
	shard := "0"
	hc := newFakeHealthCheck()
	dg := createDiscoveryGateway(hc, topo.Server{}, nil, "local", 2, nil).(*discoveryGateway)

	// replica should only use local ones
	hc.Reset()
	hc.addTestTablet("remote", "1.1.1.1", 1001, keyspace, shard, topodatapb.TabletType_REPLICA, true, 10, nil, nil)
	ep1 := hc.addTestTablet("local", "2.2.2.2", 1001, keyspace, shard, topodatapb.TabletType_REPLICA, true, 10, nil, nil)
	eps := dg.getTablets(keyspace, shard, topodatapb.TabletType_REPLICA)
	if len(eps) != 1 || !topo.TabletEquality(eps[0], ep1) {
		t.Errorf("want %+v, got %+v", ep1, eps)
	}

	// master should use the one with newer timestamp regardless of cell
	hc.Reset()
	hc.addTestTablet("remote", "1.1.1.1", 1001, keyspace, shard, topodatapb.TabletType_MASTER, true, 5, nil, nil)
	ep1 = hc.addTestTablet("remote", "2.2.2.2", 1001, keyspace, shard, topodatapb.TabletType_MASTER, true, 10, nil, nil)
	eps = dg.getTablets(keyspace, shard, topodatapb.TabletType_MASTER)
	if len(eps) != 1 || !topo.TabletEquality(eps[0], ep1) {
		t.Errorf("want %+v, got %+v", ep1, eps)
	}
}

func testDiscoveryGatewayGeneric(t *testing.T, streaming bool, f func(dg Gateway, keyspace, shard string, tabletType topodatapb.TabletType) error) {
	keyspace := "ks"
	shard := "0"
	tabletType := topodatapb.TabletType_REPLICA
	hc := newFakeHealthCheck()
	dg := createDiscoveryGateway(hc, topo.Server{}, nil, "cell", 2, nil)

	// no tablet
	hc.Reset()
	want := "shard, host: ks.0.replica, <nil>, no valid tablet"
	err := f(dg, keyspace, shard, tabletType)
	verifyShardError(t, err, want, vtrpcpb.ErrorCode_INTERNAL_ERROR)
	if hc.GetStatsFromTargetCounter != 1 {
		t.Errorf("hc.GetStatsFromTargetCounter = %v; want 1", hc.GetStatsFromTargetCounter)
	}

	// tablet with error
	hc.Reset()
	hc.addTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, false, 10, fmt.Errorf("no connection"), nil)
	want = "shard, host: ks.0.replica, <nil>, no valid tablet"
	err = f(dg, keyspace, shard, tabletType)
	verifyShardError(t, err, want, vtrpcpb.ErrorCode_INTERNAL_ERROR)
	if hc.GetStatsFromTargetCounter != 1 {
		t.Errorf("hc.GetStatsFromTargetCounter = %v; want 1", hc.GetStatsFromTargetCounter)
	}

	// tablet without connection
	hc.Reset()
	ep1 := hc.addTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, false, 10, nil, nil)
	want = fmt.Sprintf(`shard, host: ks.0.replica, <nil>, no valid tablet`)
	err = f(dg, keyspace, shard, tabletType)
	verifyShardError(t, err, want, vtrpcpb.ErrorCode_INTERNAL_ERROR)
	if hc.GetStatsFromTargetCounter != 1 {
		t.Errorf("hc.GetStatsFromTargetCounter = %v; want 1", hc.GetStatsFromTargetCounter)
	}

	// retry error
	hc.Reset()
	ep1 = hc.addTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil, &sandboxConn{mustFailRetry: 1})
	ep2 := hc.addTestTablet("cell", "1.1.1.1", 1002, keyspace, shard, tabletType, true, 10, nil, &sandboxConn{mustFailRetry: 1})
	wants := map[string]int{
		fmt.Sprintf(`shard, host: ks.0.replica, %+v, retry: err`, ep1): 0,
		fmt.Sprintf(`shard, host: ks.0.replica, %+v, retry: err`, ep2): 0,
	}
	err = f(dg, keyspace, shard, tabletType)
	if _, ok := wants[fmt.Sprintf("%v", err)]; !ok {
		t.Errorf("wanted error: %+v, got error: %v", wants, err)
	}
	if hc.GetStatsFromTargetCounter != 3 {
		t.Errorf("hc.GetStatsFromTargetCounter = %v; want 3", hc.GetStatsFromTargetCounter)
	}

	// fatal error
	hc.Reset()
	ep1 = hc.addTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil, &sandboxConn{mustFailFatal: 1})
	ep2 = hc.addTestTablet("cell", "1.1.1.1", 1002, keyspace, shard, tabletType, true, 10, nil, &sandboxConn{mustFailFatal: 1})
	wants = map[string]int{
		fmt.Sprintf(`shard, host: ks.0.replica, %+v, fatal: err`, ep1): 0,
		fmt.Sprintf(`shard, host: ks.0.replica, %+v, fatal: err`, ep2): 0,
	}
	err = f(dg, keyspace, shard, tabletType)
	if _, ok := wants[fmt.Sprintf("%v", err)]; !ok {
		t.Errorf("wanted error: %+v, got error: %v", wants, err)
	}
	wantCounter := 3
	if streaming {
		// streaming query does not retry on fatal
		wantCounter = 1
	}
	if hc.GetStatsFromTargetCounter != wantCounter {
		t.Errorf("hc.GetStatsFromTargetCounter = %v; want %v", hc.GetStatsFromTargetCounter, wantCounter)
	}

	// server error - no retry
	hc.Reset()
	ep1 = hc.addTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil, &sandboxConn{mustFailServer: 1})
	want = fmt.Sprintf(`shard, host: ks.0.replica, %+v, error: err`, ep1)
	err = f(dg, keyspace, shard, tabletType)
	verifyShardError(t, err, want, vtrpcpb.ErrorCode_BAD_INPUT)
	if hc.GetStatsFromTargetCounter != 1 {
		t.Errorf("hc.GetStatsFromTargetCounter = %v; want 1", hc.GetStatsFromTargetCounter)
	}

	// conn error - no retry
	hc.Reset()
	ep1 = hc.addTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil, &sandboxConn{mustFailConn: 1})
	want = fmt.Sprintf(`shard, host: ks.0.replica, %+v, error: conn`, ep1)
	err = f(dg, keyspace, shard, tabletType)
	verifyShardError(t, err, want, vtrpcpb.ErrorCode_UNKNOWN_ERROR)
	if hc.GetStatsFromTargetCounter != 1 {
		t.Errorf("hc.GetStatsFromTargetCounter = %v; want 1", hc.GetStatsFromTargetCounter)
	}

	// no failure
	hc.Reset()
	hc.addTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil, &sandboxConn{})
	err = f(dg, keyspace, shard, tabletType)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if hc.GetStatsFromTargetCounter != 1 {
		t.Errorf("hc.GetStatsFromTargetCounter = %v; want 1", hc.GetStatsFromTargetCounter)
	}
}

func testDiscoveryGatewayTransact(t *testing.T, streaming bool, f func(dg Gateway, keyspace, shard string, tabletType topodatapb.TabletType) error) {
	keyspace := "ks"
	shard := "0"
	tabletType := topodatapb.TabletType_REPLICA
	hc := newFakeHealthCheck()
	dg := createDiscoveryGateway(hc, topo.Server{}, nil, "cell", 2, nil)

	// retry error - no retry
	hc.Reset()
	ep1 := hc.addTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil, &sandboxConn{mustFailRetry: 1})
	ep2 := hc.addTestTablet("cell", "1.1.1.1", 1002, keyspace, shard, tabletType, true, 10, nil, &sandboxConn{mustFailRetry: 1})
	wants := map[string]int{
		fmt.Sprintf(`shard, host: ks.0.replica, %+v, retry: err`, ep1): 0,
		fmt.Sprintf(`shard, host: ks.0.replica, %+v, retry: err`, ep2): 0,
	}
	err := f(dg, keyspace, shard, tabletType)
	if _, ok := wants[fmt.Sprintf("%v", err)]; !ok {
		t.Errorf("wanted error: %+v, got error: %v", wants, err)
	}
	if hc.GetStatsFromTargetCounter != 1 {
		t.Errorf("hc.GetStatsFromTargetCounter = %v; want 1", hc.GetStatsFromTargetCounter)
	}

	// conn error - no retry
	hc.Reset()
	ep1 = hc.addTestTablet("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil, &sandboxConn{mustFailConn: 1})
	want := fmt.Sprintf(`shard, host: ks.0.replica, %+v, error: conn`, ep1)
	err = f(dg, keyspace, shard, tabletType)
	verifyShardError(t, err, want, vtrpcpb.ErrorCode_UNKNOWN_ERROR)
	if hc.GetStatsFromTargetCounter != 1 {
		t.Errorf("hc.GetStatsFromTargetCounter = %v; want 1", hc.GetStatsFromTargetCounter)
	}
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
