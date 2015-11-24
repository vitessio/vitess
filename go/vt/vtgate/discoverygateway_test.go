package vtgate

import (
	"fmt"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/discovery"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
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
		queries := []tproto.BoundQuery{{"query", nil}}
		_, err := dg.ExecuteBatch(context.Background(), keyspace, shard, tabletType, queries, false, 0)
		return err
	})
	testDiscoveryGatewayTransact(t, false, func(dg Gateway, keyspace, shard string, tabletType topodatapb.TabletType) error {
		queries := []tproto.BoundQuery{{"query", nil}}
		_, err := dg.ExecuteBatch(context.Background(), keyspace, shard, tabletType, queries, false, 1)
		return err
	})
}

func TestDiscoveryGatewayExecuteStream(t *testing.T) {
	testDiscoveryGatewayGeneric(t, true, func(dg Gateway, keyspace, shard string, tabletType topodatapb.TabletType) error {
		_, errfunc := dg.StreamExecute(context.Background(), keyspace, shard, tabletType, "query", nil, 0)
		return errfunc()
	})
	testDiscoveryGatewayTransact(t, true, func(dg Gateway, keyspace, shard string, tabletType topodatapb.TabletType) error {
		_, errfunc := dg.StreamExecute(context.Background(), keyspace, shard, tabletType, "query", nil, 1)
		return errfunc()
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

func TestDiscoveryGatewayGetEndPoints(t *testing.T) {
	keyspace := "ks"
	shard := "0"
	hc := newFakeHealthCheck()
	dg := createDiscoveryGateway(hc, topo.Server{}, nil, "local", time.Millisecond, 2, time.Second, time.Second, time.Second, nil).(*discoveryGateway)

	// replica should only use local ones
	hc.Reset()
	hc.addTestEndPoint("remote", "1.1.1.1", 1001, keyspace, shard, topodatapb.TabletType_REPLICA, true, 10, nil, nil)
	ep1 := hc.addTestEndPoint("local", "2.2.2.2", 1001, keyspace, shard, topodatapb.TabletType_REPLICA, true, 10, nil, nil)
	eps := dg.getEndPoints(keyspace, shard, topodatapb.TabletType_REPLICA)
	if len(eps) != 1 || !topo.EndPointEquality(eps[0], ep1) {
		t.Errorf("want %+v, got %+v", ep1, eps)
	}

	// master should use the one with newer timestamp regardless of cell
	hc.Reset()
	hc.addTestEndPoint("remote", "1.1.1.1", 1001, keyspace, shard, topodatapb.TabletType_MASTER, true, 5, nil, nil)
	ep1 = hc.addTestEndPoint("remote", "2.2.2.2", 1001, keyspace, shard, topodatapb.TabletType_MASTER, true, 10, nil, nil)
	eps = dg.getEndPoints(keyspace, shard, topodatapb.TabletType_MASTER)
	if len(eps) != 1 || !topo.EndPointEquality(eps[0], ep1) {
		t.Errorf("want %+v, got %+v", ep1, eps)
	}
}

func testDiscoveryGatewayGeneric(t *testing.T, streaming bool, f func(dg Gateway, keyspace, shard string, tabletType topodatapb.TabletType) error) {
	keyspace := "ks"
	shard := "0"
	tabletType := topodatapb.TabletType_REPLICA
	hc := newFakeHealthCheck()
	dg := createDiscoveryGateway(hc, topo.Server{}, nil, "cell", time.Millisecond, 2, time.Second, time.Second, time.Second, nil)

	// no endpoint
	hc.Reset()
	want := "shard, host: ks.0.replica, <nil>, no valid endpoint"
	err := f(dg, keyspace, shard, tabletType)
	verifyShardConnError(t, err, want, vtrpcpb.ErrorCode_INTERNAL_ERROR)
	if hc.GetStatsFromTargetCounter != 1 {
		t.Errorf("hc.GetStatsFromTargetCounter = %v; want 1", hc.GetStatsFromTargetCounter)
	}

	// endpoint with error
	hc.Reset()
	hc.addTestEndPoint("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, false, 10, fmt.Errorf("no connection"), nil)
	want = "shard, host: ks.0.replica, <nil>, no valid endpoint"
	err = f(dg, keyspace, shard, tabletType)
	verifyShardConnError(t, err, want, vtrpcpb.ErrorCode_INTERNAL_ERROR)
	if hc.GetStatsFromTargetCounter != 1 {
		t.Errorf("hc.GetStatsFromTargetCounter = %v; want 1", hc.GetStatsFromTargetCounter)
	}

	// endpoint without connection
	hc.Reset()
	ep1 := hc.addTestEndPoint("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, false, 10, nil, nil)
	want = fmt.Sprintf(`shard, host: ks.0.replica, <nil>, no valid endpoint`)
	err = f(dg, keyspace, shard, tabletType)
	verifyShardConnError(t, err, want, vtrpcpb.ErrorCode_INTERNAL_ERROR)
	if hc.GetStatsFromTargetCounter != 1 {
		t.Errorf("hc.GetStatsFromTargetCounter = %v; want 1", hc.GetStatsFromTargetCounter)
	}

	// retry error
	hc.Reset()
	ep1 = hc.addTestEndPoint("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil, &sandboxConn{mustFailRetry: 1})
	ep2 := hc.addTestEndPoint("cell", "1.1.1.1", 1002, keyspace, shard, tabletType, true, 10, nil, &sandboxConn{mustFailRetry: 1})
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
	ep1 = hc.addTestEndPoint("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil, &sandboxConn{mustFailFatal: 1})
	ep2 = hc.addTestEndPoint("cell", "1.1.1.1", 1002, keyspace, shard, tabletType, true, 10, nil, &sandboxConn{mustFailFatal: 1})
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
	ep1 = hc.addTestEndPoint("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil, &sandboxConn{mustFailServer: 1})
	want = fmt.Sprintf(`shard, host: ks.0.replica, %+v, error: err`, ep1)
	err = f(dg, keyspace, shard, tabletType)
	verifyShardConnError(t, err, want, vtrpcpb.ErrorCode_BAD_INPUT)
	if hc.GetStatsFromTargetCounter != 1 {
		t.Errorf("hc.GetStatsFromTargetCounter = %v; want 1", hc.GetStatsFromTargetCounter)
	}

	// conn error - no retry
	hc.Reset()
	ep1 = hc.addTestEndPoint("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil, &sandboxConn{mustFailConn: 1})
	want = fmt.Sprintf(`shard, host: ks.0.replica, %+v, error: conn`, ep1)
	err = f(dg, keyspace, shard, tabletType)
	verifyShardConnError(t, err, want, vtrpcpb.ErrorCode_UNKNOWN_ERROR)
	if hc.GetStatsFromTargetCounter != 1 {
		t.Errorf("hc.GetStatsFromTargetCounter = %v; want 1", hc.GetStatsFromTargetCounter)
	}

	// no failure
	hc.Reset()
	hc.addTestEndPoint("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil, &sandboxConn{})
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
	dg := createDiscoveryGateway(hc, topo.Server{}, nil, "cell", time.Millisecond, 2, time.Second, time.Second, time.Second, nil)

	// retry error - no retry
	hc.Reset()
	ep1 := hc.addTestEndPoint("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil, &sandboxConn{mustFailRetry: 1})
	ep2 := hc.addTestEndPoint("cell", "1.1.1.1", 1002, keyspace, shard, tabletType, true, 10, nil, &sandboxConn{mustFailRetry: 1})
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
	ep1 = hc.addTestEndPoint("cell", "1.1.1.1", 1001, keyspace, shard, tabletType, true, 10, nil, &sandboxConn{mustFailConn: 1})
	want := fmt.Sprintf(`shard, host: ks.0.replica, %+v, error: conn`, ep1)
	err = f(dg, keyspace, shard, tabletType)
	verifyShardConnError(t, err, want, vtrpcpb.ErrorCode_UNKNOWN_ERROR)
	if hc.GetStatsFromTargetCounter != 1 {
		t.Errorf("hc.GetStatsFromTargetCounter = %v; want 1", hc.GetStatsFromTargetCounter)
	}
}

func newFakeHealthCheck() *fakeHealthCheck {
	return &fakeHealthCheck{items: make(map[string]*fhcItem)}
}

type fhcItem struct {
	eps  *discovery.EndPointStats
	conn tabletconn.TabletConn
}

type fakeHealthCheck struct {
	items map[string]*fhcItem

	// stats
	GetStatsFromTargetCounter        int
	GetStatsFromKeyspaceShardCounter int
}

func (fhc *fakeHealthCheck) Reset() {
	fhc.GetStatsFromTargetCounter = 0
	fhc.GetStatsFromKeyspaceShardCounter = 0
	fhc.items = make(map[string]*fhcItem)
}

// SetListener sets the listener for healthcheck updates.
func (fhc *fakeHealthCheck) SetListener(listener discovery.HealthCheckStatsListener) {
}

// AddEndPoint adds the endpoint, and starts health check.
func (fhc *fakeHealthCheck) AddEndPoint(cell, name string, endPoint *topodatapb.EndPoint) {
	key := discovery.EndPointToMapKey(endPoint)
	item := &fhcItem{
		eps: &discovery.EndPointStats{
			EndPoint: endPoint,
			Cell:     cell,
			Name:     name,
		},
	}
	fhc.items[key] = item
}

// RemoveEndPoint removes the endpoint, and stops the health check.
func (fhc *fakeHealthCheck) RemoveEndPoint(endPoint *topodatapb.EndPoint) {
	key := discovery.EndPointToMapKey(endPoint)
	delete(fhc.items, key)
}

// GetEndPointStatsFromKeyspaceShard returns all EndPointStats for the given keyspace/shard.
func (fhc *fakeHealthCheck) GetEndPointStatsFromKeyspaceShard(keyspace, shard string) []*discovery.EndPointStats {
	fhc.GetStatsFromKeyspaceShardCounter++
	var res []*discovery.EndPointStats
	for _, item := range fhc.items {
		if item.eps.Target == nil {
			continue
		}
		if item.eps.Target.Keyspace == keyspace && item.eps.Target.Shard == shard {
			res = append(res, item.eps)
		}
	}
	return res
}

// GetEndPointStatsFromTarget returns all EndPointStats for the given target.
func (fhc *fakeHealthCheck) GetEndPointStatsFromTarget(keyspace, shard string, tabletType topodatapb.TabletType) []*discovery.EndPointStats {
	fhc.GetStatsFromTargetCounter++
	var res []*discovery.EndPointStats
	for _, item := range fhc.items {
		if item.eps.Target == nil {
			continue
		}
		if item.eps.Target.Keyspace == keyspace && item.eps.Target.Shard == shard && item.eps.Target.TabletType == tabletType {
			res = append(res, item.eps)
		}
	}
	return res
}

// GetConnection returns the TabletConn of the given endpoint.
func (fhc *fakeHealthCheck) GetConnection(endPoint *topodatapb.EndPoint) tabletconn.TabletConn {
	key := discovery.EndPointToMapKey(endPoint)
	if item := fhc.items[key]; item != nil {
		return item.conn
	}
	return nil
}

// CacheStatus returns a displayable version of the cache.
func (fhc *fakeHealthCheck) CacheStatus() discovery.EndPointsCacheStatusList {
	return nil
}

func (fhc *fakeHealthCheck) addTestEndPoint(cell, host string, port int32, keyspace, shard string, tabletType topodatapb.TabletType, serving bool, reparentTS int64, err error, conn tabletconn.TabletConn) *topodatapb.EndPoint {
	ep := topo.NewEndPoint(0, host)
	ep.PortMap["vt"] = port
	key := discovery.EndPointToMapKey(ep)
	item := fhc.items[key]
	if item == nil {
		fhc.AddEndPoint(cell, "", ep)
		item = fhc.items[key]
	}
	item.eps.Target = &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: tabletType}
	item.eps.Serving = serving
	item.eps.TabletExternallyReparentedTimestamp = reparentTS
	item.eps.Stats = &querypb.RealtimeStats{}
	item.eps.LastError = err
	item.conn = conn
	return ep
}
