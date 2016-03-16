package discovery

import (
	"flag"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var connMap map[string]*fakeConn

func init() {
	tabletconn.RegisterDialer("fake_discovery", discoveryDialer)
	flag.Set("tablet_protocol", "fake_discovery")
	connMap = make(map[string]*fakeConn)
}

func TestHealthCheck(t *testing.T) {
	ep := topo.NewEndPoint(0, "a")
	ep.PortMap["vt"] = 1
	input := make(chan *querypb.StreamHealthResponse)
	fakeConn := createFakeConn(ep, input)
	t.Logf(`createFakeConn({Host: "a", PortMap: {"vt": 1}}, c)`)
	l := newListener()
	hc := NewHealthCheck(1*time.Millisecond, 1*time.Millisecond, time.Hour, "" /* statsSuffix */).(*HealthCheckImpl)
	hc.SetListener(l)
	hc.AddEndPoint("cell", "", ep)
	t.Logf(`hc = HealthCheck(); hc.AddEndPoint("cell", "", {Host: "a", PortMap: {"vt": 1}})`)

	// no endpoint before getting first StreamHealthResponse
	epsList := hc.GetEndPointStatsFromKeyspaceShard("k", "s")
	if len(epsList) != 0 {
		t.Errorf(`hc.GetEndPointStatsFromKeyspaceShard("k", "s") = %+v; want empty`, epsList)
	}

	// one endpoint after receiving a StreamHealthResponse
	shr := &querypb.StreamHealthResponse{
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Serving: true,
		TabletExternallyReparentedTimestamp: 10,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}
	want := &EndPointStats{
		EndPoint: ep,
		Cell:     "cell",
		Target:   &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Up:       true,
		Serving:  true,
		Stats:    &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		TabletExternallyReparentedTimestamp: 10,
	}
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: MASTER}, Serving: true, TabletExternallyReparentedTimestamp: 10, {SecondsBehindMaster: 1, CpuUsage: 0.2}}`)
	res := <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}
	epsList = hc.GetEndPointStatsFromKeyspaceShard("k", "s")
	if len(epsList) != 1 || !reflect.DeepEqual(epsList[0], want) {
		t.Errorf(`hc.GetEndPointStatsFromKeyspaceShard("k", "s") = %+v; want %+v`, epsList, want)
	}
	epcsl := hc.CacheStatus()
	epcslWant := EndPointsCacheStatusList{{
		Cell:   "cell",
		Target: &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		EndPointsStats: EndPointStatsList{{
			EndPoint: ep,
			Cell:     "cell",
			Target:   &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
			Up:       true,
			Serving:  true,
			Stats:    &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
			TabletExternallyReparentedTimestamp: 10,
		}},
	}}
	if !reflect.DeepEqual(epcsl, epcslWant) {
		t.Errorf(`hc.CacheStatus() = %+v; want %+v`, epcsl, epcslWant)
	}

	// TabletType changed
	shr = &querypb.StreamHealthResponse{
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving: true,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.5},
	}
	want = &EndPointStats{
		EndPoint: ep,
		Cell:     "cell",
		Target:   &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:       true,
		Serving:  true,
		Stats:    &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.5},
		TabletExternallyReparentedTimestamp: 0,
	}
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: REPLICA}, Serving: true, TabletExternallyReparentedTimestamp: 0, {SecondsBehindMaster: 1, CpuUsage: 0.5}}`)
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}
	epsList = hc.GetEndPointStatsFromTarget("k", "s", topodatapb.TabletType_REPLICA)
	if len(epsList) != 1 || !reflect.DeepEqual(epsList[0], want) {
		t.Errorf(`hc.GetEndPointStatsFromTarget("k", "s", REPLICA) = %+v; want %+v`, epsList, want)
	}

	// Serving & RealtimeStats changed
	shr = &querypb.StreamHealthResponse{
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving: false,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.3},
	}
	want = &EndPointStats{
		EndPoint: ep,
		Cell:     "cell",
		Target:   &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:       true,
		Serving:  false,
		Stats:    &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.3},
		TabletExternallyReparentedTimestamp: 0,
	}
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: REPLICA}, TabletExternallyReparentedTimestamp: 0, {SecondsBehindMaster: 1, CpuUsage: 0.3}}`)
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}

	// HealthError
	shr = &querypb.StreamHealthResponse{
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving: true,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{HealthError: "some error", SecondsBehindMaster: 1, CpuUsage: 0.3},
	}
	want = &EndPointStats{
		EndPoint: ep,
		Cell:     "cell",
		Target:   &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:       true,
		Serving:  false,
		Stats:    &querypb.RealtimeStats{HealthError: "some error", SecondsBehindMaster: 1, CpuUsage: 0.3},
		TabletExternallyReparentedTimestamp: 0,
		LastError:                           fmt.Errorf("vttablet error: some error"),
	}
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: REPLICA}, Serving: true, TabletExternallyReparentedTimestamp: 0, {HealthError: "some error", SecondsBehindMaster: 1, CpuUsage: 0.3}}`)
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}

	// remove endpoint
	hc.deleteConn(ep)
	close(fakeConn.hcChan)
	t.Logf(`hc.RemoveEndPoint({Host: "a", PortMap: {"vt": 1}})`)
	want = &EndPointStats{
		EndPoint: ep,
		Cell:     "cell",
		Target:   &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:       false,
		Serving:  false,
		Stats:    &querypb.RealtimeStats{HealthError: "some error", SecondsBehindMaster: 1, CpuUsage: 0.3},
		TabletExternallyReparentedTimestamp: 0,
		LastError:                           fmt.Errorf("recv error"),
	}
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}
	epsList = hc.GetEndPointStatsFromKeyspaceShard("k", "s")
	if len(epsList) != 0 {
		t.Errorf(`hc.GetEndPointStatsFromKeyspaceShard("k", "s") = %+v; want empty`, epsList)
	}
	// close healthcheck
	hc.Close()
}

func TestHealthCheckTimeout(t *testing.T) {
	timeout := 500 * time.Millisecond
	ep := topo.NewEndPoint(0, "a")
	ep.PortMap["vt"] = 1
	input := make(chan *querypb.StreamHealthResponse)
	createFakeConn(ep, input)
	t.Logf(`createFakeConn({Host: "a", PortMap: {"vt": 1}}, c)`)
	l := newListener()
	hc := NewHealthCheck(1*time.Millisecond, 1*time.Millisecond, timeout, "" /* statsSuffix */).(*HealthCheckImpl)
	hc.SetListener(l)
	hc.AddEndPoint("cell", "", ep)
	t.Logf(`hc = HealthCheck(); hc.AddEndPoint("cell", "", {Host: "a", PortMap: {"vt": 1}})`)

	// one endpoint after receiving a StreamHealthResponse
	shr := &querypb.StreamHealthResponse{
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Serving: true,
		TabletExternallyReparentedTimestamp: 10,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}
	want := &EndPointStats{
		EndPoint: ep,
		Cell:     "cell",
		Target:   &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Up:       true,
		Serving:  true,
		Stats:    &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		TabletExternallyReparentedTimestamp: 10,
	}
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: MASTER}, Serving: true, TabletExternallyReparentedTimestamp: 10, {SecondsBehindMaster: 1, CpuUsage: 0.2}}`)
	res := <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}
	epsList := hc.GetEndPointStatsFromKeyspaceShard("k", "s")
	if len(epsList) != 1 || !reflect.DeepEqual(epsList[0], want) {
		t.Errorf(`hc.GetEndPointStatsFromKeyspaceShard("k", "s") = %+v; want %+v`, epsList, want)
	}
	// wait for timeout period
	time.Sleep(2 * timeout)
	t.Logf(`Sleep(2 * timeout)`)
	res = <-l.output
	if res.Serving {
		t.Errorf(`<-l.output: %+v; want not serving`, res)
	}
	epsList = hc.GetEndPointStatsFromKeyspaceShard("k", "s")
	if len(epsList) != 1 || epsList[0].Serving {
		t.Errorf(`hc.GetEndPointStatsFromKeyspaceShard("k", "s") = %+v; want not serving`, epsList)
	}
	// send a healthcheck response, it should be serving again
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: MASTER}, Serving: true, TabletExternallyReparentedTimestamp: 10, {SecondsBehindMaster: 1, CpuUsage: 0.2}}`)
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}
	epsList = hc.GetEndPointStatsFromKeyspaceShard("k", "s")
	if len(epsList) != 1 || !reflect.DeepEqual(epsList[0], want) {
		t.Errorf(`hc.GetEndPointStatsFromKeyspaceShard("k", "s") = %+v; want %+v`, epsList, want)
	}
	// close healthcheck
	hc.Close()
}

type listener struct {
	output chan *EndPointStats
}

func newListener() *listener {
	return &listener{output: make(chan *EndPointStats, 1)}
}

func (l *listener) StatsUpdate(eps *EndPointStats) {
	l.output <- eps
}

func createFakeConn(endPoint *topodatapb.EndPoint, c chan *querypb.StreamHealthResponse) *fakeConn {
	key := EndPointToMapKey(endPoint)
	conn := &fakeConn{endPoint: endPoint, hcChan: c}
	connMap[key] = conn
	return conn
}

func discoveryDialer(ctx context.Context, endPoint *topodatapb.EndPoint, keyspace, shard string, tabletType topodatapb.TabletType, timeout time.Duration) (tabletconn.TabletConn, error) {
	key := EndPointToMapKey(endPoint)
	return connMap[key], nil
}

type fakeConn struct {
	endPoint *topodatapb.EndPoint
	hcChan   chan *querypb.StreamHealthResponse
}

type streamHealthReader struct {
	c       <-chan *querypb.StreamHealthResponse
	errFunc tabletconn.ErrFunc
}

// Recv implements tabletconn.StreamHealthReader.
// It returns one response from the chan.
func (r *streamHealthReader) Recv() (*querypb.StreamHealthResponse, error) {
	resp, ok := <-r.c
	if !ok {
		return nil, fmt.Errorf("recv error")
	}
	return resp, nil
}

// StreamHealth implements tabletconn.TabletConn.
func (fc *fakeConn) StreamHealth(ctx context.Context) (tabletconn.StreamHealthReader, error) {
	return &streamHealthReader{
		c:       fc.hcChan,
		errFunc: func() error { return nil },
	}, nil
}

// Execute implements tabletconn.TabletConn.
func (fc *fakeConn) Execute(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (*sqltypes.Result, error) {
	return nil, fmt.Errorf("not implemented")
}

// Execute2 implements tabletconn.TabletConn.
func (fc *fakeConn) Execute2(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (*sqltypes.Result, error) {
	return fc.Execute(ctx, query, bindVars, transactionID)
}

// ExecuteBatch implements tabletconn.TabletConn.
func (fc *fakeConn) ExecuteBatch(ctx context.Context, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64) ([]sqltypes.Result, error) {
	return nil, fmt.Errorf("not implemented")
}

// ExecuteBatch2 implements tabletconn.TabletConn.
func (fc *fakeConn) ExecuteBatch2(ctx context.Context, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64) ([]sqltypes.Result, error) {
	return fc.ExecuteBatch(ctx, queries, asTransaction, transactionID)
}

// StreamExecute implements tabletconn.TabletConn.
func (fc *fakeConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *sqltypes.Result, tabletconn.ErrFunc, error) {
	return nil, nil, fmt.Errorf("not implemented")
}

// StreamExecute2 implements tabletconn.TabletConn.
func (fc *fakeConn) StreamExecute2(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *sqltypes.Result, tabletconn.ErrFunc, error) {
	return fc.StreamExecute(ctx, query, bindVars, transactionID)
}

// Begin implements tabletconn.TabletConn.
func (fc *fakeConn) Begin(ctx context.Context) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

// Begin2 implements tabletconn.TabletConn.
func (fc *fakeConn) Begin2(ctx context.Context) (int64, error) {
	return fc.Begin(ctx)
}

// Commit implements tabletconn.TabletConn.
func (fc *fakeConn) Commit(ctx context.Context, transactionID int64) error {
	return fmt.Errorf("not implemented")
}

// Commit2 implements tabletconn.TabletConn.
func (fc *fakeConn) Commit2(ctx context.Context, transactionID int64) error {
	return fc.Commit(ctx, transactionID)
}

// Rollback implements tabletconn.TabletConn.
func (fc *fakeConn) Rollback(ctx context.Context, transactionID int64) error {
	return fmt.Errorf("not implemented")
}

// Rollback2 implements tabletconn.TabletConn.
func (fc *fakeConn) Rollback2(ctx context.Context, transactionID int64) error {
	return fc.Rollback(ctx, transactionID)
}

// SplitQuery implements tabletconn.TabletConn.
func (fc *fakeConn) SplitQuery(ctx context.Context, query querytypes.BoundQuery, splitColumn string, splitCount int64) ([]querytypes.QuerySplit, error) {
	return nil, fmt.Errorf("not implemented")
}

// SetTarget implements tabletconn.TabletConn.
func (fc *fakeConn) SetTarget(keyspace, shard string, tabletType topodatapb.TabletType) error {
	return fmt.Errorf("not implemented")
}

// EndPoint returns the endpoint associated with the connection.
func (fc *fakeConn) EndPoint() *topodatapb.EndPoint {
	return fc.endPoint
}

// Close closes the connection.
func (fc *fakeConn) Close() {
}
