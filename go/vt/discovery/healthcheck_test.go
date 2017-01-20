package discovery

import (
	"bytes"
	"flag"
	"fmt"
	"html/template"
	"reflect"
	"testing"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/status"
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
	tablet := topo.NewTablet(0, "cell", "a")
	tablet.PortMap["vt"] = 1
	input := make(chan *querypb.StreamHealthResponse)
	createFakeConn(tablet, input)
	t.Logf(`createFakeConn({Host: "a", PortMap: {"vt": 1}}, c)`)
	l := newListener()
	hc := NewHealthCheck(1*time.Millisecond, 1*time.Millisecond, time.Hour).(*HealthCheckImpl)
	hc.SetListener(l, true)
	hc.AddTablet(tablet, "")
	t.Logf(`hc = HealthCheck(); hc.AddTablet({Host: "a", PortMap: {"vt": 1}}, "")`)

	// Immediately after AddTablet() there will be the first notification.
	want := &TabletStats{
		Key:     "a,vt:1",
		Tablet:  tablet,
		Target:  &querypb.Target{},
		Up:      true,
		Serving: false,
	}
	res := <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}

	// one tablet after receiving a StreamHealthResponse
	shr := &querypb.StreamHealthResponse{
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Serving: true,
		TabletExternallyReparentedTimestamp: 10,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}
	want = &TabletStats{
		Key:     "a,vt:1",
		Tablet:  tablet,
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Up:      true,
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		TabletExternallyReparentedTimestamp: 10,
	}
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: MASTER}, Serving: true, TabletExternallyReparentedTimestamp: 10, {SecondsBehindMaster: 1, CpuUsage: 0.2}}`)
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}
	tcsl := hc.CacheStatus()
	tcslWant := TabletsCacheStatusList{{
		Cell:   "cell",
		Target: &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		TabletsStats: TabletStatsList{{
			Key:     "a,vt:1",
			Tablet:  tablet,
			Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
			Up:      true,
			Serving: true,
			Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
			TabletExternallyReparentedTimestamp: 10,
		}},
	}}
	if !reflect.DeepEqual(tcsl, tcslWant) {
		t.Errorf(`hc.CacheStatus() = %+v; want %+v`, tcsl, tcslWant)
	}

	// TabletType changed, should get both old and new event
	shr = &querypb.StreamHealthResponse{
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving: true,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.5},
	}
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: REPLICA}, Serving: true, TabletExternallyReparentedTimestamp: 0, {SecondsBehindMaster: 1, CpuUsage: 0.5}}`)
	want = &TabletStats{
		Key:     "a,vt:1",
		Tablet:  tablet,
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Up:      false,
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		TabletExternallyReparentedTimestamp: 10,
	}
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}
	want = &TabletStats{
		Key:     "a,vt:1",
		Tablet:  tablet,
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:      true,
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.5},
		TabletExternallyReparentedTimestamp: 0,
	}
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}

	// Serving & RealtimeStats changed
	shr = &querypb.StreamHealthResponse{
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving: false,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.3},
	}
	want = &TabletStats{
		Key:     "a,vt:1",
		Tablet:  tablet,
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:      true,
		Serving: false,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.3},
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
	want = &TabletStats{
		Key:     "a,vt:1",
		Tablet:  tablet,
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:      true,
		Serving: false,
		Stats:   &querypb.RealtimeStats{HealthError: "some error", SecondsBehindMaster: 1, CpuUsage: 0.3},
		TabletExternallyReparentedTimestamp: 0,
		LastError:                           fmt.Errorf("vttablet error: some error"),
	}
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: REPLICA}, Serving: true, TabletExternallyReparentedTimestamp: 0, {HealthError: "some error", SecondsBehindMaster: 1, CpuUsage: 0.3}}`)
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}

	// remove tablet
	hc.deleteConn(tablet)
	t.Logf(`hc.RemoveTablet({Host: "a", PortMap: {"vt": 1}})`)
	want = &TabletStats{
		Key:     "a,vt:1",
		Tablet:  tablet,
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:      false,
		Serving: false,
		Stats:   &querypb.RealtimeStats{HealthError: "some error", SecondsBehindMaster: 1, CpuUsage: 0.3},
		TabletExternallyReparentedTimestamp: 0,
		LastError:                           context.Canceled,
	}
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}

	// close healthcheck
	hc.Close()
}

// TestHealthCheckCloseWaitsForGoRoutines tests that Close() waits for all Go
// routines to finish and the listener won't be called anymore.
func TestHealthCheckCloseWaitsForGoRoutines(t *testing.T) {
	tablet := topo.NewTablet(0, "cell", "a")
	tablet.PortMap["vt"] = 1
	input := make(chan *querypb.StreamHealthResponse, 1)
	createFakeConn(tablet, input)

	t.Logf(`createFakeConn({Host: "a", PortMap: {"vt": 1}}, c)`)

	l := newListener()
	hc := NewHealthCheck(1*time.Millisecond, 1*time.Millisecond, time.Hour).(*HealthCheckImpl)
	hc.SetListener(l, false)
	hc.AddTablet(tablet, "")
	t.Logf(`hc = HealthCheck(); hc.AddTablet({Host: "a", PortMap: {"vt": 1}}, "")`)

	// Immediately after AddTablet() there will be the first notification.
	want := &TabletStats{
		Key:     "a,vt:1",
		Tablet:  tablet,
		Target:  &querypb.Target{},
		Up:      true,
		Serving: false,
	}
	res := <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}

	// Verify that the listener works in general.
	shr := &querypb.StreamHealthResponse{
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Serving: true,
		TabletExternallyReparentedTimestamp: 10,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}
	want = &TabletStats{
		Key:     "a,vt:1",
		Tablet:  tablet,
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Up:      true,
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		TabletExternallyReparentedTimestamp: 10,
	}
	input <- shr
	t.Logf(`input <- %v`, shr)
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}

	// Change input to distinguish between stats sent before and after Close().
	shr.TabletExternallyReparentedTimestamp = 11
	// Close the healthcheck. Tablet connections are closed asynchronously and
	// Close() will block until all Go routines (one per connection) are done.
	hc.Close()

	// Try to send more updates. They should be ignored and the listener should
	// not be called from any Go routine anymore.
	// Note that this code is racy by nature. If there is a regression, it should
	// fail in some cases.
	input <- shr
	t.Logf(`input <- %v`, shr)

	// After Close() we'll receive one or two notifications with Serving == false.
	res = <-l.output
	if res.Serving {
		t.Errorf(`Received one more notification with Serving == true: %+v`, res)
	}

	select {
	case res = <-l.output:
		if res.TabletExternallyReparentedTimestamp == 10 && res.LastError == context.Canceled {
			// HealthCheck repeats the previous stats if there is an error.
			// This is expected.
			break
		}
		t.Fatalf("healthCheck still running after Close(): listener received: %v but should not have been called", res)
	case <-time.After(1 * time.Millisecond):
		// No response after timeout. Close probably closed all Go routines
		// properly and won't use the listener anymore.
	}

	// The last notification should have Up = false.
	if res.Up || res.Serving {
		t.Errorf(`Last notification doesn't have Up == false and Serving == false: %+v`, res)
	}

	// Check if there are more updates than the one emitted during Close().
	select {
	case res := <-l.output:
		t.Fatalf("healthCheck still running after Close(): listener received: %v but should not have been called", res)
	case <-time.After(1 * time.Millisecond):
		// No response after timeout. Listener probably not called again. Success.
	}
}

func TestHealthCheckTimeout(t *testing.T) {
	timeout := 500 * time.Millisecond
	tablet := topo.NewTablet(0, "cell", "a")
	tablet.PortMap["vt"] = 1
	input := make(chan *querypb.StreamHealthResponse)
	createFakeConn(tablet, input)
	t.Logf(`createFakeConn({Host: "a", PortMap: {"vt": 1}}, c)`)
	l := newListener()
	hc := NewHealthCheck(1*time.Millisecond, 1*time.Millisecond, timeout).(*HealthCheckImpl)
	hc.SetListener(l, false)
	hc.AddTablet(tablet, "")
	t.Logf(`hc = HealthCheck(); hc.AddTablet({Host: "a", PortMap: {"vt": 1}}, "")`)

	// Immediately after AddTablet() there will be the first notification.
	want := &TabletStats{
		Key:     "a,vt:1",
		Tablet:  tablet,
		Target:  &querypb.Target{},
		Up:      true,
		Serving: false,
	}
	res := <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}

	// one tablet after receiving a StreamHealthResponse
	shr := &querypb.StreamHealthResponse{
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Serving: true,
		TabletExternallyReparentedTimestamp: 10,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}
	want = &TabletStats{
		Key:     "a,vt:1",
		Tablet:  tablet,
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Up:      true,
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		TabletExternallyReparentedTimestamp: 10,
	}
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: MASTER}, Serving: true, TabletExternallyReparentedTimestamp: 10, {SecondsBehindMaster: 1, CpuUsage: 0.2}}`)
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}

	// wait for timeout period
	time.Sleep(2 * timeout)
	t.Logf(`Sleep(2 * timeout)`)
	res = <-l.output
	if res.Serving {
		t.Errorf(`<-l.output: %+v; want not serving`, res)
	}

	// send a healthcheck response, it should be serving again
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: MASTER}, Serving: true, TabletExternallyReparentedTimestamp: 10, {SecondsBehindMaster: 1, CpuUsage: 0.2}}`)
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}

	// close healthcheck
	hc.Close()
}

func TestTemplate(t *testing.T) {
	tablet := topo.NewTablet(0, "cell", "a")
	ts := []*TabletStats{
		{
			Key:     "a",
			Tablet:  tablet,
			Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
			Up:      true,
			Serving: false,
			Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.3},
			TabletExternallyReparentedTimestamp: 0,
		},
	}
	tcs := &TabletsCacheStatus{
		Cell:         "cell",
		Target:       &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		TabletsStats: ts,
	}
	templ := template.New("").Funcs(status.StatusFuncs)
	templ, err := templ.Parse(HealthCheckTemplate)
	if err != nil {
		t.Fatalf("error parsing template: %v", err)
	}
	wr := &bytes.Buffer{}
	if err := templ.Execute(wr, []*TabletsCacheStatus{tcs}); err != nil {
		t.Fatalf("error executing template: %v", err)
	}
}

type listener struct {
	output chan *TabletStats
}

func newListener() *listener {
	return &listener{output: make(chan *TabletStats, 2)}
}

func (l *listener) StatsUpdate(ts *TabletStats) {
	l.output <- ts
}

func createFakeConn(tablet *topodatapb.Tablet, c chan *querypb.StreamHealthResponse) *fakeConn {
	key := TabletToMapKey(tablet)
	conn := &fakeConn{tablet: tablet, hcChan: c}
	connMap[key] = conn
	return conn
}

func discoveryDialer(tablet *topodatapb.Tablet, timeout time.Duration) (tabletconn.TabletConn, error) {
	key := TabletToMapKey(tablet)
	return connMap[key], nil
}

type fakeConn struct {
	tablet *topodatapb.Tablet
	hcChan chan *querypb.StreamHealthResponse
}

type streamHealthReader struct {
	c <-chan *querypb.StreamHealthResponse
	// ctx is the client context which can be used to cancel an ongoing RPC.
	ctx context.Context
}

// Recv implements tabletconn.StreamHealthReader.
// It returns one response from the chan.
func (r *streamHealthReader) Recv() (*querypb.StreamHealthResponse, error) {
	select {
	case resp, ok := <-r.c:
		if !ok {
			return nil, fmt.Errorf("recv error (should not happen)")
		}
		return resp, nil
	case <-r.ctx.Done():
		// Return error because the context is done e.g. when the tablet was removed
		// from the healthcheck and the connection was closed.
		return nil, r.ctx.Err()
	}
}

// StreamHealth implements tabletconn.TabletConn.
func (fc *fakeConn) StreamHealth(ctx context.Context) (tabletconn.StreamHealthReader, error) {
	return &streamHealthReader{
		c:   fc.hcChan,
		ctx: ctx,
	}, nil
}

// Execute implements tabletconn.TabletConn.
func (fc *fakeConn) Execute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}, transactionID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return nil, fmt.Errorf("not implemented")
}

// ExecuteBatch implements tabletconn.TabletConn.
func (fc *fakeConn) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	return nil, fmt.Errorf("not implemented")
}

// StreamExecute implements tabletconn.TabletConn.
func (fc *fakeConn) StreamExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error) {
	return nil, fmt.Errorf("not implemented")
}

// Begin implements tabletconn.TabletConn.
func (fc *fakeConn) Begin(ctx context.Context, target *querypb.Target) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

// Commit implements tabletconn.TabletConn.
func (fc *fakeConn) Commit(ctx context.Context, target *querypb.Target, transactionID int64) error {
	return fmt.Errorf("not implemented")
}

// Rollback implements tabletconn.TabletConn.
func (fc *fakeConn) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) error {
	return fmt.Errorf("not implemented")
}

// Prepare implements tabletconn.TabletConn.
func (fc *fakeConn) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	return fmt.Errorf("not implemented")
}

// CommitPrepared implements tabletconn.TabletConn.
func (fc *fakeConn) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	return fmt.Errorf("not implemented")
}

// RollbackPrepared implements tabletconn.TabletConn.
func (fc *fakeConn) RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) (err error) {
	return fmt.Errorf("not implemented")
}

// CreateTransaction implements tabletconn.TabletConn.
func (fc *fakeConn) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) (err error) {
	return fmt.Errorf("not implemented")
}

// StartCommit implements tabletconn.TabletConn.
func (fc *fakeConn) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	return fmt.Errorf("not implemented")
}

// SetRollback implements tabletconn.TabletConn.
func (fc *fakeConn) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) (err error) {
	return fmt.Errorf("not implemented")
}

// ConcludeTransaction implements tabletconn.TabletConn.
func (fc *fakeConn) ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	return fmt.Errorf("not implemented")
}

// ReadTransaction implements tabletconn.TabletConn.
func (fc *fakeConn) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error) {
	return nil, fmt.Errorf("not implemented")
}

// BeginExecute implements tabletconn.TabletConn.
func (fc *fakeConn) BeginExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, error) {
	return nil, 0, fmt.Errorf("not implemented")
}

// BeginExecuteBatch implements tabletconn.TabletConn.
func (fc *fakeConn) BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) ([]sqltypes.Result, int64, error) {
	return nil, 0, fmt.Errorf("not implemented")
}

// MessageStream implements tabletconn.TabletConn.
func (fc *fakeConn) MessageStream(ctx context.Context, target *querypb.Target, name string, sendReply func(*sqltypes.Result) error) (err error) {
	return fmt.Errorf("not implemented")
}

// MessageAck implements tabletconn.TabletConn.
func (fc *fakeConn) MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (count int64, err error) {
	return 0, fmt.Errorf("not implemented")
}

// SplitQuery implements tabletconn.TabletConn.
func (fc *fakeConn) SplitQuery(
	ctx context.Context,
	target *querypb.Target,
	query querytypes.BoundQuery,
	splitColumn []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm,
) ([]querytypes.QuerySplit, error) {
	return nil, fmt.Errorf("not implemented")
}

// UpdateStream implements tabletconn.TabletConn.
func (fc *fakeConn) UpdateStream(ctx context.Context, target *querypb.Target, position string, timestamp int64) (tabletconn.StreamEventReader, error) {
	return nil, fmt.Errorf("not implemented")
}

// Tablet returns the tablet associated with the connection.
func (fc *fakeConn) Tablet() *topodatapb.Tablet {
	return fc.tablet
}

// Close closes the connection.
func (fc *fakeConn) Close(ctx context.Context) error {
	return nil
}
