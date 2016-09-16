// Package gatewaytest contains a test suite to run against a Gateway object.
// We re-use the tabletconn test suite, as it tests all queries and parameters
// go through. There are two exceptions:
// - the health check: we just make that one work, so the gateway knows the
//   tablet is healthy.
// - the error type returned: it's not a TabletError any more, but a ShardError.
//   We still check the error code is correct though which is really all we care
//   about.
package gatewaytest

import (
	"fmt"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconntest"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/gateway"
	"github.com/youtube/vitess/go/vt/zktopo/zktestserver"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// gatewayAdapter implements the TabletConn interface, but sends the
// queries to the Gateway.
type gatewayAdapter struct {
	g gateway.Gateway
}

func (ga *gatewayAdapter) Execute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}, transactionID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return ga.g.Execute(ctx, target.Keyspace, target.Shard, target.TabletType, query, bindVars, transactionID, options)
}

func (ga *gatewayAdapter) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	return ga.g.ExecuteBatch(ctx, target.Keyspace, target.Shard, target.TabletType, queries, asTransaction, transactionID, options)
}

func (ga *gatewayAdapter) StreamExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error) {
	return ga.g.StreamExecute(ctx, target.Keyspace, target.Shard, target.TabletType, query, bindVars, options)
}

func (ga *gatewayAdapter) Begin(ctx context.Context, target *querypb.Target) (transactionID int64, err error) {
	return ga.g.Begin(ctx, target.Keyspace, target.Shard, target.TabletType)
}

func (ga *gatewayAdapter) Commit(ctx context.Context, target *querypb.Target, transactionID int64) error {
	return ga.g.Commit(ctx, target.Keyspace, target.Shard, target.TabletType, transactionID)
}

func (ga *gatewayAdapter) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) error {
	return ga.g.Rollback(ctx, target.Keyspace, target.Shard, target.TabletType, transactionID)
}

func (ga *gatewayAdapter) BeginExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}, options *querypb.ExecuteOptions) (result *sqltypes.Result, transactionID int64, err error) {
	return ga.g.BeginExecute(ctx, target.Keyspace, target.Shard, target.TabletType, query, bindVars, options)
}

func (ga *gatewayAdapter) BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) (results []sqltypes.Result, transactionID int64, err error) {
	return ga.g.BeginExecuteBatch(ctx, target.Keyspace, target.Shard, target.TabletType, queries, asTransaction, options)
}

func (ga *gatewayAdapter) Close() {
}

func (ga *gatewayAdapter) Tablet() *topodatapb.Tablet {
	return &topodatapb.Tablet{}
}

func (ga *gatewayAdapter) SplitQuery(ctx context.Context, target *querypb.Target, query querytypes.BoundQuery, splitColumn string, splitCount int64) ([]querytypes.QuerySplit, error) {
	return ga.g.SplitQuery(ctx, target.Keyspace, target.Shard, target.TabletType, query.Sql, query.BindVariables, splitColumn, splitCount)
}

func (ga *gatewayAdapter) SplitQueryV2(ctx context.Context, target *querypb.Target, query querytypes.BoundQuery, splitColumns []string, splitCount int64, numRowsPerQueryPart int64, algorithm querypb.SplitQueryRequest_Algorithm) (queries []querytypes.QuerySplit, err error) {
	return ga.g.SplitQueryV2(ctx, target.Keyspace, target.Shard, target.TabletType, query.Sql, query.BindVariables, splitColumns, splitCount, numRowsPerQueryPart, algorithm)
}

func (ga *gatewayAdapter) StreamHealth(ctx context.Context) (tabletconn.StreamHealthReader, error) {
	return nil, fmt.Errorf("Not Implemented")
}

func (ga *gatewayAdapter) UpdateStream(ctx context.Context, target *querypb.Target, position string, timestamp int64) (tabletconn.StreamEventReader, error) {
	return ga.g.UpdateStream(ctx, target.Keyspace, target.Shard, target.TabletType, position, timestamp)
}

// CreateFakeServers returns the servers to use for these tests
func CreateFakeServers(t *testing.T) (*tabletconntest.FakeQueryService, topo.Server, string) {
	cell := "local"

	// the FakeServer is just slightly modified
	f := tabletconntest.CreateFakeServer(t)
	f.TestingGateway = true
	f.StreamHealthResponse = &querypb.StreamHealthResponse{
		Target:  tabletconntest.TestTarget,
		Serving: true,
		TabletExternallyReparentedTimestamp: 1234589,
		RealtimeStats: &querypb.RealtimeStats{
			SecondsBehindMaster: 1,
		},
	}

	// The topo server has a single SrvKeyspace
	ts := zktestserver.New(t, []string{cell})
	if err := ts.UpdateSrvKeyspace(context.Background(), cell, tabletconntest.TestTarget.Keyspace, &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_MASTER,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name: tabletconntest.TestTarget.Shard,
					},
				},
			},
		},
	}); err != nil {
		t.Fatalf("can't add srvKeyspace: %v", err)
	}

	return f, ts, cell
}

// TestSuite executes a set of tests on the provided gateway. The provided
// gateway needs to be configured with one established connection for
// tabletconntest.TestTarget.{Keyspace, Shard, TabletType} to the
// provided tabletconntest.FakeQueryService.
func TestSuite(t *testing.T, name string, g gateway.Gateway, f *tabletconntest.FakeQueryService) {

	protocolName := "gateway-test-" + name

	tabletconn.RegisterDialer(protocolName, func(tablet *topodatapb.Tablet, timeout time.Duration) (tabletconn.TabletConn, error) {
		return &gatewayAdapter{
			g: g,
		}, nil
	})

	tabletconntest.TestSuite(t, protocolName, &topodatapb.Tablet{
		Keyspace: tabletconntest.TestTarget.Keyspace,
		Shard:    tabletconntest.TestTarget.Shard,
		Type:     tabletconntest.TestTarget.TabletType,
	}, f)
}
