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
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/vtgate/gateway"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice"
	"github.com/youtube/vitess/go/vt/vttablet/tabletconn"
	"github.com/youtube/vitess/go/vt/vttablet/tabletconntest"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

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
	ts := memorytopo.NewServer(cell)
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

// gatewayAdapter implements the TabletConn interface, but sends the
// queries to the Gateway.
type gatewayAdapter struct {
	gateway.Gateway
}

// Close should be overridden to make sure we don't close the underlying Gateway.
func (ga gatewayAdapter) Close(ctx context.Context) error {
	return nil
}

// TestSuite executes a set of tests on the provided gateway. The provided
// gateway needs to be configured with one established connection for
// tabletconntest.TestTarget.{Keyspace, Shard, TabletType} to the
// provided tabletconntest.FakeQueryService.
func TestSuite(t *testing.T, name string, g gateway.Gateway, f *tabletconntest.FakeQueryService) {

	protocolName := "gateway-test-" + name

	tabletconn.RegisterDialer(protocolName, func(tablet *topodatapb.Tablet, timeout time.Duration) (queryservice.QueryService, error) {
		return &gatewayAdapter{Gateway: g}, nil
	})

	tabletconntest.TestSuite(t, protocolName, &topodatapb.Tablet{
		Keyspace: tabletconntest.TestTarget.Keyspace,
		Shard:    tabletconntest.TestTarget.Shard,
		Type:     tabletconntest.TestTarget.TabletType,
	}, f)
}
