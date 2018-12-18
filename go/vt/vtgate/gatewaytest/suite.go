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

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtgate/gateway"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/vttablet/tabletconntest"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// CreateFakeServers returns the servers to use for these tests
func CreateFakeServers(t *testing.T) (*tabletconntest.FakeQueryService, *topo.Server, string) {
	cell := "local"

	// the FakeServer is just slightly modified
	f := tabletconntest.CreateFakeServer(t)
	f.TestingGateway = true
	f.StreamHealthResponse = &querypb.StreamHealthResponse{
		Target:                              tabletconntest.TestTarget,
		Serving:                             true,
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
				ServedType: tabletconntest.TestTarget.TabletType,
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

	tabletconn.RegisterDialer(protocolName, func(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
		return &gatewayAdapter{Gateway: g}, nil
	})

	tabletconntest.TestSuite(t, protocolName, &topodatapb.Tablet{
		Keyspace: tabletconntest.TestTarget.Keyspace,
		Shard:    tabletconntest.TestTarget.Shard,
		Type:     tabletconntest.TestTarget.TabletType,
	}, f, nil)
}
