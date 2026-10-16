/*
Copyright 2026 The Vitess Authors.

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

package buffer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const moveTablesCell = "zone1"

// moveTablesSrvTopo serves a single keyspace with a single shard from a
// memorytopo server. Its SrvVSchema watch delivers srvVSchema once; later
// updates are delivered through KeyspaceEventWatcher.SrvVSchemaInstalled, as
// vtgate's executor does.
type moveTablesSrvTopo struct {
	ts         *topo.Server
	srvVSchema *vschemapb.SrvVSchema
}

func (s *moveTablesSrvTopo) GetTopoServer() (*topo.Server, error) {
	return s.ts, nil
}

func (s *moveTablesSrvTopo) GetSrvKeyspaceNames(ctx context.Context, cell string, staleOK bool) ([]string, error) {
	return []string{keyspace}, nil
}

func (s *moveTablesSrvTopo) GetSrvKeyspace(ctx context.Context, cell, ks string) (*topodatapb.SrvKeyspace, error) {
	return &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{{
			ServedType:      topodatapb.TabletType_PRIMARY,
			ShardReferences: []*topodatapb.ShardReference{{Name: shard}},
		}},
	}, nil
}

func (s *moveTablesSrvTopo) WatchSrvKeyspace(ctx context.Context, cell, ks string, callback func(*topodatapb.SrvKeyspace, error) bool) {
	callback(s.GetSrvKeyspace(ctx, cell, ks))
}

func (s *moveTablesSrvTopo) WatchSrvVSchema(ctx context.Context, cell string, callback func(*vschemapb.SrvVSchema, error) bool) {
	callback(s.srvVSchema, nil)
}

// TestMoveTablesBufferingEndsWhenWritesSwitch verifies that writes buffered
// because a MoveTables traffic switch denied their table on the source primary
// stay buffered until the routing rules are switched. The source primary keeps
// reporting itself as serving throughout, and its health checks must not end
// the buffering: released writes would only be denied again.
func TestMoveTablesBufferingEndsWhenWritesSwitch(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	ts := memorytopo.NewServer(ctx, moveTablesCell)
	t.Cleanup(ts.Close)
	require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}))
	require.NoError(t, ts.CreateShard(ctx, keyspace, shard))

	routingRules := func(to string) *vschemapb.SrvVSchema {
		return &vschemapb.SrvVSchema{RoutingRules: &vschemapb.RoutingRules{Rules: []*vschemapb.RoutingRule{
			{FromTable: "t1", ToTables: []string{to + ".t1"}},
			{FromTable: keyspace + ".t1", ToTables: []string{to + ".t1"}},
			{FromTable: "target.t1", ToTables: []string{to + ".t1"}},
		}}}
	}

	// The MoveTables workflow routes writes to the source, and vtgate has
	// seen its routing rules.
	healthChecks := make(chan *discovery.TabletHealth, 10)
	srvTopo := &moveTablesSrvTopo{ts: ts, srvVSchema: routingRules(keyspace)}
	kew := discovery.NewKeyspaceEventWatcher(ctx, srvTopo, discovery.NewFakeHealthCheck(healthChecks), moveTablesCell)

	cfg := NewDefaultConfig()
	cfg.Enabled = true
	// No timeout may end the buffering during the test.
	cfg.Window = time.Minute
	cfg.MaxFailoverDuration = time.Minute
	b := New(cfg)
	t.Cleanup(b.Shutdown)

	// Forward keyspace events to the buffer, as the TabletGateway does.
	events := kew.Subscribe()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-events:
				b.HandleKeyspaceEvent(ev)
			}
		}
	}()

	primaryHealth := &discovery.TabletHealth{
		Tablet: &topodatapb.Tablet{
			Alias:    &topodatapb.TabletAlias{Cell: moveTablesCell, Uid: 100},
			Keyspace: keyspace,
			Shard:    shard,
			Type:     topodatapb.TabletType_PRIMARY,
		},
		Target:               &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_PRIMARY},
		Serving:              true,
		PrimaryTermStartTime: 1,
	}
	healthChecks <- primaryHealth
	// Wait for the buffer to process the event that reports the keyspace as
	// consistent, so that it cannot end the buffering started below.
	sb := b.getOrCreateBuffer(keyspace, shard)
	require.Eventually(t, func() bool {
		sb.mu.RLock()
		defer sb.mu.RUnlock()
		return sb.currentPrimary != nil
	}, 30*time.Second, 10*time.Millisecond)

	// SwitchTraffic denies the table on the source. Nothing rebuilds the
	// SrvVSchema until it changes the routing rules.
	_, err := ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		si.TabletControls = []*topodatapb.Shard_TabletControl{{
			TabletType:   topodatapb.TabletType_PRIMARY,
			DeniedTables: []string{"t1"},
		}}
		return nil
	})
	require.NoError(t, err)

	// A write is denied by the source primary and buffered.
	denied := vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "disallowed due to rule: enforce denied tables")
	released := make(chan error, 1)
	go func() {
		retryDone, err := b.WaitForFailoverEnd(ctx, keyspace, shard, kew, denied)
		if retryDone != nil {
			retryDone()
		}
		released <- err
	}()
	require.NoError(t, waitForRequestsInFlight(b, 1))

	// The source primary is still serving and says so.
	healthChecks <- primaryHealth
	require.Never(t, func() bool {
		return len(released) > 0
	}, time.Second, 10*time.Millisecond, "a health check from the source primary ended MoveTables buffering")

	// The executor routes writes to the target: the buffered write is released.
	kew.SrvVSchemaInstalled(routingRules("target"))
	select {
	case err := <-released:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the buffered write was not released after the routing rules switched")
	}
	require.NoError(t, waitForState(b, stateIdle))
}
