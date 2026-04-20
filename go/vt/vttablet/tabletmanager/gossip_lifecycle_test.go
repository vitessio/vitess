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

package tabletmanager

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/gossip"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestTabletManagerCloseStopsGossipLifecycle(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)
	t.Cleanup(tm.Stop)

	tm.Close()

	assert.Nil(t, tm.Gossip)
	assert.Nil(t, tm.gossipCancel)
	assert.False(t, tm.GossipEnabled)
}

func TestTabletManagerStopConcurrentWithGossipConfigChange(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)
	tablet := tm.Tablet()
	tm.SetGossip(nil, false)

	enabled := &topodatapb.SrvKeyspace{GossipConfig: &topodatapb.GossipConfig{
		Enabled:      true,
		PhiThreshold: 4,
		PingInterval: "1s",
		MaxUpdateAge: "5s",
	}}
	disabled := &topodatapb.SrvKeyspace{}
	done := make(chan struct{})

	go func() {
		defer close(done)
		for range 200 {
			tm.applyGossipConfigChange(enabled, tablet)
			tm.applyGossipConfigChange(disabled, tablet)
		}
	}()

	tm.Stop()
	<-done
}

func TestWatchGossipConfigRecoversAfterSrvKeyspaceCreate(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1")
	tablet := newTestTablet(t, 1, "ks", "0", nil)
	tm := &TabletManager{BatchCtx: ctx, TopoServer: ts}

	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go tm.watchGossipConfig(watchCtx, tablet)

	require.NoError(t, ts.UpdateSrvKeyspace(ctx, tablet.Alias.Cell, tablet.Keyspace, &topodatapb.SrvKeyspace{
		GossipConfig: &topodatapb.GossipConfig{
			Enabled:      true,
			PhiThreshold: 4,
			PingInterval: "100ms",
			MaxUpdateAge: "1s",
		},
	}))

	require.Eventually(t, func() bool {
		return tm.currentGossipAgent() != nil
	}, 5*time.Second, 20*time.Millisecond)
	tm.stopGossipLifecycle()
}

func TestWatchGossipConfigRecoversAfterDeleteAndRecreate(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1")
	tablet := newTestTablet(t, 1, "ks", "0", nil)
	tm := &TabletManager{BatchCtx: ctx, TopoServer: ts}

	require.NoError(t, ts.UpdateSrvKeyspace(ctx, tablet.Alias.Cell, tablet.Keyspace, &topodatapb.SrvKeyspace{
		GossipConfig: &topodatapb.GossipConfig{
			Enabled:      true,
			PhiThreshold: 4,
			PingInterval: "100ms",
			MaxUpdateAge: "1s",
		},
	}))

	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go tm.watchGossipConfig(watchCtx, tablet)

	require.Eventually(t, func() bool {
		return tm.currentGossipAgent() != nil
	}, 5*time.Second, 20*time.Millisecond)

	require.NoError(t, ts.DeleteSrvKeyspace(ctx, tablet.Alias.Cell, tablet.Keyspace))
	require.Eventually(t, func() bool {
		return tm.currentGossipAgent() == nil
	}, 5*time.Second, 20*time.Millisecond)

	require.NoError(t, ts.UpdateSrvKeyspace(ctx, tablet.Alias.Cell, tablet.Keyspace, &topodatapb.SrvKeyspace{
		GossipConfig: &topodatapb.GossipConfig{
			Enabled:      true,
			PhiThreshold: 4,
			PingInterval: "100ms",
			MaxUpdateAge: "1s",
		},
	}))

	require.Eventually(t, func() bool {
		return tm.currentGossipAgent() != nil
	}, 5*time.Second, 20*time.Millisecond)
	tm.stopGossipLifecycle()
}

func TestCurrentProcessGossipAgentTracksLatestTabletManager(t *testing.T) {
	old := processGossipManager.Load()
	t.Cleanup(func() {
		processGossipManager.Store(old)
	})

	agent1 := gossip.New(gossip.Config{NodeID: "node1"}, nil, nil)
	agent2 := gossip.New(gossip.Config{NodeID: "node2"}, nil, nil)

	tm1 := &TabletManager{}
	tm1.SetGossip(agent1, true)
	tm2 := &TabletManager{}
	tm2.SetGossip(agent2, true)

	setProcessGossipManager(tm1)
	require.Same(t, agent1, currentProcessGossipAgent())

	setProcessGossipManager(tm2)
	require.Same(t, agent2, currentProcessGossipAgent())
}
