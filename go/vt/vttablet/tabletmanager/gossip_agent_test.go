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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/gossip"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func testGossipTablet(portMap map[string]int32) *topodatapb.Tablet {
	return &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Hostname: "host1",
		PortMap:  portMap,
		Keyspace: "ks",
		Shard:    "0",
	}
}

func TestNewGossipAgentDisabledCases(t *testing.T) {
	tests := []struct {
		name   string
		cfg    *topodatapb.GossipConfig
		tablet *topodatapb.Tablet
	}{{
		name:   "nil config",
		tablet: testGossipTablet(map[string]int32{"grpc": 15999}),
	}, {
		name:   "disabled config",
		cfg:    &topodatapb.GossipConfig{Enabled: false},
		tablet: testGossipTablet(map[string]int32{"grpc": 15999}),
	}, {
		name: "nil tablet",
		cfg:  &topodatapb.GossipConfig{Enabled: true},
	}, {
		name:   "no grpc port",
		cfg:    &topodatapb.GossipConfig{Enabled: true},
		tablet: testGossipTablet(map[string]int32{"vt": 15999}),
	}, {
		name:   "zero grpc port",
		cfg:    &topodatapb.GossipConfig{Enabled: true},
		tablet: testGossipTablet(map[string]int32{"grpc": 0}),
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agent, enabled := newGossipAgent(tt.cfg, tt.tablet, nil)
			assert.Nil(t, agent)
			assert.False(t, enabled)
		})
	}
}

func TestNewGossipAgentEnabledCases(t *testing.T) {
	tests := []struct {
		name string
		cfg  *topodatapb.GossipConfig
	}{{
		name: "explicit config",
		cfg: &topodatapb.GossipConfig{
			Enabled:      true,
			PhiThreshold: 5,
			PingInterval: "2s",
			MaxUpdateAge: "10s",
		},
	}, {
		name: "default phi threshold",
		cfg: &topodatapb.GossipConfig{
			Enabled:      true,
			PhiThreshold: 0,
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agent, enabled := newGossipAgent(tt.cfg, testGossipTablet(map[string]int32{"grpc": 15999}), nil)
			require.NotNil(t, agent)
			assert.True(t, enabled)

			members := agent.Members()
			require.Len(t, members, 1)
			assert.Equal(t, "host1:15999", members[0].Addr)
		})
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		input string
		want  time.Duration
	}{{
		input: "2s",
		want:  2 * time.Second,
	}, {
		input: "",
		want:  time.Second,
	}, {
		input: "invalid",
		want:  time.Second,
	}, {
		input: "-1s",
		want:  time.Second,
	}, {
		input: "0s",
		want:  time.Second,
	}, {
		input: "500ms",
		want:  500 * time.Millisecond,
	}}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.want, parseDuration(tt.input, time.Second))
		})
	}
}

func TestDiscoverSeeds_NilTopoServer(t *testing.T) {
	seeds := discoverSeeds(testGossipTablet(map[string]int32{"grpc": 15999}), nil)
	assert.Nil(t, seeds)
}

func TestDiscoverSeeds_WithTablets(t *testing.T) {
	ctx := t.Context()
	topoServer := memorytopo.NewServer(ctx, "zone1")

	// Create shard and add tablets.
	_, err := topoServer.GetOrCreateShard(ctx, "ks", "0")
	require.NoError(t, err)

	self := testGossipTablet(map[string]int32{"grpc": 15100})
	peer := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 200},
		Hostname: "host2",
		PortMap:  map[string]int32{"grpc": 15200},
		Keyspace: "ks",
		Shard:    "0",
	}
	peerNoGrpc := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 300},
		Hostname: "host3",
		PortMap:  map[string]int32{"vt": 15300},
		Keyspace: "ks",
		Shard:    "0",
	}

	require.NoError(t, topoServer.CreateTablet(ctx, self))
	require.NoError(t, topoServer.CreateTablet(ctx, peer))
	require.NoError(t, topoServer.CreateTablet(ctx, peerNoGrpc))

	seeds := discoverSeeds(self, topoServer)

	// Should find peer (has grpc port) but not self or peerNoGrpc.
	assert.Len(t, seeds, 1)
	assert.Equal(t, "host2:15200", seeds[0].Addr)
}

func TestDiscoverSeeds_EmptyShard(t *testing.T) {
	ctx := t.Context()
	topoServer := memorytopo.NewServer(ctx, "zone1")

	_, err := topoServer.GetOrCreateShard(ctx, "ks", "0")
	require.NoError(t, err)

	self := testGossipTablet(map[string]int32{"grpc": 15100})
	require.NoError(t, topoServer.CreateTablet(ctx, self))

	seeds := discoverSeeds(self, topoServer)
	// Only self in shard — no seeds.
	assert.Empty(t, seeds)
}

func TestSetGossip(t *testing.T) {
	tm := &TabletManager{}
	agent := gossip.New(gossip.Config{}, nil, nil)
	tm.SetGossip(agent, true)
	assert.Equal(t, agent, tm.Gossip)
	assert.True(t, tm.GossipEnabled)
}
