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

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestNewGossipAgent_NilConfig(t *testing.T) {
	tablet := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Hostname: "host1",
		PortMap:  map[string]int32{"grpc": 15999},
		Keyspace: "ks",
		Shard:    "0",
	}
	agent, enabled := newGossipAgent(nil, tablet, nil)
	assert.Nil(t, agent)
	assert.False(t, enabled)
}

func TestNewGossipAgent_Disabled(t *testing.T) {
	cfg := &topodatapb.GossipConfig{Enabled: false}
	tablet := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Hostname: "host1",
		PortMap:  map[string]int32{"grpc": 15999},
		Keyspace: "ks",
		Shard:    "0",
	}
	agent, enabled := newGossipAgent(cfg, tablet, nil)
	assert.Nil(t, agent)
	assert.False(t, enabled)
}

func TestNewGossipAgent_NilTablet(t *testing.T) {
	cfg := &topodatapb.GossipConfig{Enabled: true}
	agent, enabled := newGossipAgent(cfg, nil, nil)
	assert.Nil(t, agent)
	assert.False(t, enabled)
}

func TestNewGossipAgent_NoGRPCPort(t *testing.T) {
	cfg := &topodatapb.GossipConfig{Enabled: true}
	tablet := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Hostname: "host1",
		PortMap:  map[string]int32{"vt": 15999},
		Keyspace: "ks",
		Shard:    "0",
	}
	agent, enabled := newGossipAgent(cfg, tablet, nil)
	assert.Nil(t, agent)
	assert.False(t, enabled)
}

func TestNewGossipAgent_ZeroGRPCPort(t *testing.T) {
	cfg := &topodatapb.GossipConfig{Enabled: true}
	tablet := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Hostname: "host1",
		PortMap:  map[string]int32{"grpc": 0},
		Keyspace: "ks",
		Shard:    "0",
	}
	agent, enabled := newGossipAgent(cfg, tablet, nil)
	assert.Nil(t, agent)
	assert.False(t, enabled)
}

func TestNewGossipAgent_Success(t *testing.T) {
	cfg := &topodatapb.GossipConfig{
		Enabled:      true,
		PhiThreshold: 5,
		PingInterval: "2s",
		MaxUpdateAge: "10s",
	}
	tablet := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Hostname: "host1",
		PortMap:  map[string]int32{"grpc": 15999},
		Keyspace: "ks",
		Shard:    "0",
	}
	agent, enabled := newGossipAgent(cfg, tablet, nil)
	require.NotNil(t, agent)
	assert.True(t, enabled)

	// Verify agent has correct members (self)
	members := agent.Members()
	require.Len(t, members, 1)
	assert.Equal(t, "host1:15999", members[0].Addr)
}

func TestNewGossipAgent_DefaultPhiThreshold(t *testing.T) {
	cfg := &topodatapb.GossipConfig{
		Enabled:      true,
		PhiThreshold: 0, // Should default to 4
	}
	tablet := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Hostname: "host1",
		PortMap:  map[string]int32{"grpc": 15999},
		Keyspace: "ks",
		Shard:    "0",
	}
	agent, enabled := newGossipAgent(cfg, tablet, nil)
	require.NotNil(t, agent)
	assert.True(t, enabled)
}

func TestParseDuration(t *testing.T) {
	assert.Equal(t, 2*time.Second, parseDuration("2s", time.Second))
	assert.Equal(t, time.Second, parseDuration("", time.Second))
	assert.Equal(t, time.Second, parseDuration("invalid", time.Second))
	assert.Equal(t, time.Second, parseDuration("-1s", time.Second))
	assert.Equal(t, time.Second, parseDuration("0s", time.Second))
	assert.Equal(t, 500*time.Millisecond, parseDuration("500ms", time.Second))
}

func TestDiscoverSeeds_NilTopoServer(t *testing.T) {
	tablet := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Hostname: "host1",
		PortMap:  map[string]int32{"grpc": 15999},
		Keyspace: "ks",
		Shard:    "0",
	}
	seeds := discoverSeeds(tablet, nil)
	assert.Nil(t, seeds)
}
