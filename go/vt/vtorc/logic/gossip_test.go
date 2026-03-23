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

package logic

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/gossip"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

func TestParseDurationVTOrc(t *testing.T) {
	assert.Equal(t, 2*time.Second, parseDurationVTOrc("2s", time.Second))
	assert.Equal(t, time.Second, parseDurationVTOrc("", time.Second))
	assert.Equal(t, time.Second, parseDurationVTOrc("invalid", time.Second))
	assert.Equal(t, time.Second, parseDurationVTOrc("-1s", time.Second))
	assert.Equal(t, time.Second, parseDurationVTOrc("0s", time.Second))
	assert.Equal(t, 500*time.Millisecond, parseDurationVTOrc("500ms", time.Second))
}

func TestFindGossipConfig(t *testing.T) {
	ctx := t.Context()

	t.Run("no keyspaces", func(t *testing.T) {
		origTS := ts
		ts = memorytopo.NewServer(ctx, "zone1")
		defer func() { ts = origTS }()

		cfg, ksName := findGossipConfig()
		assert.Nil(t, cfg)
		assert.Empty(t, ksName)
	})

	t.Run("keyspace with gossip enabled", func(t *testing.T) {
		origTS := ts
		ts = memorytopo.NewServer(ctx, "zone1")
		defer func() { ts = origTS }()

		err := ts.CreateKeyspace(ctx, "ks1", &topodatapb.Keyspace{
			GossipConfig: &topodatapb.GossipConfig{
				Enabled:      true,
				PhiThreshold: 5,
				PingInterval: "2s",
			},
		})
		require.NoError(t, err)

		cfg, ksName := findGossipConfig()
		require.NotNil(t, cfg)
		assert.True(t, cfg.Enabled)
		assert.Equal(t, float64(5), cfg.PhiThreshold)
		assert.Equal(t, "ks1", ksName)
	})

	t.Run("keyspace with gossip disabled", func(t *testing.T) {
		origTS := ts
		ts = memorytopo.NewServer(ctx, "zone1")
		defer func() { ts = origTS }()

		err := ts.CreateKeyspace(ctx, "ks1", &topodatapb.Keyspace{
			GossipConfig: &topodatapb.GossipConfig{Enabled: false},
		})
		require.NoError(t, err)

		cfg, _ := findGossipConfig()
		assert.Nil(t, cfg)
	})

	t.Run("nil topo server", func(t *testing.T) {
		origTS := ts
		ts = nil
		defer func() { ts = origTS }()

		cfg, _ := findGossipConfig()
		assert.Nil(t, cfg)
	})
}

func TestStopGossipNilAgent(t *testing.T) {
	origAgent := gossipAgent
	gossipAgent = nil
	defer func() { gossipAgent = origAgent }()

	// Should not panic.
	stopGossip()
}

func TestStopGossipWithAgent(t *testing.T) {
	origAgent := gossipAgent
	defer func() { gossipAgent = origAgent }()

	agent := gossip.New(gossip.Config{
		NodeID:       "test",
		PingInterval: 100 * time.Millisecond,
	}, nil, nil)
	require.NoError(t, agent.Start(t.Context()))

	gossipAgent = agent
	stopGossip()
	// Calling stop again should be safe.
	stopGossip()
}

func TestFindGossipConfigMultipleKeyspaces(t *testing.T) {
	ctx := t.Context()
	origTS := ts
	ts = memorytopo.NewServer(ctx, "zone1")
	defer func() { ts = origTS }()

	// Create two keyspaces with different gossip configs.
	err := ts.CreateKeyspace(ctx, "ks1", &topodatapb.Keyspace{
		GossipConfig: &topodatapb.GossipConfig{
			Enabled:      true,
			PhiThreshold: 4,
			PingInterval: "1s",
		},
	})
	require.NoError(t, err)

	err = ts.CreateKeyspace(ctx, "ks2", &topodatapb.Keyspace{
		GossipConfig: &topodatapb.GossipConfig{
			Enabled:      true,
			PhiThreshold: 8,
			PingInterval: "2s",
		},
	})
	require.NoError(t, err)

	// Conflicting configs should fail fast — return nil.
	cfg, ksName := findGossipConfig()
	assert.Nil(t, cfg, "conflicting gossip configs should refuse to start")
	assert.Empty(t, ksName)
}

func TestGossipDialerEmptyTarget(t *testing.T) {
	d := gossipDialer{}
	client, err := d.Dial(t.Context(), "")
	assert.Nil(t, client)
	assert.Nil(t, err)
}

func TestWatchGossipConfigNilTopo(t *testing.T) {
	origTS := ts
	ts = nil
	defer func() { ts = origTS }()

	// Should return immediately without panic.
	watchGossipConfig(t.Context(), "")
}

func TestWatchGossipConfigEmptyKeyspace(t *testing.T) {
	ctx := t.Context()
	origTS := ts
	ts = memorytopo.NewServer(ctx, "zone1")
	defer func() { ts = origTS }()

	// Should return immediately without panic.
	watchGossipConfig(ctx, "")
}

func TestStartGossipAgent(t *testing.T) {
	origAgent := gossipAgent
	defer func() { gossipAgent = origAgent }()

	cfg := &topodatapb.GossipConfig{
		Enabled:      true,
		PhiThreshold: 5,
		PingInterval: "100ms",
		MaxUpdateAge: "500ms",
	}

	// startGossipAgent creates an agent with nil-transport seeds
	// (discoverGossipSeeds returns nil without a VTOrc DB).
	// The agent will be created and started successfully.
	startGossipAgent(cfg)
	require.NotNil(t, gossipAgent)

	gossipAgent.Stop()
}

func TestStartGossipAgentDefaultPhi(t *testing.T) {
	origAgent := gossipAgent
	defer func() { gossipAgent = origAgent }()

	cfg := &topodatapb.GossipConfig{
		Enabled:      true,
		PhiThreshold: 0, // Should default to 4.
		PingInterval: "100ms",
	}

	startGossipAgent(cfg)
	require.NotNil(t, gossipAgent)
	gossipAgent.Stop()
}

func TestDiscoverGossipSeeds(t *testing.T) {
	orcDb, err := db.OpenVTOrc()
	require.NoError(t, err)
	defer func() {
		_, _ = orcDb.Exec("delete from vitess_tablet")
	}()

	// Save two tablets with gRPC ports.
	require.NoError(t, inst.SaveTablet(&topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Hostname: "host1",
		PortMap:  map[string]int32{"grpc": 15100, "vt": 15000},
		Keyspace: "ks",
		Shard:    "0",
	}))
	require.NoError(t, inst.SaveTablet(&topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 200},
		Hostname: "host2",
		PortMap:  map[string]int32{"grpc": 15200},
		Keyspace: "ks",
		Shard:    "0",
	}))
	// Save a tablet without a gRPC port — should be skipped.
	require.NoError(t, inst.SaveTablet(&topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 300},
		Hostname: "host3",
		PortMap:  map[string]int32{"vt": 15300},
		Keyspace: "ks",
		Shard:    "0",
	}))

	seeds := discoverGossipSeeds()
	// Should find host1 and host2 (both have gRPC ports), not host3.
	assert.GreaterOrEqual(t, len(seeds), 2)
	addrs := make(map[string]bool)
	for _, s := range seeds {
		addrs[s.Addr] = true
	}
	assert.True(t, addrs["host1:15100"])
	assert.True(t, addrs["host2:15200"])
	assert.False(t, addrs["host3:15300"])
}

func TestDiscoverGossipSeedsEmpty(t *testing.T) {
	orcDb, err := db.OpenVTOrc()
	require.NoError(t, err)
	defer func() {
		_, _ = orcDb.Exec("delete from vitess_tablet")
	}()

	seeds := discoverGossipSeeds()
	assert.Empty(t, seeds)
}

type fakeState struct {
	members []gossip.Member
	states  map[gossip.NodeID]gossip.State
}

func (f *fakeState) Members() []gossip.Member                 { return f.members }
func (f *fakeState) Snapshot() map[gossip.NodeID]gossip.State { return f.states }

func TestGossipShardPrimaries(t *testing.T) {
	ctx := t.Context()
	orcDb, err := db.OpenVTOrc()
	require.NoError(t, err)
	defer func() {
		_, _ = orcDb.Exec("delete from vitess_tablet")
		_, _ = orcDb.Exec("delete from vitess_keyspace")
		_, _ = orcDb.Exec("delete from vitess_shard")
	}()

	// Set up topo for shardPrimary lookup.
	origTS := ts
	ts = memorytopo.NewServer(ctx, "zone1")
	defer func() { ts = origTS }()

	// Create keyspace and shard in topo.
	err = ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{})
	require.NoError(t, err)

	// Save primary tablet to VTOrc DB.
	primaryTablet := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Hostname: "host1",
		PortMap:  map[string]int32{"grpc": 15100},
		Keyspace: "ks",
		Shard:    "0",
		Type:     topodatapb.TabletType_PRIMARY,
	}
	require.NoError(t, inst.SaveTablet(primaryTablet))

	// Create the shard in topo with primary alias.
	_, err = ts.GetOrCreateShard(ctx, "ks", "0")
	require.NoError(t, err)
	_, err = ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = primaryTablet.Alias
		return nil
	})
	require.NoError(t, err)

	// Fake gossip state with members in shard "ks/0".
	state := &fakeState{
		members: []gossip.Member{
			{ID: "primary", Addr: "host1:15100", Meta: map[string]string{
				gossip.MetaKeyKeyspace:    "ks",
				gossip.MetaKeyShard:       "0",
				gossip.MetaKeyTabletAlias: "zone1-0000000100",
			}},
			{ID: "replica", Addr: "host2:15200", Meta: map[string]string{
				gossip.MetaKeyKeyspace:    "ks",
				gossip.MetaKeyShard:       "0",
				gossip.MetaKeyTabletAlias: "zone1-0000000200",
			}},
		},
	}

	primaries, ersFlags, err := gossipShardPrimaries(state)
	require.NoError(t, err)
	assert.Contains(t, primaries, "ks/0")
	assert.Equal(t, "zone1-0000000100", primaries["ks/0"])
	// ERS flags should not be set (no disable configured).
	assert.False(t, ersFlags["ks/0"].keyspace)
}

func TestGetGossipQuorumAnalyses(t *testing.T) {
	ctx := t.Context()
	orcDb, err := db.OpenVTOrc()
	require.NoError(t, err)
	defer func() {
		_, _ = orcDb.Exec("delete from vitess_tablet")
		_, _ = orcDb.Exec("delete from vitess_keyspace")
		_, _ = orcDb.Exec("delete from vitess_shard")
		db.ClearVTOrcDatabase()
	}()

	origTS := ts
	ts = memorytopo.NewServer(ctx, "zone1")
	defer func() { ts = origTS }()

	// Save primary tablet.
	primaryTablet := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Hostname: "host1",
		PortMap:  map[string]int32{"grpc": 15100},
		Keyspace: "ks",
		Shard:    "0",
		Type:     topodatapb.TabletType_PRIMARY,
	}
	require.NoError(t, inst.SaveTablet(primaryTablet))

	// Create shard with primary alias.
	err = ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{})
	require.NoError(t, err)
	_, err = ts.GetOrCreateShard(ctx, "ks", "0")
	require.NoError(t, err)
	_, err = ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = primaryTablet.Alias
		return nil
	})
	require.NoError(t, err)

	// Set up gossip agent with known state.
	origAgent := gossipAgent
	defer func() { gossipAgent = origAgent }()

	now := time.Now()
	gossipAgent = gossip.New(gossip.Config{NodeID: "observer"}, nil, nil)
	// Inject state: primary is Down, two replicas are Alive.
	gossipAgent.HandlePushPull(&gossip.Message{
		Members: []gossip.Member{
			{ID: "primary", Addr: "host1:15100", Meta: map[string]string{
				gossip.MetaKeyKeyspace:    "ks",
				gossip.MetaKeyShard:       "0",
				gossip.MetaKeyTabletAlias: "zone1-0000000100",
			}},
			{ID: "r1", Addr: "host2:15200", Meta: map[string]string{
				gossip.MetaKeyKeyspace:    "ks",
				gossip.MetaKeyShard:       "0",
				gossip.MetaKeyTabletAlias: "zone1-0000000200",
			}},
			{ID: "r2", Addr: "host3:15300", Meta: map[string]string{
				gossip.MetaKeyKeyspace:    "ks",
				gossip.MetaKeyShard:       "0",
				gossip.MetaKeyTabletAlias: "zone1-0000000300",
			}},
		},
		States: []gossip.StateDigest{
			{NodeID: "primary", Status: gossip.StatusDown, LastUpdate: now.Add(-10 * time.Second)},
			{NodeID: "r1", Status: gossip.StatusAlive, LastUpdate: now},
			{NodeID: "r2", Status: gossip.StatusAlive, LastUpdate: now},
		},
	})

	analyses := getGossipQuorumAnalyses()
	require.Len(t, analyses, 1)
	assert.Equal(t, inst.PrimaryTabletUnreachableByQuorum, analyses[0].Analysis)
}

func TestGetGossipQuorumAnalysesNilAgent(t *testing.T) {
	origAgent := gossipAgent
	gossipAgent = nil
	defer func() { gossipAgent = origAgent }()

	analyses := getGossipQuorumAnalyses()
	assert.Nil(t, analyses)
}
