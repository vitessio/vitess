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
	"context"
	"sync"
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
	tests := []struct {
		name  string
		input string
		want  time.Duration
	}{{
		name:  "valid seconds",
		input: "2s",
		want:  2 * time.Second,
	}, {
		name: "empty uses default",
		want: time.Second,
	}, {
		name:  "invalid uses default",
		input: "invalid",
		want:  time.Second,
	}, {
		name:  "negative uses default",
		input: "-1s",
		want:  time.Second,
	}, {
		name:  "zero uses default",
		input: "0s",
		want:  time.Second,
	}, {
		name:  "valid milliseconds",
		input: "500ms",
		want:  500 * time.Millisecond,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, parseDurationVTOrc(tt.input, time.Second))
		})
	}
}

func testEnabledGossipConfig() *topodatapb.GossipConfig {
	return &topodatapb.GossipConfig{
		Enabled:      true,
		PhiThreshold: 4,
		PingInterval: "100ms",
		MaxUpdateAge: "1s",
	}
}

func TestFindGossipConfig(t *testing.T) {
	ctx := t.Context()

	t.Run("no keyspaces", func(t *testing.T) {
		origTS := ts
		ts = memorytopo.NewServer(ctx, "zone1")
		defer func() { ts = origTS }()

		cfg, ksName, conflict := findGossipConfig()
		assert.Nil(t, cfg)
		assert.Empty(t, ksName)
		assert.False(t, conflict)
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

		cfg, ksName, conflict := findGossipConfig()
		require.NotNil(t, cfg)
		assert.True(t, cfg.Enabled)
		assert.Equal(t, float64(5), cfg.PhiThreshold)
		assert.Equal(t, "ks1", ksName)
		assert.False(t, conflict)
	})

	t.Run("keyspace with gossip disabled", func(t *testing.T) {
		origTS := ts
		ts = memorytopo.NewServer(ctx, "zone1")
		defer func() { ts = origTS }()

		err := ts.CreateKeyspace(ctx, "ks1", &topodatapb.Keyspace{
			GossipConfig: &topodatapb.GossipConfig{Enabled: false},
		})
		require.NoError(t, err)

		cfg, _, conflict := findGossipConfig()
		assert.Nil(t, cfg)
		assert.False(t, conflict)
	})

	t.Run("nil topo server", func(t *testing.T) {
		origTS := ts
		ts = nil
		defer func() { ts = origTS }()

		cfg, _, conflict := findGossipConfig()
		assert.Nil(t, cfg)
		assert.False(t, conflict)
	})

	t.Run("conflicting enabled keyspaces", func(t *testing.T) {
		origTS := ts
		ts = memorytopo.NewServer(ctx, "zone1")
		defer func() { ts = origTS }()

		require.NoError(t, ts.CreateKeyspace(ctx, "ks1", &topodatapb.Keyspace{
			GossipConfig: &topodatapb.GossipConfig{
				Enabled:      true,
				PhiThreshold: 4,
				PingInterval: "1s",
			},
		}))
		require.NoError(t, ts.CreateKeyspace(ctx, "ks2", &topodatapb.Keyspace{
			GossipConfig: &topodatapb.GossipConfig{
				Enabled:      true,
				PhiThreshold: 8,
				PingInterval: "2s",
			},
		}))

		cfg, ksName, conflict := findGossipConfig()
		assert.Nil(t, cfg)
		assert.Empty(t, ksName)
		assert.True(t, conflict)
	})
}

func TestFindGossipConfigState(t *testing.T) {
	ctx := t.Context()

	t.Run("returns enabled keyspaces even on conflict", func(t *testing.T) {
		origTS := ts
		ts = memorytopo.NewServer(ctx, "zone1")
		t.Cleanup(func() { ts = origTS })

		require.NoError(t, ts.CreateKeyspace(ctx, "ks1", &topodatapb.Keyspace{
			GossipConfig: &topodatapb.GossipConfig{
				Enabled:      true,
				PhiThreshold: 4,
				PingInterval: "1s",
			},
		}))
		require.NoError(t, ts.CreateKeyspace(ctx, "ks2", &topodatapb.Keyspace{
			GossipConfig: &topodatapb.GossipConfig{
				Enabled:      true,
				PhiThreshold: 8,
				PingInterval: "2s",
			},
		}))

		cfg, enabledKeyspaces, conflict := findGossipConfigState()
		assert.Nil(t, cfg)
		assert.ElementsMatch(t, []string{"ks1", "ks2"}, enabledKeyspaces)
		assert.True(t, conflict)
	})

	t.Run("treats equivalent effective defaults as non-conflicting", func(t *testing.T) {
		origTS := ts
		ts = memorytopo.NewServer(ctx, "zone1")
		t.Cleanup(func() { ts = origTS })

		require.NoError(t, ts.CreateKeyspace(ctx, "ks1", &topodatapb.Keyspace{
			GossipConfig: &topodatapb.GossipConfig{
				Enabled: true,
			},
		}))
		require.NoError(t, ts.CreateKeyspace(ctx, "ks2", &topodatapb.Keyspace{
			GossipConfig: &topodatapb.GossipConfig{
				Enabled:      true,
				PhiThreshold: 4,
				PingInterval: "1s",
				MaxUpdateAge: "5s",
			},
		}))

		cfg, enabledKeyspaces, conflict := findGossipConfigState()
		require.NotNil(t, cfg)
		assert.ElementsMatch(t, []string{"ks1", "ks2"}, enabledKeyspaces)
		assert.False(t, conflict)
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
	assert.Nil(t, gossipAgent)
	// Calling stop again should be safe.
	stopGossip()
}

func TestGetGossipQuorumAnalysesConcurrentWithConfigChange(t *testing.T) {
	orcDb, err := db.OpenVTOrc()
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = orcDb.Exec("delete from vitess_tablet")
		_, _ = orcDb.Exec("delete from vitess_keyspace")
		_, _ = orcDb.Exec("delete from vitess_shard")
		db.ClearVTOrcDatabase()
		stopGossip()
	})

	cfg := &topodatapb.GossipConfig{
		Enabled:      true,
		PhiThreshold: 5,
		PingInterval: "100ms",
		MaxUpdateAge: "500ms",
	}
	enabled := &topodatapb.SrvKeyspace{GossipConfig: cfg}
	disabled := &topodatapb.SrvKeyspace{}

	startGossipAgent(cfg)
	require.NotNil(t, gossipAgent)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 200 {
			_ = getGossipQuorumAnalyses()
		}
	}()
	go func() {
		defer wg.Done()
		for range 200 {
			applyGossipConfigChange(disabled)
			applyGossipConfigChange(enabled)
		}
	}()
	wg.Wait()
}

func TestWatchGossipConfigRecoversAfterSrvKeyspaceCreate(t *testing.T) {
	ctx := t.Context()
	stopGossip()
	origTS := ts
	ts = memorytopo.NewServer(ctx, "zone1")
	t.Cleanup(func() {
		stopGossip()
		ts = origTS
	})

	watchCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(func() {
		cancel()
		waitForGossipWatchesToStop(t)
	})
	go watchGossipConfig(watchCtx, "ks")

	require.NoError(t, ts.UpdateSrvKeyspace(ctx, "zone1", "ks", &topodatapb.SrvKeyspace{
		GossipConfig: testEnabledGossipConfig(),
	}))

	require.Eventually(t, func() bool {
		return currentGossipAgent() != nil
	}, 5*time.Second, 20*time.Millisecond)
}

func TestWatchGossipConfigRecoversAfterDeleteAndRecreate(t *testing.T) {
	ctx := t.Context()
	stopGossip()
	origTS := ts
	ts = memorytopo.NewServer(ctx, "zone1")
	t.Cleanup(func() {
		stopGossip()
		ts = origTS
	})

	require.NoError(t, ts.UpdateSrvKeyspace(ctx, "zone1", "ks", &topodatapb.SrvKeyspace{
		GossipConfig: testEnabledGossipConfig(),
	}))

	watchCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(func() {
		cancel()
		waitForGossipWatchesToStop(t)
	})
	go watchGossipConfig(watchCtx, "ks")

	require.Eventually(t, func() bool {
		return currentGossipAgent() != nil
	}, 5*time.Second, 20*time.Millisecond)

	require.NoError(t, ts.DeleteSrvKeyspace(ctx, "zone1", "ks"))
	require.Eventually(t, func() bool {
		return currentGossipAgent() == nil
	}, 5*time.Second, 20*time.Millisecond)

	require.NoError(t, ts.UpdateSrvKeyspace(ctx, "zone1", "ks", &topodatapb.SrvKeyspace{
		GossipConfig: testEnabledGossipConfig(),
	}))

	require.Eventually(t, func() bool {
		return currentGossipAgent() != nil
	}, 5*time.Second, 20*time.Millisecond)
}

func TestWatchExistingGossipKeyspacesReturnsFalseWithoutEnabledConfig(t *testing.T) {
	ctx := t.Context()
	stopGossip()
	origTS := ts
	ts = memorytopo.NewServer(ctx, "zone1")
	t.Cleanup(func() {
		stopGossip()
		ts = origTS
	})

	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{}))
	require.NoError(t, ts.UpdateSrvKeyspace(ctx, "zone1", "ks", &topodatapb.SrvKeyspace{}))

	watchCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(func() {
		cancel()
		waitForGossipWatchesToStop(t)
	})
	require.False(t, watchExistingGossipKeyspaces(watchCtx))
	assert.Zero(t, gossipWatchCount.Load())
}

func TestPollForGossipKeyspaceDetectsRuntimeEnableOnExistingKeyspace(t *testing.T) {
	ctx := t.Context()
	stopGossip()
	origTS := ts
	ts = memorytopo.NewServer(ctx, "zone1")
	origPollInterval := gossipPollInterval
	gossipPollInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		stopGossip()
		ts = origTS
		gossipPollInterval = origPollInterval
	})

	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{}))
	require.NoError(t, ts.UpdateSrvKeyspace(ctx, "zone1", "ks", &topodatapb.SrvKeyspace{}))

	watchCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(func() {
		cancel()
		waitForGossipWatchesToStop(t)
	})
	go pollForGossipKeyspace(watchCtx)

	cfg := testEnabledGossipConfig()
	updateKeyspaceGossipConfig(t, ts, "ks", cfg)
	require.NoError(t, ts.UpdateSrvKeyspace(ctx, "zone1", "ks", &topodatapb.SrvKeyspace{
		GossipConfig: cfg,
	}))

	require.Eventually(t, func() bool {
		return currentGossipAgent() != nil
	}, 5*time.Second, 20*time.Millisecond)
}

func TestWatchExistingGossipKeyspacesWatchesOnlyEnabledKeyspaces(t *testing.T) {
	ctx := t.Context()
	stopGossip()
	origTS := ts
	ts = memorytopo.NewServer(ctx, "zone1")
	t.Cleanup(func() {
		stopGossip()
		ts = origTS
	})

	enabledCfg := testEnabledGossipConfig()

	require.NoError(t, ts.CreateKeyspace(ctx, "ks1", &topodatapb.Keyspace{
		GossipConfig: enabledCfg,
	}))
	require.NoError(t, ts.UpdateSrvKeyspace(ctx, "zone1", "ks1", &topodatapb.SrvKeyspace{
		GossipConfig: enabledCfg,
	}))

	require.NoError(t, ts.CreateKeyspace(ctx, "ks2", &topodatapb.Keyspace{}))
	require.NoError(t, ts.UpdateSrvKeyspace(ctx, "zone1", "ks2", &topodatapb.SrvKeyspace{}))

	watchCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(func() {
		cancel()
		waitForGossipWatchesToStop(t)
	})
	require.True(t, watchExistingGossipKeyspaces(watchCtx))

	require.Eventually(t, func() bool {
		return gossipWatchCount.Load() == 1
	}, 5*time.Second, 20*time.Millisecond)
}

func TestWatchExistingGossipKeyspacesDoesNotStopEnabledConfigOnOtherKeyspaceChange(t *testing.T) {
	ctx := t.Context()
	stopGossip()
	origTS := ts
	ts = memorytopo.NewServer(ctx, "zone1")
	t.Cleanup(func() {
		stopGossip()
		ts = origTS
	})

	enabledCfg := testEnabledGossipConfig()

	require.NoError(t, ts.CreateKeyspace(ctx, "ks1", &topodatapb.Keyspace{
		GossipConfig: enabledCfg,
	}))
	require.NoError(t, ts.UpdateSrvKeyspace(ctx, "zone1", "ks1", &topodatapb.SrvKeyspace{
		GossipConfig: enabledCfg,
	}))

	require.NoError(t, ts.CreateKeyspace(ctx, "ks2", &topodatapb.Keyspace{}))
	require.NoError(t, ts.UpdateSrvKeyspace(ctx, "zone1", "ks2", &topodatapb.SrvKeyspace{}))

	watchCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(func() {
		cancel()
		waitForGossipWatchesToStop(t)
	})
	require.True(t, watchExistingGossipKeyspaces(watchCtx))

	require.Eventually(t, func() bool {
		return currentGossipAgent() != nil
	}, 5*time.Second, 20*time.Millisecond)

	require.NoError(t, ts.UpdateSrvKeyspace(ctx, "zone1", "ks2", &topodatapb.SrvKeyspace{
		GossipConfig: &topodatapb.GossipConfig{},
	}))

	require.Eventually(t, func() bool {
		return currentGossipAgent() != nil
	}, 5*time.Second, 20*time.Millisecond)
}

func TestWatchExistingGossipKeyspacesStopsAfterLastEnabledKeyspaceDisabled(t *testing.T) {
	ctx := t.Context()
	stopGossip()
	origTS := ts
	ts = memorytopo.NewServer(ctx, "zone1")
	t.Cleanup(func() {
		stopGossip()
		ts = origTS
	})

	for _, keyspace := range []string{"ks1", "ks2"} {
		require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{
			GossipConfig: testEnabledGossipConfig(),
		}))
		require.NoError(t, ts.UpdateSrvKeyspace(ctx, "zone1", keyspace, &topodatapb.SrvKeyspace{
			GossipConfig: testEnabledGossipConfig(),
		}))
	}

	watchCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(func() {
		cancel()
		waitForGossipWatchesToStop(t)
	})
	require.True(t, watchExistingGossipKeyspaces(watchCtx))

	require.Eventually(t, func() bool {
		return currentGossipAgent() != nil
	}, 5*time.Second, 20*time.Millisecond)

	updateKeyspaceGossipConfig(t, ts, "ks1", &topodatapb.GossipConfig{})
	require.NoError(t, ts.UpdateSrvKeyspace(ctx, "zone1", "ks1", &topodatapb.SrvKeyspace{}))

	require.Eventually(t, func() bool {
		return currentGossipAgent() != nil
	}, 5*time.Second, 20*time.Millisecond)

	updateKeyspaceGossipConfig(t, ts, "ks2", &topodatapb.GossipConfig{})
	require.NoError(t, ts.UpdateSrvKeyspace(ctx, "zone1", "ks2", &topodatapb.SrvKeyspace{}))

	require.Eventually(t, func() bool {
		return currentGossipAgent() == nil
	}, 5*time.Second, 20*time.Millisecond)
}

func updateKeyspaceGossipConfig(t *testing.T, ts *topo.Server, keyspace string, cfg *topodatapb.GossipConfig) {
	t.Helper()
	lockCtx, unlock, err := ts.LockKeyspace(t.Context(), keyspace, "update gossip config")
	require.NoError(t, err)
	var unlockErr error
	defer unlock(&unlockErr)

	ki, err := ts.GetKeyspace(lockCtx, keyspace)
	require.NoError(t, err)
	ki.GossipConfig = cfg
	require.NoError(t, ts.UpdateKeyspace(lockCtx, ki))
}

func waitForGossipWatchesToStop(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool {
		return gossipWatchCount.Load() == 0
	}, 5*time.Second, 20*time.Millisecond)
}

func TestReconcileGossipConfigFailsClosedOnConflict(t *testing.T) {
	ctx := t.Context()
	stopGossip()
	origTS := ts
	ts = memorytopo.NewServer(ctx, "zone1")
	t.Cleanup(func() {
		stopGossip()
		ts = origTS
	})

	cfg1 := &topodatapb.GossipConfig{
		Enabled:      true,
		PhiThreshold: 4,
		PingInterval: "1s",
		MaxUpdateAge: "5s",
	}
	cfg2 := &topodatapb.GossipConfig{
		Enabled:      true,
		PhiThreshold: 8,
		PingInterval: "2s",
		MaxUpdateAge: "10s",
	}
	require.NoError(t, ts.CreateKeyspace(ctx, "ks1", &topodatapb.Keyspace{GossipConfig: cfg1}))
	require.NoError(t, ts.CreateKeyspace(ctx, "ks2", &topodatapb.Keyspace{GossipConfig: cfg2}))

	startGossipAgent(cfg1)
	require.NotNil(t, currentGossipAgent())

	reconcileGossipConfig(cfg1)

	assert.Nil(t, currentGossipAgent())
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
	tests := []struct {
		name string
		cfg  *topodatapb.GossipConfig
	}{{
		name: "explicit config",
		cfg: &topodatapb.GossipConfig{
			Enabled:      true,
			PhiThreshold: 5,
			PingInterval: "100ms",
			MaxUpdateAge: "500ms",
		},
	}, {
		name: "default phi",
		cfg: &topodatapb.GossipConfig{
			Enabled:      true,
			PhiThreshold: 0,
			PingInterval: "100ms",
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			origAgent := gossipAgent
			defer func() { gossipAgent = origAgent }()

			startGossipAgent(tt.cfg)
			require.NotNil(t, gossipAgent)
			gossipAgent.Stop()
		})
	}
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
	ids := make(map[string]gossip.NodeID)
	for _, s := range seeds {
		addrs[s.Addr] = true
		ids[s.Addr] = s.ID
	}
	assert.True(t, addrs["host1:15100"])
	assert.True(t, addrs["host2:15200"])
	assert.False(t, addrs["host3:15300"])
	assert.Equal(t, gossip.NodeID("zone1-0000000100"), ids["host1:15100"])
	assert.Equal(t, gossip.NodeID("zone1-0000000200"), ids["host2:15200"])
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

	primaries, currentMembers, ersFlags, err := gossipShardPrimaries(state)
	require.NoError(t, err)
	assert.Contains(t, primaries, "ks/0")
	assert.Equal(t, "zone1-0000000100", primaries["ks/0"])
	assert.Contains(t, currentMembers["ks/0"], "zone1-0000000100")
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
	require.NoError(t, inst.SaveTablet(&topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 200},
		Hostname: "host2",
		PortMap:  map[string]int32{"grpc": 15200},
		Keyspace: "ks",
		Shard:    "0",
		Type:     topodatapb.TabletType_REPLICA,
	}))
	require.NoError(t, inst.SaveTablet(&topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 300},
		Hostname: "host3",
		PortMap:  map[string]int32{"grpc": 15300},
		Keyspace: "ks",
		Shard:    "0",
		Type:     topodatapb.TabletType_REPLICA,
	}))

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
	gossipAgent = gossip.New(gossip.Config{
		NodeID:       "observer",
		PhiThreshold: 4,
		PingInterval: time.Millisecond,
		MaxUpdateAge: time.Second,
	}, nil, nil)
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
			{NodeID: "primary", Status: gossip.StatusAlive, LastUpdate: now.Add(-10 * time.Second)},
			{NodeID: "r1", Status: gossip.StatusAlive, LastUpdate: now},
			{NodeID: "r2", Status: gossip.StatusAlive, LastUpdate: now},
		},
	})
	require.NoError(t, gossipAgent.Start(ctx))
	defer gossipAgent.Stop()
	require.Eventually(t, func() bool {
		return gossipAgent.Snapshot()["primary"].Status == gossip.StatusDown
	}, time.Second, 10*time.Millisecond)

	analyses := getGossipQuorumAnalyses()
	require.Len(t, analyses, 1)
	assert.Equal(t, inst.PrimaryTabletUnreachableByQuorum, analyses[0].Analysis)
}

// TestGetGossipQuorumAnalysesIgnoresPropagatedDownVerdict protects quorum decisions from relayed failure verdicts.
func TestGetGossipQuorumAnalysesIgnoresPropagatedDownVerdict(t *testing.T) {
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

	primaryTablet := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Hostname: "host1",
		PortMap:  map[string]int32{"grpc": 15100},
		Keyspace: "ks",
		Shard:    "0",
		Type:     topodatapb.TabletType_PRIMARY,
	}
	require.NoError(t, inst.SaveTablet(primaryTablet))
	require.NoError(t, inst.SaveTablet(&topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 200},
		Hostname: "host2",
		PortMap:  map[string]int32{"grpc": 15200},
		Keyspace: "ks",
		Shard:    "0",
		Type:     topodatapb.TabletType_REPLICA,
	}))
	require.NoError(t, inst.SaveTablet(&topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 300},
		Hostname: "host3",
		PortMap:  map[string]int32{"grpc": 15300},
		Keyspace: "ks",
		Shard:    "0",
		Type:     topodatapb.TabletType_REPLICA,
	}))

	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{}))
	_, err = ts.GetOrCreateShard(ctx, "ks", "0")
	require.NoError(t, err)
	_, err = ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = primaryTablet.Alias
		return nil
	})
	require.NoError(t, err)

	origAgent := gossipAgent
	defer func() { gossipAgent = origAgent }()

	now := time.Now()
	gossipAgent = gossip.New(gossip.Config{NodeID: "observer"}, nil, nil)
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

	assert.Empty(t, getGossipQuorumAnalyses())
}

func TestGetGossipQuorumAnalysesNilAgent(t *testing.T) {
	origAgent := gossipAgent
	gossipAgent = nil
	defer func() { gossipAgent = origAgent }()

	analyses := getGossipQuorumAnalyses()
	assert.Nil(t, analyses)
}
