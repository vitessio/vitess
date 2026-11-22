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

package throttle

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestThrottlerTopoServer verifies that a throttler made by NewThrottler reads the tablet records that decide
// what it probes through the topo server's StaleTolerantView, which with --topo-cell-info-cache-ttl reads no
// CellInfo once it is cached, and reads its own tablet record and SrvKeyspace, which holds its config, through
// ConnForCell as before. It checks the throttlerTopoServer's methods and then the throttler's three call sites
// that read tablet records, since the throttle package's other tests use fakes that read the same way through
// either method: readThrottlerConfig's one-time read of its own record, primaryTablet on a replica and
// refreshInventory on a primary.
func TestThrottlerTopoServer(t *testing.T) {
	ctx := t.Context()
	ttl := topo.CellInfoCacheTTL
	topo.CellInfoCacheTTL = time.Hour
	t.Cleanup(func() { topo.CellInfoCacheTTL = ttl })
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(ts.Close)
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{}))
	require.NoError(t, ts.CreateShard(ctx, "ks", "0"))
	alias := &topodatapb.TabletAlias{Cell: "zone1", Uid: 1}
	primaryAlias := &topodatapb.TabletAlias{Cell: "zone1", Uid: 2}
	require.NoError(t, ts.CreateTablet(ctx, &topodatapb.Tablet{Alias: alias, Keyspace: "ks", Shard: "0", Type: topodatapb.TabletType_REPLICA}))
	require.NoError(t, ts.CreateTablet(ctx, &topodatapb.Tablet{Alias: primaryAlias, Keyspace: "ks", Shard: "0", Type: topodatapb.TabletType_PRIMARY}))
	_, err := ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = primaryAlias
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, ts.UpdateSrvKeyspace(ctx, "zone1", "ks", &topodatapb.SrvKeyspace{}))
	gets := func() int64 { return factory.GetCallStats().Counts()["Get"] }

	env := tabletenv.NewEnv(vtenv.NewTestEnv(), tabletenv.NewDefaultConfig(), "TestThrottlerTopoServer")
	throttler := NewThrottler(env, nil, ts, alias, nil, func() topodatapb.TabletType { return topodatapb.TabletType_REPLICA }, "TestPool")
	throttler.InitDBConfig("ks", "0")
	throttler.initConfig()
	throttlerTS := throttler.ts
	_, err = throttlerTS.GetTabletStaleTolerant(ctx, alias)
	require.NoError(t, err)

	// A read through ConnForCell also reads the cell's CellInfo; a cached one does not.
	before := gets()
	aliases, err := throttlerTS.FindAllTabletAliasesInShard(ctx, "ks", "0")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{topoproto.TabletAliasString(alias), topoproto.TabletAliasString(primaryAlias)}, topoproto.TabletAliasList(aliases).ToStringSlice())
	assert.Equal(t, before+2, gets(), "FindAllTabletAliasesInShard read more than the Shard and its ShardReplication")
	before = gets()
	_, err = throttlerTS.GetTabletStaleTolerant(ctx, alias)
	require.NoError(t, err)
	assert.Equal(t, before+1, gets(), "GetTabletStaleTolerant read more than the tablet record")
	before = gets()
	_, err = throttlerTS.GetTablet(ctx, alias)
	require.NoError(t, err)
	assert.Equal(t, before+2, gets(), "GetTablet did not read the cell's CellInfo")
	before = gets()
	_, err = throttlerTS.GetSrvKeyspace(ctx, "zone1", "ks")
	require.NoError(t, err)
	assert.Equal(t, before+2, gets(), "GetSrvKeyspace did not read the cell's CellInfo")

	// readThrottlerConfig reads the throttler's own tablet record once, through ConnForCell, and its config
	// from SrvKeyspace, through ConnForCell on every call.
	require.Nil(t, throttler.tabletInfo.Load())
	before = gets()
	_, err = throttler.readThrottlerConfig(ctx)
	require.NoError(t, err)
	assert.Equal(t, before+4, gets(), "readThrottlerConfig did not read the cell's CellInfo for both its own tablet record and SrvKeyspace")
	require.NotNil(t, throttler.tabletInfo.Load())
	assert.True(t, topoproto.TabletAliasEqual(alias, throttler.tabletInfo.Load().Alias))
	before = gets()
	_, err = throttler.readThrottlerConfig(ctx)
	require.NoError(t, err)
	assert.Equal(t, before+2, gets(), "readThrottlerConfig read more than SrvKeyspace once it had its own tablet record")

	// primaryTablet, on a replica, reads the shard record from the global topo and the primary's tablet
	// record through the view.
	require.Nil(t, throttler.cachedPrimary.Load())
	before = gets()
	primary, err := throttler.primaryTablet(ctx)
	require.NoError(t, err)
	require.NotNil(t, primary)
	assert.True(t, topoproto.TabletAliasEqual(primaryAlias, primary.Alias))
	assert.Equal(t, before+2, gets(), "primaryTablet read more than the Shard and the primary's tablet record")

	// refreshInventory, on a primary, lists the shard's tablets and reads each tablet record through the view.
	// It collects in a goroutine that ends by sending the probes.
	throttler.isLeader.Store(true)
	before = gets()
	require.NoError(t, throttler.refreshInventory(ctx))
	select {
	case probes := <-throttler.clusterProbesChan:
		assert.Contains(t, probes.TabletProbes, topoproto.TabletAliasString(alias))
	case <-time.After(10 * time.Second):
		require.FailNow(t, "refreshInventory did not send the probes")
	}
	assert.Equal(t, before+4, gets(), "refreshInventory read more than the Shard, its ShardReplication and the two tablet records")
}
