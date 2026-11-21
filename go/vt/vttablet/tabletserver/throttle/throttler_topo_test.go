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
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestThrottlerTopoServer verifies that a throttler made by NewThrottler reads tablet records through the topo server's
// StaleTolerantView, which with --topo-cell-info-cache-ttl reads no CellInfo once it is cached, and reads
// SrvKeyspace, which holds its config, through ConnForCell as before.
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
	require.NoError(t, ts.CreateTablet(ctx, &topodatapb.Tablet{Alias: alias, Keyspace: "ks", Shard: "0", Type: topodatapb.TabletType_REPLICA}))
	require.NoError(t, ts.UpdateSrvKeyspace(ctx, "zone1", "ks", &topodatapb.SrvKeyspace{}))
	gets := func() int64 { return factory.GetCallStats().Counts()["Get"] }

	env := tabletenv.NewEnv(vtenv.NewTestEnv(), tabletenv.NewDefaultConfig(), "TestThrottlerTopoServer")
	throttler := NewThrottler(env, nil, ts, alias, nil, func() topodatapb.TabletType { return topodatapb.TabletType_REPLICA }, "TestPool")
	throttlerTS := throttler.ts
	_, err := throttlerTS.GetTablet(ctx, alias)
	require.NoError(t, err)

	// A read through ConnForCell also reads the cell's CellInfo; a cached one does not.
	before := gets()
	aliases, err := throttlerTS.FindAllTabletAliasesInShard(ctx, "ks", "0")
	require.NoError(t, err)
	assert.Equal(t, []*topodatapb.TabletAlias{alias}, aliases)
	assert.Equal(t, before+2, gets(), "FindAllTabletAliasesInShard read more than the Shard and its ShardReplication")
	before = gets()
	_, err = throttlerTS.GetTablet(ctx, alias)
	require.NoError(t, err)
	assert.Equal(t, before+1, gets(), "GetTablet read more than the tablet record")
	before = gets()
	_, err = throttlerTS.GetSrvKeyspace(ctx, "zone1", "ks")
	require.NoError(t, err)
	assert.Equal(t, before+2, gets(), "GetSrvKeyspace did not read the cell's CellInfo")
}
