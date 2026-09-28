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

package testlib

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/wrangler"
)

// waitForTabletType waits for the given tablet type to be reached.
func waitForTabletType(t *testing.T, wr *wrangler.Wrangler, tabletAlias *topodatapb.TabletAlias, tabletType topodatapb.TabletType) {
	timeout := time.After(15 * time.Second)
	for {
		tablet, err := wr.TopoServer().GetTablet(context.Background(), tabletAlias)
		require.NoError(t, err)
		if tablet.Type == tabletType {
			return
		}

		select {
		case <-timeout:
			t.Fatalf("%s didn't reach the tablet type %v", topoproto.TabletAliasString(tabletAlias), tabletType.String())
			return
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// waitForShardPrimary waits for the shard record to be upto date such that it has the given primary.
func waitForShardPrimary(t *testing.T, wr *wrangler.Wrangler, primaryTablet *topodatapb.Tablet) {
	timeout := time.After(15 * time.Second)
	for {
		si, err := wr.TopoServer().GetShard(context.Background(), primaryTablet.Keyspace, primaryTablet.Shard)
		require.NoError(t, err)
		if topoproto.TabletAliasEqual(si.PrimaryAlias, primaryTablet.Alias) {
			return
		}

		select {
		case <-timeout:
			t.Fatalf("%s/%s didn't see the tablet %v become the primary, instead it is %v",
				primaryTablet.Keyspace, primaryTablet.Shard,
				topoproto.TabletAliasString(primaryTablet.Alias),
				topoproto.TabletAliasString(si.PrimaryAlias),
			)
			return
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}
