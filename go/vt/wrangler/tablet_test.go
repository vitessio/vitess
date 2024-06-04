/*
Copyright 2019 The Vitess Authors.

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

package wrangler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtenv"
)

// TestInitTabletShardConversion makes sure InitTablet converts the
// shard name to lower case when it's a keyrange, and populates
// KeyRange properly.
func TestInitTabletShardConversion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cell := "cell1"
	ts := memorytopo.NewServer(ctx, cell)
	wr := New(vtenv.NewTestEnv(), logutil.NewConsoleLogger(), ts, nil)

	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  1,
		},
		Keyspace: "test",
		Shard:    "80-C0",
	}

	err := wr.TopoServer().InitTablet(context.Background(), tablet, false /*allowPrimaryOverride*/, true /*createShardAndKeyspace*/, false /*allowUpdate*/)
	require.NoError(t, err)

	ti, err := ts.GetTablet(context.Background(), tablet.Alias)
	require.NoError(t, err, "GetTablet failed")
	require.Equal(t, "80-c0", ti.Shard, "Got wrong tablet.Shard")
	require.Equal(t, "\x80", string(ti.KeyRange.Start), "Got wrong tablet.KeyRange start")
	require.Equal(t, "\xc0", string(ti.KeyRange.End), "Got wrong tablet.KeyRange end")
}

// TestDeleteTabletBasic tests delete of non-primary tablet
func TestDeleteTabletBasic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cell := "cell1"
	ts := memorytopo.NewServer(ctx, cell)
	wr := New(vtenv.NewTestEnv(), logutil.NewConsoleLogger(), ts, nil)

	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  1,
		},
		Shard:    "0",
		Keyspace: "test",
	}

	err := wr.TopoServer().InitTablet(context.Background(), tablet, false /*allowPrimaryOverride*/, true /*createShardAndKeyspace*/, false /*allowUpdate*/)
	require.NoError(t, err)

	_, err = ts.GetTablet(context.Background(), tablet.Alias)
	require.NoError(t, err, "GetTablet failed")

	err = wr.DeleteTablet(context.Background(), tablet.Alias, false)
	require.NoError(t, err, "DeleteTablet failed")
}

// TestDeleteTabletTruePrimary tests that you can delete a true primary tablet
// only if allowPrimary is set to true
func TestDeleteTabletTruePrimary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cell := "cell1"
	ts := memorytopo.NewServer(ctx, cell)
	wr := New(vtenv.NewTestEnv(), logutil.NewConsoleLogger(), ts, nil)

	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  1,
		},
		Keyspace: "test",
		Shard:    "0",
		Type:     topodatapb.TabletType_PRIMARY,
	}

	err := wr.TopoServer().InitTablet(context.Background(), tablet, false /*allowPrimaryOverride*/, true /*createShardAndKeyspace*/, false /*allowUpdate*/)
	require.NoError(t, err)

	_, err = ts.GetTablet(context.Background(), tablet.Alias)
	require.NoError(t, err, "GetTablet failed")

	// set PrimaryAlias and PrimaryTermStartTime on shard to match chosen primary tablet
	_, err = ts.UpdateShardFields(context.Background(), "test", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = tablet.Alias
		si.PrimaryTermStartTime = tablet.PrimaryTermStartTime
		return nil
	})
	require.NoError(t, err, "UpdateShardFields failed")

	err = wr.DeleteTablet(context.Background(), tablet.Alias, false)
	wantError := "as it is a primary, use allow_primary flag"
	require.ErrorContains(t, err, wantError, "DeleteTablet on primary: want specific error message")

	err = wr.DeleteTablet(context.Background(), tablet.Alias, true)
	require.NoError(t, err, "DeleteTablet failed")
}

// TestDeleteTabletFalsePrimary tests that you can delete a false primary tablet
// with allowPrimary set to false
func TestDeleteTabletFalsePrimary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cell := "cell1"
	ts := memorytopo.NewServer(ctx, cell)
	wr := New(vtenv.NewTestEnv(), logutil.NewConsoleLogger(), ts, nil)

	tablet1 := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  1,
		},
		Keyspace: "test",
		Shard:    "0",
		Type:     topodatapb.TabletType_PRIMARY,
	}

	err := wr.TopoServer().InitTablet(context.Background(), tablet1, false /*allowPrimaryOverride*/, true /*createShardAndKeyspace*/, false /*allowUpdate*/)
	require.NoError(t, err)

	tablet2 := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  2,
		},
		Keyspace: "test",
		Shard:    "0",
		Type:     topodatapb.TabletType_PRIMARY,
	}
	err = wr.TopoServer().InitTablet(context.Background(), tablet2, true /*allowPrimaryOverride*/, false /*createShardAndKeyspace*/, false /*allowUpdate*/)
	require.NoError(t, err)

	// set PrimaryAlias and PrimaryTermStartTime on shard to match chosen primary tablet
	_, err = ts.UpdateShardFields(context.Background(), "test", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = tablet2.Alias
		si.PrimaryTermStartTime = tablet2.PrimaryTermStartTime
		return nil
	})
	require.NoError(t, err, "UpdateShardFields failed")

	// Should be able to delete old (false) primary with allowPrimary = false
	err = wr.DeleteTablet(context.Background(), tablet1.Alias, false)
	require.NoError(t, err, "DeleteTablet failed")
}

// TestDeleteTabletShardNonExisting tests that you can delete a true primary
// tablet if a shard does not exists anymore.
func TestDeleteTabletShardNonExisting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cell := "cell1"
	ts := memorytopo.NewServer(ctx, cell)
	wr := New(vtenv.NewTestEnv(), logutil.NewConsoleLogger(), ts, nil)

	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  1,
		},
		Keyspace: "test",
		Shard:    "0",
		Type:     topodatapb.TabletType_PRIMARY,
	}

	err := wr.TopoServer().InitTablet(context.Background(), tablet, false /*allowPrimaryOverride*/, true /*createShardAndKeyspace*/, false /*allowUpdate*/)
	require.NoError(t, err)

	_, err = ts.GetTablet(context.Background(), tablet.Alias)
	require.NoError(t, err, "GetTablet failed")

	// set PrimaryAlias and PrimaryTermStartTime on shard to match chosen primary tablet
	_, err = ts.UpdateShardFields(context.Background(), "test", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = tablet.Alias
		si.PrimaryTermStartTime = tablet.PrimaryTermStartTime
		return nil
	})
	require.NoError(t, err, "UpdateShardFields failed")

	// trigger a shard deletion
	err = ts.DeleteShard(context.Background(), "test", "0")
	require.NoError(t, err, "DeleteShard failed")

	// DeleteTablet should not fail if a shard no longer exist
	err = wr.DeleteTablet(context.Background(), tablet.Alias, true)
	require.NoError(t, err, "DeleteTablet failed")
}
