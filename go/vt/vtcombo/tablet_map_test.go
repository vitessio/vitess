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

package vtcombo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestShardSidecarDBName(t *testing.T) {
	tests := []struct {
		keyspace string
		shard    string
		expected string
	}{
		{"commerce", "0", "_vt_commerce_0"},
		{"commerce", "-80", "_vt_commerce__80"},
		{"commerce", "80-", "_vt_commerce_80_"},
		{"ks", "-", "_vt_ks__"},
		{"myks", "80-c0", "_vt_myks_80_c0"},
	}
	for _, tt := range tests {
		t.Run(tt.keyspace+"/"+tt.shard, func(t *testing.T) {
			got := shardSidecarDBName(tt.keyspace, tt.shard)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestSetKeyspaceSidecarDBName(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()

	err := ts.CreateKeyspace(ctx, "ks1", &topodatapb.Keyspace{})
	require.NoError(t, err)

	err = setKeyspaceSidecarDBName(ctx, ts, "ks1", "_vt_ks1_0")
	require.NoError(t, err)

	ki, err := ts.GetKeyspace(ctx, "ks1")
	require.NoError(t, err)
	assert.Equal(t, "_vt_ks1_0", ki.SidecarDbName)
}

func TestVReplicationExec_TabletNotFound(t *testing.T) {
	tabletMap = map[uint32]*comboTablet{}
	itmc := &internalTabletManagerClient{}
	_, err := itmc.VReplicationExec(context.Background(), &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{Uid: 999},
	}, "select 1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot find tablet")
}

func TestVReplicationExec_VREngineNil(t *testing.T) {
	// A TabletManager with no VREngine set (VREngine is nil).
	ct := &comboTablet{
		tm: &tabletmanager.TabletManager{},
	}
	tabletMap = map[uint32]*comboTablet{1: ct}
	itmc := &internalTabletManagerClient{}
	_, err := itmc.VReplicationExec(context.Background(), &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{Uid: 1},
	}, "select 1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "VREngine not initialized")
}

func TestVReplicationWaitForPos_TabletNotFound(t *testing.T) {
	tabletMap = map[uint32]*comboTablet{}
	itmc := &internalTabletManagerClient{}
	err := itmc.VReplicationWaitForPos(context.Background(), &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{Uid: 999},
	}, 1, "pos")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot find tablet")
}
