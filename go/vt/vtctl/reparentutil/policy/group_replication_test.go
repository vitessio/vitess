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

package policy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func tablet(cell string, uid uint32, typ topodatapb.TabletType) *topodatapb.Tablet {
	return &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: cell, Uid: uid}, Type: typ}
}

func TestGroupReplicationDurability(t *testing.T) {
	for _, name := range []string{DurabilityGroupReplication, DurabilityGroupReplicationCrossCell} {
		t.Run(name, func(t *testing.T) {
			durability, err := GetDurabilityPolicy(name)
			require.NoError(t, err)
			assert.True(t, IsGroupReplication(durability))
			assert.False(t, HasSemiSync(durability))

			primary := tablet("zone1", 100, topodatapb.TabletType_PRIMARY)
			replica := tablet("zone2", 101, topodatapb.TabletType_REPLICA)
			rdonly := tablet("zone2", 102, topodatapb.TabletType_RDONLY)
			assert.Zero(t, SemiSyncAckers(durability, primary))
			assert.False(t, IsReplicaSemiSync(durability, primary, replica))
			assert.True(t, IsGroupMember(durability, primary))
			assert.True(t, IsGroupMember(durability, replica))
			assert.False(t, IsGroupMember(durability, rdonly), "rdonly tablets replicate asynchronously from the group")
			assert.False(t, IsGroupMember(durability, nil))

			grd, ok := AsGroupReplication(durability)
			require.True(t, ok)
			assert.Equal(t, 50, grd.MemberWeight(replica))
			assert.Equal(t, 0, grd.MemberWeight(rdonly))
			assert.Equal(t, name == DurabilityGroupReplicationCrossCell, grd.RequiresCrossCellMajority())
		})
	}
}

func TestAsyncDurabilityIsNotGroupReplication(t *testing.T) {
	for _, name := range []string{DurabilityNone, DurabilitySemiSync, DurabilityCrossCell} {
		durability, err := GetDurabilityPolicy(name)
		require.NoError(t, err)
		assert.Equal(t, ReplicationModeAsync, GetReplicationMode(durability))
		assert.False(t, IsGroupMember(durability, tablet("zone1", 100, topodatapb.TabletType_PRIMARY)))
	}
}

func TestCellHoldsMajority(t *testing.T) {
	_, ok := CellHoldsMajority([]*topodatapb.Tablet{
		tablet("zone1", 1, topodatapb.TabletType_PRIMARY),
		tablet("zone2", 2, topodatapb.TabletType_REPLICA),
		tablet("zone3", 3, topodatapb.TabletType_REPLICA),
	})
	assert.False(t, ok)

	cell, ok := CellHoldsMajority([]*topodatapb.Tablet{
		tablet("zone1", 1, topodatapb.TabletType_PRIMARY),
		tablet("zone1", 2, topodatapb.TabletType_REPLICA),
		tablet("zone2", 3, topodatapb.TabletType_REPLICA),
	})
	assert.True(t, ok)
	assert.Equal(t, "zone1", cell)
}
