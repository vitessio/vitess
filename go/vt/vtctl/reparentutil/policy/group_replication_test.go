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
	"vitess.io/vitess/go/vt/topo/topoproto"
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

func TestGroupName(t *testing.T) {
	name := GroupName("commerce", "0")
	assert.Equal(t, name, GroupName("commerce", "0"), "every tablet of the shard computes the same name")
	assert.NotEqual(t, GroupName("commerce", "0"), GroupName("commerce", "-80"))
	assert.NotEqual(t, GroupName("commerce", "0"), GroupName("customer", "0"))
	assert.Len(t, GroupName("commerce", "0"), 36)
}

func aliases(tablets ...*topodatapb.Tablet) []*topodatapb.TabletAlias {
	var out []*topodatapb.TabletAlias
	for _, t := range tablets {
		out = append(out, t.Alias)
	}
	return out
}

func TestSelectVoters(t *testing.T) {
	crossCell, err := GetDurabilityPolicy(DurabilityGroupReplicationCrossCell)
	require.NoError(t, err)
	grCrossCell, _ := AsGroupReplication(crossCell)
	plain, err := GetDurabilityPolicy(DurabilityGroupReplication)
	require.NoError(t, err)
	grPlain, _ := AsGroupReplication(plain)

	z1a := tablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	z1b := tablet("zone1", 102, topodatapb.TabletType_REPLICA)
	z1rdonly := tablet("zone1", 103, topodatapb.TabletType_RDONLY)
	z2a := tablet("zone2", 201, topodatapb.TabletType_REPLICA)
	z2b := tablet("zone2", 202, topodatapb.TabletType_REPLICA)
	z3a := tablet("zone3", 301, topodatapb.TabletType_REPLICA)
	all := func(mutate func(c *VoterCandidate)) []VoterCandidate {
		var cs []VoterCandidate
		for _, tb := range []*topodatapb.Tablet{z1a, z1b, z1rdonly, z2a, z2b, z3a} {
			c := VoterCandidate{Tablet: tb}
			if mutate != nil {
				mutate(&c)
			}
			cs = append(cs, c)
		}
		return cs
	}

	t.Run("first selection picks one tablet per cell", func(t *testing.T) {
		voters := SelectVoters(grCrossCell, nil, nil, all(nil))
		assert.Equal(t, aliases(z1a, z2a, z3a), voters)
	})

	t.Run("the plain policy takes every eligible tablet", func(t *testing.T) {
		voters := SelectVoters(grPlain, nil, nil, all(nil))
		assert.Equal(t, aliases(z1a, z1b, z2a, z2b, z3a), voters)
	})

	t.Run("current voters are kept although a lower alias exists", func(t *testing.T) {
		voters := SelectVoters(grCrossCell, aliases(z1a, z2b, z3a), nil, all(nil))
		assert.Equal(t, aliases(z1a, z2b, z3a), voters)
	})

	t.Run("a briefly unreachable voter keeps its seat", func(t *testing.T) {
		// Not failed yet: the caller's grace period has not expired.
		voters := SelectVoters(grCrossCell, aliases(z1a, z2a, z3a), nil, all(nil))
		assert.Equal(t, aliases(z1a, z2a, z3a), voters)
	})

	t.Run("a failed voter is replaced by a tablet of the same cell", func(t *testing.T) {
		voters := SelectVoters(grCrossCell, aliases(z1a, z2a, z3a), nil, all(func(c *VoterCandidate) {
			c.Failed = topoproto.TabletAliasEqual(c.Tablet.Alias, z2a.Alias)
		}))
		assert.Equal(t, aliases(z1a, z2b, z3a), voters)
	})

	t.Run("a cell without a replacement is left empty", func(t *testing.T) {
		voters := SelectVoters(grCrossCell, aliases(z1a, z2a, z3a), nil, all(func(c *VoterCandidate) {
			c.Failed = c.Tablet.Alias.Cell == "zone3"
		}))
		assert.Equal(t, aliases(z1a, z2a), voters)
	})

	t.Run("active members are preferred when filling a seat", func(t *testing.T) {
		voters := SelectVoters(grCrossCell, nil, nil, all(func(c *VoterCandidate) {
			c.Active = topoproto.TabletAliasEqual(c.Tablet.Alias, z1b.Alias)
		}))
		assert.Equal(t, aliases(z1b, z2a, z3a), voters)
	})

	t.Run("the group primary keeps its seat over the listed voter of its cell", func(t *testing.T) {
		voters := SelectVoters(grCrossCell, aliases(z1a, z2a, z3a), z1b.Alias, all(func(c *VoterCandidate) {
			c.Active = c.Tablet.Alias.Cell != "zone1" || topoproto.TabletAliasEqual(c.Tablet.Alias, z1b.Alias)
		}))
		assert.Equal(t, aliases(z1b, z2a, z3a), voters)
	})

	t.Run("tablets that cannot be promoted never vote", func(t *testing.T) {
		voters := SelectVoters(grCrossCell, aliases(z1rdonly), nil, []VoterCandidate{{Tablet: z1rdonly, Active: true}})
		assert.Empty(t, voters)
	})
}
