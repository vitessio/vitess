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

package reparentutil

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// supersededTestStatus returns the FullStatus of a tablet whose MySQL is an ONLINE member, with
// quorum, of the view viewID, whose primary is primary and which holds the given members ONLINE.
func supersededTestStatus(tablet, primary *topodatapb.Tablet, viewID string, members ...*topodatapb.Tablet) *fullStatusResult {
	uuid := func(t *topodatapb.Tablet) string { return fmt.Sprintf("00000000-0000-0000-0000-%012d", t.Alias.Uid) }
	role := mysql.GroupMemberRoleSecondary
	if tablet == primary {
		role = mysql.GroupMemberRolePrimary
	}
	gs := &replicationdatapb.GroupReplicationStatus{
		PluginActive: true, GroupName: policy.GroupName("ks", "-"), SinglePrimaryMode: true,
		MemberState: mysql.GroupMemberStateOnline, MemberRole: role, HasQuorum: true,
		PrimaryUuid: uuid(primary), ViewId: viewID,
	}
	for _, m := range members {
		memberRole := mysql.GroupMemberRoleSecondary
		if m == primary {
			memberRole = mysql.GroupMemberRolePrimary
		}
		gs.Members = append(gs.Members, &replicationdatapb.GroupReplicationMember{MemberUuid: uuid(m), State: mysql.GroupMemberStateOnline, Role: memberRole})
	}
	return &fullStatusResult{tablet: tablet, status: &replicationdatapb.FullStatus{ServerUuid: uuid(tablet), GroupReplicationStatus: gs, GroupReplicationEnabled: true}}
}

// TestEmergencyReparentGroupReplicationIgnoresSupersededView checks the ERS path of FLAG 1 of the
// soak: a deposed primary resumed with its stale view, in which it is the ONLINE primary, with quorum,
// of the three voters in the recorded incarnation, while another member is the ONLINE primary, with
// quorum, of a newer view of the same incarnation. ERS follows the newer primary, and never promotes
// the deposed member, even on request.
func TestEmergencyReparentGroupReplicationIgnoresSupersededView(t *testing.T) {
	deposed := &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101}, Type: topodatapb.TabletType_REPLICA, Keyspace: "ks", Shard: "-"}
	newer := &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone2", Uid: 200}, Type: topodatapb.TabletType_REPLICA, Keyspace: "ks", Shard: "-"}
	other := &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone3", Uid: 300}, Type: topodatapb.TabletType_REPLICA, Keyspace: "ks", Shard: "-"}
	voters := []*topodatapb.TabletAlias{deposed.Alias, newer.Alias, other.Alias}
	statuses := map[string]*fullStatusResult{
		topoproto.TabletAliasString(deposed.Alias): supersededTestStatus(deposed, deposed, "1790000001:5", deposed, newer, other),
		topoproto.TabletAliasString(newer.Alias):   supersededTestStatus(newer, newer, "1790000001:6", newer, other),
		topoproto.TabletAliasString(other.Alias):   supersededTestStatus(other, newer, "1790000001:6", newer, other),
	}
	durability, err := policy.GetDurabilityPolicy(policy.DurabilityGroupReplicationCrossCell)
	require.NoError(t, err)
	opts := EmergencyReparentOptions{durability: durability}

	gv, err := findGroupWithQuorum(statuses, legitimateGroup("1790000001", voters, statuses))
	require.NoError(t, err, "the deposed member's view is superseded, and does not count")
	chosen, err := chooseGroupReplicationPrimary(statuses, gv, voters, nil, opts)
	require.NoError(t, err)
	assert.Equal(t, "zone2-0000000200", topoproto.TabletAliasString(chosen.tablet.Alias))

	opts.NewPrimaryAlias = deposed.Alias
	_, err = chooseGroupReplicationPrimary(statuses, gv, voters, nil, opts)
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
}
