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

package inst

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	planIncarnation = "1790000001"
	planGTIDSID     = "6f1c2c2e-5a8e-4b8e-9d3a-7c1f0b6e2a41"
)

func planUUID(tablet *topodatapb.Tablet) string {
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", tablet.Alias.Uid)
}

func planGTIDs(t *testing.T, intervals string) replication.GTIDSet {
	set, err := replication.ParseMysql56GTIDSet(planGTIDSID + ":" + intervals)
	require.NoError(t, err)
	return set
}

// planFixture is a shard with a settled group: a, the group primary, in zone1, and the voters b and c
// in zone2 and zone3, all ONLINE in every view; b2 and c2 are spares of zone2 and zone3.
type planFixture struct {
	in                *VoterPlanInput
	a, b, c, b2, c2   *topodatapb.Tablet
	byAlias           map[string]*VoterTablet
	primaryOfTheGroup *topodatapb.Tablet
}

func newPlanFixture(t *testing.T) *planFixture {
	grd, ok := policy.AsGroupReplication(mustDurability(t))
	require.True(t, ok)
	f := &planFixture{
		a:       grTablet("zone1", 101, topodatapb.TabletType_PRIMARY),
		b:       grTablet("zone2", 200, topodatapb.TabletType_REPLICA),
		c:       grTablet("zone3", 300, topodatapb.TabletType_REPLICA),
		b2:      grTablet("zone2", 201, topodatapb.TabletType_REPLICA),
		c2:      grTablet("zone3", 301, topodatapb.TabletType_REPLICA),
		byAlias: make(map[string]*VoterTablet),
	}
	f.primaryOfTheGroup = f.a
	f.in = &VoterPlanInput{
		Durability:  grd,
		Voters:      []*topodatapb.TabletAlias{f.a.Alias, f.b.Alias, f.c.Alias},
		Incarnation: planIncarnation,
		GracePeriod: time.Minute,
		Fresh:       true,
	}
	for _, tablet := range []*topodatapb.Tablet{f.a, f.b, f.c} {
		f.add(t, tablet, f.member(tablet, f.a, f.b, f.c), "1-10")
	}
	for _, tablet := range []*topodatapb.Tablet{f.b2, f.c2} {
		f.add(t, tablet, &replicationdatapb.GroupReplicationStatus{PluginActive: true, MemberState: mysql.GroupMemberStateOffline}, "1-5")
	}
	return f
}

func mustDurability(t *testing.T) policy.Durabler {
	d, err := policy.GetDurabilityPolicy(policy.DurabilityGroupReplicationCrossCell)
	require.NoError(t, err)
	return d
}

// member returns the status of an ONLINE member of the group led by f.primaryOfTheGroup, whose view
// holds the given members ONLINE.
func (f *planFixture) member(tablet *topodatapb.Tablet, online ...*topodatapb.Tablet) *replicationdatapb.GroupReplicationStatus {
	role := mysql.GroupMemberRoleSecondary
	if tablet == f.primaryOfTheGroup {
		role = mysql.GroupMemberRolePrimary
	}
	status := &replicationdatapb.GroupReplicationStatus{
		PluginActive: true,
		MemberState:  mysql.GroupMemberStateOnline,
		MemberRole:   role,
		HasQuorum:    true,
		PrimaryUuid:  planUUID(f.primaryOfTheGroup),
		ViewId:       planIncarnation + ":7",
	}
	for _, m := range online {
		status.Members = append(status.Members, &replicationdatapb.GroupReplicationMember{MemberUuid: planUUID(m), State: mysql.GroupMemberStateOnline})
	}
	return status
}

func (f *planFixture) add(t *testing.T, tablet *topodatapb.Tablet, status *replicationdatapb.GroupReplicationStatus, executed string) *VoterTablet {
	vt := &VoterTablet{Tablet: tablet, Reachable: true, Status: status, ServerUUID: planUUID(tablet), Executed: planGTIDs(t, executed)}
	f.in.Tablets = append(f.in.Tablets, vt)
	f.byAlias[topoproto.TabletAliasString(tablet.Alias)] = vt
	return vt
}

func (f *planFixture) tablet(tablet *topodatapb.Tablet) *VoterTablet {
	return f.byAlias[topoproto.TabletAliasString(tablet.Alias)]
}

// fail makes the tablet unreachable for the given time, and drops its MySQL from every view.
func (f *planFixture) fail(tablet *topodatapb.Tablet, unreachableFor time.Duration) {
	vt := f.tablet(tablet)
	vt.Reachable, vt.Status, vt.Executed, vt.UnreachableFor = false, nil, nil, unreachableFor
	f.dropFromViews(tablet)
}

func (f *planFixture) dropFromViews(tablet *topodatapb.Tablet) {
	for _, vt := range f.in.Tablets {
		if vt.Status == nil {
			continue
		}
		vt.Status.Members = slices.DeleteFunc(vt.Status.Members, func(m *replicationdatapb.GroupReplicationMember) bool {
			return m.GetMemberUuid() == planUUID(tablet)
		})
	}
}

// deleteRecord removes the tablet record of the tablet: it is then a deleted voter, whose server_uuid
// VTOrc knows when knownUUID is set.
func (f *planFixture) deleteRecord(tablet *topodatapb.Tablet, knownUUID bool) {
	f.in.Tablets = slices.DeleteFunc(f.in.Tablets, func(vt *VoterTablet) bool { return vt.Tablet == tablet })
	if !policy.IsVoter(f.in.Voters, tablet.Alias) {
		return
	}
	if f.in.DeletedVoters == nil {
		f.in.DeletedVoters = make(map[string]*DeletedVoter)
	}
	// VTOrc kept its tablet record, and its probe failed.
	f.in.DeletedVoters[topoproto.TabletAliasString(tablet.Alias)] = &DeletedVoter{Alias: tablet.Alias, Tablet: tablet, Down: true}
	if knownUUID {
		f.in.DeletedVoters[topoproto.TabletAliasString(tablet.Alias)].ServerUUID = planUUID(tablet)
	}
}

func addView(status *replicationdatapb.GroupReplicationStatus, uuid, state string) {
	status.Members = append(status.Members, &replicationdatapb.GroupReplicationMember{MemberUuid: uuid, State: state})
}

// TestPlanGroupVoters checks each precondition of the changes that VTOrc makes to a shard's voters:
// every case changes one input so that one check decides, and fails without that check.
func TestPlanGroupVoters(t *testing.T) {
	type want struct {
		action  VoterAction
		voters  []string
		removed string
		added   string
		alert   AnalysisCode
		reason  string
	}
	swapC := want{action: VoterActionSwap, voters: []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000301"}, removed: "zone3-0000000300", added: "zone3-0000000301"}
	tests := []struct {
		name  string
		setup func(t *testing.T, f *planFixture)
		want  want
	}{{
		name:  "a settled group with a voter in every cell: nothing to change",
		setup: func(t *testing.T, f *planFixture) {},
	}, {
		name:  "SwapVoter: a voter unreachable for the grace period, active in no view, with a spare in its cell",
		setup: func(t *testing.T, f *planFixture) { f.fail(f.c, time.Hour) },
		want:  swapC,
	}, {
		name:  "a voter unreachable within the grace period keeps its seat",
		setup: func(t *testing.T, f *planFixture) { f.fail(f.c, 10*time.Second) },
	}, {
		name: "P1: no incarnation recorded",
		setup: func(t *testing.T, f *planFixture) {
			f.fail(f.c, time.Hour)
			f.in.Incarnation = ""
		},
		want: want{reason: "the shard record lists no incarnation"},
	}, {
		name: "P1: the primary's view is of another incarnation",
		setup: func(t *testing.T, f *planFixture) {
			f.fail(f.c, time.Hour)
			f.tablet(f.a).Status.ViewId = "1790000002:1"
		},
		want: want{reason: `the group primary zone1-0000000101 is in incarnation "1790000002", not the recorded "1790000001"`},
	}, {
		name: "P1: the election of the primary is in progress",
		setup: func(t *testing.T, f *planFixture) {
			f.fail(f.c, time.Hour)
			f.tablet(f.a).Status.PrimaryElectionInProgress = true
		},
		want: want{reason: "the election of the group primary zone1-0000000101 is in progress"},
	}, {
		name: "P1: the analysis, without primary_election_in_progress, proposes the swap; the recovery decides",
		setup: func(t *testing.T, f *planFixture) {
			f.fail(f.c, time.Hour)
			f.tablet(f.a).Status.PrimaryElectionInProgress = true
			f.in.Fresh = false
		},
		want: swapC,
	}, {
		name: "P1: the primary's view holds a minority of the voters",
		setup: func(t *testing.T, f *planFixture) {
			f.fail(f.c, time.Hour)
			f.fail(f.b, 10*time.Second)
		},
		want: want{reason: "the view of the group primary zone1-0000000101 holds 1 of the 3 voters ONLINE, not a majority"},
	}, {
		name: "P2: a reachable member reports the failed voter's MySQL RECOVERING",
		setup: func(t *testing.T, f *planFixture) {
			f.fail(f.c, time.Hour)
			addView(f.tablet(f.b).Status, planUUID(f.c), mysql.GroupMemberStateRecovering)
		},
	}, {
		// Its MySQL got a new server_uuid (re-initialized): it is found by its MySQL address, as the
		// voter majority of P1 finds the voters.
		name: "P2: a reachable member reports the failed voter's MySQL address ONLINE, under another server_uuid",
		setup: func(t *testing.T, f *planFixture) {
			f.fail(f.c, time.Hour)
			f.tablet(f.b).Status.Members = append(f.tablet(f.b).Status.Members, &replicationdatapb.GroupReplicationMember{
				MemberUuid: "00000000-0000-0000-0000-000000000399", Host: f.c.MysqlHostname, Port: f.c.MysqlPort, State: mysql.GroupMemberStateOnline,
			})
			// That member is the MySQL of a tablet that answers, as far as VTOrc can tell.
			f.add(t, grTablet("zone4", 399, topodatapb.TabletType_RDONLY), &replicationdatapb.GroupReplicationStatus{}, "1-1").ServerUUID = "00000000-0000-0000-0000-000000000399"
		},
	}, {
		name: "P2: the failed voter's server_uuid is unknown, and an active member is the MySQL of no tablet that answers",
		setup: func(t *testing.T, f *planFixture) {
			f.fail(f.c, time.Hour)
			f.tablet(f.c).ServerUUID = ""
			addView(f.tablet(f.b).Status, "00000000-0000-0000-0000-000000000999", mysql.GroupMemberStateOnline)
		},
	}, {
		name: "P2: the failed voter's server_uuid is unknown, and every active member answers",
		setup: func(t *testing.T, f *planFixture) {
			f.fail(f.c, time.Hour)
			f.tablet(f.c).ServerUUID = ""
		},
		want: swapC,
	}, {
		name: "P3: the only other tablet of the cell is RDONLY",
		setup: func(t *testing.T, f *planFixture) {
			f.fail(f.c, time.Hour)
			f.c2.Type = topodatapb.TabletType_RDONLY
		},
		want: want{alert: GroupVoterUnreplaceable, reason: "zone3-0000000301: not a REPLICA that the policy allows as a voter"},
	}, {
		name: "P3: the spare does not answer",
		setup: func(t *testing.T, f *planFixture) {
			f.fail(f.c, time.Hour)
			f.fail(f.c2, time.Second)
		},
		want: want{alert: GroupVoterUnreplaceable, reason: "zone3-0000000301: unreachable"},
	}, {
		name: "P3: the spare is an active member",
		setup: func(t *testing.T, f *planFixture) {
			f.fail(f.c, time.Hour)
			f.tablet(f.c2).Status = f.member(f.c2, f.a, f.b, f.c2)
		},
		want: want{alert: GroupVoterUnreplaceable, reason: "zone3-0000000301: an active group member"},
	}, {
		name: "P3: a START GROUP_REPLICATION runs on the spare",
		setup: func(t *testing.T, f *planFixture) {
			f.fail(f.c, time.Hour)
			f.tablet(f.c2).Status.StartInProgress = true
		},
		want: want{alert: GroupVoterUnreplaceable, reason: "zone3-0000000301: a START GROUP_REPLICATION runs"},
	}, {
		name: "P3: the spare is in the ERROR state of a group",
		setup: func(t *testing.T, f *planFixture) {
			f.fail(f.c, time.Hour)
			f.tablet(f.c2).Status.MemberState = mysql.GroupMemberStateError
		},
		want: want{alert: GroupVoterUnreplaceable, reason: "zone3-0000000301: in a group (ERROR)"},
	}, {
		name: "P3: the spare executed a transaction that the primary lacks",
		setup: func(t *testing.T, f *planFixture) {
			f.fail(f.c, time.Hour)
			f.tablet(f.c2).Executed = planGTIDs(t, "1-11")
		},
		want: want{alert: GroupVoterUnreplaceable, reason: "zone3-0000000301: it executed transactions that the primary lacks"},
	}, {
		name: "P3: the spare's executed transactions are unknown",
		setup: func(t *testing.T, f *planFixture) {
			f.fail(f.c, time.Hour)
			f.tablet(f.c2).Executed = nil
		},
		want: want{alert: GroupVoterUnreplaceable, reason: "zone3-0000000301: its executed transactions, or the primary's, are unknown"},
	}, {
		name: "P3: another voter of the new list is in the spare's cell",
		setup: func(t *testing.T, f *planFixture) {
			c3 := grTablet("zone3", 302, topodatapb.TabletType_REPLICA)
			f.in.Voters = append(f.in.Voters, c3.Alias)
			f.add(t, c3, f.member(c3, f.a, f.b, f.c, c3), "1-10")
			for _, vt := range []*VoterTablet{f.tablet(f.a), f.tablet(f.b)} {
				addView(vt.Status, planUUID(c3), mysql.GroupMemberStateOnline)
			}
			f.fail(f.c, time.Hour)
		},
		want: want{alert: GroupVoterUnreplaceable, reason: "zone3-0000000301: its cell has a voter"},
	}, {
		name: "SwapVoter: a voter whose tablet record was deleted, and whose MySQL is active in no view",
		setup: func(t *testing.T, f *planFixture) {
			f.dropFromViews(f.c)
			f.deleteRecord(f.c, true)
		},
		want: swapC,
	}, {
		name: "RemoveVoter: a voter whose tablet record was deleted, active in no view, and no spare in its cell",
		setup: func(t *testing.T, f *planFixture) {
			f.dropFromViews(f.c)
			f.deleteRecord(f.c, true)
			f.deleteRecord(f.c2, false)
		},
		want: want{action: VoterActionRemove, voters: []string{"zone1-0000000101", "zone2-0000000200"}, removed: "zone3-0000000300"},
	}, {
		name: "P2: a voter whose tablet record was deleted, but whose MySQL is still ONLINE: an alert",
		setup: func(t *testing.T, f *planFixture) {
			f.deleteRecord(f.c, true)
		},
		want: want{alert: GroupVoterRecordDeleted, reason: "voter zone3-0000000300 has no tablet record, but a reachable member reports its MySQL (00000000-0000-0000-0000-000000000300) active"},
	}, {
		name: "P2: a deleted voter whose vttablet answers at the address VTOrc last knew: an alert",
		setup: func(t *testing.T, f *planFixture) {
			f.dropFromViews(f.c)
			f.deleteRecord(f.c, true)
			f.in.DeletedVoters["zone3-0000000300"].Down = false
		},
		want: want{alert: GroupVoterRecordDeleted, reason: "voter zone3-0000000300 has no tablet record, but VTOrc cannot tell that its vttablet is down " +
			"(it answers, VTOrc reached it within the grace period, or VTOrc has no address for it): " +
			"it keeps its seat; stop its vttablet and MySQL to let VTOrc replace or remove it, or restart its vttablet, which records the tablet again"},
	}, {
		name: "P2: a deleted voter whose server_uuid is unknown, while an active member is the MySQL of no tablet that answers",
		setup: func(t *testing.T, f *planFixture) {
			f.deleteRecord(f.c, false)
		},
		want: want{alert: GroupVoterRecordDeleted, reason: "its server_uuid is unknown, and the active member 00000000-0000-0000-0000-000000000300 is the MySQL of no tablet that answers"},
	}, {
		name: "RemoveVoter: a deleted voter that is still in the view of the primary, UNREACHABLE: an alert",
		setup: func(t *testing.T, f *planFixture) {
			f.dropFromViews(f.c)
			addView(f.tablet(f.a).Status, planUUID(f.c), mysql.GroupMemberStateUnreachable)
			f.deleteRecord(f.c, true)
			f.deleteRecord(f.c2, false)
		},
		want: want{alert: GroupVoterRecordDeleted, reason: "it is UNREACHABLE in the view of the primary zone1-0000000101"},
	}, {
		name: "RemoveVoter needs P1: the primary's view holds a minority of the voters",
		setup: func(t *testing.T, f *planFixture) {
			f.dropFromViews(f.c)
			f.deleteRecord(f.c, true)
			f.deleteRecord(f.c2, false)
			f.fail(f.b, 10*time.Second)
		},
		want: want{reason: "holds 1 of the 3 voters ONLINE, not a majority"},
	}, {
		name: "GrowVoter: a cell with an eligible tablet and no voter",
		setup: func(t *testing.T, f *planFixture) {
			d := grTablet("zone4", 400, topodatapb.TabletType_REPLICA)
			f.add(t, d, &replicationdatapb.GroupReplicationStatus{}, "1-3")
		},
		want: want{action: VoterActionGrow, voters: []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000300", "zone4-0000000400"}, added: "zone4-0000000400"},
	}, {
		name: "GrowVoter needs a majority of the grown list ONLINE in the primary's view",
		setup: func(t *testing.T, f *planFixture) {
			d := grTablet("zone4", 400, topodatapb.TabletType_REPLICA)
			f.add(t, d, &replicationdatapb.GroupReplicationStatus{}, "1-3")
			f.fail(f.c, 10*time.Second)
		},
		want: want{alert: GroupVotersBelowTarget, reason: "holds 2 voters ONLINE, fewer than a majority of the 4 voters it would have"},
	}, {
		name: "GrowVoter needs P3: the tablet of the new cell executed a transaction that the primary lacks",
		setup: func(t *testing.T, f *planFixture) {
			d := grTablet("zone4", 400, topodatapb.TabletType_REPLICA)
			f.add(t, d, &replicationdatapb.GroupReplicationStatus{}, "1-20")
		},
		want: want{alert: GroupVotersBelowTarget, reason: "cell zone4 has no voter, and no valid spare (zone4-0000000400: it executed transactions that the primary lacks"},
	}, {
		name: "fewer than three voters, and no cell to grow into: an alert",
		setup: func(t *testing.T, f *planFixture) {
			f.in.Voters = []*topodatapb.TabletAlias{f.a.Alias, f.b.Alias}
			f.dropFromViews(f.c)
			f.deleteRecord(f.c, false)
			f.deleteRecord(f.c2, false)
		},
		want: want{alert: GroupVotersBelowTarget, reason: "the group has 2 voters"},
	}, {
		name: "MoveGroupPrimaryToVoter: the primary of the legitimate group is not a voter",
		setup: func(t *testing.T, f *planFixture) {
			a2 := grTablet("zone1", 102, topodatapb.TabletType_REPLICA)
			f.in.Voters = []*topodatapb.TabletAlias{a2.Alias, f.b.Alias, f.c.Alias}
			f.add(t, a2, f.member(a2, f.a, a2, f.b, f.c), "1-10")
			addView(f.tablet(f.a).Status, planUUID(a2), mysql.GroupMemberStateOnline)
		},
		want: want{action: VoterActionMovePrimary},
	}, {
		name: "MoveGroupPrimaryToVoter: the primary is a voter whose tablet record was deleted",
		setup: func(t *testing.T, f *planFixture) {
			f.deleteRecord(f.a, true)
		},
		want: want{action: VoterActionMovePrimary, reason: "is voter zone1-0000000101, whose tablet record was deleted"},
	}, {
		name: "MoveGroupPrimaryToVoter: the primary is a voter whose tablet record was deleted, and whose server_uuid is unknown, found by its MySQL address",
		setup: func(t *testing.T, f *planFixture) {
			f.deleteRecord(f.a, false)
			for _, vt := range []*VoterTablet{f.tablet(f.b), f.tablet(f.c)} {
				for _, m := range vt.Status.Members {
					if m.GetMemberUuid() == planUUID(f.a) {
						m.Host, m.Port = f.a.MysqlHostname, f.a.MysqlPort
					}
				}
			}
		},
		want: want{action: VoterActionMovePrimary, reason: "is voter zone1-0000000101, whose tablet record was deleted"},
	}, {
		name: "MoveGroupPrimaryToVoter: the primary is a voter whose tablet record was deleted, of which VTOrc knows nothing, and no other tablet is the primary",
		setup: func(t *testing.T, f *planFixture) {
			f.deleteRecord(f.a, false)
			f.in.DeletedVoters["zone1-0000000101"].Tablet = nil
		},
		want: want{action: VoterActionMovePrimary, reason: "is voter zone1-0000000101, whose tablet record was deleted"},
	}, {
		// The primary is voter b, whose vttablet is down and whose server_uuid VTOrc does not know, but
		// whose MySQL address the view reports: it is no deleted voter, and nothing moves.
		name: "a deleted voter whose server_uuid is unknown, while the primary is a voter with a record whose server_uuid is unknown",
		setup: func(t *testing.T, f *planFixture) {
			f.dropFromViews(f.c)
			f.deleteRecord(f.c, false)
			f.in.DeletedVoters["zone3-0000000300"].Tablet = nil
			b := f.tablet(f.b)
			b.Reachable, b.Status, b.ServerUUID, b.UnreachableFor = false, nil, "", time.Second
			f.primaryOfTheGroup = f.b
			a := f.tablet(f.a)
			a.Status = f.member(f.a, f.a)
			a.Status.Members = append(a.Status.Members, &replicationdatapb.GroupReplicationMember{
				MemberUuid: planUUID(f.b), Host: f.b.MysqlHostname, Port: f.b.MysqlPort, State: mysql.GroupMemberStateOnline, Role: mysql.GroupMemberRolePrimary,
			})
		},
		want: want{alert: GroupVoterRecordDeleted, reason: "voter zone3-0000000300 has no tablet record, but its server_uuid is unknown"},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newPlanFixture(t)
			tt.setup(t, f)
			plan := PlanGroupVoters(f.in)
			assert.Equal(t, tt.want.action, plan.Action, plan.Reason)
			assert.Equal(t, tt.want.alert, plan.Alert, plan.Reason)
			var voters []string
			for _, voter := range plan.Voters {
				voters = append(voters, topoproto.TabletAliasString(voter))
			}
			assert.Equal(t, tt.want.voters, voters)
			if tt.want.removed != "" {
				assert.Equal(t, tt.want.removed, topoproto.TabletAliasString(plan.Removed))
			}
			if tt.want.added != "" {
				require.NotNil(t, plan.Added)
				assert.Equal(t, tt.want.added, topoproto.TabletAliasString(plan.Added.Alias))
			}
			if tt.want.reason != "" {
				assert.Contains(t, plan.Reason, tt.want.reason)
			}
		})
	}
}

// TestPlanGroupVotersInitial checks the first voter list of a shard: one voter per cell, only while no
// member is active, and only with eligible tablets in at least three cells.
func TestPlanGroupVotersInitial(t *testing.T) {
	offline := func() *replicationdatapb.GroupReplicationStatus {
		return &replicationdatapb.GroupReplicationStatus{PluginActive: true, MemberState: mysql.GroupMemberStateOffline}
	}
	tests := []struct {
		name    string
		tablets []*topodatapb.Tablet
		active  bool
		// unreachable is the uid of a tablet that does not answer, if any.
		unreachable uint32
		incarnation string
		want        []string
		alert       AnalysisCode
	}{{
		name: "eligible tablets in three cells: one voter per cell",
		tablets: []*topodatapb.Tablet{
			grTablet("zone1", 101, topodatapb.TabletType_REPLICA), grTablet("zone1", 102, topodatapb.TabletType_REPLICA),
			grTablet("zone2", 200, topodatapb.TabletType_REPLICA), grTablet("zone3", 300, topodatapb.TabletType_REPLICA),
		},
		want: []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000300"},
	}, {
		name: "eligible tablets in two cells: an alert",
		tablets: []*topodatapb.Tablet{
			grTablet("zone1", 101, topodatapb.TabletType_REPLICA), grTablet("zone2", 200, topodatapb.TabletType_REPLICA),
			grTablet("zone3", 300, topodatapb.TabletType_RDONLY),
		},
		alert: GroupVotersBelowTarget,
	}, {
		name: "a member is active: an alert",
		tablets: []*topodatapb.Tablet{
			grTablet("zone1", 101, topodatapb.TabletType_REPLICA), grTablet("zone2", 200, topodatapb.TabletType_REPLICA),
			grTablet("zone3", 300, topodatapb.TabletType_REPLICA),
		},
		active: true,
		alert:  GroupVotersBelowTarget,
	}, {
		// Its MySQL may be an active member: VTOrc cannot tell that no group runs.
		name: "an eligible tablet does not answer: nothing",
		tablets: []*topodatapb.Tablet{
			grTablet("zone1", 101, topodatapb.TabletType_REPLICA), grTablet("zone2", 200, topodatapb.TabletType_REPLICA),
			grTablet("zone3", 300, topodatapb.TabletType_REPLICA), grTablet("zone3", 301, topodatapb.TabletType_REPLICA),
		},
		unreachable: 301,
	}, {
		// A group was bootstrapped, and its voter list is gone: not a new shard.
		name: "an incarnation is recorded: nothing",
		tablets: []*topodatapb.Tablet{
			grTablet("zone1", 101, topodatapb.TabletType_REPLICA), grTablet("zone2", 200, topodatapb.TabletType_REPLICA),
			grTablet("zone3", 300, topodatapb.TabletType_REPLICA),
		},
		incarnation: "1790000001",
		alert:       GroupVotersBelowTarget,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			grd, _ := policy.AsGroupReplication(mustDurability(t))
			in := &VoterPlanInput{Durability: grd, GracePeriod: time.Minute, Fresh: true, Incarnation: tt.incarnation}
			for i, tablet := range tt.tablets {
				status := offline()
				if tt.active && i == 0 {
					status.MemberState = mysql.GroupMemberStateOnline
				}
				vt := &VoterTablet{Tablet: tablet, Reachable: true, Status: status, ServerUUID: planUUID(tablet)}
				if tablet.Alias.Uid == tt.unreachable {
					vt.Reachable, vt.Status, vt.UnreachableFor = false, nil, time.Hour
				}
				in.Tablets = append(in.Tablets, vt)
			}
			plan := PlanGroupVoters(in)
			assert.Equal(t, tt.alert, plan.Alert, plan.Reason)
			if tt.want == nil {
				assert.Equal(t, VoterActionNone, plan.Action)
				return
			}
			assert.Equal(t, VoterActionInitial, plan.Action)
			var voters []string
			for _, voter := range plan.Voters {
				voters = append(voters, topoproto.TabletAliasString(voter))
			}
			assert.Equal(t, tt.want, voters)
		})
	}
}

// TestPlanGroupVotersRemoveNoGroup checks each precondition of RemoveVoterNoGroup: a voter whose tablet
// record was deleted leaves the list while no group runs, so that the group can be bootstrapped from
// the other voters.
func TestPlanGroupVotersRemoveNoGroup(t *testing.T) {
	removeC := []string{"zone1-0000000101", "zone2-0000000200"}
	tests := []struct {
		name  string
		setup func(t *testing.T, f *planFixture)
		// want is the new list; nil means that nothing is removed.
		want []string
	}{{
		name:  "no group runs, and a voter's tablet record was deleted: it is removed",
		setup: func(t *testing.T, f *planFixture) {},
		want:  removeC,
	}, {
		name: "its server_uuid is unknown: it is removed too, since no member is active",
		setup: func(t *testing.T, f *planFixture) {
			f.in.DeletedVoters["zone3-0000000300"].ServerUUID = ""
		},
		want: removeC,
	}, {
		name: "the deleted voter's vttablet answers at the address VTOrc last knew",
		setup: func(t *testing.T, f *planFixture) {
			f.in.DeletedVoters["zone3-0000000300"].Down = false
		},
	}, {
		// The FullStatus of a live member timed out: a group may run there.
		name: "another voter does not answer",
		setup: func(t *testing.T, f *planFixture) {
			f.fail(f.b, time.Second)
		},
	}, {
		name: "no other voter answers",
		setup: func(t *testing.T, f *planFixture) {
			f.fail(f.a, time.Second)
			f.fail(f.b, time.Second)
		},
	}, {
		name: "a reachable tablet is an active member",
		setup: func(t *testing.T, f *planFixture) {
			f.tablet(f.a).Status = f.member(f.a, f.a)
		},
	}, {
		name: "a reachable tablet is an active member of another incarnation",
		setup: func(t *testing.T, f *planFixture) {
			f.tablet(f.b).Status = f.member(f.b, f.b)
			f.tablet(f.b).Status.ViewId = "1790000009:1"
		},
	}, {
		name: "a START GROUP_REPLICATION runs on a reachable tablet",
		setup: func(t *testing.T, f *planFixture) {
			f.tablet(f.b).Status.StartInProgress = true
		},
	}, {
		name: "a bootstrap intent is live",
		setup: func(t *testing.T, f *planFixture) {
			f.in.BootstrapIntentLive = true
		},
	}, {
		name: "the voter has a tablet record, and is unreachable",
		setup: func(t *testing.T, f *planFixture) {
			delete(f.in.DeletedVoters, "zone3-0000000300")
			f.add(t, f.c, nil, "1-10")
			f.fail(f.c, time.Hour)
		},
	}, {
		name: "the last voter is not removed",
		setup: func(t *testing.T, f *planFixture) {
			f.in.Voters = []*topodatapb.TabletAlias{f.c.Alias}
		},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newPlanFixture(t)
			// The group lost its majority: no member is active, and voter c is dead for good; the
			// operator deleted its tablet record.
			for _, vt := range f.in.Tablets {
				vt.Status = &replicationdatapb.GroupReplicationStatus{PluginActive: true, MemberState: mysql.GroupMemberStateOffline}
			}
			f.deleteRecord(f.c, true)
			tt.setup(t, f)
			plan := PlanGroupVoters(f.in)
			if tt.want == nil {
				assert.NotEqual(t, VoterActionRemoveNoGroup, plan.Action, plan.Reason)
				assert.False(t, plan.Action.ChangesVoters(), plan.Reason)
				return
			}
			require.Equal(t, VoterActionRemoveNoGroup, plan.Action, plan.Reason)
			var voters []string
			for _, voter := range plan.Voters {
				voters = append(voters, topoproto.TabletAliasString(voter))
			}
			assert.Equal(t, tt.want, voters)
			assert.Equal(t, "zone3-0000000300", topoproto.TabletAliasString(plan.Removed))
		})
	}
}
