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

package tabletmanager

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// nonVoterView returns the status of tablet 1's MySQL in a view of the recorded incarnation that holds
// the members 1, 2 and 3, ONLINE, with primary the given member.
func nonVoterView(primary int) *replicationdatapb.GroupReplicationStatus {
	role := func(uid int) string {
		if uid == primary {
			return mysql.GroupMemberRolePrimary
		}
		return mysql.GroupMemberRoleSecondary
	}
	return withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, role(1)),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, role(2)),
		groupMember(testServerUUID(3), mysql.GroupMemberStateOnline, role(3))), "1780000001:30")
}

// TestGroupReplicationNonVoterGroupPrimaryDoesNotServe checks that a tablet whose MySQL Group Replication
// elected primary serves as PRIMARY only while it is a listed voter. A join that started while the tablet
// was a voter can complete after the voter list dropped it (MySQL finishes a START whose client gave
// up), and the group can elect such a member: a transaction it acknowledges is certified by a majority
// of the view, which need not hold a majority of the voters, and a later bootstrap from the voters would
// lose it. Once VTOrc gives the elected member a seat (the selection keeps the group primary's seat), it
// serves.
func TestGroupReplicationNonVoterGroupPrimaryDoesNotServe(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _, ts := newLegitimacyTestTM(t)
	// Tablet 1 is not a voter; the voters 2 and 3 are ONLINE in its view, a majority of them.
	setGroupReplicationVoters(t, ts, 2, 3)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	s := newGroupReplicationSync(tm)
	fmd.SetGroupReplicationStatus(nonVoterView(1))
	fmd.SuperReadOnly.Store(true)

	s.reconcile(ctx)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type, "the group primary's tablet is PRIMARY, so that vtgate buffers")
	assert.False(t, qsc.IsServing(), "a primary that is not a voter must not serve")
	assert.True(t, fmd.SuperReadOnly.Load(), "a primary that is not a voter must stay read-only")
	reason, _ := tm.tmState.GroupReplicationNotServingState()
	assert.Equal(t, groupReplicationNotVoter, reason)
	_, stops, _ := fmd.GroupReplicationCalls()
	assert.Zero(t, stops, "the group primary does not leave its group: VTOrc moves the group primary to a voter")

	// The voter list gains tablet 1 (for example, an operator rewrote it).
	setGroupReplicationVoters(t, ts, 1, 2)
	s.recordRead = time.Time{}
	s.reconcile(ctx)
	assert.True(t, qsc.IsServing())
	assert.False(t, fmd.SuperReadOnly.Load())
}

// TestGroupReplicationSyncLeavesAsNonVoter checks that the sync loop makes MySQL leave its group when
// the tablet is not a listed voter, rather than waiting for VTOrc's GroupVotersOutOfDate: such a member
// counts in the certification majority of the view, and the group may elect it. The voter list is read
// again right before the leave, so that a tablet that VTOrc just gave a seat stays. The group primary
// does not leave (see TestGroupReplicationNonVoterGroupPrimaryDoesNotServe), nor a member whose group
// would not keep a majority of its members without it.
func TestGroupReplicationSyncLeavesAsNonVoter(t *testing.T) {
	tests := []struct {
		name       string
		voters     []uint32
		status     *replicationdatapb.GroupReplicationStatus
		tabletType topodatapb.TabletType
		wantLeave  bool
	}{{
		name:      "a secondary that is not a voter leaves",
		voters:    []uint32{2, 3},
		status:    nonVoterView(2),
		wantLeave: true,
	}, {
		// VTOrc gave the seat of a voter whose type changed to RDONLY or DRAINED to a spare.
		name:       "a RDONLY secondary that is not a voter leaves",
		voters:     []uint32{2, 3},
		status:     nonVoterView(2),
		tabletType: topodatapb.TabletType_RDONLY,
		wantLeave:  true,
	}, {
		name:       "a DRAINED secondary that is not a voter leaves",
		voters:     []uint32{2, 3},
		status:     nonVoterView(2),
		tabletType: topodatapb.TabletType_DRAINED,
		wantLeave:  true,
	}, {
		name:       "a secondary that takes a backup stays",
		voters:     []uint32{2, 3},
		status:     nonVoterView(2),
		tabletType: topodatapb.TabletType_BACKUP,
	}, {
		name:   "a secondary that is a voter stays",
		voters: []uint32{1, 2, 3},
		status: nonVoterView(2),
	}, {
		name:   "the group primary stays",
		voters: []uint32{2, 3},
		status: nonVoterView(1),
	}, {
		name:   "a secondary whose group would lose its majority without it stays",
		voters: []uint32{2, 3},
		status: withViewID(groupStatus(testServerUUID(1),
			groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
			groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
			groupMember(testServerUUID(3), mysql.GroupMemberStateUnreachable, mysql.GroupMemberRoleSecondary)), "1780000001:31"),
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withGroupReplication(t)
			tm, fmd, _, ts := newLegitimacyTestTM(t)
			setGroupReplicationVoters(t, ts, tt.voters...)
			fmd.SetGroupReplicationStatus(tt.status)
			if tt.tabletType != topodatapb.TabletType_UNKNOWN {
				setTabletType(t, tm, tt.tabletType)
			}
			s := newGroupReplicationSync(tm)

			s.reconcile(t.Context())
			_, stops, _ := fmd.GroupReplicationCalls()
			if tt.wantLeave {
				assert.Equal(t, 1, stops)
				status, err := fmd.GroupReplicationStatus(t.Context())
				require.NoError(t, err)
				assert.Equal(t, mysql.GroupMemberStateOffline, status.MemberState)
				return
			}
			assert.Zero(t, stops)
		})
	}
}

// TestGroupReplicationLeaveAsNonVoterReadsVotersFresh checks that the leave of a member that is not a
// voter does not undo a seat that VTOrc gave the tablet since the sync loop cached the voter list: the
// list is read again under the action lock, right before the leave.
func TestGroupReplicationLeaveAsNonVoterReadsVotersFresh(t *testing.T) {
	withGroupReplication(t)
	tm, fmd, _, ts := newLegitimacyTestTM(t)
	setGroupReplicationVoters(t, ts, 2, 3)
	fmd.SetGroupReplicationStatus(nonVoterView(2))
	s := newGroupReplicationSync(tm)
	// The loop read the voters before the list gained tablet 1.
	voters, err := s.getVoters(t.Context())
	require.NoError(t, err)
	require.Len(t, voters, 2)
	setGroupReplicationVoters(t, ts, 1, 2, 3)

	s.reconcile(t.Context())
	_, stops, _ := fmd.GroupReplicationCalls()
	assert.Zero(t, stops, "a tablet that the list gained stays in the group")
}

// TestGroupReplicationVoterListDropFencesServingPrimary reproduces the TLA+ model's
// voters_split_nonvoter trace on the tablet: the primary decided to serve while it was a voter, and a
// voter list written later drops it. The serving decision is not taken again by itself: the tablet
// reads the new list from its shard watch, and the fence check makes MySQL read-only and stops serving
// right away, as for a lost voter majority.
func TestGroupReplicationVoterListDropFencesServingPrimary(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _, ts := newLegitimacyTestTM(t)
	fmd.SetGroupReplicationStatus(nonVoterView(1))
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	s := newGroupReplicationSync(tm)
	s.reconcile(ctx)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	require.True(t, qsc.IsServing())
	require.False(t, fmd.SuperReadOnly.Load())

	// A voter list that drops tablet 1 is written; the tablet learns it from its shard watch.
	setGroupReplicationVoters(t, ts, 2, 3)
	assert.Eventually(t, func() bool {
		s.checkFence(ctx)
		return fmd.SuperReadOnly.Load()
	}, 30*time.Second, 50*time.Millisecond, "MySQL must be fenced")
	assert.Eventually(t, func() bool { return !qsc.IsServing() }, 30*time.Second, 10*time.Millisecond)
	reason, _ := tm.tmState.GroupReplicationNotServingState()
	assert.Equal(t, groupReplicationNotVoter, reason)
}

// TestMemberMayLeave checks which members leave their group when their tablet is not a voter: an
// active member that is not the group primary, when its group keeps a majority of its members
// without it. The group primary stays, also before its tablet became PRIMARY.
func TestMemberMayLeave(t *testing.T) {
	assert.True(t, memberMayLeave(nonVoterView(2)))
	assert.False(t, memberMayLeave(nonVoterView(1)), "the group primary stays")
	assert.False(t, memberMayLeave(groupStatus(testServerUUID(1), groupMember(testServerUUID(1), mysql.GroupMemberStateOffline, ""))), "not a member")
}
