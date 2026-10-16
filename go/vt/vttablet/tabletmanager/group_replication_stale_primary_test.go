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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// staleView returns the status of tablet 1's MySQL as the stale primary of a view of the recorded
// incarnation that holds the three voters ONLINE: the view it saw before it was paused.
func staleView() *replicationdatapb.GroupReplicationStatus {
	return withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(3), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), "1780000001:5")
}

// recordShardPrimary makes cell1-<uid> the shard record's primary, with a primary term that started now.
func recordShardPrimary(t *testing.T, ts *topo.Server, uid uint32) {
	t.Helper()
	_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = &topodatapb.TabletAlias{Cell: "cell1", Uid: uid}
		si.PrimaryTermStartTime = protoutil.TimeToProto(time.Now())
		return nil
	})
	require.NoError(t, err)
}

// peerPrimary returns the FullStatus of cell1-<uid> as the ONLINE primary, with quorum, of a view of
// the recorded incarnation with the given id, that also holds voter 3.
func peerPrimary(uid int, viewID string) *replicationdatapb.FullStatus {
	return &replicationdatapb.FullStatus{
		ServerUuid: testServerUUID(uid),
		GroupReplicationStatus: withViewID(groupStatus(testServerUUID(uid),
			groupMember(testServerUUID(uid), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
			groupMember(testServerUUID(3), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), viewID),
	}
}

// peerSecondary returns the FullStatus of cell1-<uid> as an ONLINE secondary, with quorum, of a view of
// the recorded incarnation with the given id, whose primary is cell1-<primary>.
func peerSecondary(uid, primary int, viewID string) *replicationdatapb.FullStatus {
	return &replicationdatapb.FullStatus{
		ServerUuid: testServerUUID(uid),
		GroupReplicationStatus: withViewID(groupStatus(testServerUUID(uid),
			groupMember(testServerUUID(primary), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
			groupMember(testServerUUID(uid), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), viewID),
	}
}

// TestGroupReplicationSyncDoesNotPromoteDeposedPrimary reproduces FLAG 1 of the soak: a primary that
// was paused (SIGSTOP) while the group expelled it and elected another member resumes with its stale
// view, in which it is still the ONLINE primary, with quorum, of the three voters, in the recorded
// incarnation, until MySQL learns of its expulsion. Its tablet, which stepped down when the shard
// record named the new primary, promoted itself again on that view and wrote a newer primary term to
// the shard record, and the legitimate primary's tablet then stepped down: the two flipped the shard
// record for up to about 3s. The tablet now asks every other tablet of the shard first: one that
// reports an active member of the same incarnation, with quorum, whose view has another ONLINE
// primary, keeps it as it is (the TLA+ model's PROMOTE_CHECK_ALL).
func TestGroupReplicationSyncDoesNotPromoteDeposedPrimary(t *testing.T) {
	for _, tt := range []struct {
		name string
		// recorded is the shard record's primary.
		recorded uint32
		// peers are what cell1-2 and cell1-3 report; nil means that the tablet does not answer.
		peer2, peer3 *replicationdatapb.FullStatus
		wantPrimary  bool
	}{{
		name:     "the recorded primary answers as the primary of the group: the tablet stays REPLICA",
		recorded: 2,
		peer2:    peerPrimary(2, "1780000001:6"),
		peer3:    peerSecondary(3, 2, "1780000001:6"),
	}, {
		// The narrow check that asked only the recorded primary failed here (flag1_fixed_faults): the
		// shard record names cell1-3, a REPLICA, and cell1-2 is the primary of the newer view.
		name:     "the recorded primary is a replica, and another tablet is the primary of the group: the tablet stays REPLICA",
		recorded: 3,
		peer2:    peerPrimary(2, "1780000001:6"),
		peer3:    &replicationdatapb.FullStatus{ServerUuid: testServerUUID(3), GroupReplicationStatus: withViewID(groupStatus(testServerUUID(3)), "")},
	}, {
		name:        "no other tablet answers: the tablet is promoted, as in a failover",
		recorded:    2,
		wantPrimary: true,
	}, {
		name:     "the other tablets answer, but none is an active member: the tablet is promoted",
		recorded: 2,
		peer2: &replicationdatapb.FullStatus{ServerUuid: testServerUUID(2), GroupReplicationStatus: withViewID(groupStatus(testServerUUID(2),
			groupMember(testServerUUID(2), mysql.GroupMemberStateError, "")), "1780000001:4")},
		wantPrimary: true,
	}, {
		name:        "the other tablets report this member as their primary: the tablet is promoted",
		recorded:    2,
		peer2:       peerSecondary(2, 1, "1780000001:5"),
		peer3:       peerSecondary(3, 1, "1780000001:5"),
		wantPrimary: true,
	}} {
		t.Run(tt.name, func(t *testing.T) {
			withGroupReplication(t)
			tm, fmd, peers, ts := newLegitimacyTestTM(t)
			recordShardPrimary(t, ts, tt.recorded)
			// The tablet learned the voters' server_uuids before, as its sync loop does: legitimacy does
			// not depend on whether they answer now.
			tm.groupReplicationPeers.setServerUUID("cell1-0000000002", testServerUUID(2))
			tm.groupReplicationPeers.setServerUUID("cell1-0000000003", testServerUUID(3))
			peers.mu.Lock()
			delete(peers.statuses, "cell1-0000000002")
			delete(peers.statuses, "cell1-0000000003")
			peers.mu.Unlock()
			if tt.peer2 != nil {
				peers.set(2, tt.peer2)
			}
			if tt.peer3 != nil {
				peers.set(3, tt.peer3)
			}
			fmd.SetGroupReplicationStatus(staleView())
			fmd.SuperReadOnly.Store(true)

			newGroupReplicationSync(tm).reconcile(t.Context())

			if tt.wantPrimary {
				assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
				return
			}
			assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type, "a deposed primary must not promote its tablet on its stale view")
			assert.True(t, fmd.SuperReadOnly.Load(), "MySQL must stay super_read_only")
			si, err := ts.GetShard(t.Context(), "ks", "0")
			require.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("cell1-%010d", tt.recorded), topoproto.TabletAliasString(si.PrimaryAlias), "the shard record is not changed")
		})
	}
}

// TestGroupReplicationSyncLegitimatePrimaryWaitsForDeposedMember checks the price of the check: while
// a deposed member still reports itself the primary of its stale view, with quorum, the legitimate
// primary's tablet is not promoted either. It is once the deposed member learned of its expulsion.
func TestGroupReplicationSyncLegitimatePrimaryWaitsForDeposedMember(t *testing.T) {
	withGroupReplication(t)
	tm, fmd, peers, ts := newLegitimacyTestTM(t)
	recordShardPrimary(t, ts, 2)
	// Tablet 1's MySQL is the legitimate primary of the newer view, with voter 3; cell1-2's MySQL,
	// the old primary, still reports its stale view of the three voters.
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(3), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), "1780000001:6"))
	peers.set(2, &replicationdatapb.FullStatus{ServerUuid: testServerUUID(2), GroupReplicationStatus: withViewID(groupStatus(testServerUUID(2),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(3), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), "1780000001:5")})
	peers.set(3, peerSecondary(3, 1, "1780000001:6"))
	s := newGroupReplicationSync(tm)

	s.reconcile(t.Context())
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type, "the legitimate primary waits while the deposed member claims its stale view")

	// The deposed member learned of its expulsion.
	peers.set(2, &replicationdatapb.FullStatus{ServerUuid: testServerUUID(2), GroupReplicationStatus: withViewID(groupStatus(testServerUUID(2),
		groupMember(testServerUUID(2), mysql.GroupMemberStateError, "")), "")})
	s.reconcile(t.Context())
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
}

// TestGroupReplicationSyncPromotionWaitsBoundedForFrozenTablet checks that a tablet that does not answer,
// such as the frozen old primary of a failover, delays the promotion of the new primary by at most
// groupReplicationPeerTimeout, and does not prevent it.
func TestGroupReplicationSyncPromotionWaitsBoundedForFrozenTablet(t *testing.T) {
	withGroupReplication(t)
	oldPeerTimeout := groupReplicationPeerTimeout
	groupReplicationPeerTimeout = time.Second
	t.Cleanup(func() { groupReplicationPeerTimeout = oldPeerTimeout })
	tm, fmd, peers, ts := newLegitimacyTestTM(t)
	recordShardPrimary(t, ts, 2)
	peers.mu.Lock()
	peers.frozen = map[string]bool{"cell1-0000000002": true}
	peers.mu.Unlock()
	peers.set(3, peerSecondary(3, 1, "1780000001:6"))
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(3), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), "1780000001:6"))

	start := time.Now()
	newGroupReplicationSync(tm).reconcile(t.Context())
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	assert.Less(t, time.Since(start), groupReplicationPeerTimeout+5*time.Second, "the promotion waits for the frozen tablet at most groupReplicationPeerTimeout")
}

// TestGroupReplicationSyncDeposedPrimaryDoesNotServeAgain checks the same for a PRIMARY tablet that
// stopped serving while its MySQL was paused: it does not serve again on its stale view while the
// shard record's newer primary answers as the primary of a newer view.
func TestGroupReplicationSyncDeposedPrimaryDoesNotServeAgain(t *testing.T) {
	withGroupReplication(t)
	tm, fmd, peers, ts := newLegitimacyTestTM(t)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	// The primary lost the majority of its voters: it does not serve, and MySQL is fenced.
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)), "1780000001:4"))
	s := newGroupReplicationSync(tm)
	s.reconcile(t.Context())
	require.False(t, qsc.IsServing())
	fmd.SuperReadOnly.Store(true)

	recordShardPrimary(t, ts, 2)
	peers.set(2, peerPrimary(2, "1780000001:6"))
	fmd.SetGroupReplicationStatus(staleView())
	s.recordRead = time.Time{}
	s.reconcile(t.Context())
	// The shard watch may have made the tablet a REPLICA meanwhile (endPrimaryTerm), which serves
	// reads; it must not serve as the primary.
	assert.False(t, qsc.IsServing() && tm.Tablet().Type == topodatapb.TabletType_PRIMARY, "a deposed primary must not serve again on its stale view")
	assert.True(t, fmd.SuperReadOnly.Load(), "MySQL must stay super_read_only")
}
