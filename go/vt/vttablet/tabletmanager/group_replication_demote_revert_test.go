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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestDemotePrimaryRevertKeepsServingInvariant reproduces the TLA+ model's prs_demote_fail trace
// (doc/design-docs/group_replication_tla): the group's view shrank to the primary (the other two voters
// were expelled) while it still served, as in the window before the fence check fences it, and PRS's
// DemotePrimary then fails after it set super_read_only (here, the read of the primary status fails).
// The revert must not make MySQL writable again, nor the tablet serve, without a decision on the serving
// invariant: the primary's view holds one of three voters. Before, the revert redid the prepared
// transactions with super_read_only OFF, and served again, because no not-serving reason was set yet.
func TestDemotePrimaryRevertKeepsServingInvariant(t *testing.T) {
	withGroupReplication(t)
	tm, fmd, _, _ := newLegitimacyTestTM(t)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	require.True(t, qsc.IsServing())
	fmd.SuperReadOnly.Store(false)
	fmd.ReadOnly = false

	// The primary of the recorded incarnation, alone in its view: a single voter of three.
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)), "1780000001:9"))
	fmd.PrimaryStatusError = errors.New("lost connection to MySQL server during query")

	_, err := tm.DemotePrimary(t.Context(), false)
	require.Error(t, err)
	assert.True(t, fmd.SuperReadOnly.Load(), "the revert must not make MySQL writable for a view without the voter majority")
	assert.False(t, qsc.IsServing(), "the revert must not make the tablet serve for a view without the voter majority")
}

// TestDemotePrimaryRevertServesWithVoterMajority checks that the revert of a failed DemotePrimary
// still makes MySQL writable, redoing the prepared transactions, and the tablet serve, when the
// decision on the serving invariant allows it: MySQL is the primary of the recorded incarnation, with
// the three voters ONLINE in its view.
func TestDemotePrimaryRevertServesWithVoterMajority(t *testing.T) {
	withGroupReplication(t)
	tm, fmd, _, _ := newLegitimacyTestTM(t)
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(3), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), "1780000001:9"))
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	require.True(t, qsc.IsServing())
	fmd.SuperReadOnly.Store(false)
	fmd.ReadOnly = false
	fmd.PrimaryStatusError = errors.New("lost connection to MySQL server during query")

	_, err := tm.DemotePrimary(t.Context(), false)
	require.Error(t, err)
	assert.False(t, fmd.SuperReadOnly.Load(), "the revert must make MySQL writable again")
	assert.True(t, qsc.IsServing(), "the revert must make the tablet serve again")
	assert.True(t, qsc.MethodCalled["RedoPreparedTransactions"], "the revert must redo the prepared transactions")
	reason, _ := tm.tmState.GroupReplicationNotServingState()
	assert.Empty(t, reason)
}

// TestDemotePrimaryRevertRefusedServesAgainLater checks that a tablet whose failed DemotePrimary was
// not reverted, because its view lacked the voter majority, stays PRIMARY, read-only and not serving
// with the reason recorded, and that the sync loop makes it serve once the view holds the majority
// again, making MySQL writable and redoing the prepared transactions then.
func TestDemotePrimaryRevertRefusedServesAgainLater(t *testing.T) {
	withGroupReplication(t)
	tm, fmd, _, _ := newLegitimacyTestTM(t)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	fmd.SuperReadOnly.Store(false)
	fmd.ReadOnly = false
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)), "1780000001:9"))
	fmd.PrimaryStatusError = errors.New("lost connection to MySQL server during query")

	_, err := tm.DemotePrimary(t.Context(), false)
	require.Error(t, err)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	assert.True(t, fmd.SuperReadOnly.Load())
	assert.False(t, qsc.IsServing())
	assert.False(t, qsc.MethodCalled["RedoPreparedTransactions"], "the prepared transactions are redone once MySQL is writable")
	reason, _ := tm.tmState.GroupReplicationNotServingState()
	assert.Equal(t, groupReplicationVoterMajorityLost, reason)

	// The two other voters are back in the view: the sync loop decides that the tablet serves.
	fmd.PrimaryStatusError = nil
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
		groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
		groupMember(testServerUUID(3), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), "1780000001:10"))
	newGroupReplicationSync(tm).reconcile(t.Context())
	assert.False(t, fmd.SuperReadOnly.Load())
	assert.True(t, qsc.IsServing())
	assert.True(t, qsc.MethodCalled["RedoPreparedTransactions"])
}

// TestDemotePrimaryRevertWithoutGroupReplication checks that the revert of a failed DemotePrimary is the
// one of a semi-sync shard, without any decision on the serving invariant, on a tablet that does not
// support Group Replication, and on one that does in a shard whose durability policy does not use it:
// the tablet serves again, and MySQL is writable again, with the prepared transactions redone, if the
// demotion made it read-only.
func TestDemotePrimaryRevertWithoutGroupReplication(t *testing.T) {
	tests := []struct {
		name string
		// newTM returns the tablet manager of a serving PRIMARY with a writable MySQL.
		newTM func(t *testing.T) (*TabletManager, *mysqlctl.FakeMysqlDaemon)
		// fail makes the demotion fail; superReadOnly is whether it sets super_read_only first.
		fail          func(fmd *mysqlctl.FakeMysqlDaemon)
		superReadOnly bool
		// cancelAtReadOnly ends the demotion's context when it sets super_read_only, as when the
		// caller gives up: the revert still reads the policy to choose its path.
		cancelAtReadOnly bool
	}{{
		name: "a tablet without Group Replication, the demotion fails after super_read_only",
		newTM: func(t *testing.T) (*TabletManager, *mysqlctl.FakeMysqlDaemon) {
			ts := memorytopo.NewServer(t.Context(), "cell1")
			t.Cleanup(ts.Close)
			tm := newTestTM(t, ts, 1, "ks", "0", nil)
			t.Cleanup(tm.Stop)
			return tm, tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
		},
		fail: func(fmd *mysqlctl.FakeMysqlDaemon) {
			fmd.PrimaryStatusError = errors.New("lost connection to MySQL server during query")
		},
		superReadOnly: true,
	}, {
		name: "a tablet with Group Replication in a semi-sync shard, the demotion fails after super_read_only",
		newTM: func(t *testing.T) (*TabletManager, *mysqlctl.FakeMysqlDaemon) {
			withGroupReplication(t)
			return newGroupReplicationTestTM(t, newGroupReplicationTopo(t, policy.DurabilitySemiSync), 1, nil)
		},
		fail: func(fmd *mysqlctl.FakeMysqlDaemon) {
			fmd.PrimaryStatusError = errors.New("lost connection to MySQL server during query")
		},
		superReadOnly: true,
	}, {
		// The decision on the serving invariant cannot read MySQL's group replication status either:
		// under a group replication policy, the tablet would not serve.
		name: "a tablet with Group Replication in a semi-sync shard, the demotion fails before super_read_only",
		newTM: func(t *testing.T) (*TabletManager, *mysqlctl.FakeMysqlDaemon) {
			withGroupReplication(t)
			return newGroupReplicationTestTM(t, newGroupReplicationTopo(t, policy.DurabilitySemiSync), 1, nil)
		},
		fail: func(fmd *mysqlctl.FakeMysqlDaemon) {
			fmd.GroupReplicationError = errors.New("lost connection to MySQL server during query")
		},
	}, {
		name: "a tablet with Group Replication in a semi-sync shard, the demotion's caller gives up after super_read_only",
		newTM: func(t *testing.T) (*TabletManager, *mysqlctl.FakeMysqlDaemon) {
			withGroupReplication(t)
			return newGroupReplicationTestTM(t, newGroupReplicationTopo(t, policy.DurabilitySemiSync), 1, nil)
		},
		fail:             func(fmd *mysqlctl.FakeMysqlDaemon) { fmd.PrimaryStatusError = errors.New("context canceled") },
		superReadOnly:    true,
		cancelAtReadOnly: true,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm, fmd := tt.newTM(t)
			setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
			qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
			require.True(t, qsc.IsServing())
			fmd.SuperReadOnly.Store(false)
			fmd.ReadOnly = false
			tt.fail(fmd)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tt.cancelAtReadOnly {
				fmd.SetSuperReadOnlyHook = func(on bool) {
					if on {
						cancel()
					}
				}
			}

			_, err := tm.DemotePrimary(ctx, false)
			require.Error(t, err)
			assert.False(t, fmd.SuperReadOnly.Load())
			assert.False(t, fmd.ReadOnly)
			assert.True(t, qsc.IsServing())
			assert.Equal(t, tt.superReadOnly, qsc.MethodCalled["RedoPreparedTransactions"])
		})
	}
}
