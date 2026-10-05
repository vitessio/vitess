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

package logic

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TestUpdateGroupReplicationVotersRechecksBeforeWrite reproduces the TLA+ model's voters_split traces:
// VTOrc selected new voters on statuses that it read, and wrote them later. Meanwhile the voter that
// it dropped as failed came back, rejoined, was elected, acknowledged a transaction that the kept
// voters had not received, and failed again, or the group died: the bootstrap from the kept voters
// lost that transaction. Right before the write, VTOrc checks again, on statuses read then, that the
// group is still active with quorum, that no voter dropped as unreachable is back, and that a voter
// dropped for another reason runs no join and holds no transaction that the kept voters lack.
//
// Of three voters, one per cell, the one of zone3 failed; the other tablet of zone3 takes its seat,
// unless the second read of the statuses, right before the write, shows otherwise. A voter that is
// still unreachable is replaced (TestUpdateGroupReplicationVotersReplacesFailedVoterWithSpare).
func TestUpdateGroupReplicationVotersRechecksBeforeWrite(t *testing.T) {
	prevGrace := config.GetGroupReplicationVoterReplacementGracePeriod()
	t.Cleanup(func() {
		config.SetGroupReplicationVoterReplacementGracePeriod(prevGrace)
		inst.UnreachableGroupTablets.Reset()
	})
	errUnreachable := errors.New("unreachable")
	primary := recoveryTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	voter2 := recoveryTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	voter3 := recoveryTablet("zone3", 300, topodatapb.TabletType_REPLICA)
	spare3 := recoveryTablet("zone3", 301, topodatapb.TabletType_REPLICA)
	rdonly3 := recoveryTablet("zone3", 300, topodatapb.TabletType_RDONLY)
	online := func(tablet *topodatapb.Tablet) *replicationdatapb.FullStatus {
		return withPosition(groupMemberStatus(tablet, primary, primary, voter2), "1-10")
	}
	tests := []struct {
		name string
		// third is voter zone3-300's tablet: REPLICA, or RDONLY, which the policy keeps out of the group.
		third *topodatapb.Tablet
		// selected and recheck are the statuses of the tablets at the selection and right before
		// the write, by uid; a missing entry is unreachable.
		selected, recheck map[uint32]*replicationdatapb.FullStatus
		// seenAfterSelection makes VTOrc's discovery reach zone3-300 after the selection read.
		seenAfterSelection bool
		// instancesUnreadable makes VTOrc's backend fail its reads of instances at the re-check.
		instancesUnreadable bool
		wantVoters          []string
		wantErr             string
	}{{
		name:     "the group died before the write",
		third:    voter3,
		selected: map[uint32]*replicationdatapb.FullStatus{101: online(primary), 200: online(voter2), 301: notMemberStatus(spare3)},
		recheck: map[uint32]*replicationdatapb.FullStatus{
			101: withPosition(errorStatus(primary), "1-10"), 200: withPosition(errorStatus(voter2), "1-10"), 301: notMemberStatus(spare3),
		},
		wantErr: "no member of the shard's replication group is active with quorum",
	}, {
		// The members report a primary that no tablet that answers accounts for, and VTOrc never saw the
		// server_uuid of the dropped voter: it may be that primary.
		name:     "the group's primary may be the voter dropped as unreachable",
		third:    voter3,
		selected: map[uint32]*replicationdatapb.FullStatus{101: online(primary), 200: online(voter2), 301: notMemberStatus(spare3)},
		recheck: map[uint32]*replicationdatapb.FullStatus{
			101: withPrimaryUUID(online(primary), "00000000-0000-0000-0000-00000000dead"), 200: online(voter2), 301: notMemberStatus(spare3),
		},
		wantErr: "cannot tell whether voter zone3-0000000300",
	}, {
		name:     "the voter dropped as unreachable rejoined",
		third:    voter3,
		selected: map[uint32]*replicationdatapb.FullStatus{101: online(primary), 200: online(voter2), 301: notMemberStatus(spare3)},
		recheck: map[uint32]*replicationdatapb.FullStatus{
			101: online(primary), 200: online(voter2), 300: withPosition(groupMemberStatus(voter3, primary, primary, voter2, voter3), "1-11"), 301: notMemberStatus(spare3),
		},
		wantErr: "dropped as unreachable, answers again",
	}, {
		name:     "the voter dropped as unreachable is back, its join START in progress",
		third:    voter3,
		selected: map[uint32]*replicationdatapb.FullStatus{101: online(primary), 200: online(voter2), 301: notMemberStatus(spare3)},
		recheck: map[uint32]*replicationdatapb.FullStatus{
			101: online(primary), 200: online(voter2), 300: startInProgress(voter3), 301: notMemberStatus(spare3),
		},
		wantErr: "dropped as unreachable, answers again",
	}, {
		name:               "the voter dropped as unreachable was reached since the selection, and failed again",
		third:              voter3,
		selected:           map[uint32]*replicationdatapb.FullStatus{101: online(primary), 200: online(voter2), 301: notMemberStatus(spare3)},
		recheck:            map[uint32]*replicationdatapb.FullStatus{101: online(primary), 200: online(voter2), 301: notMemberStatus(spare3)},
		seenAfterSelection: true,
		wantErr:            "was reached",
	}, {
		// Whether VTOrc reached the voter since the selection cannot be told: the write is refused.
		name:                "VTOrc's backend fails to tell whether the voter dropped as unreachable was reached since",
		third:               voter3,
		selected:            map[uint32]*replicationdatapb.FullStatus{101: online(primary), 200: online(voter2), 301: notMemberStatus(spare3)},
		recheck:             map[uint32]*replicationdatapb.FullStatus{101: online(primary), 200: online(voter2), 301: notMemberStatus(spare3)},
		instancesUnreadable: true,
		wantErr:             "cannot tell whether voter zone3-0000000300",
	}, {
		name:     "a voter dropped as no longer eligible has a transaction that the kept voters lack",
		third:    rdonly3,
		selected: map[uint32]*replicationdatapb.FullStatus{101: online(primary), 200: online(voter2), 300: withPosition(notMemberStatus(rdonly3), "1-10"), 301: notMemberStatus(spare3)},
		recheck: map[uint32]*replicationdatapb.FullStatus{
			101: online(primary), 200: online(voter2), 300: withPosition(notMemberStatus(rdonly3), "1-11"), 301: notMemberStatus(spare3),
		},
		wantErr: "has transactions that the kept voters lack",
	}, {
		name:     "a voter dropped as no longer eligible runs a START",
		third:    rdonly3,
		selected: map[uint32]*replicationdatapb.FullStatus{101: online(primary), 200: online(voter2), 300: withPosition(notMemberStatus(rdonly3), "1-10"), 301: notMemberStatus(spare3)},
		recheck: map[uint32]*replicationdatapb.FullStatus{
			101: online(primary), 200: online(voter2), 300: withPosition(startInProgress(rdonly3), "1-10"), 301: notMemberStatus(spare3),
		},
		wantErr: "runs a START GROUP_REPLICATION",
	}, {
		name:       "a voter dropped as no longer eligible, whose transactions the kept voters hold, is replaced",
		third:      rdonly3,
		selected:   map[uint32]*replicationdatapb.FullStatus{101: online(primary), 200: online(voter2), 300: withPosition(notMemberStatus(rdonly3), "1-10"), 301: notMemberStatus(spare3)},
		recheck:    map[uint32]*replicationdatapb.FullStatus{101: online(primary), 200: online(voter2), 300: withPosition(notMemberStatus(rdonly3), "1-10"), 301: notMemberStatus(spare3)},
		wantVoters: []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000301"},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inst.UnreachableGroupTablets.Reset()
			config.SetGroupReplicationVoterReplacementGracePeriod(0)
			mockTMC := groupReplicationRecoveryTestWithPolicy(t, policy.DurabilityGroupReplicationCrossCell, primary, voter2, tt.third, spare3)
			setVoters(t, primary, voter2, voter3)
			setIncarnation(t, "")
			if tt.seenAfterSelection {
				// The selection read the statuses 3s ago, as VTOrc's clock tells.
				prevNow := voterSelectionNow
				voterSelectionNow = func() time.Time { return time.Now().Add(-3 * time.Second) }
				t.Cleanup(func() { voterSelectionNow = prevNow })
			}
			if tt.instancesUnreadable {
				t.Cleanup(db.ClearVTOrcDatabase)
			}
			for _, tablet := range []*topodatapb.Tablet{primary, voter2, tt.third, spare3} {
				calls := 0
				mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(tablet)).DoAndReturn(
					func(context.Context, *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
						calls++
						statuses := tt.recheck
						if calls == 1 {
							statuses = tt.selected
							if tt.seenAfterSelection && tablet.Alias.Uid == 301 {
								// VTOrc's discovery reaches zone3-300 while the recovery runs.
								assert.NoError(t, inst.WriteInstance(&inst.Instance{InstanceAlias: voter3.Alias, Hostname: voter3.MysqlHostname, Port: int(voter3.MysqlPort)}, true, nil))
							}
						} else if tt.instancesUnreadable && tablet.Alias.Uid == 301 {
							_, err := db.ExecVTOrc("DROP TABLE database_instance")
							assert.NoError(t, err)
						}
						if status, ok := statuses[tablet.Alias.Uid]; ok {
							return status, nil
						}
						return nil, errUnreachable
					}).AnyTimes()
			}
			mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(spare3), gomock.Any()).Return(&replicationdatapb.GroupReplicationStatus{}, nil).AnyTimes()

			_, _, err := updateGroupReplicationVoters(t.Context(), &inst.DetectionAnalysis{
				Analysis:              inst.GroupVotersOutOfDate,
				AnalyzedInstanceAlias: primary.Alias,
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
			}, log.NewPrefixedLogger("test"))
			if tt.wantErr == "" {
				require.NoError(t, err)
				assert.Equal(t, tt.wantVoters, readVoters(t))
				return
			}
			require.Error(t, err)
			assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
			require.ErrorContains(t, err, tt.wantErr)
			assert.Equal(t, []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000300"}, readVoters(t), "the voters must stay as they are")
		})
	}
}

// errorStatus is the status of a member that left its group in the ERROR state.
func errorStatus(tablet *topodatapb.Tablet) *replicationdatapb.FullStatus {
	status := notMemberStatus(tablet)
	status.GroupReplicationStatus.MemberState = mysql.GroupMemberStateError
	return status
}

// startInProgress is the status of a member whose START GROUP_REPLICATION runs.
func startInProgress(tablet *topodatapb.Tablet) *replicationdatapb.FullStatus {
	status := notMemberStatus(tablet)
	status.GroupReplicationStatus.StartInProgress = true
	return withPosition(status, "1-10")
}

// TestUpdateGroupReplicationVotersKeepsPrimaryElectedSinceSelection reproduces the TLA+ model's
// voters_split_nonvoter trace: the selection drops a voter of a cell whose other tablet, not a voter,
// is the group primary (the group primary keeps its seat), and the group elects the dropped voter
// before the write. A list that drops the current primary would leave it serving on the decision it took
// as a voter: the write is refused, and the next selection keeps its seat.
func TestUpdateGroupReplicationVotersKeepsPrimaryElectedSinceSelection(t *testing.T) {
	primary := recoveryTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	replica := recoveryTablet("zone1", 100, topodatapb.TabletType_REPLICA)
	crossCellVoter := recoveryTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	mockTMC := groupReplicationRecoveryTestWithPolicy(t, policy.DurabilityGroupReplicationCrossCell, primary, replica, crossCellVoter)
	setVoters(t, primary, crossCellVoter)
	for _, tablet := range []*topodatapb.Tablet{primary, replica, crossCellVoter} {
		calls := 0
		mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(tablet)).DoAndReturn(
			func(context.Context, *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
				calls++
				// At the selection, the group primary is the replica, which is not a voter; right
				// before the write, the group elected zone1-101 again.
				groupPrimary := replica
				if calls > 1 {
					groupPrimary = primary
				}
				return withPosition(groupMemberStatus(tablet, groupPrimary, primary, replica, crossCellVoter), "1-10"), nil
			}).AnyTimes()
	}
	mockTMC.EXPECT().StopGroupReplication(gomock.Any(), gomock.Any()).Times(0)

	_, _, err := updateGroupReplicationVoters(t.Context(), &inst.DetectionAnalysis{
		Analysis:              inst.GroupVotersOutOfDate,
		AnalyzedInstanceAlias: primary.Alias,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}, log.NewPrefixedLogger("test"))
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	require.ErrorContains(t, err, "is the primary of the shard's replication group now")
	assert.Equal(t, []string{"zone1-0000000101", "zone2-0000000200"}, readVoters(t))
}

// withPrimaryUUID sets the group primary that a member reports.
func withPrimaryUUID(status *replicationdatapb.FullStatus, uuid string) *replicationdatapb.FullStatus {
	status.GroupReplicationStatus.PrimaryUuid = uuid
	return status
}

// TestUpdateGroupReplicationVotersRecheckIsFreshAtWrite checks that the statuses on which the re-check
// right before the voter write decides are fresh when the write happens: the re-check does not wait
// for a tablet that does not answer, typically the voter that the selection dropped as unreachable,
// longer than groupVoterRecheckTimeout. Waiting for it up to the RPC's timeout (15s) left a window as
// long between the decisive reads and the write, in which the trace of voters_split fits.
func TestUpdateGroupReplicationVotersRecheckIsFreshAtWrite(t *testing.T) {
	prevGrace := config.GetGroupReplicationVoterReplacementGracePeriod()
	t.Cleanup(func() {
		config.SetGroupReplicationVoterReplacementGracePeriod(prevGrace)
		inst.UnreachableGroupTablets.Reset()
	})
	inst.UnreachableGroupTablets.Reset()
	config.SetGroupReplicationVoterReplacementGracePeriod(0)
	primary := recoveryTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	voter2 := recoveryTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	voter3 := recoveryTablet("zone3", 300, topodatapb.TabletType_REPLICA)
	spare3 := recoveryTablet("zone3", 301, topodatapb.TabletType_REPLICA)
	mockTMC := groupReplicationRecoveryTestWithPolicy(t, policy.DurabilityGroupReplicationCrossCell, primary, voter2, voter3, spare3)
	setVoters(t, primary, voter2, voter3)
	setIncarnation(t, "")
	var mu sync.Mutex
	var lastRead time.Time
	answer := func(status *replicationdatapb.FullStatus) func(context.Context, *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
		return func(context.Context, *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
			mu.Lock()
			defer mu.Unlock()
			lastRead = time.Now()
			return status, nil
		}
	}
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(primary)).DoAndReturn(answer(withPosition(groupMemberStatus(primary, primary, primary, voter2), "1-10"))).AnyTimes()
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(voter2)).DoAndReturn(answer(withPosition(groupMemberStatus(voter2, primary, primary, voter2), "1-10"))).AnyTimes()
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(spare3)).DoAndReturn(answer(notMemberStatus(spare3))).AnyTimes()
	// The dropped voter fails right away at the selection, and then does not answer at all.
	voter3Calls := 0
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(voter3)).DoAndReturn(
		func(ctx context.Context, _ *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
			mu.Lock()
			voter3Calls++
			first := voter3Calls == 1
			mu.Unlock()
			if first {
				return nil, errors.New("connection refused")
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}).AnyTimes()
	mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(spare3), gomock.Any()).Return(&replicationdatapb.GroupReplicationStatus{}, nil).AnyTimes()

	_, _, err := updateGroupReplicationVoters(t.Context(), &inst.DetectionAnalysis{
		Analysis:              inst.GroupVotersOutOfDate,
		AnalyzedInstanceAlias: primary.Alias,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}, log.NewPrefixedLogger("test"))
	require.NoError(t, err)
	assert.Equal(t, []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000301"}, readVoters(t))
	mu.Lock()
	age := time.Since(lastRead)
	mu.Unlock()
	assert.Less(t, age, groupVoterRecheckTimeout+time.Second, "the statuses of the re-check must be fresh at the write")
}

// TestUpdateGroupReplicationVotersRechecksVoterWithoutTabletRecord checks that the re-check before a
// voter write also covers a dropped voter whose tablet record is gone: the tablet list that the
// recovery reads skips it, and VTOrc no longer knows its server_uuid, but its MySQL may still run. Here
// the group's members report a member ONLINE again right before the write that no tablet of the shard
// accounts for: it may be that voter, and the write is refused.
func TestUpdateGroupReplicationVotersRechecksVoterWithoutTabletRecord(t *testing.T) {
	prevGrace := config.GetGroupReplicationVoterReplacementGracePeriod()
	t.Cleanup(func() {
		config.SetGroupReplicationVoterReplacementGracePeriod(prevGrace)
		inst.UnreachableGroupTablets.Reset()
	})
	inst.UnreachableGroupTablets.Reset()
	config.SetGroupReplicationVoterReplacementGracePeriod(0)
	primary := recoveryTablet("zone1", 101, topodatapb.TabletType_PRIMARY)
	voter2 := recoveryTablet("zone2", 200, topodatapb.TabletType_REPLICA)
	voter3 := recoveryTablet("zone3", 300, topodatapb.TabletType_REPLICA)
	spare3 := recoveryTablet("zone3", 301, topodatapb.TabletType_REPLICA)
	// zone3-300 has no tablet record.
	mockTMC := groupReplicationRecoveryTestWithPolicy(t, policy.DurabilityGroupReplicationCrossCell, primary, voter2, spare3)
	setVoters(t, primary, voter2, voter3)
	setIncarnation(t, "")
	for _, tablet := range []*topodatapb.Tablet{primary, voter2} {
		calls := 0
		mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(tablet)).DoAndReturn(
			func(context.Context, *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
				calls++
				if calls == 1 {
					return withPosition(groupMemberStatus(tablet, primary, primary, voter2), "1-10"), nil
				}
				return withPosition(groupMemberStatus(tablet, primary, primary, voter2, voter3), "1-10"), nil
			}).AnyTimes()
	}
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(spare3)).Return(notMemberStatus(spare3), nil).AnyTimes()
	mockTMC.EXPECT().StartGroupReplication(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	_, _, err := updateGroupReplicationVoters(t.Context(), &inst.DetectionAnalysis{
		Analysis:              inst.GroupVotersOutOfDate,
		AnalyzedInstanceAlias: primary.Alias,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}, log.NewPrefixedLogger("test"))
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	require.ErrorContains(t, err, "zone3-0000000300")
	assert.Equal(t, []string{"zone1-0000000101", "zone2-0000000200", "zone3-0000000300"}, readVoters(t))
}
