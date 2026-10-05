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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtorc/inst"
	tmcmock "vitess.io/vitess/go/vt/vttablet/tmclient/mock"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TestAdoptUnrecordedGroup checks how VTOrc records the incarnation of a group that runs while the
// shard record lists none and holds no bootstrap intent for its primary: PlannedReparentShard's
// initial promotion bootstrapped it with InitPrimary and failed to record it, or VTOrc's bootstrap
// reply was lost and its intent was replaced. The voters do not join a group whose incarnation is not
// recorded (the TLA+ model's init_orc_lost), so without the record the shard never gets the
// majority of its voters back. VTOrc records it only when it is the only group the shard can have:
// every voter answers, no other tablet is active in another group or runs a START, and its primary
// executed every transaction that a voter executed or received, as a bootstrap on that member would
// require. Otherwise it records nothing.
func TestAdoptUnrecordedGroup(t *testing.T) {
	const incarnation = "17908000000000001"
	const groupName = "6f1c2c2e-5a8e-4b8e-9d3a-7c1f0b6e2a41"
	// primaryOf makes tablet zone2-101, whose MySQL executed the most transactions, the primary of
	// a group of one.
	primaryOf := func(tablet *topodatapb.Tablet, status *replicationdatapb.FullStatus) {
		if tablet.Alias.Uid != 101 {
			return
		}
		group := newGroupStatus(tablet, incarnation)
		status.GroupReplicationStatus = group.GroupReplicationStatus
	}
	tests := []struct {
		name string
		edit func(tablet *topodatapb.Tablet, status *replicationdatapb.FullStatus)
		// unreachable is the voter whose tablet does not answer, if any.
		unreachable uint32
		// liveIntent records a live bootstrap intent for another tablet.
		liveIntent bool
		// noRecord is the voter whose tablet record is deleted, if any.
		noRecord   uint32
		wantRecord bool
	}{{
		name:       "the only group, whose primary holds every voter's transactions, is recorded",
		edit:       primaryOf,
		wantRecord: true,
	}, {
		name: "another voter is active in a group of another incarnation",
		edit: func(tablet *topodatapb.Tablet, status *replicationdatapb.FullStatus) {
			primaryOf(tablet, status)
			if tablet.Alias.Uid == 102 {
				status.GroupReplicationStatus = newGroupStatus(tablet, "17908000000000002").GroupReplicationStatus
			}
		},
	}, {
		name: "another voter runs a START",
		edit: func(tablet *topodatapb.Tablet, status *replicationdatapb.FullStatus) {
			primaryOf(tablet, status)
			if tablet.Alias.Uid == 100 {
				status.GroupReplicationStatus.StartInProgress = true
			}
		},
	}, {
		name: "another voter received a transaction that the primary lacks",
		edit: func(tablet *topodatapb.Tablet, status *replicationdatapb.FullStatus) {
			primaryOf(tablet, status)
			if tablet.Alias.Uid == 100 {
				status.GroupReplicationStatus.ReceivedTransactionSet = groupName + ":13"
			}
		},
	}, {
		name:        "a voter does not answer",
		edit:        primaryOf,
		unreachable: 102,
	}, {
		name:       "a bootstrap intent for another tablet is live",
		edit:       primaryOf,
		liveIntent: true,
	}, {
		name:     "a voter has no tablet record",
		edit:     primaryOf,
		noRecord: 102,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			edit := tt.edit
			if tt.unreachable != 0 {
				edit = func(tablet *topodatapb.Tablet, status *replicationdatapb.FullStatus) {
					tt.edit(tablet, status)
					if tablet.Alias.Uid == tt.unreachable {
						status.GroupReplicationStatus = nil
						status.PrimaryStatus = nil
					}
				}
			}
			mockTMC, tablets := bootstrapIntentTestWithErr(t, "", edit, tt.unreachable)
			target := tablets[1]
			if tt.noRecord != 0 {
				require.NoError(t, ts.DeleteTablet(t.Context(), &topodatapb.TabletAlias{Cell: "zone3", Uid: tt.noRecord}))
			}
			if tt.liveIntent {
				_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error {
					si.GroupReplicationBootstrapIntent = &topodatapb.GroupReplicationBootstrapIntent{
						Target: tablets[0].Alias, Time: protoutil.TimeToProto(time.Now().Add(-10 * time.Second)), Token: "other-vtorc",
					}
					return nil
				})
				require.NoError(t, err)
			}
			var joined func() int32
			if tt.wantRecord {
				joins := expectJoins(mockTMC, tablets[0], tablets[2])
				joined = func() int32 { return joins.Load() }
			} else {
				mockTMC.EXPECT().StartGroupReplication(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			}

			attempted, topologyRecovery, err := runLocked(t, adoptGroupReplicationBootstrap, inst.GroupBootstrapNotRecorded, target)
			si, rerr := ts.GetShard(t.Context(), "ks", "0")
			require.NoError(t, rerr)
			if !tt.wantRecord {
				require.Error(t, err)
				assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err), "%v", err)
				assert.True(t, attempted)
				assert.Empty(t, si.GroupReplicationIncarnation, "nothing is recorded")
				return
			}
			require.NoError(t, err)
			require.True(t, attempted)
			assert.True(t, topologyRecovery.IsSuccessful)
			assert.Equal(t, incarnation, si.GroupReplicationIncarnation)
			assert.Eventually(t, func() bool { return joined() == 2 }, 30*time.Second, 10*time.Millisecond)
		})
	}
}

// bootstrapIntentTestWithErr is bootstrapIntentTestWith, with the FullStatus of every tablet read any
// number of times, as edit changes it, and an error for the tablet whose uid is unreachable.
func bootstrapIntentTestWithErr(t *testing.T, recorded string, edit func(*topodatapb.Tablet, *replicationdatapb.FullStatus), unreachable uint32) (*tmcmock.MockTabletManagerClient, []*topodatapb.Tablet) {
	t.Helper()
	if unreachable == 0 {
		return bootstrapIntentTestWith(t, recorded, 0, edit)
	}
	tablets := []*topodatapb.Tablet{
		recoveryTablet("zone1", 100, topodatapb.TabletType_REPLICA),
		recoveryTablet("zone2", 101, topodatapb.TabletType_REPLICA),
		recoveryTablet("zone3", 102, topodatapb.TabletType_REPLICA),
	}
	mockTMC := groupReplicationRecoveryTest(t, tablets...)
	setVoters(t, tablets...)
	setIncarnation(t, recorded)
	for i, last := range []string{"10", "12", "11"} {
		if tablets[i].Alias.Uid == unreachable {
			mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(tablets[i])).Return(nil, errors.New("unreachable")).AnyTimes()
			continue
		}
		status := notMemberStatus(tablets[i])
		status.PrimaryStatus = &replicationdatapb.PrimaryStatus{Position: "MySQL56/6f1c2c2e-5a8e-4b8e-9d3a-7c1f0b6e2a41:1-" + last}
		edit(tablets[i], status)
		mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(tablets[i])).Return(status, nil).AnyTimes()
	}
	return mockTMC, tablets
}
