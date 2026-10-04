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
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtorc/inst"
	tmcmock "vitess.io/vitess/go/vt/vttablet/tmclient/mock"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// bootstrapIntentTest sets up shard ks/0 with three voters, 100, 101 and 102, whose MySQL left the
// group of incarnation recorded; 101 has the most transactions. The FullStatus of every tablet is
// read once, for the choice of the bootstrap candidate.
func bootstrapIntentTest(t *testing.T, recorded string) (*tmcmock.MockTabletManagerClient, []*topodatapb.Tablet) {
	t.Helper()
	tablets := []*topodatapb.Tablet{
		recoveryTablet("zone1", 100, topodatapb.TabletType_REPLICA),
		recoveryTablet("zone2", 101, topodatapb.TabletType_REPLICA),
		recoveryTablet("zone3", 102, topodatapb.TabletType_REPLICA),
	}
	mockTMC := groupReplicationRecoveryTest(t, tablets...)
	setVoters(t, tablets...)
	_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error {
		si.GroupReplicationIncarnation = recorded
		return nil
	})
	require.NoError(t, err)
	const groupName = "6f1c2c2e-5a8e-4b8e-9d3a-7c1f0b6e2a41"
	for i, last := range []int{10, 12, 11} {
		status := notMemberStatus(tablets[i])
		status.PrimaryStatus = &replicationdatapb.PrimaryStatus{Position: "MySQL56/" + groupName + ":1-" + strconv.Itoa(last)}
		mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(tablets[i])).Return(status, nil)
	}
	return mockTMC, tablets
}

// newGroupStatus returns the FullStatus of the tablet whose MySQL is alone in a group of the given
// incarnation, as its ONLINE primary with quorum.
func newGroupStatus(tablet *topodatapb.Tablet, incarnation string) *replicationdatapb.FullStatus {
	status := groupMemberStatus(tablet, tablet, tablet)
	status.GroupReplicationStatus.GroupName = policy.GroupName("ks", "0")
	status.GroupReplicationStatus.ViewId = incarnation + ":1"
	return status
}

// expectJoins expects the given tablets to be made to join the group once, and returns how many did.
func expectJoins(mockTMC *tmcmock.MockTabletManagerClient, tablets ...*topodatapb.Tablet) *atomic.Int32 {
	var joins atomic.Int32
	for _, tablet := range tablets {
		mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(tablet), startRequest(false)).
			DoAndReturn(func(context.Context, *topodatapb.Tablet, *tabletmanagerdatapb.StartGroupReplicationRequest) (*replicationdatapb.GroupReplicationStatus, error) {
				joins.Add(1)
				return &replicationdatapb.GroupReplicationStatus{}, nil
			})
	}
	return &joins
}

// runLocked runs a VTOrc recovery under the shard lock.
func runLocked(t *testing.T, recovery func(ctx context.Context, analysisEntry *inst.DetectionAnalysis, logger *log.PrefixedLogger) (bool, *TopologyRecovery, error), analysis inst.AnalysisCode, tablet *topodatapb.Tablet) (bool, *TopologyRecovery, error) {
	t.Helper()
	lockedCtx, unlock, err := ts.LockShard(t.Context(), "ks", "0", "test")
	require.NoError(t, err)
	defer unlock(&err)
	return recovery(lockedCtx, &inst.DetectionAnalysis{
		Analysis:              analysis,
		AnalyzedInstanceAlias: tablet.Alias,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}, log.NewPrefixedLogger("test"))
}

// incarnationAt returns the incarnation that MySQL gives a group created at the given time.
func incarnationAt(created time.Time) string {
	return strconv.FormatInt(created.UnixNano()/100, 10)
}

// TestBootstrapGroupReplicationAdoptsGroupAfterLostReply reproduces run r3 of the S7d chaos
// scenario: VTOrc bootstrapped the group on the isolated old primary right after a heal, and the
// RPC returned an error once the next isolation cut VTOrc off from the tablet, although MySQL had
// bootstrapped the group. The new incarnation was never recorded: the tablet trusted the group it
// bootstrapped for a minute, no other bootstrap could start, and the other voters did not join it.
// VTOrc now records an intent before it bootstraps, and adopts the target's new group.
func TestBootstrapGroupReplicationAdoptsGroupAfterLostReply(t *testing.T) {
	const recorded = "17908000000000000"
	mockTMC, tablets := bootstrapIntentTest(t, recorded)
	target := tablets[1]
	incarnation := incarnationAt(time.Now())
	mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(target), startRequest(true)).
		Return(nil, vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "context deadline exceeded"))
	// The bootstrap happened: the target is the primary of a new group.
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(target)).Return(newGroupStatus(target, incarnation), nil)
	joins := expectJoins(mockTMC, tablets[0], tablets[2])

	attempted, topologyRecovery, err := runLocked(t, bootstrapGroupReplication, inst.GroupNotBootstrapped, tablets[0])
	require.NoError(t, err)
	require.True(t, attempted)
	assert.True(t, topologyRecovery.IsSuccessful)
	si, err := ts.GetShard(t.Context(), "ks", "0")
	require.NoError(t, err)
	assert.Equal(t, incarnation, si.GroupReplicationIncarnation)
	assert.Nil(t, si.GroupReplicationBootstrapIntent, "the recorded incarnation clears the intent")
	saved, err := inst.ReadShardGroupReplicationIncarnation("ks", "0")
	require.NoError(t, err)
	assert.Equal(t, incarnation, saved)
	assert.Eventually(t, func() bool { return joins.Load() == 2 }, 30*time.Second, 10*time.Millisecond)
}

// TestAdoptGroupReplicationBootstrapOnLaterPass checks the adoption on a later pass of VTOrc, by this
// VTOrc or another one: the bootstrap RPC failed while MySQL was still bootstrapping the group, so
// the bootstrap recovery itself found nothing to adopt and left its intent. Once the target is the
// primary of its new group (GroupBootstrapNotRecorded), the group is adopted. A member that a
// failed join left alone in a group of its own, which is not the intent's target, is not.
func TestAdoptGroupReplicationBootstrapOnLaterPass(t *testing.T) {
	const recorded = "17908000000000000"
	mockTMC, tablets := bootstrapIntentTest(t, recorded)
	target := tablets[1]
	mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(target), startRequest(true)).
		Return(nil, vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "context deadline exceeded"))
	// MySQL's START still runs: the member is not in a group yet.
	recovering := notMemberStatus(target)
	recovering.GroupReplicationStatus.MemberState = mysql.GroupMemberStateRecovering
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(target)).Return(recovering, nil)

	attempted, topologyRecovery, err := runLocked(t, bootstrapGroupReplication, inst.GroupNotBootstrapped, tablets[0])
	require.Error(t, err)
	require.True(t, attempted)
	assert.False(t, topologyRecovery.IsSuccessful)
	si, err := ts.GetShard(t.Context(), "ks", "0")
	require.NoError(t, err)
	assert.Equal(t, recorded, si.GroupReplicationIncarnation)
	require.NotNil(t, si.GroupReplicationBootstrapIntent, "the intent stays, for the adoption and as a fence")

	// Another voter is alone in a stray group of its own: it is not adopted.
	stray := tablets[2]
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(stray)).Return(newGroupStatus(stray, incarnationAt(time.Now())), nil).AnyTimes()
	attempted, _, err = runLocked(t, adoptGroupReplicationBootstrap, inst.GroupBootstrapNotRecorded, stray)
	require.NoError(t, err)
	assert.False(t, attempted)
	si, err = ts.GetShard(t.Context(), "ks", "0")
	require.NoError(t, err)
	assert.Equal(t, recorded, si.GroupReplicationIncarnation)

	// The target's START completed: its group is adopted, and the other voters join it.
	incarnation := incarnationAt(time.Now())
	mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(target)).Return(newGroupStatus(target, incarnation), nil)
	joins := expectJoins(mockTMC, tablets[0], stray)
	attempted, topologyRecovery, err = runLocked(t, adoptGroupReplicationBootstrap, inst.GroupBootstrapNotRecorded, target)
	require.NoError(t, err)
	require.True(t, attempted)
	assert.True(t, topologyRecovery.IsSuccessful)
	si, err = ts.GetShard(t.Context(), "ks", "0")
	require.NoError(t, err)
	assert.Equal(t, incarnation, si.GroupReplicationIncarnation)
	assert.Nil(t, si.GroupReplicationBootstrapIntent)
	assert.Eventually(t, func() bool { return joins.Load() == 2 }, 30*time.Second, 10*time.Millisecond)
}

// TestBootstrapGroupReplicationFencedByIntent checks that a recent bootstrap intent for another
// tablet, recorded by another VTOrc whose bootstrap's reply may have been lost, keeps this VTOrc
// from bootstrapping a second group: the other bootstrap may still be running, its member not
// active yet. The expiry of the fence is covered in reparentutil.
func TestBootstrapGroupReplicationFencedByIntent(t *testing.T) {
	const recorded = "17908000000000000"
	mockTMC, tablets := bootstrapIntentTest(t, recorded)
	other := tablets[0]
	// Another VTOrc started a bootstrap on another tablet 10s ago.
	_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error {
		si.GroupReplicationBootstrapIntent = &topodatapb.GroupReplicationBootstrapIntent{
			Target: other.Alias, Time: protoutil.TimeToProto(time.Now().Add(-10 * time.Second)), PreviousIncarnation: recorded, Token: "other-vtorc",
		}
		return nil
	})
	require.NoError(t, err)
	mockTMC.EXPECT().StartGroupReplication(gomock.Any(), gomock.Any(), startRequest(true)).Times(0)

	attempted, topologyRecovery, err := runLocked(t, bootstrapGroupReplication, inst.GroupNotBootstrapped, tablets[0])
	require.True(t, attempted)
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err), "%v", err)
	assert.False(t, topologyRecovery.IsSuccessful)
	si, err := ts.GetShard(t.Context(), "ks", "0")
	require.NoError(t, err)
	assert.Equal(t, "other-vtorc", si.GroupReplicationBootstrapIntent.GetToken())
}

// TestBootstrapGroupReplicationPrefersIntentTarget reproduces a run of the S7d chaos scenario in
// which VTOrc was killed while it bootstrapped the group on the old primary: the bootstrap did not
// happen, and the voters had equal GTID sets. Once the old primary's tablet demoted itself, the
// other VTOrcs chose another voter, the lowest alias, and the intent fenced that bootstrap for
// two minutes. A voter with all the transactions that is the target of a recent intent is chosen
// again instead: a bootstrap on the same target is not fenced.
func TestBootstrapGroupReplicationPrefersIntentTarget(t *testing.T) {
	const recorded = "17908000000000000"
	tablets := []*topodatapb.Tablet{
		recoveryTablet("zone1", 100, topodatapb.TabletType_REPLICA),
		recoveryTablet("zone2", 101, topodatapb.TabletType_REPLICA),
		recoveryTablet("zone3", 102, topodatapb.TabletType_REPLICA),
	}
	mockTMC := groupReplicationRecoveryTest(t, tablets...)
	setVoters(t, tablets...)
	target := tablets[2]
	_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error {
		si.GroupReplicationIncarnation = recorded
		// Another VTOrc started a bootstrap on zone3-102 10s ago, and was killed.
		si.GroupReplicationBootstrapIntent = &topodatapb.GroupReplicationBootstrapIntent{
			Target: target.Alias, Time: protoutil.TimeToProto(time.Now().Add(-10 * time.Second)), PreviousIncarnation: recorded, Token: "killed-vtorc",
		}
		return nil
	})
	require.NoError(t, err)
	const groupName = "6f1c2c2e-5a8e-4b8e-9d3a-7c1f0b6e2a41"
	for _, tablet := range tablets {
		status := notMemberStatus(tablet)
		status.PrimaryStatus = &replicationdatapb.PrimaryStatus{Position: "MySQL56/" + groupName + ":1-10"}
		mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(tablet)).Return(status, nil)
	}
	incarnation := incarnationAt(time.Now())
	mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(target), startRequest(true)).
		Return(&replicationdatapb.GroupReplicationStatus{ViewId: incarnation + ":1"}, nil)
	joins := expectJoins(mockTMC, tablets[0], tablets[1])

	attempted, topologyRecovery, err := runLocked(t, bootstrapGroupReplication, inst.GroupNotBootstrapped, tablets[0])
	require.NoError(t, err)
	require.True(t, attempted)
	assert.True(t, topologyRecovery.IsSuccessful)
	assert.Equal(t, target.Alias.Uid, topologyRecovery.SuccessorAlias.Uid)
	si, err := ts.GetShard(t.Context(), "ks", "0")
	require.NoError(t, err)
	assert.Equal(t, incarnation, si.GroupReplicationIncarnation)
	assert.Eventually(t, func() bool { return joins.Load() == 2 }, 30*time.Second, 10*time.Millisecond)
}
