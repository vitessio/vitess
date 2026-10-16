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
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
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
	return bootstrapIntentTestWith(t, recorded, 1, nil)
}

// bootstrapIntentTestWith is bootstrapIntentTest, with the FullStatus of every tablet read up to reads
// times (any number if reads is 0), as edit changes it.
func bootstrapIntentTestWith(t *testing.T, recorded string, reads int, edit func(*topodatapb.Tablet, *replicationdatapb.FullStatus)) (*tmcmock.MockTabletManagerClient, []*topodatapb.Tablet) {
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
		if edit != nil {
			edit(tablets[i], status)
		}
		call := mockTMC.EXPECT().FullStatus(gomock.Any(), sameTablet(tablets[i])).Return(status, nil)
		if reads == 0 {
			call.AnyTimes()
		} else {
			call.Times(reads)
		}
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
// active yet. Here MySQL's START still runs on the intent's target, past VTOrc's grace for it, so
// VTOrc does not send the intent's bootstrap to it again either (see
// TestBootstrapGroupReplicationReprobesStaleIntentTarget). The expiry of the fence is covered in
// reparentutil.
func TestBootstrapGroupReplicationFencedByIntent(t *testing.T) {
	const recorded = "17908000000000000"
	previous := inst.SetGroupStartInProgressGrace(0)
	t.Cleanup(func() { inst.SetGroupStartInProgressGrace(previous) })
	inst.GroupReplicationConditions.Reset()
	mockTMC, tablets := bootstrapIntentTestWith(t, recorded, 1, func(tablet *topodatapb.Tablet, status *replicationdatapb.FullStatus) {
		status.GroupReplicationStatus.StartInProgress = tablet.Alias.Uid == 100
	})
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

// staleIntentToken is the token of the bootstrap intent that an earlier pass recorded for 100.
const staleIntentToken = "earlier-pass"

// staleIntentTest sets up the layout of bootstrapIntentTest, read on every pass, and the live bootstrap
// intent that an earlier pass of VTOrc recorded for 100, 10s ago, and returns it. 100 was the candidate
// then; its bootstrap RPC failed without a definitive refusal (it timed out, or reached the tablet while
// mysqld was down), and its mysqld restarted, which discarded transactions from its relay log that 101
// holds. 100 is now reachable, in no group and without a START, but 101 is the candidate.
func staleIntentTest(t *testing.T, recorded string) (*tmcmock.MockTabletManagerClient, []*topodatapb.Tablet, *topodatapb.GroupReplicationBootstrapIntent) {
	t.Helper()
	mockTMC, tablets := bootstrapIntentTestWith(t, recorded, 0, nil)
	intent := &topodatapb.GroupReplicationBootstrapIntent{
		Target:              tablets[0].Alias,
		Time:                protoutil.TimeToProto(time.Now().Add(-10 * time.Second)),
		PreviousIncarnation: recorded,
		Token:               staleIntentToken,
	}
	_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error {
		si.GroupReplicationBootstrapIntent = proto.Clone(intent).(*topodatapb.GroupReplicationBootstrapIntent)
		return nil
	})
	require.NoError(t, err)
	return mockTMC, tablets, intent
}

// TestBootstrapGroupReplicationReprobesStaleIntentTarget checks what VTOrc does with a live bootstrap
// intent whose target is no longer the candidate. Before, the intent fenced the bootstrap of the
// candidate until it expired, two minutes after it was recorded: nothing refused that intent's
// bootstrap, since VTOrc no longer chose its target (the TLA+ model's reprobe_stuck configuration, in
// doc/design-docs/group_replication_tla). VTOrc now sends the intent's own bootstrap to its target
// again: the same token and expected incarnation, the transactions that every voter holds now, and
// without rewriting the intent, whose time, and fence, must not be extended.
//   - The target refuses definitively: VTOrc withdraws the intent and bootstraps the candidate in the
//     same pass.
//   - Any other failure keeps the intent, as it is: the bootstrap may still run.
//   - The target bootstraps after all (the tablet checked that MySQL executed every required
//     transaction): its group is recorded for the intent, and the other voters join it.
func TestBootstrapGroupReplicationReprobesStaleIntentTarget(t *testing.T) {
	const recorded = "17908000000000000"
	const groupName = "6f1c2c2e-5a8e-4b8e-9d3a-7c1f0b6e2a41"
	refusal := vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "refusing to bootstrap the replication group: MySQL has not executed %s:11-12", groupName)

	// checkReprobe checks the request that sends the stale intent's bootstrap again, and that the
	// shard record holds the intent as it was when the request is sent.
	checkReprobe := func(t *testing.T, intent *topodatapb.GroupReplicationBootstrapIntent, req *tabletmanagerdatapb.StartGroupReplicationRequest) {
		assert.Equal(t, staleIntentToken, req.GetBootstrapIntentToken())
		assert.Equal(t, recorded, req.GetExpectedIncarnation())
		assert.Equal(t, groupName+":1-12", req.GetRequiredGtidSet(), "the transactions that the voters hold now")
		assert.True(t, req.GetReportDefinitiveRefusal())
		si, err := ts.GetShard(t.Context(), "ks", "0")
		if assert.NoError(t, err) {
			assert.True(t, proto.Equal(intent, si.GroupReplicationBootstrapIntent), "the intent must not be rewritten: %v", si.GroupReplicationBootstrapIntent)
		}
	}

	t.Run("definitive refusal", func(t *testing.T) {
		mockTMC, tablets, intent := staleIntentTest(t, recorded)
		var reprobed atomic.Bool
		mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(tablets[0]), startRequest(true)).
			DoAndReturn(func(_ context.Context, _ *topodatapb.Tablet, req *tabletmanagerdatapb.StartGroupReplicationRequest) (*replicationdatapb.GroupReplicationStatus, error) {
				checkReprobe(t, intent, req)
				reprobed.Store(true)
				return nil, tmclient.NewGroupBootstrapRefusedError(refusal)
			})
		incarnation := incarnationAt(time.Now())
		mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(tablets[1]), startRequest(true)).
			DoAndReturn(func(_ context.Context, _ *topodatapb.Tablet, req *tabletmanagerdatapb.StartGroupReplicationRequest) (*replicationdatapb.GroupReplicationStatus, error) {
				assert.True(t, reprobed.Load(), "the intent's target must refuse before the candidate bootstraps")
				si, err := ts.GetShard(t.Context(), "ks", "0")
				if assert.NoError(t, err) {
					assert.Equal(t, "zone2-0000000101", topoproto.TabletAliasString(si.GroupReplicationBootstrapIntent.GetTarget()))
					assert.NotEqual(t, staleIntentToken, req.GetBootstrapIntentToken())
					assert.Equal(t, si.GroupReplicationBootstrapIntent.GetToken(), req.GetBootstrapIntentToken())
				}
				return &replicationdatapb.GroupReplicationStatus{ViewId: incarnation + ":1"}, nil
			})
		joins := expectJoins(mockTMC, tablets[0], tablets[2])

		attempted, topologyRecovery, err := runLocked(t, bootstrapGroupReplication, inst.GroupNotBootstrapped, tablets[0])
		require.NoError(t, err)
		require.True(t, attempted)
		assert.True(t, topologyRecovery.IsSuccessful)
		assert.EqualValues(t, 101, topologyRecovery.SuccessorAlias.Uid, "the candidate is bootstrapped in the same pass")
		si, err := ts.GetShard(t.Context(), "ks", "0")
		require.NoError(t, err)
		assert.Equal(t, incarnation, si.GroupReplicationIncarnation)
		assert.Nil(t, si.GroupReplicationBootstrapIntent)
		assert.Eventually(t, func() bool { return joins.Load() == 2 }, 30*time.Second, 10*time.Millisecond)
	})

	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "timeout", err: vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "context deadline exceeded")},
		{name: "transport error", err: vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "connection error: connection refused")},
		{name: "refusal that is not definitive", err: refusal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockTMC, tablets, intent := staleIntentTest(t, recorded)
			mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(tablets[0]), startRequest(true)).
				DoAndReturn(func(_ context.Context, _ *topodatapb.Tablet, req *tabletmanagerdatapb.StartGroupReplicationRequest) (*replicationdatapb.GroupReplicationStatus, error) {
					checkReprobe(t, intent, req)
					return nil, tc.err
				})
			mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(tablets[1]), gomock.Any()).Times(0)

			attempted, topologyRecovery, err := runLocked(t, bootstrapGroupReplication, inst.GroupNotBootstrapped, tablets[0])
			require.Error(t, err)
			require.True(t, attempted)
			assert.False(t, topologyRecovery.IsSuccessful)
			si, err := ts.GetShard(t.Context(), "ks", "0")
			require.NoError(t, err)
			assert.True(t, proto.Equal(intent, si.GroupReplicationBootstrapIntent), "the intent must stay as it was: %v", si.GroupReplicationBootstrapIntent)
			assert.Equal(t, recorded, si.GroupReplicationIncarnation)
		})
	}

	t.Run("target bootstraps", func(t *testing.T) {
		mockTMC, tablets, intent := staleIntentTest(t, recorded)
		incarnation := incarnationAt(time.Now())
		mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(tablets[0]), startRequest(true)).
			DoAndReturn(func(_ context.Context, _ *topodatapb.Tablet, req *tabletmanagerdatapb.StartGroupReplicationRequest) (*replicationdatapb.GroupReplicationStatus, error) {
				checkReprobe(t, intent, req)
				return &replicationdatapb.GroupReplicationStatus{ViewId: incarnation + ":1"}, nil
			})
		mockTMC.EXPECT().StartGroupReplication(gomock.Any(), sameTablet(tablets[1]), startRequest(true)).Times(0)
		joins := expectJoins(mockTMC, tablets[1], tablets[2])

		attempted, topologyRecovery, err := runLocked(t, bootstrapGroupReplication, inst.GroupNotBootstrapped, tablets[0])
		require.NoError(t, err)
		require.True(t, attempted)
		assert.True(t, topologyRecovery.IsSuccessful)
		assert.EqualValues(t, 100, topologyRecovery.SuccessorAlias.Uid)
		si, err := ts.GetShard(t.Context(), "ks", "0")
		require.NoError(t, err)
		assert.Equal(t, incarnation, si.GroupReplicationIncarnation)
		assert.Nil(t, si.GroupReplicationBootstrapIntent, "the recorded incarnation clears the intent")
		assert.Eventually(t, func() bool { return joins.Load() == 2 }, 30*time.Second, 10*time.Millisecond)
	})
}

// TestStaleGroupBootstrapIntentTarget checks when VTOrc sends the bootstrap of a live intent to its
// target again: only when the target is a voter other than the candidate whose tablet answered on this
// pass, whose MySQL is in no group and runs no START, and the intent has a token.
func TestStaleGroupBootstrapIntentTarget(t *testing.T) {
	target := recoveryTablet("zone1", 100, topodatapb.TabletType_REPLICA)
	candidate := recoveryTablet("zone2", 101, topodatapb.TabletType_REPLICA)
	other := recoveryTablet("zone3", 102, topodatapb.TabletType_REPLICA)
	voters := []*topodatapb.TabletAlias{target.Alias, candidate.Alias, other.Alias}
	liveIntent := func() *topodatapb.GroupReplicationBootstrapIntent {
		return &topodatapb.GroupReplicationBootstrapIntent{Target: target.Alias, Token: staleIntentToken, PreviousIncarnation: "1"}
	}
	statuses := func(edit func(*shardTabletStatus)) []*shardTabletStatus {
		list := []*shardTabletStatus{
			{tablet: target, status: notMemberStatus(target)},
			{tablet: candidate, status: notMemberStatus(candidate)},
			{tablet: other, status: notMemberStatus(other)},
		}
		if edit != nil {
			edit(list[0])
		}
		return list
	}
	tests := []struct {
		name     string
		intent   *topodatapb.GroupReplicationBootstrapIntent
		voters   []*topodatapb.TabletAlias
		statuses []*shardTabletStatus
		reprobe  bool
	}{
		{name: "reachable, out of any group, no START", intent: liveIntent(), voters: voters, statuses: statuses(nil), reprobe: true},
		{name: "no intent", voters: voters, statuses: statuses(nil)},
		{name: "intent without a token", intent: &topodatapb.GroupReplicationBootstrapIntent{Target: target.Alias, PreviousIncarnation: "1"}, voters: voters, statuses: statuses(nil)},
		{name: "intent for the candidate", intent: &topodatapb.GroupReplicationBootstrapIntent{Target: candidate.Alias, Token: staleIntentToken}, voters: voters, statuses: statuses(nil)},
		{name: "target no longer a voter", intent: liveIntent(), voters: voters[1:], statuses: statuses(nil)},
		{name: "target without a tablet record", intent: liveIntent(), voters: voters, statuses: statuses(nil)[1:]},
		{name: "target unreachable", intent: liveIntent(), voters: voters, statuses: statuses(func(st *shardTabletStatus) {
			st.status, st.err = nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "connection refused")
		})},
		{name: "target ONLINE in a group", intent: liveIntent(), voters: voters, statuses: statuses(func(st *shardTabletStatus) {
			st.status.GroupReplicationStatus.MemberState = mysql.GroupMemberStateOnline
		})},
		{name: "target RECOVERING", intent: liveIntent(), voters: voters, statuses: statuses(func(st *shardTabletStatus) {
			st.status.GroupReplicationStatus.MemberState = mysql.GroupMemberStateRecovering
		})},
		{name: "START in progress", intent: liveIntent(), voters: voters, statuses: statuses(func(st *shardTabletStatus) {
			st.status.GroupReplicationStatus.StartInProgress = true
		})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := staleGroupBootstrapIntentTarget(tt.intent, &groupBootstrapCandidate{tablet: candidate}, tt.voters, tt.statuses)
			if tt.reprobe {
				require.NotNil(t, got)
				assert.Equal(t, "zone1-0000000100", topoproto.TabletAliasString(got.Alias))
			} else {
				assert.Nil(t, got)
			}
		})
	}
}
