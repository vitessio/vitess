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
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// statusTMC answers FullStatus with a fixed status per tablet.
type statusTMC struct {
	tmclient.TabletManagerClient
	statuses map[string]*replicationdatapb.FullStatus
}

// FullStatus is part of the tmclient.TabletManagerClient interface.
func (c *statusTMC) FullStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
	status, ok := c.statuses[topoproto.TabletAliasString(tablet.Alias)]
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "tablet unreachable")
	}
	return status, nil
}

// intentTestShard returns a memory topology with shard ks/0, whose recorded incarnation is
// recorded, and a context that holds the shard lock.
func intentTestShard(t *testing.T, recorded string) (context.Context, *topo.Server) {
	t.Helper()
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "zone1")
	t.Cleanup(ts.Close)
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{}))
	require.NoError(t, ts.CreateShard(ctx, "ks", "0"))
	_, err := ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
		si.GroupReplicationIncarnation = recorded
		return nil
	})
	require.NoError(t, err)
	lockedCtx, unlock, err := ts.LockShard(ctx, "ks", "0", "test")
	require.NoError(t, err)
	t.Cleanup(func() { unlock(&err) })
	return lockedCtx, ts
}

func intentTestTablet(uid uint32) *topodatapb.Tablet {
	return &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: uid}, Keyspace: "ks", Shard: "0"}
}

func intentTestUUID(uid uint32) string {
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", uid)
}

// groupPrimaryStatus returns the FullStatus of a tablet whose MySQL is the primary, with quorum or
// not, of a view of the given id of the shard's group.
func groupPrimaryStatus(uid uint32, viewID string, quorum bool) *replicationdatapb.FullStatus {
	uuid := intentTestUUID(uid)
	return &replicationdatapb.FullStatus{
		ServerUuid: uuid,
		GroupReplicationStatus: &replicationdatapb.GroupReplicationStatus{
			PluginActive: true,
			GroupName:    policy.GroupName("ks", "0"),
			MemberState:  mysql.GroupMemberStateOnline,
			MemberRole:   mysql.GroupMemberRolePrimary,
			PrimaryUuid:  uuid,
			HasQuorum:    quorum,
			ViewId:       viewID,
			Members: []*replicationdatapb.GroupReplicationMember{
				{MemberUuid: uuid, State: mysql.GroupMemberStateOnline, Role: mysql.GroupMemberRolePrimary},
			},
		},
	}
}

func readShard(t *testing.T, ts *topo.Server) *topo.ShardInfo {
	t.Helper()
	si, err := ts.GetShard(t.Context(), "ks", "0")
	require.NoError(t, err)
	return si
}

// TestGroupReplicationBootstrapIntentRoundTrip checks the new Shard field through the generated
// marshalling code: the vtprotobuf functions and the reflection-based ones agree, and a shard
// record without an intent encodes as before.
func TestGroupReplicationBootstrapIntentRoundTrip(t *testing.T) {
	shard := &topodatapb.Shard{
		PrimaryAlias:                &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		GroupReplicationVoters:      []*topodatapb.TabletAlias{{Cell: "zone1", Uid: 100}, {Cell: "zone2", Uid: 200}},
		GroupReplicationIncarnation: "17908892198863259",
		GroupReplicationBootstrapIntent: &topodatapb.GroupReplicationBootstrapIntent{
			Target:              &topodatapb.TabletAlias{Cell: "zone2", Uid: 200},
			Time:                protoutil.TimeToProto(time.Unix(1790889219, 886325900)),
			PreviousIncarnation: "17908892198863259",
			Token:               "1790889219886325900-0123456789abcdef",
		},
	}
	vt, err := shard.MarshalVT()
	require.NoError(t, err)
	assert.Len(t, vt, shard.SizeVT())
	reflected, err := proto.Marshal(shard)
	require.NoError(t, err)

	fromVT := &topodatapb.Shard{}
	require.NoError(t, proto.Unmarshal(vt, fromVT))
	assert.True(t, proto.Equal(shard, fromVT), "vtprotobuf encoding, reflection decoding")
	fromReflected := &topodatapb.Shard{}
	require.NoError(t, fromReflected.UnmarshalVT(reflected))
	assert.True(t, proto.Equal(shard, fromReflected), "reflection encoding, vtprotobuf decoding")
	assert.True(t, proto.Equal(shard, shard.CloneVT()))
	assert.NotSame(t, shard.GroupReplicationBootstrapIntent, shard.CloneVT().GroupReplicationBootstrapIntent)

	// Readers that do not know the field skip it: decoding with the field number unknown keeps the
	// other fields.
	withoutIntent := shard.CloneVT()
	withoutIntent.GroupReplicationBootstrapIntent = nil
	old, err := withoutIntent.MarshalVT()
	require.NoError(t, err)
	assert.Equal(t, old, vt[:len(old)], "the intent is encoded after the existing fields")
}

// TestWriteGroupReplicationIncarnationCompareAndSwap checks that the incarnation is only recorded
// over the incarnation the caller read before it changed the group, and that recording it clears
// the bootstrap intent.
func TestWriteGroupReplicationIncarnationCompareAndSwap(t *testing.T) {
	ctx, ts := intentTestShard(t, "1790000001")
	_, err := WriteGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", intentTestTablet(100).Alias, "1790000001", time.Now())
	require.NoError(t, err)

	// Another component recorded another incarnation since the caller read 1790000000.
	err = WriteGroupReplicationIncarnation(ctx, ts, "ks", "0", "1790000000", "1790000002")
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err), "%v", err)
	assert.Equal(t, "1790000001", readShard(t, ts).GroupReplicationIncarnation)

	require.NoError(t, WriteGroupReplicationIncarnation(ctx, ts, "ks", "0", "1790000001", "1790000002"))
	si := readShard(t, ts)
	assert.Equal(t, "1790000002", si.GroupReplicationIncarnation)
	assert.Nil(t, si.GroupReplicationBootstrapIntent, "the recorded incarnation clears the intent")
	// Recording the same incarnation again is a no-op.
	require.NoError(t, WriteGroupReplicationIncarnation(ctx, ts, "ks", "0", "1790000001", "1790000002"))
}

// TestWriteGroupReplicationBootstrapIntentFencesAnotherBootstrap checks that a recent intent keeps
// a second bootstrap of the group on another tablet from starting: the first bootstrap's reply may
// have been lost while its MySQL still creates the group. The same target, an intent older than the
// fence, and an intent that an incarnation recorded since superseded do not fence.
func TestWriteGroupReplicationBootstrapIntentFencesAnotherBootstrap(t *testing.T) {
	ctx, ts := intentTestShard(t, "1790000001")
	now := time.Now()
	first, err := WriteGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", intentTestTablet(100).Alias, "1790000001", now)
	require.NoError(t, err)

	_, err = WriteGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", intentTestTablet(101).Alias, "1790000001", now.Add(10*time.Second))
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err), "%v", err)
	assert.Equal(t, first.Token, readShard(t, ts).GroupReplicationBootstrapIntent.GetToken(), "the first intent stands")

	// The same target may bootstrap again: its tablet stops a START still in progress first.
	second, err := WriteGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", intentTestTablet(100).Alias, "1790000001", now.Add(10*time.Second))
	require.NoError(t, err)
	assert.NotEqual(t, first.Token, second.Token)

	// Once the fence passed, another tablet may.
	third, err := WriteGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", intentTestTablet(101).Alias, "1790000001", now.Add(10*time.Second+GroupReplicationBootstrapIntentFence))
	require.NoError(t, err)
	assert.True(t, topoproto.TabletAliasEqual(intentTestTablet(101).Alias, third.Target))

	// The incarnation changed since the caller read it.
	_, err = WriteGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", intentTestTablet(101).Alias, "1790000000", now)
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err), "%v", err)

	// An intent recorded for an incarnation that is no longer the shard's does not fence: a
	// component that does not know intents recorded a new incarnation without clearing it.
	_, err = ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
		si.GroupReplicationIncarnation = "1790000005"
		return nil
	})
	require.NoError(t, err)
	_, err = WriteGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", intentTestTablet(100).Alias, "1790000005", now.Add(11*time.Second+GroupReplicationBootstrapIntentFence))
	require.NoError(t, err)
}

// TestAdoptGroupReplicationBootstrap checks when the group that a bootstrap whose reply was lost
// created is adopted: only the intent's target's own new group, with quorum, created after the
// intent. A member that a failed join left alone in a group of its own (a stray incarnation) is
// never adopted, nor the group the intent was recorded for.
func TestAdoptGroupReplicationBootstrap(t *testing.T) {
	const recorded = "17908000000000000"
	now := time.Unix(1790889219, 0)
	newIncarnation := "17908892200000000" // 1790889220, one second after the intent
	tests := []struct {
		name string
		// adopt is the tablet whose group is adopted; the intent's target is tablet 100.
		adopt    uint32
		status   *replicationdatapb.FullStatus
		recorded string
		wantErr  bool
	}{
		{name: "the target is the primary of its new group", adopt: 100, status: groupPrimaryStatus(100, newIncarnation+":1", true)},
		{name: "a stray group of another voter", adopt: 101, status: groupPrimaryStatus(101, newIncarnation+":1", true), wantErr: true},
		{name: "the target is in the incarnation the intent was recorded for", adopt: 100, status: groupPrimaryStatus(100, recorded+":12", true), wantErr: true},
		{name: "the target's group has no quorum", adopt: 100, status: groupPrimaryStatus(100, newIncarnation+":1", false), wantErr: true},
		{name: "the target's group was created long before the intent", adopt: 100, status: groupPrimaryStatus(100, "17908890000000000:1", true), wantErr: true},
		{name: "the target is a secondary of another member's group", adopt: 100, status: func() *replicationdatapb.FullStatus {
			status := groupPrimaryStatus(100, newIncarnation+":2", true)
			status.GroupReplicationStatus.MemberRole = mysql.GroupMemberRoleSecondary
			status.GroupReplicationStatus.PrimaryUuid = intentTestUUID(101)
			return status
		}(), wantErr: true},
		{name: "another incarnation was recorded since the intent", adopt: 100, status: groupPrimaryStatus(100, newIncarnation+":1", true), recorded: "17908500000000000", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, ts := intentTestShard(t, recorded)
			intent, err := WriteGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", intentTestTablet(100).Alias, recorded, now)
			require.NoError(t, err)
			if tt.recorded != "" {
				require.NoError(t, WriteGroupReplicationIncarnation(ctx, ts, "ks", "0", recorded, tt.recorded))
			}
			adopt := intentTestTablet(tt.adopt)
			tmc := &statusTMC{statuses: map[string]*replicationdatapb.FullStatus{topoproto.TabletAliasString(adopt.Alias): tt.status}}
			si := readShard(t, ts)
			incarnation, err := AdoptGroupReplicationBootstrap(ctx, ts, tmc, "ks", "0", si.GroupReplicationIncarnation, intent, adopt)
			if tt.wantErr {
				assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err), "%v", err)
				assert.Equal(t, si.GroupReplicationIncarnation, readShard(t, ts).GroupReplicationIncarnation, "nothing is recorded")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, newIncarnation, incarnation)
			si = readShard(t, ts)
			assert.Equal(t, newIncarnation, si.GroupReplicationIncarnation)
			assert.Nil(t, si.GroupReplicationBootstrapIntent)
		})
	}
}

// TestAdoptGroupReplicationBootstrapCompareAndSwapConflict checks that the adoption does not
// overwrite a shard record that changed after the caller read it: another VTOrc recorded another
// incarnation, or replaced the intent.
func TestAdoptGroupReplicationBootstrapCompareAndSwapConflict(t *testing.T) {
	const recorded = "17908000000000000"
	now := time.Now()
	newIncarnation := policyIncarnationAt(now.Add(time.Second))
	target := intentTestTablet(100)
	tmc := &statusTMC{statuses: map[string]*replicationdatapb.FullStatus{topoproto.TabletAliasString(target.Alias): groupPrimaryStatus(100, newIncarnation+":1", true)}}

	t.Run("another incarnation was recorded", func(t *testing.T) {
		ctx, ts := intentTestShard(t, recorded)
		intent, err := WriteGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", target.Alias, recorded, now)
		require.NoError(t, err)
		// Another VTOrc recorded another incarnation after this one read the shard record.
		_, err = ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
			si.GroupReplicationIncarnation = "17908500000000000"
			return nil
		})
		require.NoError(t, err)
		_, err = AdoptGroupReplicationBootstrap(ctx, ts, tmc, "ks", "0", recorded, intent, target)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err), "%v", err)
		assert.Equal(t, "17908500000000000", readShard(t, ts).GroupReplicationIncarnation)
	})
	t.Run("the intent was replaced", func(t *testing.T) {
		ctx, ts := intentTestShard(t, recorded)
		intent, err := WriteGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", target.Alias, recorded, now)
		require.NoError(t, err)
		replaced, err := WriteGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", target.Alias, recorded, now.Add(time.Second))
		require.NoError(t, err)
		_, err = AdoptGroupReplicationBootstrap(ctx, ts, tmc, "ks", "0", recorded, intent, target)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err), "%v", err)
		si := readShard(t, ts)
		assert.Equal(t, recorded, si.GroupReplicationIncarnation)
		assert.Equal(t, replaced.Token, si.GroupReplicationBootstrapIntent.GetToken())
	})
}

// policyIncarnationAt returns the incarnation that MySQL gives a group created at t.
func policyIncarnationAt(t time.Time) string {
	return strconv.FormatInt(t.UnixNano()/100, 10)
}

// TestGroupIncarnationTime checks the decoding of the time MySQL encodes in an incarnation.
func TestGroupIncarnationTime(t *testing.T) {
	created, ok := policy.GroupIncarnationTime("17908892198863259")
	require.True(t, ok)
	assert.Equal(t, time.Date(2026, 10, 1, 21, 13, 39, 886325900, time.UTC), created)
	for _, incarnation := range []string{"", "1790000001", "abc", "-1", "99999999999999999999"} {
		_, ok := policy.GroupIncarnationTime(incarnation)
		assert.False(t, ok, incarnation)
	}
}

// TestRecordGroupReplicationBootstrapKeepsNewerIntent checks that recording an incarnation that the
// shard record lists already does not clear an intent recorded after it. The TLA+ model found the
// interleaving: VTOrc o2 adopted the group that o1 bootstrapped for intent 1; the group lost its
// majority again, and o2 recorded intent 2 to bootstrap the group again; o1's bootstrap reply then
// arrived and recorded the same incarnation again, which cleared intent 2 while its bootstrap still
// ran. Without the intent, nothing fenced a bootstrap on another voter, and the group of intent 2
// could no longer be adopted.
func TestRecordGroupReplicationBootstrapKeepsNewerIntent(t *testing.T) {
	const recorded = "17908000000000000"
	now := time.Now()
	newIncarnation := policyIncarnationAt(now.Add(time.Second))
	ctx, ts := intentTestShard(t, recorded)
	target := intentTestTablet(100)

	first, err := WriteGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", target.Alias, recorded, now)
	require.NoError(t, err)
	// Another VTOrc adopted the group of the first intent.
	_, err = ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
		si.GroupReplicationIncarnation = newIncarnation
		si.GroupReplicationBootstrapIntent = nil
		return nil
	})
	require.NoError(t, err)
	second, err := WriteGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", target.Alias, newIncarnation, now.Add(2*time.Second))
	require.NoError(t, err)

	// The reply of the first bootstrap arrives.
	require.NoError(t, RecordGroupReplicationBootstrap(ctx, ts, "ks", "0", first, newIncarnation))
	si := readShard(t, ts)
	assert.Equal(t, newIncarnation, si.GroupReplicationIncarnation)
	assert.Equal(t, second.Token, si.GroupReplicationBootstrapIntent.GetToken(), "the newer intent must stay")

	// An intent of an earlier incarnation, which a component that does not know intents left
	// behind, is cleared.
	_, err = ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
		si.GroupReplicationBootstrapIntent = first
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, RecordGroupReplicationBootstrap(ctx, ts, "ks", "0", first, newIncarnation))
	assert.Nil(t, readShard(t, ts).GroupReplicationBootstrapIntent)
}

// TestWithdrawGroupReplicationBootstrapIntent checks the compare-and-swap with which VTOrc withdraws
// its bootstrap intent once the intent's target refused the bootstrap definitively: it removes the
// intent only while the shard record holds the same token, for the incarnation it was recorded for.
// A newer intent, which another VTOrc recorded after the caller's shard lock expired, and an
// incarnation recorded since, stay as they are; a caller that lost its shard lock writes nothing.
func TestWithdrawGroupReplicationBootstrapIntent(t *testing.T) {
	const recorded = "1790000001"
	now := time.Now()
	target := intentTestTablet(100)

	t.Run("its own intent", func(t *testing.T) {
		ctx, ts := intentTestShard(t, recorded)
		intent, err := WriteGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", target.Alias, recorded, now)
		require.NoError(t, err)
		withdrawn, err := WithdrawGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", intent)
		require.NoError(t, err)
		assert.True(t, withdrawn)
		si := readShard(t, ts)
		assert.Nil(t, si.GroupReplicationBootstrapIntent)
		assert.Equal(t, recorded, si.GroupReplicationIncarnation)
		// The intent no longer fences a bootstrap on another tablet.
		_, err = WriteGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", intentTestTablet(101).Alias, recorded, now.Add(time.Second))
		require.NoError(t, err)
		// Withdrawing it again changes nothing.
		withdrawn, err = WithdrawGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", intent)
		require.NoError(t, err)
		assert.False(t, withdrawn)
		assert.True(t, topoproto.TabletAliasEqual(intentTestTablet(101).Alias, readShard(t, ts).GroupReplicationBootstrapIntent.GetTarget()))
	})

	t.Run("a newer intent", func(t *testing.T) {
		ctx, ts := intentTestShard(t, recorded)
		first, err := WriteGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", target.Alias, recorded, now)
		require.NoError(t, err)
		// Another VTOrc chose the same target again and replaced the intent.
		second, err := WriteGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", target.Alias, recorded, now.Add(time.Second))
		require.NoError(t, err)
		withdrawn, err := WithdrawGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", first)
		require.NoError(t, err)
		assert.False(t, withdrawn)
		assert.Equal(t, second.GetToken(), readShard(t, ts).GroupReplicationBootstrapIntent.GetToken(), "the newer intent must stay")
	})

	t.Run("an incarnation recorded since", func(t *testing.T) {
		ctx, ts := intentTestShard(t, recorded)
		intent, err := WriteGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", target.Alias, recorded, now)
		require.NoError(t, err)
		// A component that does not know intents recorded another incarnation, and left the intent.
		_, err = ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
			si.GroupReplicationIncarnation = "1790000002"
			return nil
		})
		require.NoError(t, err)
		withdrawn, err := WithdrawGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", intent)
		require.NoError(t, err)
		assert.False(t, withdrawn)
		si := readShard(t, ts)
		assert.Equal(t, "1790000002", si.GroupReplicationIncarnation)
		assert.Equal(t, intent.GetToken(), si.GroupReplicationBootstrapIntent.GetToken())

		// The incarnation recorded for the intent cleared it; withdrawing writes nothing.
		require.NoError(t, WriteGroupReplicationIncarnation(ctx, ts, "ks", "0", "1790000002", "1790000003"))
		withdrawn, err = WithdrawGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", intent)
		require.NoError(t, err)
		assert.False(t, withdrawn)
		assert.Equal(t, "1790000003", readShard(t, ts).GroupReplicationIncarnation)
	})

	t.Run("without the shard lock", func(t *testing.T) {
		ctx, ts := intentTestShard(t, recorded)
		intent, err := WriteGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", target.Alias, recorded, now)
		require.NoError(t, err)
		_, err = WithdrawGroupReplicationBootstrapIntent(t.Context(), ts, "ks", "0", intent)
		require.Error(t, err)
		assert.Equal(t, intent.GetToken(), readShard(t, ts).GroupReplicationBootstrapIntent.GetToken())
	})

	t.Run("without a token", func(t *testing.T) {
		ctx, ts := intentTestShard(t, recorded)
		_, err := WithdrawGroupReplicationBootstrapIntent(ctx, ts, "ks", "0", &topodatapb.GroupReplicationBootstrapIntent{PreviousIncarnation: recorded})
		assert.Equal(t, vtrpcpb.Code_INVALID_ARGUMENT, vterrors.Code(err), "%v", err)
	})
}
