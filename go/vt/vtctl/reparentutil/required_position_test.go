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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/logutil"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/reparenttestutil"
	"vitess.io/vitess/go/vt/vterrors"
	tmcmock "vitess.io/vitess/go/vt/vttablet/tmclient/mock"
)

const (
	requiredUUID          = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	requiredLow           = "MySQL56/" + requiredUUID + ":1-10"
	requiredHigh          = "MySQL56/" + requiredUUID + ":1-20"
	requiredMissing       = "MySQL56/" + requiredUUID + ":1-30"
	requiredBehindAlias   = "zone1-0000000101"
	requiredAdvancedAlias = "zone1-0000000102"
)

// requiredPosition decodes encoded and stops the test on error.
func requiredPosition(t *testing.T, encoded string) replication.Position {
	t.Helper()
	position, err := replication.DecodePosition(encoded)
	require.NoError(t, err)
	return position
}

// TestValidateRequiredPositionFlavor checks the flavor rule.
func TestValidateRequiredPositionFlavor(t *testing.T) {
	for _, tc := range []struct {
		name     string
		required string
		wantCode vtrpcpb.Code
	}{
		{name: "zero position", required: "", wantCode: vtrpcpb.Code_OK},
		{name: "MySQL56 position", required: requiredHigh, wantCode: vtrpcpb.Code_OK},
		{name: "typed empty MySQL56 position", required: "MySQL56/", wantCode: vtrpcpb.Code_OK},
		{name: "MariaDB position", required: "MariaDB/0-1-20", wantCode: vtrpcpb.Code_INVALID_ARGUMENT},
		{name: "typed empty MariaDB position", required: "MariaDB/", wantCode: vtrpcpb.Code_INVALID_ARGUMENT},
		{name: "FilePos position", required: "FilePos/mysql-bin.000001:20", wantCode: vtrpcpb.Code_INVALID_ARGUMENT},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRequiredPositionFlavor(requiredPosition(t, tc.required))
			assert.Equal(t, tc.wantCode, vterrors.Code(err))
		})
	}
}

// TestValidateRequiredPosition checks the shard type rule.
func TestValidateRequiredPosition(t *testing.T) {
	for _, tc := range []struct {
		name        string
		required    string
		isGTIDBased bool
		wantCode    vtrpcpb.Code
	}{
		{name: "zero position on non GTID shard", required: "", isGTIDBased: false, wantCode: vtrpcpb.Code_OK},
		{name: "MySQL56 position on GTID shard", required: requiredHigh, isGTIDBased: true, wantCode: vtrpcpb.Code_OK},
		{name: "MySQL56 position on non GTID shard", required: requiredHigh, isGTIDBased: false, wantCode: vtrpcpb.Code_INVALID_ARGUMENT},
		{name: "typed empty MySQL56 position on non GTID shard", required: "MySQL56/", isGTIDBased: false, wantCode: vtrpcpb.Code_INVALID_ARGUMENT},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRequiredPosition(requiredPosition(t, tc.required), tc.isGTIDBased)
			assert.Equal(t, tc.wantCode, vterrors.Code(err))
		})
	}
}

// TestCheckRequiredPosition checks that a candidate has the position and which positions the error reports.
func TestCheckRequiredPosition(t *testing.T) {
	t.Run("no candidates report none", func(t *testing.T) {
		err := checkRequiredPosition(requiredPosition(t, requiredHigh), nil)
		require.ErrorContains(t, err, "most advanced received positions: none")
	})

	t.Run("divergent histories name every maximum", func(t *testing.T) {
		divergent := "MySQL56/4e11fa47-71ca-11e1-9e33-c80aa9429562:1-10"
		candidates := map[string]*RelayLogPositions{
			"a":         {Combined: requiredPosition(t, requiredHigh)},
			"b":         {Combined: requiredPosition(t, divergent)},
			"dominated": {Combined: requiredPosition(t, requiredLow)},
		}

		err := checkRequiredPosition(requiredPosition(t, requiredMissing), candidates)
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		assert.EqualError(t, err, "no candidate received required position "+requiredMissing+": most advanced received positions: a="+requiredHigh+", b="+divergent)
	})

	t.Run("maxima sort by alias", func(t *testing.T) {
		// Five incomparable histories. Map iteration order is random, and a
		// three entry map lands sorted by chance half the time.
		candidates := map[string]*RelayLogPositions{}
		var want []string
		for _, alias := range []string{"e", "c", "a", "d", "b"} {
			position := "MySQL56/" + alias + alias + "11fa47-71ca-11e1-9e33-c80aa9429562:1-10"
			candidates[alias] = &RelayLogPositions{Combined: requiredPosition(t, position)}
			want = append(want, alias+"="+position)
		}
		slices.Sort(want)

		assert.Equal(t, want, mostAdvancedReceivedPositions(candidates))
	})

	t.Run("zero positions report <zero>", func(t *testing.T) {
		candidates := map[string]*RelayLogPositions{"a": {}, "b": {}}
		err := checkRequiredPosition(requiredPosition(t, requiredHigh), candidates)
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		assert.ErrorContains(t, err, "a=<zero>, b=<zero>")
	})

	t.Run("GTID uses Combined rather than executed", func(t *testing.T) {
		candidates := map[string]*RelayLogPositions{"a": {
			Executed: requiredPosition(t, requiredLow), Combined: requiredPosition(t, requiredHigh),
		}}
		require.NoError(t, checkRequiredPosition(requiredPosition(t, requiredHigh), candidates))
	})
}

// requiredPositionFixture holds an ERS setup with a lagging replica at index 0
// and an advanced replica at index 1.
type requiredPositionFixture struct {
	t       *testing.T
	erp     *EmergencyReparenter
	tmc     *tmcmock.MockTabletManagerClient
	opts    EmergencyReparentOptions
	tablets []*topodatapb.Tablet

	// applied records the tablets whose WaitForPosition succeeded. The
	// reparent journal count of a tablet only advances once it applied.
	applied map[string]bool

	behind   string
	advanced replicaPositions
}

// newRequiredPositionFixtureOptions holds the replica positions of a fixture.
type newRequiredPositionFixtureOptions struct {
	// behind is the executed and relay log position of the lagging replica.
	behind string

	// applied is the executed position of the advanced replica.
	applied string

	// received is the relay log position of the advanced replica.
	received string

	// required is the position ERS must require.
	required string
}

// newRequiredPositionFixture builds two replicas and a mock tablet manager
// client with no expectations. Each test declares the RPCs it allows.
func newRequiredPositionFixture(t *testing.T, opts newRequiredPositionFixtureOptions) *requiredPositionFixture {
	t.Helper()

	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "zone1")
	t.Cleanup(ts.Close)
	testutil.AddShards(ctx, t, ts, &vtctldatapb.Shard{Keyspace: "ks", Name: "0", Shard: &topodatapb.Shard{}})
	tablets := []*topodatapb.Tablet{
		{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101}, Keyspace: "ks", Shard: "0", Type: topodatapb.TabletType_REPLICA},
		{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 102}, Keyspace: "ks", Shard: "0", Type: topodatapb.TabletType_REPLICA},
	}
	testutil.AddTablets(ctx, t, ts, nil, tablets...)
	reparenttestutil.SetKeyspaceDurability(ctx, t, ts, "ks", policy.DurabilityNone)

	tmc := tmcmock.NewMockTabletManagerClient(gomock.NewController(t))

	return &requiredPositionFixture{
		t:        t,
		erp:      NewEmergencyReparenter(ts, tmc, logutil.NewMemoryLogger()),
		tmc:      tmc,
		opts:     EmergencyReparentOptions{RequiredPosition: requiredPosition(t, opts.required), WaitReplicasTimeout: 30 * time.Second},
		tablets:  tablets,
		applied:  map[string]bool{},
		behind:   opts.behind,
		advanced: replicaPositions{executed: opts.applied, relay: opts.received},
	}
}

// replicaPositions holds the executed and relay log positions of one replica.
type replicaPositions struct {
	executed string
	relay    string
}

// stopStatus returns the stopped replication status of a replica.
func stopStatus(positions replicaPositions) *replicationdatapb.StopReplicationStatus {
	after := &replicationdatapb.Status{Position: positions.executed, SourceUuid: requiredUUID}
	if strings.HasPrefix(positions.relay, "FilePos/") {
		after.RelayLogSourceBinlogEquivalentPosition = positions.relay
	} else {
		after.RelayLogPosition = positions.relay
	}

	return &replicationdatapb.StopReplicationStatus{
		Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
		After:  after,
	}
}

// expectNoStops fails the test on any StopReplicationAndGetStatus call. It
// returns an error so ERS aborts instead of continuing on a stub.
func (f *requiredPositionFixture) expectNoStops() {
	f.tmc.EXPECT().StopReplicationAndGetStatus(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, tablet *topodatapb.Tablet, _ replicationdatapb.StopReplicationMode) (*replicationdatapb.StopReplicationStatus, error) {
			f.t.Errorf("unexpected StopReplicationAndGetStatus on %s", topoproto.TabletAliasString(tablet.Alias))
			return nil, assert.AnError
		}).
		AnyTimes()
}

// expectStops allows StopReplicationAndGetStatus on both replicas and returns
// their positions.
func (f *requiredPositionFixture) expectStops() {
	f.tmc.EXPECT().StopReplicationAndGetStatus(gomock.Any(), tabletAliasMatcher(requiredBehindAlias), gomock.Any()).
		Return(stopStatus(replicaPositions{executed: f.behind, relay: f.behind}), nil)
	f.tmc.EXPECT().StopReplicationAndGetStatus(gomock.Any(), tabletAliasMatcher(requiredAdvancedAlias), gomock.Any()).
		Return(stopStatus(f.advanced), nil)
}

// expectWaits allows n WaitForPosition calls, each of which succeeds and
// marks the tablet as applied. A call past n fails the test and returns an
// error so ERS aborts instead of waiting on a stub. gomock's own Times check
// cannot be used here. It calls Fatalf from the ERS goroutine, which never
// returns, and ERS then waits on it until the test timeout.
func (f *requiredPositionFixture) expectWaits(n int) {
	var calls int
	f.tmc.EXPECT().WaitForPosition(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, tablet *topodatapb.Tablet, _ string) error {
			calls++
			if calls > n {
				f.t.Errorf("WaitForPosition call %d on %s, want at most %d", calls, topoproto.TabletAliasString(tablet.Alias), n)
				return assert.AnError
			}

			f.applied[topoproto.TabletAliasString(tablet.Alias)] = true
			return nil
		}).
		AnyTimes()
	f.t.Cleanup(func() {
		assert.Equal(f.t, n, calls, "WaitForPosition calls")
	})
}

// expectJournal answers ReadReparentJournalInfo with before for a tablet that
// has not applied its relay logs and after once it has. A tablet absent from
// after keeps its before count.
func (f *requiredPositionFixture) expectJournal(before, after map[string]int32) {
	f.tmc.EXPECT().ReadReparentJournalInfo(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, tablet *topodatapb.Tablet) (int32, error) {
			alias := topoproto.TabletAliasString(tablet.Alias)
			if count, ok := after[alias]; ok && f.applied[alias] {
				return count, nil
			}

			return before[alias], nil
		}).
		AnyTimes()
}

// expectPromotion allows the RPCs of a successful promotion and repointing.
func (f *requiredPositionFixture) expectPromotion() {
	f.tmc.EXPECT().PromoteReplica(gomock.Any(), gomock.Any(), gomock.Any()).Return(f.advanced.relay, nil).AnyTimes()
	f.tmc.EXPECT().InitPrimary(gomock.Any(), gomock.Any(), gomock.Any()).Return(f.advanced.relay, nil).AnyTimes()
	f.tmc.EXPECT().PrimaryPosition(gomock.Any(), gomock.Any()).Return(f.advanced.relay, nil).AnyTimes()
	f.tmc.EXPECT().PopulateReparentJournal(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	f.tmc.EXPECT().SetReplicationSource(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	f.tmc.EXPECT().StartReplication(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
}

// TestERSRequiredPositionPromotesReceiver checks that ERS promotes the
// candidate that received the required position.
func TestERSRequiredPositionPromotesReceiver(t *testing.T) {
	fixture := newRequiredPositionFixture(t, newRequiredPositionFixtureOptions{
		behind: requiredLow, applied: requiredLow, received: requiredHigh, required: requiredHigh,
	})
	fixture.expectStops()
	fixture.expectWaits(1)
	fixture.expectJournal(nil, nil)
	fixture.expectPromotion()

	ev, err := fixture.erp.ReparentShard(t.Context(), "ks", "0", fixture.opts)
	require.NoError(t, err)
	assert.Equal(t, requiredAdvancedAlias, topoproto.TabletAliasString(ev.NewPrimary.Alias))
}

// TestERSRequiredPositionRequestedPrimaryCatchesUp checks that the requirement
// is on any candidate, not on the requested primary. A requested primary behind
// the position catches up to the candidate that has it and is promoted.
func TestERSRequiredPositionRequestedPrimaryCatchesUp(t *testing.T) {
	fixture := newRequiredPositionFixture(t, newRequiredPositionFixtureOptions{
		behind: requiredLow, applied: requiredLow, received: requiredHigh, required: requiredHigh,
	})
	fixture.opts.NewPrimaryAlias = fixture.tablets[0].Alias
	fixture.expectStops()
	// One relay log wait on the advanced replica, then one catch up wait on
	// the requested primary.
	fixture.expectWaits(2)
	fixture.expectJournal(nil, nil)
	fixture.expectPromotion()

	ev, err := fixture.erp.ReparentShard(t.Context(), "ks", "0", fixture.opts)
	require.NoError(t, err)
	assert.Equal(t, requiredBehindAlias, topoproto.TabletAliasString(ev.NewPrimary.Alias))
}

// TestERSRequiredPositionFailsBeforeAnyWait checks that ERS fails before any
// relay log wait when no candidate received the required position.
func TestERSRequiredPositionFailsBeforeAnyWait(t *testing.T) {
	fixture := newRequiredPositionFixture(t, newRequiredPositionFixtureOptions{
		behind: requiredLow, applied: requiredLow, received: requiredHigh, required: requiredMissing,
	})
	fixture.expectStops()
	fixture.expectWaits(0)
	fixture.tmc.EXPECT().StartReplication(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	_, err := fixture.erp.ReparentShard(t.Context(), "ks", "0", fixture.opts)
	require.ErrorContains(t, err, "no candidate received required position "+requiredMissing)
	require.ErrorContains(t, err, requiredAdvancedAlias+"="+requiredHigh)
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
}

// TestERSRequiredPositionFailsAfterSelection checks the checks that run after
// a step removed candidates.
func TestERSRequiredPositionFailsAfterSelection(t *testing.T) {
	t.Run("split brain override drops the only candidate that received it", func(t *testing.T) {
		divergent := "MySQL56/4e11fa47-71ca-11e1-9e33-c80aa9429562:1-10"
		fixture := newRequiredPositionFixture(t, newRequiredPositionFixtureOptions{
			behind: divergent, applied: requiredHigh, received: requiredHigh, required: requiredHigh,
		})
		fixture.opts.NewPrimaryAlias = fixture.tablets[0].Alias
		fixture.opts.AllowSplitBrainPromotion = true
		fixture.expectStops()
		fixture.expectWaits(0)
		fixture.tmc.EXPECT().StartReplication(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		_, err := fixture.erp.ReparentShard(t.Context(), "ks", "0", fixture.opts)
		require.ErrorContains(t, err, "most advanced received positions: "+requiredBehindAlias+"="+divergent)
		require.ErrorContains(t, err, "split-brain override discarded the other leading candidates")
		require.ErrorContains(t, err, requiredAdvancedAlias)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	})

	t.Run("errant GTID detection removes the only candidate that received it", func(t *testing.T) {
		fixture := newRequiredPositionFixture(t, newRequiredPositionFixtureOptions{
			behind: requiredLow, applied: requiredHigh, received: requiredHigh, required: requiredHigh,
		})
		fixture.expectStops()
		// One relay log wait on the advanced replica. The lagging replica has the
		// newer journal entry, and detection then removes the advanced one. ERS
		// must fail before the rescue wait on the survivor.
		fixture.expectWaits(1)
		fixture.expectJournal(map[string]int32{requiredBehindAlias: 2, requiredAdvancedAlias: 1}, nil)
		fixture.tmc.EXPECT().StartReplication(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		_, err := fixture.erp.ReparentShard(t.Context(), "ks", "0", fixture.opts)
		require.ErrorContains(t, err, "most advanced received positions: "+requiredBehindAlias+"="+requiredLow)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	})

	t.Run("second errant GTID detection removes the only candidate that received it", func(t *testing.T) {
		fixture := newRequiredPositionFixture(t, newRequiredPositionFixtureOptions{
			behind: requiredLow, applied: requiredHigh, received: requiredHigh, required: requiredHigh,
		})
		// The extra transactions of the advanced replica come from a foreign
		// source UUID, and the lagging replica reaches journal count 2 only
		// after its rescue wait. The second detection pass then removes the
		// advanced replica.
		fixture.advanced.executed = requiredHigh
		fixture.tmc.EXPECT().StopReplicationAndGetStatus(gomock.Any(), tabletAliasMatcher(requiredBehindAlias), gomock.Any()).
			Return(stopStatus(replicaPositions{executed: requiredLow, relay: requiredLow}), nil)
		foreign := stopStatus(fixture.advanced)
		foreign.After.SourceUuid = "4e11fa47-71ca-11e1-9e33-c80aa9429562"
		fixture.tmc.EXPECT().StopReplicationAndGetStatus(gomock.Any(), tabletAliasMatcher(requiredAdvancedAlias), gomock.Any()).
			Return(foreign, nil)
		// One relay log wait on the advanced replica, then the rescue wait on the
		// lagging one.
		fixture.expectWaits(2)
		fixture.expectJournal(map[string]int32{requiredBehindAlias: 1, requiredAdvancedAlias: 2}, map[string]int32{requiredBehindAlias: 2})
		fixture.tmc.EXPECT().StartReplication(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		_, err := fixture.erp.ReparentShard(t.Context(), "ks", "0", fixture.opts)
		require.ErrorContains(t, err, "most advanced received positions: "+requiredBehindAlias+"="+requiredLow)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	})
}

// TestERSRequiredPositionRejectsUnsupportedShards checks that a required
// position on a non GTID shard fails before any relay log wait. The per flavor
// rules are covered by TestValidateRequiredPosition.
func TestERSRequiredPositionRejectsUnsupportedShards(t *testing.T) {
	const filePos = "FilePos/mysql-bin.000001:20"
	fixture := newRequiredPositionFixture(t, newRequiredPositionFixtureOptions{
		behind: filePos, applied: filePos, received: filePos, required: requiredHigh,
	})
	fixture.expectStops()
	fixture.expectWaits(0)
	fixture.tmc.EXPECT().StartReplication(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	_, err := fixture.erp.ReparentShard(t.Context(), "ks", "0", fixture.opts)
	require.ErrorContains(t, err, "required position is only supported on MySQL GTID shards")
	assert.Equal(t, vtrpcpb.Code_INVALID_ARGUMENT, vterrors.Code(err))
}

// TestERSRequiredPositionRejectsUnsupportedFlavors checks that a required
// position that is not a MySQL56 position fails before replication is stopped.
// The per flavor rules are covered by TestValidateRequiredPositionFlavor.
func TestERSRequiredPositionRejectsUnsupportedFlavors(t *testing.T) {
	const mariadb = "MariaDB/0-1-20"
	fixture := newRequiredPositionFixture(t, newRequiredPositionFixtureOptions{
		behind: requiredLow, applied: requiredHigh, received: requiredHigh, required: mariadb,
	})
	fixture.expectNoStops()

	_, err := fixture.erp.ReparentShard(t.Context(), "ks", "0", fixture.opts)
	require.ErrorContains(t, err, "required position must be a MySQL56 GTID position, got "+mariadb)
	assert.Equal(t, vtrpcpb.Code_INVALID_ARGUMENT, vterrors.Code(err))
}
