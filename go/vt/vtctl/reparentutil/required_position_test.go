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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
)

const (
	// requiredUUID is the source UUID of the shared history.
	requiredUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	// requiredLow is the position of the lagging replica.
	requiredLow = "MySQL56/" + requiredUUID + ":1-10"
	// requiredHigh holds transactions the lagging replica lacks.
	requiredHigh = "MySQL56/" + requiredUUID + ":1-20"
	// requiredMissing holds transactions no replica has.
	requiredMissing = "MySQL56/" + requiredUUID + ":1-30"
	// requiredBehindAlias identifies the lagging replica.
	requiredBehindAlias = "zone1-0000000101"
	// requiredAdvancedAlias identifies the advanced replica.
	requiredAdvancedAlias = "zone1-0000000102"
)

// requiredPosition decodes encoded and stops the test on error.
func requiredPosition(t *testing.T, encoded string) replication.Position {
	t.Helper()
	position, err := replication.DecodePosition(encoded)
	require.NoError(t, err)
	return position
}

// TestCheckRequiredPosition checks receipt and the reported received positions.
func TestCheckRequiredPosition(t *testing.T) {
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

	t.Run("former primary uses Combined", func(t *testing.T) {
		candidates := map[string]*RelayLogPositions{"former-primary": {Combined: requiredPosition(t, requiredHigh)}}
		require.NoError(t, checkRequiredPosition(requiredPosition(t, requiredHigh), candidates))

		err := checkRequiredPosition(requiredPosition(t, requiredMissing), candidates)
		require.Error(t, err)
		assert.ErrorContains(t, err, "former-primary="+requiredHigh)
	})
}

// requiredPositionClient counts WaitForPosition calls.
type requiredPositionClient struct {
	// TabletManagerClient answers every other RPC.
	*testutil.TabletManagerClient
	// waits counts WaitForPosition calls.
	waits atomic.Int32
}

// WaitForPosition counts the call and returns the fake result.
func (client *requiredPositionClient) WaitForPosition(ctx context.Context, tablet *topodatapb.Tablet, position string) error {
	client.waits.Add(1)
	return client.TabletManagerClient.WaitForPosition(ctx, tablet, position)
}

// requiredPositionFixture holds an ERS setup with two replicas.
type requiredPositionFixture struct {
	// erp runs the reparent.
	erp *EmergencyReparenter
	// client answers RPCs and counts waits.
	client *requiredPositionClient
	// opts holds the required position and the wait timeout.
	opts EmergencyReparentOptions
	// tablets holds the lagging replica, then the advanced replica.
	tablets []*topodatapb.Tablet
}

// newRequiredPositionFixtureOptions holds the inputs of newRequiredPositionFixture.
type newRequiredPositionFixtureOptions struct {
	// t owns assertions and cleanup for the fixture.
	t *testing.T
	// behind is the executed and relay log position of the lagging replica.
	behind string
	// applied is the executed position of the advanced replica.
	applied string
	// received is the relay log position of the advanced replica.
	received string
	// required is the RequiredPosition option.
	required string
}

// newRequiredPositionFixture builds two replicas whose RPCs let any promotion succeed.
func newRequiredPositionFixture(opts newRequiredPositionFixtureOptions) *requiredPositionFixture {
	opts.t.Helper()

	t := opts.t
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
	fake := &testutil.TabletManagerClient{
		StopReplicationAndGetStatusResults: map[string]struct {
			// StopStatus supplies the stopped positions.
			StopStatus *replicationdatapb.StopReplicationStatus
			// Error supplies the RPC error.
			Error error
		}{},
		WaitForPositionResults:         map[string]map[string]error{},
		SetReplicationSourceResults:    map[string]error{},
		StartReplicationResults:        map[string]error{},
		PopulateReparentJournalResults: map[string]error{},
		PromoteReplicaResults: map[string]struct {
			// Result supplies the promotion position.
			Result string
			// Error supplies the RPC error.
			Error error
		}{},
		PrimaryPositionResults: map[string]struct {
			// Position supplies the catch-up target.
			Position string
			// Error supplies the RPC error.
			Error error
		}{},
	}
	for i, tablet := range tablets {
		alias := topoproto.TabletAliasString(tablet.Alias)

		executed, relay := opts.behind, opts.behind
		if i == 1 {
			executed, relay = opts.applied, opts.received
		}

		after := &replicationdatapb.Status{Position: executed, SourceUuid: requiredUUID}
		if strings.HasPrefix(relay, "FilePos/") {
			after.RelayLogSourceBinlogEquivalentPosition = relay
		} else {
			after.RelayLogPosition = relay
		}

		stopResult := fake.StopReplicationAndGetStatusResults[alias]
		stopResult.StopStatus = &replicationdatapb.StopReplicationStatus{
			Before: &replicationdatapb.Status{IoState: int32(replication.ReplicationStateRunning), SqlState: int32(replication.ReplicationStateRunning)},
			After:  after,
		}
		fake.StopReplicationAndGetStatusResults[alias] = stopResult

		fake.WaitForPositionResults[alias] = map[string]error{relay: nil, opts.received: nil}
		fake.SetReplicationSourceResults[alias] = nil
		fake.StartReplicationResults[alias] = nil
		fake.PopulateReparentJournalResults[alias] = nil

		promoteResult := fake.PromoteReplicaResults[alias]
		promoteResult.Result = opts.received
		fake.PromoteReplicaResults[alias] = promoteResult

		primaryResult := fake.PrimaryPositionResults[alias]
		primaryResult.Position = opts.received
		fake.PrimaryPositionResults[alias] = primaryResult
	}

	fake.InitPrimaryResults = fake.PromoteReplicaResults
	client := &requiredPositionClient{TabletManagerClient: fake}

	return &requiredPositionFixture{
		erp:     NewEmergencyReparenter(ts, client, logutil.NewMemoryLogger()),
		client:  client,
		opts:    EmergencyReparentOptions{RequiredPosition: requiredPosition(t, opts.required), WaitAllTablets: true, WaitReplicasTimeout: 30 * time.Second},
		tablets: tablets,
	}
}

// TestERSRequiredPositionFailsBeforeAnyWait checks that ERS fails before any
// WaitForPosition call when no candidate received the position.
func TestERSRequiredPositionFailsBeforeAnyWait(t *testing.T) {
	fixture := newRequiredPositionFixture(newRequiredPositionFixtureOptions{
		t: t, behind: requiredLow, applied: requiredLow, received: requiredHigh, required: requiredMissing,
	})

	_, err := fixture.erp.ReparentShard(t.Context(), "ks", "0", fixture.opts)
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	require.ErrorContains(t, err, requiredMissing)
	assert.Zero(t, fixture.client.waits.Load())
}

// TestERSRequiredPositionFailsAfterSelection checks that ERS fails when a
// selection step leaves no candidate that received the position.
func TestERSRequiredPositionFailsAfterSelection(t *testing.T) {
	t.Run("split brain override drops the only candidate that received it", func(t *testing.T) {
		divergent := "MySQL56/4e11fa47-71ca-11e1-9e33-c80aa9429562:1-10"
		fixture := newRequiredPositionFixture(newRequiredPositionFixtureOptions{
			t: t, behind: divergent, applied: requiredHigh, received: requiredHigh, required: requiredHigh,
		})

		fixture.opts.NewPrimaryAlias = fixture.tablets[0].Alias
		fixture.opts.AllowSplitBrainPromotion = true
		_, err := fixture.erp.ReparentShard(t.Context(), "ks", "0", fixture.opts)
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		require.ErrorContains(t, err, "most advanced received positions: "+requiredBehindAlias+"="+divergent)
		assert.Zero(t, fixture.client.waits.Load())
	})

	t.Run("errant GTID detection removes the only candidate that received it", func(t *testing.T) {
		fixture := newRequiredPositionFixture(newRequiredPositionFixtureOptions{
			t: t, behind: requiredLow, applied: requiredHigh, received: requiredHigh, required: requiredHigh,
		})

		fixture.client.ReadReparentJournalInfoResults = map[string]int32{requiredBehindAlias: 2, requiredAdvancedAlias: 1}
		_, err := fixture.erp.ReparentShard(t.Context(), "ks", "0", fixture.opts)
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		require.ErrorContains(t, err, "most advanced received positions: "+requiredBehindAlias+"="+requiredLow)
		assert.EqualValues(t, 2, fixture.client.waits.Load(), "check receipt after the surviving candidate applies its relay logs")
	})

	t.Run("requested primary cannot catch up to a position no candidate received", func(t *testing.T) {
		fixture := newRequiredPositionFixture(newRequiredPositionFixtureOptions{
			t: t, behind: requiredLow, applied: requiredLow, received: requiredHigh, required: requiredMissing,
		})

		fixture.opts.NewPrimaryAlias = fixture.tablets[0].Alias

		// Fail even though the operator requested a primary. It cannot catch up
		// to a position no candidate received.
		_, err := fixture.erp.ReparentShard(t.Context(), "ks", "0", fixture.opts)
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		assert.Zero(t, fixture.client.waits.Load())
	})

	t.Run("second errant GTID detection removes the only candidate that received it", func(t *testing.T) {
		fixture := newRequiredPositionFixture(newRequiredPositionFixtureOptions{
			t: t, behind: requiredLow, applied: requiredHigh, received: requiredHigh, required: requiredHigh,
		})

		// Make the second detection pass flag the leader. The skipped replica
		// reaches journal count 2 after its rescue wait, and the extra
		// transactions of the leader come from a foreign source UUID.
		fixture.client.StopReplicationAndGetStatusResults[requiredAdvancedAlias].StopStatus.After.SourceUuid = "4e11fa47-71ca-11e1-9e33-c80aa9429562"
		fixture.client.ReadReparentJournalInfoResults = map[string]int32{requiredBehindAlias: 1, requiredAdvancedAlias: 2}
		fixture.client.ReadReparentJournalInfoAfterApplyResults = map[string]int32{requiredBehindAlias: 2}
		_, err := fixture.erp.ReparentShard(t.Context(), "ks", "0", fixture.opts)
		require.Error(t, err)
		assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
		require.ErrorContains(t, err, "most advanced received positions: "+requiredBehindAlias+"="+requiredLow)
		assert.EqualValues(t, 2, fixture.client.waits.Load(), "check receipt after the rescue wait and second detection pass")
	})
}

// TestERSRequiredPositionRejectsUnsupportedFlavors checks that unsupported
// shards and required positions fail before any relay log wait.
func TestERSRequiredPositionRejectsUnsupportedFlavors(t *testing.T) {
	for _, tc := range []struct {
		// name identifies the unsupported input.
		name string
		// position supplies all shard positions.
		position string
		// required supplies the required position.
		required string
	}{
		{name: "non GTID shard", position: "FilePos/mysql-bin.000001:20", required: "FilePos/mysql-bin.000001:20"},
		{name: "MariaDB required position on MySQL56 shard", position: requiredHigh, required: "MariaDB/0-1-20"},
		{name: "MySQL56 required position on MariaDB shard", position: "MariaDB/0-1-20", required: requiredHigh},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newRequiredPositionFixture(newRequiredPositionFixtureOptions{
				t: t, behind: tc.position, applied: tc.position, received: tc.position, required: tc.required,
			})

			_, err := fixture.erp.ReparentShard(t.Context(), "ks", "0", fixture.opts)
			require.Error(t, err)
			assert.Equal(t, vtrpcpb.Code_INVALID_ARGUMENT, vterrors.Code(err))
			assert.ErrorContains(t, err, "required position is only supported on MySQL GTID shards")
			assert.Zero(t, fixture.client.waits.Load())
		})
	}
}
