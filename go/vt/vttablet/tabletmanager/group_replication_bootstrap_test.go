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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// startRequest returns a StartGroupReplication request that bootstraps the group or, with bootstrap
// unset, joins it, without any check.
func startRequest(bootstrap bool) *tabletmanagerdatapb.StartGroupReplicationRequest {
	return &tabletmanagerdatapb.StartGroupReplicationRequest{Bootstrap: bootstrap}
}

const bootstrapTestGroupUUID = "6f1c2c2e-5a8e-4b8e-9d3a-7c1f0b6e2a41"

// setExecutedAndReceived sets what the fake MySQL executed, and what it received from its last group
// into its relay log.
func setExecutedAndReceived(t *testing.T, fmd *mysqlctl.FakeMysqlDaemon, executed, received string) {
	t.Helper()
	set, err := replication.ParseMysql56GTIDSet(executed)
	require.NoError(t, err)
	fmd.SetPrimaryPositionLocked(replication.Position{GTIDSet: set})
	status := tmStatus(t, fmd)
	status.ReceivedTransactionSet = received
	fmd.SetGroupReplicationStatus(status)
}

func executedSet(t *testing.T, fmd *mysqlctl.FakeMysqlDaemon) string {
	t.Helper()
	return fmd.GetPrimaryPositionLocked().GTIDSet.String()
}

// TestBootstrapAppliesRelayLogBeforeStart checks that a bootstrap whose caller requires transactions
// that MySQL holds only in the relay log of its group_replication_applier channel applies that relay
// log first, so that they are in its binlog before MySQL's START: a restart of mysqld after the check
// can no longer discard them (relay_log_recovery). MySQL's bootstrap would apply them too, but only
// if mysqld did not restart since VTOrc read its status.
func TestBootstrapAppliesRelayLogBeforeStart(t *testing.T) {
	withGroupReplication(t)
	tm, fmd, _, _ := newLegitimacyTestTM(t)
	fmd.StartGroupReplicationError = nil
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}
	setExecutedAndReceived(t, fmd, bootstrapTestGroupUUID+":1-10", bootstrapTestGroupUUID+":1-15")

	status, err := tm.StartGroupReplication(t.Context(), &tabletmanagerdatapb.StartGroupReplicationRequest{
		Bootstrap:       true,
		RequiredGtidSet: bootstrapTestGroupUUID + ":1-15",
	})
	require.NoError(t, err)
	assert.True(t, mysql.IsGroupPrimary(status))
	assert.True(t, fmd.GroupReplicationBootstrapped)
	assert.Equal(t, 1, fmd.ApplyGroupReplicationRelayLogCalls)
	assert.Equal(t, bootstrapTestGroupUUID+":1-15", executedSet(t, fmd), "the relay log must be applied before the START")
}

// TestBootstrapRefusesWithoutRequiredTransactions reproduces the interleaving that the TLA+ model in
// doc/design-docs/group_replication_tla found: VTOrc chose a voter that held acknowledged
// transactions only in its relay log, and its mysqld restarted before the bootstrap's START, which
// discarded the relay log (relay_log_recovery). The tablet must refuse the bootstrap with
// FAILED_PRECONDITION, rather than create a group without them, and must not start MySQL's group.
func TestBootstrapRefusesWithoutRequiredTransactions(t *testing.T) {
	withGroupReplication(t)
	tm, fmd, _, _ := newLegitimacyTestTM(t)
	fmd.StartGroupReplicationError = nil
	setExecutedAndReceived(t, fmd, bootstrapTestGroupUUID+":1-10", "")

	_, err := tm.StartGroupReplication(t.Context(), &tabletmanagerdatapb.StartGroupReplicationRequest{
		Bootstrap:       true,
		RequiredGtidSet: bootstrapTestGroupUUID + ":1-15",
	})
	requireCode(t, err, vtrpcpb.Code_FAILED_PRECONDITION)
	require.ErrorContains(t, err, bootstrapTestGroupUUID+":11-15")
	start, _, _ := fmd.GroupReplicationCalls()
	assert.Zero(t, start, "MySQL must not start group replication")
	assert.False(t, fmd.GroupReplicationBootstrapped)
	assert.Zero(t, fmd.ApplyGroupReplicationRelayLogCalls, "nothing in the relay log to apply")

	// Without a required set, the bootstrap is not checked: the migration and InitPrimary bootstrap
	// the group on the shard primary, which has every transaction.
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}
	_, err = tm.StartGroupReplication(t.Context(), startRequest(true))
	require.NoError(t, err)
	assert.True(t, fmd.GroupReplicationBootstrapped)
}

// TestBootstrapRefusesWhenRelayLogIsNotApplied checks that a bootstrap is refused when MySQL's
// applier does not apply the relay log in time: the transactions are still only in the relay log.
func TestBootstrapRefusesWhenRelayLogIsNotApplied(t *testing.T) {
	withGroupReplication(t)
	tm, fmd, _, _ := newLegitimacyTestTM(t)
	fmd.StartGroupReplicationError = nil
	setExecutedAndReceived(t, fmd, bootstrapTestGroupUUID+":1-10", bootstrapTestGroupUUID+":1-15")
	fmd.ApplyGroupReplicationRelayLogError = assert.AnError

	_, err := tm.StartGroupReplication(t.Context(), &tabletmanagerdatapb.StartGroupReplicationRequest{
		Bootstrap:       true,
		RequiredGtidSet: bootstrapTestGroupUUID + ":1-15",
	})
	requireCode(t, err, vtrpcpb.Code_FAILED_PRECONDITION)
	start, _, _ := fmd.GroupReplicationCalls()
	assert.Zero(t, start)
	assert.Equal(t, 1, fmd.ApplyGroupReplicationRelayLogCalls)
}

// TestBootstrapRefusalReachesCallerWhenRelayLogApplyStalls checks that a bootstrap whose relay log
// MySQL does not apply still refuses before its caller's deadline. VTOrc withdraws the bootstrap
// intent only on a definitive refusal that reaches it; with the apply bounded only by
// groupReplicationRelayLogApplyTimeout, which equals VTOrc's RPC timeout, the refusal came after
// VTOrc gave up, and the intent fenced every other voter until it expired.
func TestBootstrapRefusalReachesCallerWhenRelayLogApplyStalls(t *testing.T) {
	withGroupReplication(t)
	oldReserve := groupReplicationRefusalReserve
	t.Cleanup(func() { groupReplicationRefusalReserve = oldReserve })
	// The caller's deadline equals groupReplicationRelayLogApplyTimeout, as VTOrc's does; the
	// reserve leaves the apply one second of it.
	groupReplicationRefusalReserve = groupReplicationRelayLogApplyTimeout - time.Second
	tm, fmd, _, _ := newLegitimacyTestTM(t)
	fmd.StartGroupReplicationError = nil
	setExecutedAndReceived(t, fmd, bootstrapTestGroupUUID+":1-10", bootstrapTestGroupUUID+":1-15")
	fmd.ApplyGroupReplicationRelayLogBlocks = true
	ctx, cancel := context.WithTimeout(t.Context(), groupReplicationRelayLogApplyTimeout)
	defer cancel()

	_, err := tm.StartGroupReplication(ctx, &tabletmanagerdatapb.StartGroupReplicationRequest{
		Bootstrap:       true,
		RequiredGtidSet: bootstrapTestGroupUUID + ":1-15",
	})
	require.NoError(t, ctx.Err(), "the refusal must return before the caller's deadline")
	requireCode(t, err, vtrpcpb.Code_FAILED_PRECONDITION)
	assert.True(t, tmclient.IsGroupBootstrapRefused(err), "%v", err)
	assert.Equal(t, 1, fmd.ApplyGroupReplicationRelayLogCalls)
	start, _, _ := fmd.GroupReplicationCalls()
	assert.Zero(t, start)
}

// TestRelayLogApplyTimeoutKeepsRefusalReserve checks the bound on the relay-log apply.
func TestRelayLogApplyTimeoutKeepsRefusalReserve(t *testing.T) {
	assert.Equal(t, groupReplicationRelayLogApplyTimeout, relayLogApplyTimeout(t.Context()), "no deadline")
	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	defer cancel()
	assert.Equal(t, groupReplicationRelayLogApplyTimeout, relayLogApplyTimeout(ctx), "a distant deadline")
	ctx, cancel = context.WithTimeout(t.Context(), groupReplicationRelayLogApplyTimeout)
	defer cancel()
	timeout := relayLogApplyTimeout(ctx)
	assert.LessOrEqual(t, timeout, groupReplicationRelayLogApplyTimeout-groupReplicationRefusalReserve)
	assert.Positive(t, timeout)
	ctx, cancel = context.WithTimeout(t.Context(), groupReplicationRefusalReserve/2)
	defer cancel()
	assert.LessOrEqual(t, relayLogApplyTimeout(ctx), time.Duration(0), "less than the reserve remains")
}

// TestStartGroupReplicationRejectsInvalidRequiredSet checks that a required set that is not a MySQL
// GTID set is refused before anything changes.
func TestStartGroupReplicationRejectsInvalidRequiredSet(t *testing.T) {
	withGroupReplication(t)
	tm, fmd, _, _ := newLegitimacyTestTM(t)
	_, err := tm.StartGroupReplication(t.Context(), &tabletmanagerdatapb.StartGroupReplicationRequest{
		Bootstrap:       true,
		RequiredGtidSet: "not a gtid set",
	})
	requireCode(t, err, vtrpcpb.Code_INVALID_ARGUMENT)
	start, _, _ := fmd.GroupReplicationCalls()
	assert.Zero(t, start)
}

// setBootstrapIntentToken records a bootstrap intent of tablet cell1-1, with the given token, in the
// shard record of ks/0.
func setBootstrapIntentToken(t *testing.T, ts *topo.Server, token string) {
	t.Helper()
	_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error {
		si.GroupReplicationBootstrapIntent = &topodatapb.GroupReplicationBootstrapIntent{
			Target:              &topodatapb.TabletAlias{Cell: "cell1", Uid: 1},
			Time:                protoutil.TimeToProto(time.Now()),
			PreviousIncarnation: si.GroupReplicationIncarnation,
			Token:               token,
		}
		return nil
	})
	require.NoError(t, err)
}

func intentRequest(token, expectedIncarnation string) *tabletmanagerdatapb.StartGroupReplicationRequest {
	return &tabletmanagerdatapb.StartGroupReplicationRequest{Bootstrap: true, BootstrapIntentToken: token, ExpectedIncarnation: expectedIncarnation}
}

// TestBootstrapRefusesSupersededIntent reproduces the TLA+ model's integrated simulation: a VTOrc's
// bootstrap RPC waited for the tablet's action lock while another VTOrc, after the first one's shard
// lock expired, replaced its intent, bootstrapped this tablet and recorded the group, which then lost
// its majority again. The stale RPC must not bootstrap a second group from the recorded incarnation.
// The tablet refuses a bootstrap whose intent the shard record no longer holds, or that was recorded
// for another incarnation, before it changes anything: a PRIMARY tablet keeps serving.
func TestBootstrapRefusesSupersededIntent(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _, ts := newLegitimacyTestTM(t)
	fmd.StartGroupReplicationError = nil
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	require.True(t, qsc.IsServing())
	setBootstrapIntentToken(t, ts, "token-2")

	for _, req := range []*tabletmanagerdatapb.StartGroupReplicationRequest{
		intentRequest("token-1", "1780000001"), // another VTOrc replaced the intent
		intentRequest("token-2", "1780000000"), // the intent was recorded for another incarnation
	} {
		_, err := tm.StartGroupReplication(ctx, req)
		requireCode(t, err, vtrpcpb.Code_FAILED_PRECONDITION)
		start, stop, _ := fmd.GroupReplicationCalls()
		assert.Zero(t, start, "MySQL must not start group replication")
		assert.Zero(t, stop)
		assert.True(t, qsc.IsServing(), "a refused bootstrap must not stop serving")
	}

	// The request of the shard record's intent bootstraps the group.
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}
	status, err := tm.StartGroupReplication(ctx, intentRequest("token-2", "1780000001"))
	require.NoError(t, err)
	assert.True(t, mysql.IsGroupPrimary(status))
	assert.True(t, fmd.GroupReplicationBootstrapped)
}

// TestBootstrapOfSupersededIntentDoesNotStopNewerStart checks that a bootstrap whose intent is
// superseded while it waits for a START GROUP_REPLICATION in progress does not stop that START: it
// may be the bootstrap of the newer intent, whose RPC gave up, and the STOP would make MySQL leave
// the group that VTOrc is to adopt.
func TestBootstrapOfSupersededIntentDoesNotStopNewerStart(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _, ts := newLegitimacyTestTM(t)
	fmd.StartGroupReplicationError = nil
	require.NoError(t, fmd.ConfigureGroupReplication(ctx, mysql.GroupReplicationConfig{GroupName: policy.GroupName("ks", "0")}))
	setBootstrapIntentToken(t, ts, "token-1")
	blockedStart(t, fmd)
	// Another VTOrc replaces the intent once the RPC got the action lock and read MySQL's status.
	var replace sync.Once
	fmd.SetGroupReplicationStatusHook(func() {
		replace.Do(func() { setBootstrapIntentToken(t, ts, "token-2") })
	})
	t.Cleanup(func() { fmd.SetGroupReplicationStatusHook(nil) })

	_, err := tm.StartGroupReplication(ctx, intentRequest("token-1", "1780000001"))
	requireCode(t, err, vtrpcpb.Code_FAILED_PRECONDITION)
	_, stop, _ := fmd.GroupReplicationCalls()
	assert.Zero(t, stop, "the START of the newer bootstrap must keep running")
}

// TestBootstrapIntentCheckDoesNotWaitForCutOffTopo checks that a tablet whose topology does not
// answer bootstraps without checking the request's intent, rather than waiting for the topology or
// failing: VTOrc reaches such tablets after a partition, and the check only protects the
// availability of the shard's group, which a bootstrap that cannot run would cost.
func TestBootstrapIntentCheckDoesNotWaitForCutOffTopo(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, f, ts := newCutOffTestTM(t)
	setBootstrapIntentToken(t, ts, "token-2")
	_, err := tm.readShardGroupRecord(ctx, nil)
	require.NoError(t, err)
	durability, err := tm.shardDurability(ctx)
	require.NoError(t, err)
	_, err = tm.groupReplicationConfig(ctx, durability)
	require.NoError(t, err)
	fmd.StartGroupReplicationError = nil
	fmd.ExpectedExecuteSuperQueryList = []string{resetDefaultChannel}
	f.cut()

	rpcCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	start := time.Now()
	_, err = tm.StartGroupReplication(rpcCtx, intentRequest("token-1", "1780000001"))
	require.NoError(t, err)
	assert.True(t, fmd.GroupReplicationBootstrapped)
	assert.Less(t, time.Since(start), 3*groupReplicationTopoReadTimeout+3*time.Second, "the bootstrap must not wait for the topology")
}

// TestBootstrapRefusalForMissingTransactionsIsDefinitive checks which refusals of a bootstrap the
// tablet reports as definitive (tmclient.GroupBootstrapRefusedError), after which VTOrc withdraws the
// bootstrap intent it recorded for the request: only the refusal for a transaction that MySQL lacks,
// decided while no START GROUP_REPLICATION runs, also when the RPC first stopped a START that was in
// progress when it came. A refusal of a superseded intent, whose withdrawal would be a no-op anyway,
// and of a member that is already active, possibly in the group that an earlier RPC of the same intent
// bootstrapped, which VTOrc is to adopt, are not definitive.
func TestBootstrapRefusalForMissingTransactionsIsDefinitive(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _, ts := newLegitimacyTestTM(t)
	fmd.StartGroupReplicationError = nil
	require.NoError(t, fmd.ConfigureGroupReplication(ctx, mysql.GroupReplicationConfig{GroupName: policy.GroupName("ks", "0")}))
	setExecutedAndReceived(t, fmd, bootstrapTestGroupUUID+":1-10", "")
	setBootstrapIntentToken(t, ts, "token-1")
	request := func(token string) *tabletmanagerdatapb.StartGroupReplicationRequest {
		req := intentRequest(token, "1780000001")
		req.RequiredGtidSet = bootstrapTestGroupUUID + ":1-15"
		return req
	}

	_, err := tm.StartGroupReplication(ctx, request("token-1"))
	requireCode(t, err, vtrpcpb.Code_FAILED_PRECONDITION)
	assert.True(t, tmclient.IsGroupBootstrapRefused(err), "MySQL lacks 11-15 and runs no START: %v", err)

	// A superseded intent.
	_, err = tm.StartGroupReplication(ctx, request("token-0"))
	requireCode(t, err, vtrpcpb.Code_FAILED_PRECONDITION)
	assert.False(t, tmclient.IsGroupBootstrapRefused(err), "%v", err)

	// A START that was in progress when the RPC came, the bootstrap of an earlier RPC whose client gave
	// up, ends, and MySQL accepts the STOP that the tablet then issues: nothing runs any more.
	release := blockedStart(t, fmd)
	fmd.StopGroupReplicationHook = func() { release() }
	t.Cleanup(func() { fmd.StopGroupReplicationHook = nil })
	_, err = tm.StartGroupReplication(ctx, request("token-1"))
	requireCode(t, err, vtrpcpb.Code_FAILED_PRECONDITION)
	assert.True(t, tmclient.IsGroupBootstrapRefused(err), "the START ended before the refusal: %v", err)
	assert.Contains(t, err.Error(), bootstrapTestGroupUUID+":11-15")
	start, _, _ := fmd.GroupReplicationCalls()
	assert.Equal(t, 1, start, "only the START of the earlier RPC")

	// MySQL is already an active member: the group of an earlier bootstrap, which VTOrc adopts.
	fmd.StopGroupReplicationHook = nil
	status := tmStatus(t, fmd)
	status.MemberState = mysql.GroupMemberStateOnline
	fmd.SetGroupReplicationStatus(status)
	_, err = tm.StartGroupReplication(ctx, request("token-1"))
	requireCode(t, err, vtrpcpb.Code_FAILED_PRECONDITION)
	assert.False(t, tmclient.IsGroupBootstrapRefused(err), "%v", err)
}

// TestBootstrapRefusalIsNotDefinitiveWhileStartInProgress checks that the tablet does not report a
// refusal as definitive while MySQL runs a START GROUP_REPLICATION, which MySQL lists in its
// processlist for as long as it runs, also after its client gave up: that START can still form a
// group of one. Were VTOrc to withdraw the intent, it could bootstrap another voter next to it, and
// could no longer adopt the group that START forms. MySQL accepts some of the configuration while a
// START runs, so only the processlist tells.
func TestBootstrapRefusalIsNotDefinitiveWhileStartInProgress(t *testing.T) {
	withGroupReplication(t)
	ctx := t.Context()
	tm, fmd, _, ts := newLegitimacyTestTM(t)
	fmd.StartGroupReplicationError = nil
	require.NoError(t, fmd.ConfigureGroupReplication(ctx, mysql.GroupReplicationConfig{GroupName: policy.GroupName("ks", "0")}))
	setExecutedAndReceived(t, fmd, bootstrapTestGroupUUID+":1-10", "")
	status := tmStatus(t, fmd)
	status.StartInProgress = true
	fmd.SetGroupReplicationStatus(status)
	setBootstrapIntentToken(t, ts, "token-1")

	req := intentRequest("token-1", "1780000001")
	req.RequiredGtidSet = bootstrapTestGroupUUID + ":1-15"
	_, err := tm.StartGroupReplication(ctx, req)
	requireCode(t, err, vtrpcpb.Code_FAILED_PRECONDITION)
	assert.Contains(t, err.Error(), bootstrapTestGroupUUID+":11-15")
	assert.False(t, tmclient.IsGroupBootstrapRefused(err), "a START runs: %v", err)
	start, _, _ := fmd.GroupReplicationCalls()
	assert.Zero(t, start)
}
