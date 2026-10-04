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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/mysqlctl"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
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
