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

package grouprepl

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vttablet/grpctmclient"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// plantedIntentToken is the token of the bootstrap intent that the test records itself, for a
// bootstrap that VTOrc chose on an earlier pass.
const plantedIntentToken = "planted-by-test"

// TestGroupReplicationWithdrawsRefusedBootstrapIntent reproduces, on MySQL 8.4, the delay that the
// bootstrap intent of a candidate whose mysqld restarted after VTOrc chose it added to an outage,
// and checks that VTOrc now withdraws the intent at once when the candidate refuses definitively.
//
// The group lost its majority while its primary had committed transactions that the candidate
// had received into its relay log, without applying them (FLUSH TABLES WITH READ LOCK holds its
// applier, as in the lab of doc/failover-audit/GroupReplication.md, "Bootstrap candidate"). An
// intent of an earlier pass names the candidate, so VTOrc chooses it again, and sends it the
// bootstrap RPC, which waits for the tablet's action lock (held by SleepTablet). Meanwhile, the
// candidate's mysqld restarts, which discards its relay log (relay_log_recovery). Its tablet then
// refuses the bootstrap: MySQL lacks transactions that the primary holds, and no START
// GROUP_REPLICATION runs. Before the fix, the intent fenced the bootstrap of the primary for two
// minutes; now VTOrc withdraws it, and bootstraps the primary on its next pass.
func TestGroupReplicationWithdrawsRefusedBootstrapIntent(t *testing.T) {
	// Without heartbeats, the primary commits nothing on its own once the candidate left with its
	// backlog: the candidate must hold every transaction the primary executed.
	tc := setupCluster(t, clusterOptions{vtorc: vtorcConfig, replicaCells: cells, pollingLag: true})
	primary, candidate, other := tc.replicas[0], tc.replicas[1], tc.replicas[2]
	out, err := tc.VtctldClientProcess.ExecuteCommandWithOutput("MigrateReplicationMode",
		"--durability-policy", policy.DurabilityGroupReplicationCrossCell, keyspaceName)
	require.NoError(t, err, out)
	waitForGroup(t, tc, primary, tc.replicas)

	ctx := t.Context()
	ts := tc.TopoProcess.Server
	tmc := grpctmclient.NewClient()
	t.Cleanup(tmc.Close)
	tabletOf := func(vttablet *cluster.Vttablet) *topodatapb.Tablet {
		tablet, err := tc.VtctldClientProcess.GetTablet(vttablet.Alias)
		require.NoError(t, err)
		return tablet
	}
	vtorc := tc.VTOrcProcesses[0]
	vtorc.DisableGlobalRecoveries(t)

	// The group of the primary and the candidate: the candidate's acceptance makes every commit.
	_, err = tmc.StopGroupReplication(ctx, tabletOf(other))
	require.NoError(t, err)

	// The candidate's applier cannot commit: what the primary commits now stays in its relay log.
	hold, err := candidate.VttabletProcess.TabletConnWithContext(ctx, keyspaceName, false)
	require.NoError(t, err)
	_, err = hold.ExecuteFetch("FLUSH TABLES WITH READ LOCK", 1, false)
	require.NoError(t, err)
	const backlog = 100
	for i := range backlog {
		_, err := primary.VttabletProcess.QueryTablet(fmt.Sprintf("insert into writes (val) values ('backlog %d')", i), keyspaceName, true)
		require.NoError(t, err)
	}
	// The candidate leaves its group with its backlog: MySQL's STOP waits for the read lock, then
	// stops the applier.
	stopped := make(chan error, 1)
	go func() {
		_, err := tmc.StopGroupReplication(ctx, tabletOf(candidate))
		stopped <- err
	}()
	// Release the lock only once the candidate left the group: the STOP then waits for the applier,
	// which applies one more transaction and stops (lab, "Bootstrap candidate"). Released earlier,
	// the applier applies the whole backlog while the member leaves.
	require.Eventually(t, func() bool {
		qr, err := candidate.VttabletProcess.QueryTablet("SELECT MEMBER_STATE FROM performance_schema.replication_group_members "+
			"WHERE MEMBER_ID = @@global.server_uuid", keyspaceName, false)
		return err == nil && len(qr.Rows) == 1 && qr.Rows[0][0].ToString() != mysql.GroupMemberStateOnline
	}, waitTimeout, pollInterval, "the candidate leaves its group")
	time.Sleep(2 * time.Second)
	hold.Close()
	require.NoError(t, <-stopped)

	// An intent of an earlier pass names the candidate.
	_, err = ts.UpdateShardFields(ctx, keyspaceName, shardName, func(si *topo.ShardInfo) error {
		si.GroupReplicationBootstrapIntent = &topodatapb.GroupReplicationBootstrapIntent{
			Target:              candidate.GetAlias(),
			Time:                protoutil.TimeToProto(time.Now()),
			PreviousIncarnation: si.GroupReplicationIncarnation,
			Token:               plantedIntentToken,
		}
		return nil
	})
	require.NoError(t, err)
	recorded := shardRecord(t, ts).GetGroupReplicationIncarnation()

	// The group loses its last member.
	_, err = primary.VttabletProcess.QueryTablet("STOP GROUP_REPLICATION", keyspaceName, false)
	require.NoError(t, err)
	primaryExecuted := executedGTIDSet(t, primary)
	candidateExecuted := executedGTIDSet(t, candidate)
	candidateReceived := receivedGTIDSet(t, candidate)
	t.Logf("the primary executed %v; the candidate executed %v and received %v", primaryExecuted, candidateExecuted, candidateReceived)
	require.False(t, candidateExecuted.Contains(primaryExecuted), "the candidate applied its whole backlog")
	require.True(t, candidateExecuted.Union(candidateReceived).Contains(primaryExecuted), "the candidate lacks transactions that the primary executed")

	// The bootstrap RPC is to wait for the candidate's action lock while its mysqld restarts.
	const lockHeld = 25 * time.Second
	sleep := exec.Command(tc.VtctldClientProcess.Binary, "--server", tc.VtctldClientProcess.Server, "SleepTablet", candidate.Alias, lockHeld.String())
	require.NoError(t, sleep.Start())
	sleepStart := time.Now()
	t.Cleanup(func() { _ = sleep.Wait() })
	time.Sleep(time.Second)
	vtorc.EnableGlobalRecoveries(t)

	var intent *topodatapb.GroupReplicationBootstrapIntent
	require.Eventually(t, func() bool {
		intent = shardRecord(t, ts).GetGroupReplicationBootstrapIntent()
		return intent.GetToken() != "" && intent.GetToken() != plantedIntentToken
	}, waitTimeout, 10*time.Millisecond, "VTOrc records its own intent")
	require.Equal(t, candidate.Alias, topoproto.TabletAliasString(intent.GetTarget()), "VTOrc chooses the intent's target again")
	intentTime := time.Now()
	t.Logf("VTOrc recorded intent %s for %s %.1fs after the lock was taken", intent.GetToken(), candidate.Alias, intentTime.Sub(sleepStart).Seconds())

	// mysqld_safe restarts the killed mysqld, which discards the relay log (relay_log_recovery).
	killMysqld(t, candidate)
	time.Sleep(time.Second)
	require.Eventually(t, func() bool {
		qr, err := candidate.VttabletProcess.QueryTablet("SELECT RECEIVED_TRANSACTION_SET FROM performance_schema.replication_connection_status "+
			"WHERE CHANNEL_NAME = 'group_replication_applier'", keyspaceName, false)
		return err == nil && (len(qr.Rows) == 0 || qr.Rows[0][0].ToString() == "")
	}, waitTimeout, 100*time.Millisecond, "the candidate's mysqld restarts without its relay log")
	require.Less(t, time.Since(sleepStart), lockHeld, "mysqld must be back before the RPC gets the lock")
	t.Logf("the candidate's mysqld restarted %.1fs after the lock was taken; it executed %v", time.Since(sleepStart).Seconds(), executedGTIDSet(t, candidate))

	// The RPC gets the lock, the tablet refuses, and VTOrc bootstraps the primary.
	var bootstrapped time.Time
	require.Eventually(t, func() bool {
		if shardRecord(t, ts).GetGroupReplicationIncarnation() == recorded {
			return false
		}
		bootstrapped = time.Now()
		return true
	}, 3*time.Minute, 50*time.Millisecond, "VTOrc bootstraps the group")
	lockReleased := sleepStart.Add(lockHeld)
	t.Logf("the group was bootstrapped %.1fs after the candidate's lock was released, %.1fs after VTOrc's intent",
		bootstrapped.Sub(lockReleased).Seconds(), bootstrapped.Sub(intentTime).Seconds())
	assert.Less(t, bootstrapped.Sub(lockReleased), 30*time.Second, "the refused intent must not fence the bootstrap of the primary")

	// The primary, which executed every transaction, bootstraps; the others join, the candidate
	// after its read lock was released, and no acknowledged write is lost.
	waitForGroup(t, tc, primary, tc.replicas)
	newPrimaryExecuted := executedGTIDSet(t, primary)
	assert.True(t, newPrimaryExecuted.Contains(primaryExecuted), "the new group lacks acknowledged transactions: %v, want %v", newPrimaryExecuted, primaryExecuted)
	waitForRowCounts(t, tc, primary)
}

// executedGTIDSet returns the executed GTID set of the tablet's MySQL.
func executedGTIDSet(t *testing.T, tablet *cluster.Vttablet) replication.Mysql56GTIDSet {
	t.Helper()
	qr, err := tablet.VttabletProcess.QueryTablet("SELECT @@global.gtid_executed", keyspaceName, false)
	require.NoError(t, err)
	set, err := replication.ParseMysql56GTIDSet(strings.ReplaceAll(qr.Rows[0][0].ToString(), "\n", ""))
	require.NoError(t, err)
	return set
}

// receivedGTIDSet returns the transactions that the tablet's MySQL received from its group into the
// relay log of its group_replication_applier channel.
func receivedGTIDSet(t *testing.T, tablet *cluster.Vttablet) replication.Mysql56GTIDSet {
	t.Helper()
	qr, err := tablet.VttabletProcess.QueryTablet("SELECT RECEIVED_TRANSACTION_SET FROM performance_schema.replication_connection_status "+
		"WHERE CHANNEL_NAME = 'group_replication_applier'", keyspaceName, false)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 1)
	set, err := replication.ParseMysql56GTIDSet(strings.ReplaceAll(qr.Rows[0][0].ToString(), "\n", ""))
	require.NoError(t, err)
	return set
}

// shardRecord reads the shard record from the global topology.
func shardRecord(t *testing.T, ts *topo.Server) *topodatapb.Shard {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	si, err := ts.GetShard(ctx, keyspaceName, shardName)
	require.NoError(t, err)
	return si.Shard
}
