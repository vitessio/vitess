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
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
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

// refusalSetup is a shard whose group lost its last member while its primary held transactions
// that the candidate had only received, and an intent of an earlier pass that names the candidate.
type refusalSetup struct {
	tc              *testCluster
	primary         *cluster.Vttablet
	candidate       *cluster.Vttablet
	ts              *topo.Server
	vtorc           *cluster.VTOrcProcess
	recorded        string
	primaryExecuted replication.Mysql56GTIDSet
}

// setupRefusedBootstrap sets up the shard of refusalSetup, with VTOrc's recoveries disabled.
//
// The group lost its majority while its primary had committed transactions that the candidate had
// received into its relay log, without applying them (FLUSH TABLES WITH READ LOCK holds its applier,
// as in the lab of doc/failover-audit/GroupReplication.md, "Bootstrap candidate"). An intent of an
// earlier pass names the candidate, so VTOrc chooses it again once its recoveries are enabled.
func setupRefusedBootstrap(t *testing.T) *refusalSetup {
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
	return &refusalSetup{tc: tc, primary: primary, candidate: candidate, ts: ts, vtorc: vtorc, recorded: recorded, primaryExecuted: primaryExecuted}
}

// holdActionLock takes the tablet's action lock for the given time (SleepTablet), and returns when it
// was taken.
func holdActionLock(t *testing.T, tc *testCluster, tablet *cluster.Vttablet, held time.Duration) time.Time {
	sleep := exec.Command(tc.VtctldClientProcess.Binary, "--server", tc.VtctldClientProcess.Server, "SleepTablet", tablet.Alias, held.String())
	require.NoError(t, sleep.Start())
	t.Cleanup(func() { _ = sleep.Wait() })
	return time.Now()
}

// waitForOwnIntent waits until VTOrc recorded its own bootstrap intent, which must name the candidate,
// and returns it.
func waitForOwnIntent(t *testing.T, r *refusalSetup) *topodatapb.GroupReplicationBootstrapIntent {
	var intent *topodatapb.GroupReplicationBootstrapIntent
	require.Eventually(t, func() bool {
		intent = shardRecord(t, r.ts).GetGroupReplicationBootstrapIntent()
		return intent.GetToken() != "" && intent.GetToken() != plantedIntentToken
	}, waitTimeout, 10*time.Millisecond, "VTOrc records its own intent")
	require.Equal(t, r.candidate.Alias, topoproto.TabletAliasString(intent.GetTarget()), "VTOrc chooses the intent's target again")
	return intent
}

// waitForRelayLogDiscarded waits until the candidate's mysqld is back without its relay log.
func waitForRelayLogDiscarded(t *testing.T, candidate *cluster.Vttablet) {
	require.Eventually(t, func() bool {
		qr, err := candidate.VttabletProcess.QueryTablet("SELECT RECEIVED_TRANSACTION_SET FROM performance_schema.replication_connection_status "+
			"WHERE CHANNEL_NAME = 'group_replication_applier'", keyspaceName, false)
		return err == nil && (len(qr.Rows) == 0 || qr.Rows[0][0].ToString() == "")
	}, waitTimeout, 100*time.Millisecond, "the candidate's mysqld restarts without its relay log")
}

// waitForBootstrap waits until VTOrc records a new incarnation, and returns when.
func waitForBootstrap(t *testing.T, r *refusalSetup) time.Time {
	var bootstrapped time.Time
	require.Eventually(t, func() bool {
		if shardRecord(t, r.ts).GetGroupReplicationIncarnation() == r.recorded {
			return false
		}
		bootstrapped = time.Now()
		return true
	}, 3*time.Minute, 50*time.Millisecond, "VTOrc bootstraps the group")
	return bootstrapped
}

// checkPrimaryBootstrapped checks that the primary, which executed every transaction, bootstrapped,
// that the others joined, the candidate after its read lock was released, and that no acknowledged
// write is lost.
func checkPrimaryBootstrapped(t *testing.T, r *refusalSetup) {
	waitForGroup(t, r.tc, r.primary, r.tc.replicas)
	newPrimaryExecuted := executedGTIDSet(t, r.primary)
	assert.True(t, newPrimaryExecuted.Contains(r.primaryExecuted), "the new group lacks acknowledged transactions: %v, want %v", newPrimaryExecuted, r.primaryExecuted)
	waitForRowCounts(t, r.tc, r.primary)
}

// TestGroupReplicationWithdrawsRefusedBootstrapIntent reproduces, on MySQL 8.4, the delay that the
// bootstrap intent of a candidate whose mysqld restarted after VTOrc chose it added to an outage,
// and checks that VTOrc now withdraws the intent at once when the candidate refuses definitively.
//
// VTOrc chooses the candidate of setupRefusedBootstrap again, and sends it the bootstrap RPC, which
// waits for the tablet's action lock (held by SleepTablet). Meanwhile, the candidate's mysqld
// restarts, which discards its relay log (relay_log_recovery). Its tablet then refuses the bootstrap:
// MySQL lacks transactions that the primary holds, and no START GROUP_REPLICATION runs. Before the
// fix, the intent fenced the bootstrap of the primary for two minutes; now VTOrc withdraws it, and
// bootstraps the primary on its next pass.
func TestGroupReplicationWithdrawsRefusedBootstrapIntent(t *testing.T) {
	r := setupRefusedBootstrap(t)

	// The bootstrap RPC is to wait for the candidate's action lock while its mysqld restarts.
	const lockHeld = 25 * time.Second
	sleepStart := holdActionLock(t, r.tc, r.candidate, lockHeld)
	time.Sleep(time.Second)
	r.vtorc.EnableGlobalRecoveries(t)

	intent := waitForOwnIntent(t, r)
	intentTime := time.Now()
	t.Logf("VTOrc recorded intent %s for %s %.1fs after the lock was taken", intent.GetToken(), r.candidate.Alias, intentTime.Sub(sleepStart).Seconds())

	// mysqld_safe restarts the killed mysqld, which discards the relay log (relay_log_recovery).
	killMysqld(t, r.candidate)
	time.Sleep(time.Second)
	waitForRelayLogDiscarded(t, r.candidate)
	require.Less(t, time.Since(sleepStart), lockHeld, "mysqld must be back before the RPC gets the lock")
	t.Logf("the candidate's mysqld restarted %.1fs after the lock was taken; it executed %v", time.Since(sleepStart).Seconds(), executedGTIDSet(t, r.candidate))

	// The RPC gets the lock, the tablet refuses, and VTOrc bootstraps the primary.
	bootstrapped := waitForBootstrap(t, r)
	lockReleased := sleepStart.Add(lockHeld)
	t.Logf("the group was bootstrapped %.1fs after the candidate's lock was released, %.1fs after VTOrc's intent",
		bootstrapped.Sub(lockReleased).Seconds(), bootstrapped.Sub(intentTime).Seconds())
	assert.Less(t, bootstrapped.Sub(lockReleased), 30*time.Second, "the refused intent must not fence the bootstrap of the primary")
	checkPrimaryBootstrapped(t, r)
}

// TestGroupReplicationReprobesStaleBootstrapIntent reproduces, on MySQL 8.4, the delay that the
// bootstrap intent of a candidate added to an outage when its bootstrap RPC failed without a
// definitive refusal, and the candidate's mysqld then restarted without its relay log; and checks that
// VTOrc now ends it by sending the intent's bootstrap to the candidate again.
//
// VTOrc chooses the candidate of setupRefusedBootstrap again, and sends it the bootstrap RPC, which
// waits for the tablet's action lock (held by SleepTablet). Meanwhile, the candidate's mysqld dies,
// and stays down (mysqld_safe is stopped) until the RPC got the lock: the RPC fails on its first
// MySQL read, which is not a definitive refusal, and VTOrc keeps the intent. mysqld then restarts,
// which discards its relay log (relay_log_recovery). VTOrc no longer chooses the candidate, but the
// primary, which its intent fenced until it expired, two minutes after VTOrc recorded it. Now VTOrc
// sends the intent's bootstrap to the candidate again, which refuses it definitively, withdraws the
// intent, and bootstraps the primary in the same pass.
func TestGroupReplicationReprobesStaleBootstrapIntent(t *testing.T) {
	r := setupRefusedBootstrap(t)

	// The bootstrap RPC is to wait for the candidate's action lock while its mysqld is down.
	const lockHeld = 10 * time.Second
	sleepStart := holdActionLock(t, r.tc, r.candidate, lockHeld)
	time.Sleep(time.Second)
	r.vtorc.EnableGlobalRecoveries(t)

	intent := waitForOwnIntent(t, r)
	intentTime := time.Now()
	t.Logf("VTOrc recorded intent %s for %s %.1fs after the lock was taken", intent.GetToken(), r.candidate.Alias, intentTime.Sub(sleepStart).Seconds())

	// mysqld dies, and mysqld_safe does not restart it until the RPC failed.
	resume := pauseMysqldSafe(t, r.candidate)
	killMysqld(t, r.candidate)
	require.Less(t, time.Since(sleepStart), lockHeld, "mysqld must be down before the RPC gets the lock")
	time.Sleep(time.Until(sleepStart.Add(lockHeld + 3*time.Second)))
	current := shardRecord(t, r.ts).GetGroupReplicationBootstrapIntent()
	require.Equal(t, intent.GetToken(), current.GetToken(), "the RPC that failed without a definitive refusal keeps the intent")
	require.Equal(t, r.recorded, shardRecord(t, r.ts).GetGroupReplicationIncarnation())

	// mysqld_safe restarts mysqld, which discards the relay log (relay_log_recovery).
	resume()
	waitForRelayLogDiscarded(t, r.candidate)
	back := time.Now()
	t.Logf("the candidate's mysqld is back %.1fs after VTOrc's intent; it executed %v", back.Sub(intentTime).Seconds(), executedGTIDSet(t, r.candidate))

	bootstrapped := waitForBootstrap(t, r)
	t.Logf("the group was bootstrapped %.1fs after the candidate's mysqld was back, %.1fs after VTOrc's intent",
		bootstrapped.Sub(back).Seconds(), bootstrapped.Sub(intentTime).Seconds())
	assert.Less(t, bootstrapped.Sub(back), 30*time.Second, "the stale intent must not fence the bootstrap of the primary")
	checkPrimaryBootstrapped(t, r)
}

// pauseMysqldSafe stops (SIGSTOP) the mysqld_safe that runs the tablet's mysqld, so that it does not
// restart a mysqld that dies, and returns the function that resumes it (SIGCONT). It is resumed at the
// end of the test in any case.
func pauseMysqldSafe(t *testing.T, tablet *cluster.Vttablet) (resume func()) {
	pidFile := path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("vt_%010d", tablet.TabletUID), "mysql.pid")
	data, err := os.ReadFile(pidFile)
	require.NoError(t, err)
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%s/stat", strings.TrimSpace(string(data))))
	require.NoError(t, err)
	// The fields after the command name, which ends with the last ')': state, then the parent's pid.
	fields := strings.Fields(string(stat[bytes.LastIndexByte(stat, ')')+1:]))
	require.GreaterOrEqual(t, len(fields), 2)
	ppid, err := strconv.Atoi(fields[1])
	require.NoError(t, err)
	cmdline, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", ppid))
	require.NoError(t, err)
	require.Contains(t, string(cmdline), "mysqld_safe", "the parent of mysqld")
	require.NoError(t, syscall.Kill(ppid, syscall.SIGSTOP))
	var once sync.Once
	resume = func() { once.Do(func() { _ = syscall.Kill(ppid, syscall.SIGCONT) }) }
	t.Cleanup(resume)
	return resume
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
