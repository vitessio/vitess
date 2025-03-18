/*
Copyright 2022 The Vitess Authors.

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

package newfeaturetest

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/reparent/utils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

// TestRecoverWithMultipleVttabletFailures tests that ERS succeeds with the default values
// even when there are multiple vttablet failures. In this test we use the semi_sync policy
// to allow multiple failures to happen and still be recoverable.
// The test takes down the vttablets of the primary and a rdonly tablet and runs ERS with the
// default values of remote_operation_timeout, lock-timeout flags and wait_replicas_timeout subflag.
func TestRecoverWithMultipleVttabletFailures(t *testing.T) {
	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	// make tablets[1] a rdonly tablet.
	err := clusterInstance.VtctldClientProcess.ExecuteCommand("ChangeTabletType", tablets[1].Alias, "rdonly")
	require.NoError(t, err)

	// Confirm that replication is still working as intended
	utils.ConfirmReplication(t, tablets[0], tablets[1:])

	// Make the rdonly and primary tablets and databases unavailable.
	utils.StopTablet(t, tablets[1], true)
	utils.StopTablet(t, tablets[0], true)

	// We expect this to succeed since we only have 1 primary eligible tablet which is down
	out, err := utils.Ers(clusterInstance, nil, "", "")
	require.NoError(t, err, out)

	newPrimary := utils.GetNewPrimary(t, clusterInstance)
	utils.ConfirmReplication(t, newPrimary, []*cluster.Vttablet{tablets[2], tablets[3]})
}

// TetsSingeReplicaERS tests that ERS works even when there is only 1 tablet left
// as long the durability policy allows this failure. Moreover, this also tests that the
// replica is one such that it was a primary itself before. This way its executed gtid set
// will have atleast 2 tablets in it. We want to make sure this tablet is not marked as errant
// and ERS succeeds.
func TestSingleReplicaERS(t *testing.T) {
	// Set up a cluster with none durability policy
	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilityNone)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	// Confirm that the replication is setup correctly in the beginning.
	// tablets[0] is the primary tablet in the beginning.
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	// Delete and stop two tablets. We only want to have 2 tablets for this test.
	utils.DeleteTablet(t, clusterInstance, tablets[2])
	utils.DeleteTablet(t, clusterInstance, tablets[3])
	utils.StopTablet(t, tablets[2], true)
	utils.StopTablet(t, tablets[3], true)

	// Reparent to the other replica
	output, err := utils.Prs(t, clusterInstance, tablets[1])
	require.NoError(t, err, "error in PlannedReparentShard output - %s", output)

	// Check the replication is set up correctly before we failover
	utils.ConfirmReplication(t, tablets[1], []*cluster.Vttablet{tablets[0]})

	// Make the current primary vttablet unavailable.
	utils.StopTablet(t, tablets[1], true)

	// Run an ERS with only one replica reachable. Also, this replica is such that it was a primary before.
	output, err = utils.Ers(clusterInstance, tablets[0], "", "")
	require.NoError(t, err, "error in Emergency Reparent Shard output - %s", output)

	// Check the tablet is indeed promoted
	utils.CheckPrimaryTablet(t, clusterInstance, tablets[0])
	// Also check the writes succeed after failover
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{})
}

// TestTabletRestart tests that a running tablet can be  restarted and everything is still fine
func TestTabletRestart(t *testing.T) {
	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	utils.StopTablet(t, tablets[1], false)
	tablets[1].VttabletProcess.ServingStatus = "SERVING"
	err := tablets[1].VttabletProcess.Setup()
	require.NoError(t, err)
}

// Tests ensures that ChangeTabletType works even when semi-sync plugins are not loaded.
func TestChangeTypeWithoutSemiSync(t *testing.T) {
	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilityNone)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	ctx := context.Background()

	primary, replica := tablets[0], tablets[1]

	// Unload semi sync plugins
	for _, tablet := range tablets[0:4] {
		qr := utils.RunSQL(ctx, t, "select @@global.super_read_only", tablet)
		result := fmt.Sprintf("%v", qr.Rows[0][0].ToString())
		if result == "1" {
			utils.RunSQL(ctx, t, "set global super_read_only = 0", tablet)
		}

		semisyncType, err := utils.SemiSyncExtensionLoaded(ctx, tablet)
		require.NoError(t, err)
		switch semisyncType {
		case mysql.SemiSyncTypeSource:
			utils.RunSQL(ctx, t, "UNINSTALL PLUGIN rpl_semi_sync_replica", tablet)
			utils.RunSQL(ctx, t, "UNINSTALL PLUGIN rpl_semi_sync_source", tablet)
		case mysql.SemiSyncTypeMaster:
			utils.RunSQL(ctx, t, "UNINSTALL PLUGIN rpl_semi_sync_slave", tablet)
			utils.RunSQL(ctx, t, "UNINSTALL PLUGIN rpl_semi_sync_master", tablet)
		default:
			require.Fail(t, "Unknown semi sync type")
		}
	}

	utils.ValidateTopology(t, clusterInstance, true)
	utils.CheckPrimaryTablet(t, clusterInstance, primary)

	// Change replica's type to rdonly
	err := clusterInstance.VtctldClientProcess.ExecuteCommand("ChangeTabletType", replica.Alias, "rdonly")
	require.NoError(t, err)

	// Change tablets type from rdonly back to replica
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("ChangeTabletType", replica.Alias, "replica")
	require.NoError(t, err)
}

// TestERSWithWriteInPromoteReplica tests that ERS doesn't fail even if there is a
// write that happens when PromoteReplica is called.
func TestERSWithWriteInPromoteReplica(t *testing.T) {
	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	// Drop a table so that when sidecardb changes are checked, we run a DML query.
	utils.RunSQLs(context.Background(), t, []string{
		"set sql_log_bin=0",
		`SET @@global.super_read_only=0`,
		`DROP TABLE _vt.heartbeat`,
		"set sql_log_bin=1",
	}, tablets[3])
	_, err := utils.Ers(clusterInstance, tablets[3], "60s", "30s")
	require.NoError(t, err, "ERS should not fail even if there is a sidecardb change")
}

func TestBufferingWithMultipleDisruptions(t *testing.T) {
	clusterInstance := utils.SetupShardedReparentCluster(t, policy.DurabilitySemiSync, nil)
	defer utils.TeardownCluster(clusterInstance)

	// Stop all VTOrc instances, so that they don't interfere with the test.
	for _, vtorc := range clusterInstance.VTOrcProcesses {
		err := vtorc.TearDown()
		require.NoError(t, err)
	}

	// Start by reparenting all the shards to the first tablet.
	keyspace := clusterInstance.Keyspaces[0]
	shards := keyspace.Shards
	for _, shard := range shards {
		err := clusterInstance.VtctldClientProcess.PlannedReparentShard(keyspace.Name, shard.Name, shard.Vttablets[0].Alias)
		require.NoError(t, err)
	}

	// We simulate start of external reparent or a PRS where the healthcheck update from the tablet gets lost in transit
	// to vtgate by just setting the primary read only. This is also why we needed to shutdown all VTOrcs, so that they don't
	// fix this.
	utils.RunSQL(context.Background(), t, "set global read_only=1", shards[0].Vttablets[0])
	utils.RunSQL(context.Background(), t, "set global read_only=1", shards[1].Vttablets[0])

	wg := sync.WaitGroup{}
	rowCount := 10
	vtParams := clusterInstance.GetVTParams(keyspace.Name)
	// We now spawn writes for a bunch of go routines.
	// The ones going to shard 1 and shard 2 should block, since
	// they're in the midst of a reparenting operation (as seen by the buffering code).
	for i := 1; i <= rowCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := mysql.Connect(context.Background(), &vtParams)
			if err != nil {
				return
			}
			defer conn.Close()
			_, err = conn.ExecuteFetch(utils.GetInsertQuery(i), 0, false)
			require.NoError(t, err)
		}(i)
	}

	// Now, run a PRS call on the last shard. This shouldn't unbuffer the queries that are buffered for shards 1 and 2
	// since the disruption on the two shards hasn't stopped.
	err := clusterInstance.VtctldClientProcess.PlannedReparentShard(keyspace.Name, shards[2].Name, shards[2].Vttablets[1].Alias)
	require.NoError(t, err)
	// We wait a second just to make sure the PRS changes are processed by the buffering logic in vtgate.
	time.Sleep(1 * time.Second)
	// Finally, we'll now make the 2 shards healthy again by running PRS.
	err = clusterInstance.VtctldClientProcess.PlannedReparentShard(keyspace.Name, shards[0].Name, shards[0].Vttablets[1].Alias)
	require.NoError(t, err)
	err = clusterInstance.VtctldClientProcess.PlannedReparentShard(keyspace.Name, shards[1].Name, shards[1].Vttablets[1].Alias)
	require.NoError(t, err)
	// Wait for all the writes to have succeeded.
	wg.Wait()
}

// TestSemiSyncBlockDueToDisruption tests that Vitess can recover from a situation
// where a primary is stuck waiting for semi-sync ACKs due to a network issue,
// even if no new writes from the user arrives.
func TestSemiSyncBlockDueToDisruption(t *testing.T) {
	// This is always set to "true" on GitHub Actions runners:
	// https://docs.github.com/en/actions/learn-github-actions/variables#default-environment-variables
	ci, ok := os.LookupEnv("CI")
	if ok && strings.ToLower(ci) == "true" {
		t.Skip("Test not meant to be run on CI")
	}
	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})

	// stop heartbeats on all the replicas
	for idx, tablet := range tablets {
		if idx == 0 {
			continue
		}
		utils.RunSQLs(context.Background(), t, []string{
			"stop slave;",
			"change master to MASTER_HEARTBEAT_PERIOD = 0;",
			"start slave;",
		}, tablet)
	}

	// Take a backup of the pf.conf file
	runCommandWithSudo(t, "cp", "/etc/pf.conf", "/etc/pf.conf.backup")
	defer func() {
		// Restore the file from backup
		runCommandWithSudo(t, "mv", "/etc/pf.conf.backup", "/etc/pf.conf")
		runCommandWithSudo(t, "pfctl", "-f", "/etc/pf.conf")
	}()
	// Disrupt the network between the primary and the replicas
	runCommandWithSudo(t, "sh", "-c", fmt.Sprintf("echo 'block in proto tcp from any to any port %d' | sudo tee -a /etc/pf.conf > /dev/null", tablets[0].MySQLPort))

	// This following command is only required if pfctl is not already enabled
	//runCommandWithSudo(t, "pfctl", "-e")
	runCommandWithSudo(t, "pfctl", "-f", "/etc/pf.conf")
	rules := runCommandWithSudo(t, "pfctl", "-s", "rules")
	log.Errorf("Rules enforced - %v", rules)

	// Start a write that will be blocked by the primary waiting for semi-sync ACKs
	ch := make(chan any)
	go func() {
		defer func() {
			close(ch)
		}()
		utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[1], tablets[2], tablets[3]})
	}()

	// Starting VTOrc later now, because we don't want it to fix the heartbeat interval
	// on the replica's before the disruption has been introduced.
	err := clusterInstance.StartVTOrc(clusterInstance.Keyspaces[0].Name)
	require.NoError(t, err)
	go func() {
		for {
			select {
			case <-ch:
				return
			case <-time.After(1 * time.Second):
				str, isPresent := tablets[0].VttabletProcess.GetVars()["SemiSyncMonitorWritesBlocked"]
				if isPresent {
					log.Errorf("SemiSyncMonitorWritesBlocked - %v", str)
				}
			}
		}
	}()
	// If the network disruption is too long lived, then we will end up running ERS from VTOrc.
	networkDisruptionDuration := 43 * time.Second
	time.Sleep(networkDisruptionDuration)

	// Restore the network
	runCommandWithSudo(t, "cp", "/etc/pf.conf.backup", "/etc/pf.conf")
	runCommandWithSudo(t, "pfctl", "-f", "/etc/pf.conf")

	// We expect the problem to be resolved in less than 30 seconds.
	select {
	case <-time.After(30 * time.Second):
		t.Errorf("Timed out waiting for semi-sync to be unblocked")
	case <-ch:
		log.Errorf("Woohoo, write finished!")
	}
}

// runCommandWithSudo runs the provided command with sudo privileges
// when the command is run, it prompts the user for the password, and it must be
// entered for the program to resume.
func runCommandWithSudo(t *testing.T, args ...string) string {
	cmd := exec.Command("sudo", args...)
	out, err := cmd.CombinedOutput()
	assert.NoError(t, err, string(out))
	return string(out)
}
