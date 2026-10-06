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
	"fmt"
	"net/http"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

// crashMysqld kills the tablet's mysqld, which mysqld_safe then restarts; vttablet stays up.
func crashMysqld(t *testing.T, tablet *cluster.Vttablet) {
	dir := path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("vt_%010d", tablet.TabletUID))
	data, err := os.ReadFile(path.Join(dir, "mysql.pid"))
	require.NoError(t, err)
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	require.NoError(t, err)
	require.NoError(t, syscall.Kill(pid, syscall.SIGKILL))
}

// voterGracePeriod is VTOrc's --group-replication-voter-replacement-grace-period in migratedCluster.
const voterGracePeriod = 10 * time.Second

// migratedCluster returns the default cluster, one REPLICA per cell in three cells, converted to
// group_replication_cross_cell, with its group of three voters running.
func migratedCluster(t *testing.T) *testCluster {
	opts := defaultClusterOptions()
	opts.rdonly = false
	opts.vtorcExtraArgs = []string{"--group-replication-voter-replacement-grace-period", voterGracePeriod.String()}
	tc := setupCluster(t, opts)
	out, err := tc.VtctldClientProcess.ExecuteCommandWithOutput("MigrateReplicationMode",
		"--durability-policy", policy.DurabilityGroupReplicationCrossCell, keyspaceName)
	require.NoError(t, err, out)
	waitForGroup(t, tc, tc.replicas[0], tc.replicas)
	return tc
}

// TestGroupReplicationRemovesDeletedVoter checks that deleting the tablet record of a dead voter
// whose cell has no other tablet removes it from the voter list (RemoveVoter), while the group keeps
// serving: the operator's decommission of a cell.
func TestGroupReplicationRemovesDeletedVoter(t *testing.T) {
	tc := migratedCluster(t)
	primary, zone2, zone3 := tc.replicas[0], tc.replicas[1], tc.replicas[2]

	w := startWriter(t, tc)
	killHost(t, zone3)
	// The voter of zone3 is dead for good, and its cell has no other tablet: VTOrc keeps its seat
	// (GroupVoterUnreplaceable) until the operator deletes its tablet record.
	out, err := tc.VtctldClientProcess.ExecuteCommandWithOutput("DeleteTablets", zone3.Alias)
	require.NoError(t, err, out)

	// Once the group expelled it, VTOrc removes it from the list.
	waitForGroup(t, tc, primary, []*cluster.Vttablet{primary, zone2})
	ok, fail, lastErr := w.stop()
	assert.Positive(t, ok)
	// The group kept the majority of its voters throughout.
	assert.Zero(t, fail, "writes failed while a voter was removed, last error: %v", lastErr)

	// The primary still serves, under the list of two voters.
	w = startWriter(t, tc)
	require.Eventually(t, func() bool { return w.ok.Load() > 10 }, waitTimeout, pollInterval)
	_, fail, lastErr = w.stop()
	assert.Zero(t, fail, "last error: %v", lastErr)
}

// TestGroupReplicationBootstrapsAfterDeletedVoter checks that deleting the tablet record of a dead
// voter unblocks a shard whose group lost its majority (RemoveVoterNoGroup): one voter dies for good,
// a second one's mysqld crashes and restarts, and the group, without a majority, stops. VTOrc does not
// bootstrap it while a voter is unreachable; once the operator deletes the dead voter's tablet record,
// it removes the voter from the list and bootstraps the group from the two others, without losing a
// write that they held.
func TestGroupReplicationBootstrapsAfterDeletedVoter(t *testing.T) {
	tc := migratedCluster(t)
	primary, zone2, zone3 := tc.replicas[0], tc.replicas[1], tc.replicas[2]

	w := startWriter(t, tc)
	require.Eventually(t, func() bool { return w.ok.Load() > 20 }, waitTimeout, pollInterval)
	killHost(t, zone3)
	// The group of the two others expels it, and keeps serving.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		status, err := fullStatus(t, tc, primary)
		require.NoError(c, err)
		assert.Len(c, status.GroupReplicationStatus.GetMembers(), 2)
	}, waitTimeout, pollInterval)
	before := w.ok.Load()
	require.Eventually(t, func() bool { return w.ok.Load() > before+20 }, waitTimeout, pollInterval)

	// The second voter's mysqld crashes: the primary loses the majority of its group, and leaves it.
	crashMysqld(t, zone2)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for _, tablet := range []*cluster.Vttablet{primary, zone2} {
			status, err := fullStatus(t, tc, tablet)
			require.NoError(c, err)
			assert.False(c, mysql.IsGroupMemberActive(status.GroupReplicationStatus), tablet.Alias)
		}
	}, waitTimeout, pollInterval)
	_, _, _ = w.stop()
	acknowledged := w.ackedIDs()
	require.NotEmpty(t, acknowledged)

	// The group stays down while the dead voter is listed: VTOrc bootstraps only when every voter
	// answers.
	assert.ElementsMatch(t, aliasesOf(tc.replicas), shardVoters(t, tc))

	out, err := tc.VtctldClientProcess.ExecuteCommandWithOutput("DeleteTablets", zone3.Alias)
	require.NoError(t, err, out)

	// VTOrc removes the dead voter and bootstraps the group from the two others.
	var newPrimary *cluster.Vttablet
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		newPrimary = nil
		for _, tablet := range []*cluster.Vttablet{primary, zone2} {
			if tablet.Alias == shardPrimary(t, tc) {
				newPrimary = tablet
			}
		}
		require.NotNil(c, newPrimary)
	}, waitTimeout, pollInterval)
	waitForGroup(t, tc, newPrimary, []*cluster.Vttablet{primary, zone2})

	// Every write that the group acknowledged is there: the two remaining voters held them all.
	requireAckedWrites(t, newPrimary, acknowledged)

	// And the shard serves writes again.
	w = startWriter(t, tc)
	require.Eventually(t, func() bool { return w.ok.Load() > 10 }, waitTimeout, pollInterval)
	_, _, _ = w.stop()
}

// mysqlGroupMemberState returns the state of the tablet's MySQL in its replication group, read from
// MySQL itself: the tablet may have no tablet record.
func mysqlGroupMemberState(tablet *cluster.Vttablet) (string, error) {
	qr, err := tablet.VttabletProcess.QueryTablet(
		"SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_ID = @@server_uuid", keyspaceName, false)
	if err != nil {
		return "", err
	}
	if len(qr.Rows) == 0 {
		return mysql.GroupMemberStateOffline, nil
	}
	return qr.Rows[0][0].ToString(), nil
}

// TestGroupReplicationKeepsDeletedVoterThatRuns checks that deleting the tablet record of a voter
// whose vttablet and MySQL still run, while no group runs, neither removes it from the voter list
// (RemoveVoterNoGroup needs it down) nor lets VTOrc bootstrap the group without it: VTOrc reports
// GroupVoterRecordDeleted instead. Restarting its vttablet, which records the tablet again, ends it.
func TestGroupReplicationKeepsDeletedVoterThatRuns(t *testing.T) {
	tc := migratedCluster(t)
	primary, zone2, zone3 := tc.replicas[0], tc.replicas[1], tc.replicas[2]
	voters := slices.Sorted(slices.Values(aliasesOf(tc.replicas)))

	// The mysqld of two voters crash together: the third one loses the majority of its group and
	// leaves it, and no group runs. The record of the third one is deleted meanwhile, while its
	// vttablet and MySQL run.
	crashMysqld(t, primary)
	crashMysqld(t, zone2)
	out, err := tc.VtctldClientProcess.ExecuteCommandWithOutput("DeleteTablets", zone3.Alias)
	require.NoError(t, err, out)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for _, tablet := range []*cluster.Vttablet{primary, zone2} {
			status, err := fullStatus(t, tc, tablet)
			require.NoError(c, err)
			assert.False(c, mysql.IsGroupMemberActive(status.GroupReplicationStatus), tablet.Alias)
		}
		state, err := mysqlGroupMemberState(zone3)
		require.NoError(c, err)
		assert.NotContains(c, []string{mysql.GroupMemberStateOnline, mysql.GroupMemberStateRecovering}, state, zone3.Alias)
	}, waitTimeout, pollInterval)

	// VTOrc reports the deleted voter that still runs.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		status, response, err := tc.VTOrcProcesses[0].MakeAPICall("/api/detection-analysis")
		require.NoError(c, err)
		assert.Equal(c, http.StatusOK, status)
		assert.Contains(c, response, string(inst.GroupVoterRecordDeleted))
	}, waitTimeout, pollInterval)

	// Well past the grace period, the voter is still listed, and no group runs: VTOrc neither removes
	// it nor bootstraps the group without it.
	assert.Never(t, func() bool {
		if !slices.Equal(voters, slices.Sorted(slices.Values(shardVoters(t, tc)))) {
			return true
		}
		for _, tablet := range []*cluster.Vttablet{primary, zone2} {
			status, err := fullStatus(t, tc, tablet)
			if err == nil && mysql.IsGroupMemberActive(status.GroupReplicationStatus) {
				return true
			}
		}
		return false
	}, 3*voterGracePeriod, pollInterval)

	// The operator restarts its vttablet, which records the tablet again: VTOrc bootstraps the group
	// of the three voters.
	require.NoError(t, zone3.VttabletProcess.TearDown())
	require.NoError(t, zone3.VttabletProcess.Setup())
	var newPrimary *cluster.Vttablet
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		newPrimary = nil
		for _, tablet := range tc.replicas {
			if tablet.Alias == shardPrimary(t, tc) {
				newPrimary = tablet
			}
		}
		require.NotNil(c, newPrimary)
	}, waitTimeout, pollInterval)
	waitForGroup(t, tc, newPrimary, tc.replicas)
	assert.ElementsMatch(t, voters, shardVoters(t, tc))
}
