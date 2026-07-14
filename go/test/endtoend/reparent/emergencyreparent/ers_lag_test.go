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

package emergencyreparent

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/reparent/utils"
	endtoendutils "vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

// replicaStatusField returns the named field from `show replica status`, or an empty
// string if the row or field is missing.
func replicaStatusField(t *testing.T, tablet *cluster.Vttablet, field string) string {
	res := utils.RunSQL(t.Context(), t, `show replica status`, tablet)
	if len(res.Rows) != 1 {
		return ""
	}
	for i, f := range res.Fields {
		if f.Name == field {
			return res.Rows[0][i].ToString()
		}
	}
	return ""
}

// waitForReceivedPosition waits until the tablet has received (not necessarily applied)
// everything in the given GTID set.
func waitForReceivedPosition(t *testing.T, tablet *cluster.Vttablet, position string) {
	require.Eventually(t, func() bool {
		retrieved := strings.ReplaceAll(replicaStatusField(t, tablet, "Retrieved_Gtid_Set"), "\n", "")
		if retrieved == "" {
			return false
		}
		res := utils.RunSQL(t.Context(), t, fmt.Sprintf(`select gtid_subset('%s', concat(@@global.gtid_executed, ',', '%s'))`, position, retrieved), tablet)
		return len(res.Rows) == 1 && res.Rows[0][0].ToString() == "1"
	}, 30*time.Second, time.Second)
}

// waitForReplicationSource waits until the tablet's replication source is the given
// tablet's mysqld.
func waitForReplicationSource(t *testing.T, tablet, source *cluster.Vttablet) {
	require.Eventually(t, func() bool {
		return replicaStatusField(t, tablet, "Source_Port") == strconv.Itoa(source.MySQLPort)
	}, 30*time.Second, time.Second)
}

// TestERSSucceedsWithLaggingSQLThreadReplica checks that a replica that has received all
// changes but is lagging on applying them neither fails nor delays ERS: a peer that has
// applied everything is promoted instead, and the lagging replica is repointed to the new
// primary and catches up afterwards. tablets[3] is demoted to RDONLY so the lagging
// replica is also the new primary's only possible semi-sync acker, proving the promoted
// primary accepts writes while its acker is still behind on apply: semi-sync ACKs happen
// at relay log receipt, not apply.
func TestERSSucceedsWithLaggingSQLThreadReplica(t *testing.T) {
	endtoendutils.SkipIfBinaryIsBelowVersion(t, 25, "vtctld")

	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	// With tablets[3] as RDONLY, the only candidates after the primary dies are
	// tablets[1] (lagged) and tablets[2], and the only semi-sync acker for the promoted
	// tablets[2] is the lagged tablets[1].
	err := clusterInstance.VtctldClientProcess.ExecuteCommand("ChangeTabletType", tablets[3].Alias, "rdonly")
	require.NoError(t, err)

	utils.ConfirmReplication(t, tablets[0], tablets[1:])

	// Delay applies on tablets[1] by an hour: it keeps receiving changes into its relay
	// logs (and keeps sending semi-sync ACKs) but stops applying them, like a replica
	// lagging behind on a heavy write load. A delay outlives the post-ERS repoint, unlike
	// a stopped SQL thread which ERS force-starts.
	utils.RunSQLs(t.Context(), t, []string{
		`STOP REPLICA SQL_THREAD`,
		`CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 3600`,
		`START REPLICA SQL_THREAD`,
	}, tablets[1])

	// This write is received by every replica but applied only on tablets[2] and tablets[3].
	insertedVal := utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[2], tablets[3]})

	// Confirm that tablets[1] has indeed not applied the write.
	res := utils.RunSQL(t.Context(), t, `select msg from vt_insert_test`, tablets[1])
	assert.Len(t, res.Rows, 1)

	// Make sure tablets[1] has received the write before ERS freezes the positions: the
	// semi-sync ACK can come from the other replicas, so the insert returning doesn't
	// guarantee it. Without this the test can degrade into filtering tablets[1] out
	// instead of racing it against the applied replicas.
	primaryPosition := strings.ReplaceAll(utils.RunSQL(t.Context(), t, `select @@global.gtid_executed`, tablets[0]).Rows[0][0].ToString(), "\n", "")
	waitForReceivedPosition(t, tablets[1], primaryPosition)

	// Kill the primary's vttablet (mysqld keeps running, so the replicas' IO threads stay
	// connected). The lagging replica used to make ERS wait for its whole relay log
	// backlog and time out the reparent; now ERS waits only on the candidates that can win.
	utils.StopTablet(t, tablets[0], false)

	out, err := utils.Ers(clusterInstance, nil, "120s", "30s")
	require.NoError(t, err, out)

	// The lagging replica must not win the election; tablets[2] is the only candidate
	// that can.
	newPrimary := utils.GetNewPrimary(t, clusterInstance)
	require.Equal(t, tablets[2].Alias, newPrimary.Alias, "the lagged replica and the rdonly must not be promoted")
	err = utils.CheckInsertedValues(t.Context(), t, newPrimary, insertedVal)
	require.NoError(t, err)

	// The new primary must accept writes while its only semi-sync acker, the repointed
	// tablets[1], is still behind on apply: ACKs are sent at relay log receipt.
	waitForReplicationSource(t, tablets[1], newPrimary)
	availabilityVal := utils.ConfirmReplication(t, newPrimary, nil)
	res = utils.RunSQL(t.Context(), t, `select msg from vt_insert_test`, tablets[1])
	assert.Len(t, res.Rows, 1, "the acker must still be behind on apply when the write commits")

	// Clear the apply delay (it survives the repoint) and tablets[1] catches up on
	// everything through the new primary.
	utils.RunSQLs(t.Context(), t, []string{
		`STOP REPLICA`,
		`CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 0`,
		`START REPLICA`,
	}, tablets[1])
	err = utils.CheckInsertedValues(t.Context(), t, tablets[1], insertedVal)
	require.NoError(t, err)
	err = utils.CheckInsertedValues(t.Context(), t, tablets[1], availabilityVal)
	require.NoError(t, err)
	utils.CheckReplicationStatus(t.Context(), t, tablets[1], true, true)

	// New writes replicate to every remaining tablet, including the former lagger.
	var replicas []*cluster.Vttablet
	for _, tablet := range tablets[1:] {
		if tablet.Alias != newPrimary.Alias {
			replicas = append(replicas, tablet)
		}
	}
	utils.ConfirmReplication(t, newPrimary, replicas)
}

// TestERSFiltersReplicaBehindOnRelayLogReceipt checks that a replica that is behind on
// received relay logs — and additionally has an unapplied relay log backlog it can never
// drain — is excluded from the relay log wait entirely and cannot fail the reparent.
func TestERSFiltersReplicaBehindOnRelayLogReceipt(t *testing.T) {
	endtoendutils.SkipIfBinaryIsBelowVersion(t, 25, "vtctld")

	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	utils.ConfirmReplication(t, tablets[0], tablets[1:])

	// First stop applying on tablets[1] while still receiving...
	utils.RunSQL(t.Context(), t, `STOP REPLICA SQL_THREAD`, tablets[1])

	// ...so this write lands in tablets[1]'s relay log without being applied...
	utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[2], tablets[3]})

	// ...then stop receiving too. tablets[1] is now fully stopped with a
	// received-but-unapplied backlog.
	utils.RunSQL(t.Context(), t, `STOP REPLICA IO_THREAD`, tablets[1])

	// This write is only received by tablets[2] and tablets[3]: tablets[1] is now strictly
	// behind on received relay logs and cannot win the election.
	insertedVal := utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[2], tablets[3]})

	utils.StopTablet(t, tablets[0], true)

	out, err := utils.Ers(clusterInstance, nil, "120s", "30s")
	require.NoError(t, err, out)

	// One of the up-to-date replicas must have been promoted.
	newPrimary := utils.GetNewPrimary(t, clusterInstance)
	require.NotEqual(t, tablets[1].Alias, newPrimary.Alias, "lagging replica must not be promoted")
	err = utils.CheckInsertedValues(t.Context(), t, newPrimary, insertedVal)
	require.NoError(t, err)

	// tablets[1] was fully stopped when the reparent began, so it is repointed to the new
	// primary but not started. The repoint of non-winning replicas completes
	// asynchronously after ERS returns, so wait for it to land; otherwise START REPLICA
	// races the in-flight CHANGE REPLICATION SOURCE.
	waitForReplicationSource(t, tablets[1], newPrimary)
	utils.CheckReplicationStatus(t.Context(), t, tablets[1], false, false)

	// Once started, it catches up on everything through the new primary.
	utils.RunSQL(t.Context(), t, `START REPLICA`, tablets[1])
	err = utils.CheckInsertedValues(t.Context(), t, tablets[1], insertedVal)
	require.NoError(t, err)
	utils.ConfirmReplication(t, newPrimary, []*cluster.Vttablet{tablets[1]})
}

// TestERSFailsWhenNoCandidateAppliesRelayLogs checks that ERS still refuses to promote
// when no candidate manages to apply its relay logs: racing the leading candidates never
// promotes a tablet with received-but-unapplied transactions.
func TestERSFailsWhenNoCandidateAppliesRelayLogs(t *testing.T) {
	endtoendutils.SkipIfBinaryIsBelowVersion(t, 25, "vtctld")

	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	utils.ConfirmReplication(t, tablets[0], tablets[1:])

	// Stop the SQL thread on every replica: they all keep receiving (and sending
	// semi-sync ACKs) but none of them applies.
	for _, tablet := range tablets[1:] {
		utils.RunSQL(t.Context(), t, `STOP REPLICA SQL_THREAD`, tablet)
	}

	// This write is received by every replica but applied by none.
	insertedVal := utils.ConfirmReplication(t, tablets[0], nil)

	// Kill the primary's vttablet (mysqld keeps running, so the replicas' IO threads stay
	// connected).
	utils.StopTablet(t, tablets[0], false)

	// No candidate can finish applying its relay logs, so the reparent must fail.
	out, err := utils.Ers(clusterInstance, nil, "120s", "30s")
	require.Error(t, err)
	assert.Contains(t, out, "all candidates failed to apply relay logs")

	// The failed reparent restarts replication on the replicas it stopped, so their SQL
	// threads drain the received backlog. Wait for that, then the reparent succeeds.
	err = utils.CheckInsertedValues(t.Context(), t, tablets[1], insertedVal)
	require.NoError(t, err)

	out, err = utils.Ers(clusterInstance, nil, "120s", "30s")
	require.NoError(t, err, out)

	newPrimary := utils.GetNewPrimary(t, clusterInstance)
	err = utils.CheckInsertedValues(t.Context(), t, newPrimary, insertedVal)
	require.NoError(t, err)
}

// TestERSExcludesErrantGTIDCandidateAndRewaits checks that a candidate with an errant GTID
// is still detected and excluded even though the leading-group filter skipped waiting on
// its peers: the errant tablet has the most-advanced received position, so it alone is
// waited on and wins, errant GTID detection then removes it, and ERS waits on the
// surviving candidates before electing one of them. An apply-lagged replica is in the mix
// so the reparent also exercises the second wait's race (and fails on older builds, which
// time out waiting for the lagged replica).
func TestERSExcludesErrantGTIDCandidateAndRewaits(t *testing.T) {
	endtoendutils.SkipIfBinaryIsBelowVersion(t, 25, "vtctld")

	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	utils.ConfirmReplication(t, tablets[0], tablets[1:])

	// Give tablets[2] an errant GTID: a direct binlogged write to its mysqld mints a GTID
	// under its own server uuid that no other tablet has, which also makes its received
	// position a strict superset of everyone else's.
	utils.RunSQLs(t.Context(), t, []string{
		`SET GLOBAL super_read_only = 0`,
		`INSERT INTO vt_insert_test(id, msg) VALUES (999999, 'errant write')`,
		`SET GLOBAL super_read_only = 1`,
	}, tablets[2])

	// Stop the SQL thread on tablets[1]: it keeps receiving but stops applying.
	utils.RunSQL(t.Context(), t, `STOP REPLICA SQL_THREAD`, tablets[1])

	// This write is received by every replica but applied only on tablets[2] and tablets[3].
	insertedVal := utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[2], tablets[3]})

	// Make sure tablets[1] has received the write before ERS freezes the positions.
	primaryPosition := strings.ReplaceAll(utils.RunSQL(t.Context(), t, `select @@global.gtid_executed`, tablets[0]).Rows[0][0].ToString(), "\n", "")
	waitForReceivedPosition(t, tablets[1], primaryPosition)

	// Kill the primary's vttablet (mysqld keeps running).
	utils.StopTablet(t, tablets[0], false)

	out, err := utils.Ers(clusterInstance, nil, "120s", "30s")
	require.NoError(t, err, out)

	// The errant tablet had the most-advanced received position but must not win; the
	// second wait races the survivors and tablets[3], the only one that can finish
	// applying, must be elected.
	newPrimary := utils.GetNewPrimary(t, clusterInstance)
	require.Equal(t, tablets[3].Alias, newPrimary.Alias, "the errant and lagged tablets must not be promoted")
	err = utils.CheckInsertedValues(t.Context(), t, newPrimary, insertedVal)
	require.NoError(t, err)

	// The lagged replica was repointed with a forced start and catches up.
	err = utils.CheckInsertedValues(t.Context(), t, tablets[1], insertedVal)
	require.NoError(t, err)

	// The errant tablet's repoint is refused by the tablet-side errant GTID check, so it
	// still points at the old primary's mysqld; recovering it is an operator decision.
	require.Equal(t, strconv.Itoa(tablets[0].MySQLPort), replicaStatusField(t, tablets[2], "Source_Port"))
}
