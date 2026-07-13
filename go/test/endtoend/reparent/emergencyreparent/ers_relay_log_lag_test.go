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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/reparent/utils"
	endtoendutils "vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

// TestERSSucceedsWithLaggingSQLThreadReplica checks that a replica that has received all
// changes but is lagging on applying them (stopped SQL thread) neither fails nor delays
// ERS: a peer that has applied everything is promoted instead, and the lagging replica is
// repointed to the new primary and catches up afterwards.
func TestERSSucceedsWithLaggingSQLThreadReplica(t *testing.T) {
	endtoendutils.SkipIfBinaryIsBelowVersion(t, 25, "vtctld")

	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	utils.ConfirmReplication(t, tablets[0], tablets[1:])

	// Stop the SQL thread on tablets[1]: it keeps receiving changes into its relay logs
	// but stops applying them, like a replica lagging behind on a heavy write load.
	utils.RunSQL(t.Context(), t, `STOP REPLICA SQL_THREAD`, tablets[1])

	// This write is received by every replica but applied only on tablets[2] and tablets[3].
	insertedVal := utils.ConfirmReplication(t, tablets[0], []*cluster.Vttablet{tablets[2], tablets[3]})

	// Confirm that tablets[1] has indeed not applied the write.
	res := utils.RunSQL(t.Context(), t, `select msg from vt_insert_test`, tablets[1])
	assert.Len(t, res.Rows, 1)

	// Kill the primary's vttablet (mysqld keeps running, so the replicas' IO threads stay
	// connected). The lagging replica used to make ERS wait for its whole relay log
	// backlog and time out the reparent; now ERS waits only on the candidates that can win.
	utils.StopTablet(t, tablets[0], false)

	out, err := utils.Ers(clusterInstance, nil, "120s", "30s")
	require.NoError(t, err, out)

	// The lagging replica must not win the election.
	newPrimary := utils.GetNewPrimary(t, clusterInstance)
	require.NotEqual(t, tablets[1].Alias, newPrimary.Alias, "lagging replica must not be promoted")
	err = utils.CheckInsertedValues(t.Context(), t, newPrimary, insertedVal)
	require.NoError(t, err)

	// The lagging replica was replicating when the reparent began, so it is repointed to
	// the new primary with a forced start and catches up. Wait for the data to arrive
	// first: the repoint of non-winning replicas completes asynchronously after ERS
	// returns.
	err = utils.CheckInsertedValues(t.Context(), t, tablets[1], insertedVal)
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
	// primary but not started.
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
