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
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

// gtidExecuted returns the tablet's @@global.gtid_executed.
func gtidExecuted(t *testing.T, tablet *cluster.Vttablet) string {
	res := utils.RunSQL(t.Context(), t, `select @@global.gtid_executed`, tablet)
	return strings.ReplaceAll(res.Rows[0][0].ToString(), "\n", "")
}

// gtidSubset reports whether the tablet evaluates GTID_SUBSET(subset, set) as true.
func gtidSubset(t *testing.T, tablet *cluster.Vttablet, subset, set string) bool {
	res := utils.RunSQL(t.Context(), t, fmt.Sprintf(`select gtid_subset('%s', '%s')`, subset, set), tablet)
	return res.Rows[0][0].ToString() == "1"
}

// receivedGTIDs returns the GTIDs the tablet has received: executed, plus retrieved into the
// relay log.
func receivedGTIDs(t *testing.T, tablet *cluster.Vttablet) string {
	retrieved := strings.ReplaceAll(replicaStatusField(t, tablet, "Retrieved_Gtid_Set"), "\n", "")
	executed := gtidExecuted(t, tablet)
	if retrieved == "" {
		return executed
	}
	return executed + "," + retrieved
}

// TestERSKeepsAckedTransactionsAcrossReplicaRepoint reproduces how repointing a replica used to
// silently lose semi-sync acknowledged writes. A semi-sync replica acknowledges a transaction
// once it is in its relay log, and ERS elects the new primary by received position. Repointing
// the replica with STOP REPLICA + CHANGE REPLICATION SOURCE discarded its relay log, so if it
// could not fetch the transactions again before the primary died, they were gone and ERS
// promoted a tablet without them.
//
// tablets[1] is the only acker (the other replicas' receivers are stopped) and its applier is
// delayed, so the acknowledged writes exist only in its relay log. It is then repointed to the
// primary it replicates from, exactly as VTOrc's fixReplica does (with a heartbeat interval),
// while its receiver cannot reconnect to the primary (the replication user is locked there).
// The primary then dies, and every acknowledged write must survive ERS.
func TestERSKeepsAckedTransactionsAcrossReplicaRepoint(t *testing.T) {
	endtoendutils.SkipIfBinaryIsBelowVersion(t, 25, "vttablet")

	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	primary, acker, others := tablets[0], tablets[1], tablets[2:]

	utils.ConfirmReplication(t, primary, tablets[1:])

	// Only the acker receives from here on.
	for _, tablet := range others {
		utils.RunSQL(t.Context(), t, `STOP REPLICA IO_THREAD`, tablet)
	}
	// The acker keeps its replication connection, but can't open a new one. The lock is not
	// binlogged, so it doesn't replicate.
	utils.RunSQLs(t.Context(), t, []string{
		`SET sql_log_bin = 0`,
		`ALTER USER 'vt_repl'@'%' ACCOUNT LOCK`,
	}, primary)
	// The acker receives and acknowledges transactions, but applies them only after the delay.
	// The delay must outlast the steps up to the repoint, and ERS waits for it to pass while
	// holding the shard lock, which must not expire (topo.LockTimeout).
	const applyDelay = 25 * time.Second
	utils.RunSQLs(t.Context(), t, []string{
		`STOP REPLICA SQL_THREAD`,
		fmt.Sprintf(`CHANGE REPLICATION SOURCE TO SOURCE_DELAY = %d`, int(applyDelay.Seconds())),
		`START REPLICA SQL_THREAD`,
	}, acker)

	// Each write commits once the acker has it in its relay log.
	before := gtidExecuted(t, primary)
	var ids []int
	for i := range 20 {
		id := 1000 + i
		utils.RunSQL(t.Context(), t, utils.GetInsertQuery(id), primary)
		ids = append(ids, id)
	}
	res := utils.RunSQL(t.Context(), t, fmt.Sprintf(`select gtid_subtract('%s', '%s')`, gtidExecuted(t, primary), before), primary)
	acked := strings.ReplaceAll(res.Rows[0][0].ToString(), "\n", "")
	require.NotEmpty(t, acked)
	require.True(t, gtidSubset(t, acker, acked, receivedGTIDs(t, acker)), "the acker must have received the acknowledged writes")
	require.False(t, gtidSubset(t, acker, acked, gtidExecuted(t, acker)), "the acker must not have applied the acknowledged writes yet")

	// Repoint the acker like VTOrc's fixReplica does.
	ackerTablet, err := clusterInstance.VtctldClientProcess.GetTablet(acker.Alias)
	require.NoError(t, err)
	primaryAlias, err := topoproto.ParseTabletAlias(primary.Alias)
	require.NoError(t, err)
	tmc := grpctmclient.NewClient()
	t.Cleanup(tmc.Close)
	err = tmc.SetReplicationSource(t.Context(), ackerTablet, primaryAlias, 0, "", true, true, 5)
	require.NoError(t, err)

	// The repoint must keep the acknowledged writes, which the acker cannot fetch again.
	assert.Equal(t, "Yes", replicaStatusField(t, acker, "Replica_SQL_Running"))
	assert.NotEqual(t, "Yes", replicaStatusField(t, acker, "Replica_IO_Running"), "the acker must not be able to reconnect to the primary")
	require.True(t, gtidSubset(t, acker, acked, receivedGTIDs(t, acker)),
		"the repoint discarded acknowledged writes %s from the acker's relay log (received now: %s)", acked, receivedGTIDs(t, acker))

	// The primary dies, and ERS must promote the acker once it has applied its relay log.
	utils.StopTablet(t, primary, true)
	out, err := utils.Ers(clusterInstance, nil, "120s", "60s")
	require.NoError(t, err, out)

	newPrimary := utils.GetNewPrimary(t, clusterInstance)
	res = utils.RunSQL(t.Context(), t, fmt.Sprintf(`select count(*) from vt_insert_test where id between %d and %d`, ids[0], ids[len(ids)-1]), newPrimary)
	assert.Equal(t, strconv.Itoa(len(ids)), res.Rows[0][0].ToString(), "acknowledged writes are missing on the new primary %s", newPrimary.Alias)
	assert.Equal(t, acker.Alias, newPrimary.Alias, "only the acker has the acknowledged writes")
}
