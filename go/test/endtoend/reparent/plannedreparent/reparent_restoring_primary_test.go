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

package plannedreparent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/reparent/utils"
	endtoendutils "vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

// TestPRSRefusedWhileShardPrimaryRestores checks that PlannedReparentShard does not
// promote a replica past a shard primary that is restoring from a backup.
//
// The shard primary lost its data directory and restarted with --restore-from-backup,
// so its tablet is RESTORE while the shard record still names it as primary. Every
// transaction it acknowledged was acknowledged by a replica, but the primary-elect
// never received them: only the other two replicas did, and they have not applied
// them yet. PlannedReparentShard skips RESTORE tablets, so without a refusal it would
// promote the primary-elect, and the acknowledged transactions would exist only in the
// relay logs of the other replicas.
//
// The PRS has to be refused, and EmergencyReparentShard, which the error points to,
// has to recover every acknowledged transaction.
func TestPRSRefusedWhileShardPrimaryRestores(t *testing.T) {
	endtoendutils.SkipIfBinaryIsBelowVersion(t, 25, "vtctld")
	runPRSWithRestoringShardPrimary(t, false /* electReceivesTransactions */)
}

// acknowledgedRows is the number of transactions that the old primary commits while
// the replicas are not applying.
const acknowledgedRows = 20

// runPRSWithRestoringShardPrimary runs the scenario that TestPRSRefusedWhileShardPrimaryRestores
// describes. electReceivesTransactions says whether the primary-elect receives the
// acknowledged transactions into its relay log (and does not apply them), or never
// receives them.
func runPRSWithRestoringShardPrimary(t *testing.T, electReceivesTransactions bool) {
	ctx := t.Context()
	clusterInstance := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	oldPrimary, primaryElect := tablets[0], tablets[1]
	replicas := tablets[1:]

	// The backup that the old primary will start restoring from.
	out, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("Backup", tablets[2].Alias)
	require.NoError(t, err, out)
	utils.ConfirmReplication(t, oldPrimary, replicas)
	rowsBefore := rowCount(ctx, t, oldPrimary)

	// The replicas keep receiving but stop applying, which is what a replica that lags
	// behind its primary looks like. If the primary-elect misses the transactions, its
	// receiver stops too.
	for _, tablet := range replicas {
		utils.RunSQL(ctx, t, "STOP REPLICA SQL_THREAD", tablet)
	}
	if !electReceivesTransactions {
		utils.RunSQL(ctx, t, "STOP REPLICA IO_THREAD", primaryElect)
	}
	ackedBefore := semiSyncAcknowledgedTransactions(ctx, t, oldPrimary)
	for i := range acknowledgedRows {
		utils.RunSQL(ctx, t, utils.GetInsertQuery(1001+i), oldPrimary)
	}
	require.Equal(t, acknowledgedRows, semiSyncAcknowledgedTransactions(ctx, t, oldPrimary)-ackedBefore,
		"the old primary should have committed every transaction after a replica acknowledged it")

	// The old primary loses its data and restarts into a restore that does not finish.
	releaseRestore := stallRestore(t, clusterInstance, oldPrimary)
	defer releaseRestore()
	require.Equal(t, oldPrimary.Alias, utils.GetShardPrimary(t, clusterInstance).Alias, "the shard record should still name the restoring tablet")

	out, err = utils.Prs(t, clusterInstance, primaryElect)
	require.Equal(t, oldPrimary.Alias, utils.GetShardPrimary(t, clusterInstance).Alias,
		"PlannedReparentShard promoted a replica while the shard primary was restoring:\n%s", out)
	require.Error(t, err)
	assert.Contains(t, out, "restoring from a backup")

	// Let the appliers run again, as they would, and recover the way the error says to.
	for _, tablet := range replicas {
		utils.RunSQL(ctx, t, "START REPLICA SQL_THREAD", tablet)
	}
	out, err = utils.ErsIgnoreTablet(clusterInstance, nil, "90s", "30s", []*cluster.Vttablet{oldPrimary}, false)
	require.NoError(t, err, out)

	newPrimary := utils.GetShardPrimary(t, clusterInstance)
	require.NotEqual(t, oldPrimary.Alias, newPrimary.Alias)
	wantRows := rowsBefore + acknowledgedRows
	assert.Equal(t, wantRows, rowCount(ctx, t, newPrimary), "transactions that the old primary acknowledged are missing on the new primary")
	for _, tablet := range replicas {
		if tablet.Alias == newPrimary.Alias {
			continue
		}
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, wantRows, rowCount(ctx, t, tablet), "%s", tablet.Alias)
		}, 30*time.Second, 200*time.Millisecond)
	}
}

// stallRestore kills the tablet, wipes its data directory and starts it again with
// --restore_from_backup, with the MANIFEST of every backup replaced by a FIFO that
// nobody writes to, so that the restore blocks. It returns once the tablet is RESTORE.
// The returned function lets the restore fail, and has to run before the cluster is
// torn down so that the vttablet is not left waiting on the FIFO.
func stallRestore(t *testing.T, clusterInstance *cluster.LocalProcessCluster, tablet *cluster.Vttablet) (release func()) {
	utils.KillTablet(t, tablet)
	require.Eventually(t, func() bool {
		// mysqld may still be writing to the directory right after it was killed.
		return os.RemoveAll(tablet.MysqlctlProcess.BasePath()) == nil
	}, 30*time.Second, 200*time.Millisecond, "could not remove the data directory of %s", tablet.Alias)

	manifests, err := filepath.Glob(filepath.Join(tablet.VttabletProcess.FileBackupStorageRoot, utils.KeyspaceName, utils.ShardName, "*", "MANIFEST"))
	require.NoError(t, err)
	require.NotEmpty(t, manifests, "there is no backup to restore from")
	for _, manifest := range manifests {
		require.NoError(t, os.Remove(manifest))
		require.NoError(t, syscall.Mkfifo(manifest, 0o600))
	}
	release = func() {
		for _, manifest := range manifests {
			go func() {
				// Opening the FIFO for writing unblocks the restore, which then reads an empty manifest and fails.
				if fifo, err := os.OpenFile(manifest, os.O_WRONLY, 0); err == nil {
					fifo.Close()
				}
			}()
		}
	}

	tablet.MysqlctlProcess.InitMysql = true
	require.NoError(t, tablet.MysqlctlProcess.Start())
	tablet.VttabletProcess.SupportsBackup = true
	tablet.VttabletProcess.ServingStatus = ""
	require.NoError(t, tablet.VttabletProcess.Setup())

	restoring := regexp.MustCompile(`"type":\s*"RESTORE"`)
	require.Eventually(t, func() bool {
		out, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("GetTablet", tablet.Alias)
		return err == nil && restoring.MatchString(out)
	}, 90*time.Second, 500*time.Millisecond, "%s never became RESTORE", tablet.Alias)
	return release
}

// semiSyncAcknowledgedTransactions returns how many transactions the primary has committed
// after a replica acknowledged them.
func semiSyncAcknowledgedTransactions(ctx context.Context, t *testing.T, primary *cluster.Vttablet) int {
	semiSyncType, err := utils.SemiSyncExtensionLoaded(ctx, primary)
	require.NoError(t, err)
	status := "Rpl_semi_sync_source_yes_tx"
	if semiSyncType == mysql.SemiSyncTypeMaster {
		status = "Rpl_semi_sync_master_yes_tx"
	}
	n, err := strconv.Atoi(scalar(ctx, t, primary, fmt.Sprintf("select variable_value from performance_schema.global_status where variable_name = '%s'", status)))
	require.NoError(t, err)
	return n
}

func rowCount(ctx context.Context, t *testing.T, tablet *cluster.Vttablet) int {
	n, err := strconv.Atoi(scalar(ctx, t, tablet, "select count(*) from vt_insert_test"))
	require.NoError(t, err)
	return n
}

func scalar(ctx context.Context, t *testing.T, tablet *cluster.Vttablet, query string) string {
	qr := utils.RunSQL(ctx, t, query, tablet)
	require.Len(t, qr.Rows, 1, query)
	return qr.Rows[0][0].ToString()
}
