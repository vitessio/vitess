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

package clone

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vitesst"
)

var (
	cell              = "zone1"
	keyspaceName      = "ks"
	shardName         = "0"
	vttabletExtraArgs = []string{
		"--vreplication-retry-delay", "1s",
		"--degraded-threshold", "5s",
		"--lock-tables-timeout", "5s",
		"--enable-replication-reporter",
		"--serving-state-grace-period", "1s",
		"--mysql-clone-enabled",
	}
	vtInsertTest = `
		create table if not exists vt_insert_test (
		id bigint auto_increment,
		msg varchar(64),
		primary key (id)
		) Engine=InnoDB;`
)

// cloneCnfPath is the clone plugin configuration the image carries; every
// mysqld picks it up through EXTRA_MY_CNF.
const cloneCnfPath = "/vt/config/mycnf/clone.cnf"

// initCloneSQL creates the user MySQL CLONE operations connect to the donor
// with. BACKUP_ADMIN is required on the donor for clone operations.
const initCloneSQL = `CREATE USER IF NOT EXISTS 'vt_clone'@'%';
GRANT BACKUP_ADMIN ON *.* TO 'vt_clone'@'%';`

// startCluster brings up a cluster for the shard under test: file backup
// storage, the CLONE plugin loaded in every mysqld, and the vt_clone user
// seeded through init_db.sql.
func startCluster(t *testing.T, opts ...vitesst.ClusterOption) *vitesst.Cluster {
	t.Helper()

	base := []vitesst.ClusterOption{
		vitesst.WithBackupStorage(),
		vitesst.WithVTOrc(),
		vitesst.WithoutVTGate(),
		vitesst.WithTabletEnv(map[string]string{"EXTRA_MY_CNF": cloneCnfPath}),
		vitesst.WithInitDBSQLExtra(initCloneSQL),
		vitesst.WithVTTabletArgs(vttabletExtraArgs...),
	}

	cluster, err := vitesst.NewCluster(t, append(base, opts...)...)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t, t.Context())
	t.Cleanup(func() {
		ctx := context.WithoutCancel(t.Context())
		if err := cleanup(ctx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)

	return cluster
}

// disableVTOrcRecoveries stops VTOrc from running any recoveries.
func disableVTOrcRecoveries(t *testing.T, cluster *vitesst.Cluster) {
	status, _, err := cluster.VTOrc().MakeAPICallRetry(t.Context(), "/api/disable-global-recoveries",
		30*time.Second, func(status int, _ string) bool {
			return status == http.StatusOK
		})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
}

// removeBackups removes all backups for the test shard.
func removeBackups(t *testing.T, cluster *vitesst.Cluster) {
	ctx := context.WithoutCancel(t.Context())
	require.NoError(t, cluster.RemoveAllBackups(ctx, keyspaceName, shardName))
}

// verifyRowsInTablet verifies the total number of rows in a tablet.
func verifyRowsInTablet(t *testing.T, tablet *vitesst.Tablet, expectedRows int) {
	lastNumRowsFound := 0
	require.Eventuallyf(t, func() bool {
		// Ignoring the error check, if the newly created table is not replicated, then there might be error and we
		// should ignore it, but eventually it will catch up and if not caught up in required time, testcase will fail.
		qr, err := tablet.QueryTablet(t.Context(), "select * from vt_insert_test")
		if err != nil {
			return false
		}
		lastNumRowsFound = len(qr.Rows)
		return lastNumRowsFound == expectedRows
	}, time.Minute, 300*time.Millisecond,
		"unexpected number of rows in %s (%s.vt_insert_test): found %d", tablet.Alias(), keyspaceName, lastNumRowsFound)
}

// waitInsertedRows checks that the specific test data we inserted on primary
// exists on the cloned replica. This proves data was actually transferred.
func waitInsertedRows(
	t *testing.T,
	tablet *vitesst.Tablet,
	expectedValues []string,
	waitFor time.Duration,
	tickInterval time.Duration,
) {
	require.Eventually(t, func() bool {
		qr, err := tablet.QueryTablet(t.Context(), "SELECT msg FROM vt_insert_test ORDER BY id")
		if err != nil {
			return false
		}
		if len(qr.Rows) != len(expectedValues) {
			return false
		}

		for i, row := range qr.Rows {
			if row[0].ToString() != expectedValues[i] {
				return false
			}
		}
		return true
	}, waitFor, tickInterval)
}
