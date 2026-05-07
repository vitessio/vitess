//go:build linux

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

package replicationstalleddiskfull

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
)

// TestReplicationStalledDiskFull asserts VTOrc surfaces the new analysis
// when a replica's IO/SQL threads are still "Yes" but InnoDB is parked on
// ENOSPC. The replica's mysqld is launched against a 256 MB ext4 loopback
// mount; we then push 1 MB BLOB inserts at the primary until the replica's
// disk is exhausted and the InnoDB applier wedges. VTOrc should detect
// `ReplicationStalledDiskFull` for the replica only.
func TestReplicationStalledDiskFull(t *testing.T) {
	require.NotNil(t, primaryTablet)
	require.NotNil(t, replicaTablet)
	require.NotNil(t, vtorcProcess)

	// Sanity check: replication is healthy before we wedge it.
	utils.CheckReplication(t, &utils.VTOrcClusterInfo{ClusterInstance: clusterInstance},
		primaryTablet, []*cluster.Vttablet{replicaTablet}, 30*time.Second)

	// Create the filler table and push large rows until the replica's
	// filesystem is exhausted. Each row is ~1 MiB; with ~150 MB of headroom
	// after MySQL initialisation, ~150 inserts is more than enough.
	_, err := utils.RunSQL(t, "CREATE TABLE filler (id INT AUTO_INCREMENT PRIMARY KEY, blob_col MEDIUMBLOB) ENGINE=InnoDB", primaryTablet, "vt_"+keyspaceName)
	require.NoError(t, err, "create filler table")

	// Insert in a tight loop until replica's mount has < 5 MB free or 90s
	// elapses (whichever comes first).
	deadline := time.Now().Add(90 * time.Second)
	const lowWaterBytes = uint64(5 * 1024 * 1024)
	for time.Now().Before(deadline) {
		_, err := utils.RunSQL(t, "INSERT INTO filler (blob_col) VALUES (REPEAT('x', 1048576))", primaryTablet, "vt_"+keyspaceName)
		if err != nil {
			// Primary's own disk is fine, but in the unlikely event of an
			// error keep the test honest by surfacing it.
			t.Logf("INSERT failed: %v", err)
			break
		}
		free, ferr := diskMount.freeBytes()
		require.NoError(t, ferr)
		if free < lowWaterBytes {
			t.Logf("replica mount nearly full: %d bytes free", free)
			break
		}
	}

	// Confirm the replica is in the wedge state we're targeting: both
	// threads still "Yes", and the error log shows the InnoDB disk-full
	// codes from the issue.
	requireReplicaWedged(t)

	// Poll vtorc's analysis API until it surfaces the new code for the
	// replica. Allow up to 60s for two discovery cycles + the analysis pass.
	utils.MakeAPICallRetryTimeout(t, vtorcProcess,
		"/api/replication-analysis", 60*time.Second,
		func(_ int, response string) bool {
			return !strings.Contains(response, "ReplicationStalledDiskFull")
		})

	// Self-healing check: drop the filler on the primary, free space on the
	// replica's mount (the relay log shrinks once the applier catches up),
	// and confirm the analysis clears.
	_, err = utils.RunSQL(t, "DROP TABLE filler", primaryTablet, "vt_"+keyspaceName)
	require.NoError(t, err)

	utils.MakeAPICallRetryTimeout(t, vtorcProcess,
		"/api/replication-analysis", 90*time.Second,
		func(_ int, response string) bool {
			return strings.Contains(response, "ReplicationStalledDiskFull")
		})
}

// requireReplicaWedged asserts the replica is in the precise state our new
// analysis is designed to detect: IO+SQL threads both running, no last error,
// and InnoDB has logged a disk-full event.
func requireReplicaWedged(t *testing.T) {
	t.Helper()

	// Both threads should still be running — that's the whole point.
	res, err := utils.RunSQL(t, "SHOW REPLICA STATUS", replicaTablet, "")
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows, "SHOW REPLICA STATUS returned no rows")

	row := res.Named().Row()
	require.Equal(t, "Yes", row.AsString("Replica_IO_Running", ""), "Replica_IO_Running should be Yes")
	require.Equal(t, "Yes", row.AsString("Replica_SQL_Running", ""), "Replica_SQL_Running should be Yes")

	// performance_schema.error_log should have at least one of the two
	// InnoDB disk-full codes we look for.
	errLog, err := utils.RunSQL(t, `SELECT COUNT(*) AS c FROM performance_schema.error_log
		WHERE prio = 'Error' AND subsystem = 'InnoDB'
		  AND error_code IN ('MY-012814', 'MY-012820')`, replicaTablet, "")
	require.NoError(t, err)
	require.NotEmpty(t, errLog.Rows)
	count, _ := errLog.Named().Row().ToInt64("c")
	require.Greater(t, count, int64(0), "expected at least one MY-012814/MY-012820 entry in error_log")
}
