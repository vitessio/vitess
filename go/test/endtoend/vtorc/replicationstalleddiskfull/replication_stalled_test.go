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
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
)

// TestReplicationStalledDiskFull asserts VTOrc surfaces the new analysis
// when a replica's IO/SQL threads are still "Yes" but InnoDB is parked on
// ENOSPC. The replica's mysqld is launched against a 256 MB ext4 loopback
// mount that holds *only* the InnoDB filesystem (datadir, redo log, undo,
// tmpdir) — relay log + binlog stay on the regular disk so the IO thread
// keeps running while InnoDB's tablespace extends silently fail.
//
// The test runs inserts in a goroutine and polls performance_schema.error_log
// for the disk-full InnoDB messages (MY-012814 / MY-012820). Once they
// appear we assert SHOW REPLICA STATUS still reports both threads as Yes
// (the wedge state) and then poll vtorc's analysis API for the new code.
func TestReplicationStalledDiskFull(t *testing.T) {
	require.NotNil(t, primaryTablet)
	require.NotNil(t, replicaTablet)
	require.NotNil(t, vtorcProcess)

	// Sanity check: replication is healthy before we wedge it.
	utils.CheckReplication(t, &utils.VTOrcClusterInfo{ClusterInstance: clusterInstance},
		primaryTablet, []*cluster.Vttablet{replicaTablet}, 30*time.Second)

	_, err := utils.RunSQL(t, "CREATE TABLE filler (id INT AUTO_INCREMENT PRIMARY KEY, blob_col MEDIUMBLOB) ENGINE=InnoDB",
		primaryTablet, "vt_"+keyspaceName)
	require.NoError(t, err, "create filler table")

	// Drive inserts continuously in a goroutine so InnoDB keeps pressure on
	// the small mount even after the disk is "full". InnoDB doesn't fail
	// fast — it retries — and we need sustained writes to provoke the
	// MY-012814 / MY-012820 log entries our analysis keys on.
	//
	// runSQLNoFail is used here instead of utils.RunSQL because the latter
	// calls require.Nil from inside, which is unsafe from a goroutine.
	insertCtx, stopInserts := context.WithCancel(context.Background())
	defer stopInserts()

	var (
		inserts      atomic.Int64
		insertErrors atomic.Int64
		lastErr      atomic.Pointer[string]
	)
	go func() {
		for insertCtx.Err() == nil {
			err := runSQLNoFail(insertCtx,
				"INSERT INTO filler (blob_col) VALUES (REPEAT('x', 1048576))",
				primaryTablet, "vt_"+keyspaceName)
			if err != nil {
				if insertCtx.Err() != nil {
					return // shutting down
				}
				insertErrors.Add(1)
				s := err.Error()
				lastErr.Store(&s)
				time.Sleep(50 * time.Millisecond)
				continue
			}
			inserts.Add(1)
		}
	}()

	// Poll error_log for the InnoDB disk-full codes. Up to 120s — InnoDB's
	// dirty-page flush cadence + buffer pool size mean we may need a fair
	// bit of cumulative writes before ibdata1 actually fails to extend.
	deadline := time.Now().Add(120 * time.Second)
	gotDiskFullEntry := false
	for time.Now().Before(deadline) {
		errLog, err := utils.RunSQL(t, `SELECT COUNT(*) AS c FROM performance_schema.error_log
			WHERE prio = 'Error' AND subsystem = 'InnoDB'
			  AND error_code IN ('MY-012814', 'MY-012820')`,
			replicaTablet, "")
		if err == nil && len(errLog.Rows) > 0 {
			if count, _ := errLog.Named().Row().ToInt64("c"); count > 0 {
				gotDiskFullEntry = true
				break
			}
		}
		time.Sleep(2 * time.Second)
		t.Logf("waiting for InnoDB ENOSPC: inserts=%d, insertErrors=%d, free=%s",
			inserts.Load(), insertErrors.Load(), formatBytes(diskMount))
	}
	if !gotDiskFullEntry {
		var lastErrStr string
		if p := lastErr.Load(); p != nil {
			lastErrStr = *p
		}
		t.Fatalf("performance_schema.error_log never recorded MY-012814/MY-012820: inserts=%d insertErrors=%d lastErr=%q",
			inserts.Load(), insertErrors.Load(), lastErrStr)
	}
	t.Logf("InnoDB ENOSPC observed after %d primary inserts (%d errors)", inserts.Load(), insertErrors.Load())

	// Confirm we hit case 2 (silent InnoDB retry, IO thread unaffected) and
	// not case 1 (relay log filesystem fails, IO thread stops). With relay
	// log on the regular disk this should always be case 2.
	res, err := utils.RunSQL(t, "SHOW REPLICA STATUS", replicaTablet, "")
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows, "SHOW REPLICA STATUS returned no rows")
	row := res.Named().Row()
	require.Equal(t, "Yes", row.AsString("Replica_IO_Running", ""), "Replica_IO_Running should be Yes")
	require.Equal(t, "Yes", row.AsString("Replica_SQL_Running", ""), "Replica_SQL_Running should be Yes")

	// Poll vtorc's analysis API until it surfaces the new code for the
	// replica. Allow up to 60s for two discovery cycles + the analysis pass.
	utils.MakeAPICallRetryTimeout(t, vtorcProcess,
		"/api/replication-analysis", 60*time.Second,
		func(_ int, response string) bool {
			return !strings.Contains(response, "ReplicationStalledDiskFull")
		})

	// Self-healing check: stop inserts, drop the filler on the primary, and
	// verify the analysis clears once InnoDB resumes commits.
	stopInserts()
	_, err = utils.RunSQL(t, "DROP TABLE filler", primaryTablet, "vt_"+keyspaceName)
	require.NoError(t, err)

	utils.MakeAPICallRetryTimeout(t, vtorcProcess,
		"/api/replication-analysis", 90*time.Second,
		func(_ int, response string) bool {
			return strings.Contains(response, "ReplicationStalledDiskFull")
		})
}

// formatBytes returns a short human-readable free-space figure for the
// loopback mount, used in periodic diagnostic logs while we wait for the
// InnoDB ENOSPC entry.
func formatBytes(m *mount) string {
	if m == nil {
		return "?"
	}
	free, err := m.freeBytes()
	if err != nil {
		return "?"
	}
	return fmt.Sprintf("%d KiB", free/1024)
}

// runSQLNoFail executes a query against a tablet's mysqld without using
// require.Nil; safe to call from a goroutine. Errors are returned for the
// caller to inspect; the insert loop above swallows them because we only
// care that *some* inserts land, not that every one succeeds.
func runSQLNoFail(ctx context.Context, sql string, tablet *cluster.Vttablet, db string) error {
	params := mysql.ConnParams{
		Uname:      "vt_dba",
		UnixSocket: path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/mysql.sock", tablet.TabletUID)),
	}
	if db != "" {
		params.DbName = db
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := mysql.Connect(cctx, &params)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.ExecuteFetch(sql, 1000, false)
	return err
}
