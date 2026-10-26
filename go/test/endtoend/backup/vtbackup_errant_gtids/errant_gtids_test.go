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

package vtbackuperrantgtids

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

const (
	vtInsertTest = `create table if not exists vt_insert_test (
		id bigint auto_increment,
		msg varchar(64),
		primary key (id)
		) Engine=InnoDB`

	// initSQLOpen is the max_connections value that vtbackup's init SQL sets
	// right after it restarts replication, to show that replication is open.
	initSQLOpen = 13337

	// initSQLSleepSeconds holds vtbackup's replication open long enough for a
	// subtest to write on the primary and see vtbackup apply the write.
	initSQLSleepSeconds = 15
)

// TestVtbackupErrantGTIDs runs vtbackup against base backups and data that have
// GTIDs the shard primary lacks, and checks that it fails each such run without
// completing a backup. The subtests share one shard and run in order; the last
// two kill primaries.
func TestVtbackupErrantGTIDs(t *testing.T) {
	// --enable-errant-gtid-checks is new in v25.
	utils.SkipIfBinaryIsBelowVersion(t, 25, "vtbackup")
	initTablets(t)
	_, err := primary.VttabletProcess.QueryTablet(vtInsertTest, keyspaceName, true)
	require.NoError(t, err)
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('baseline')", keyspaceName, true)
	require.NoError(t, err)
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 1)
	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 1)

	require.True(t, t.Run("poisoned base backup", testPoisonedBaseBackup))
	require.True(t, t.Run("binlogged init SQL", testBinloggedInitSQL))
	require.True(t, t.Run("primary blocked on semi-sync", testPrimaryBlockedOnSemiSync))
	require.True(t, t.Run("reparent during the upload", testReparentDuringUpload))
}

// testPoisonedBaseBackup checks that vtbackup refuses a base backup with an
// errant GTID, takes a backup anyway with the checks off, and succeeds once the
// primary has the GTID, which is the remedy the error names.
func testPoisonedBaseBackup(t *testing.T) {
	// An empty transaction under a new UUID changes no data, so injecting it
	// on the primary afterwards is a safe repair.
	const errantGTID = "00000000-0000-0000-0000-0000000e4407:1"
	// Replication on replica2 stays stopped until the primary has the GTID. A
	// backup that restarts replication waits for the replica's position to move
	// whenever it differs from the primary's, which the errant GTID makes it do
	// until the primary writes again. The tablet then refuses to repoint
	// replication, so replication also stays stopped after the backup.
	execOnTablet(t, replica2,
		"SET GLOBAL super_read_only = OFF",
		fmt.Sprintf("SET GTID_NEXT = '%s'", errantGTID),
		"BEGIN",
		"COMMIT",
		"SET GTID_NEXT = 'AUTOMATIC'",
		"SET GLOBAL super_read_only = ON",
		"STOP REPLICA",
	)
	require.NoError(t, localCluster.VtctldClientProcess.ExecuteCommand("Backup", replica2.Alias))
	backups := listBackups(t)
	require.NotEmpty(t, backups)

	err := startVtbackup(t).wait(t)
	require.ErrorContains(t, err, "restored backup "+backups[len(backups)-1]+" has errant GTIDs "+errantGTID)
	require.ErrorContains(t, err, "came from the restored backup")
	assert.Len(t, listBackups(t), len(backups))

	require.NoError(t, startVtbackup(t, "--enable-errant-gtid-checks=false").wait(t))
	assert.Len(t, listBackups(t), len(backups)+1)

	execOnTablet(t, primary,
		fmt.Sprintf("SET GTID_NEXT = '%s'", errantGTID),
		"BEGIN",
		"COMMIT",
		"SET GTID_NEXT = 'AUTOMATIC'",
	)
	execOnTablet(t, replica2, "START REPLICA")
	require.NoError(t, startVtbackup(t).wait(t))
	assert.Len(t, listBackups(t), len(backups)+2)
}

// testBinloggedInitSQL checks that vtbackup refuses to back up data that its
// own init SQL gave a GTID, before the upload, and names the remedy.
func testBinloggedInitSQL(t *testing.T) {
	backups := listBackups(t)
	err := startVtbackup(t,
		"--init-backup-sql-queries", "ANALYZE TABLE vt_"+keyspaceName+".vt_insert_test",
		"--init-backup-tablet-types", "backup",
		"--init-backup-sql-timeout", "30s",
		"--init-backup-sql-fail-on-error",
	).wait(t)
	require.ErrorContains(t, err, "the data to back up has errant GTIDs")
	require.ErrorContains(t, err, "were written by this run's --init-backup-sql-queries")
	assert.Len(t, listBackups(t), len(backups))
}

// testPrimaryBlockedOnSemiSync makes vtbackup apply a write that the primary is
// still waiting to have acknowledged. vtbackup must wait before the upload: it
// takes the backup once a replica acknowledges the write, and fails, uploading
// nothing, once a reparent loses it.
func testPrimaryBlockedOnSemiSync(t *testing.T) {
	stopReplicaIOThreads := func() {
		for _, replica := range []*cluster.Vttablet{replica1, replica2} {
			execOnTablet(t, replica, "STOP REPLICA IO_THREAD")
		}
	}

	// A replica acknowledges the write, so the backup is taken.
	backups := listBackups(t)
	run := startVtbackup(t, initSQLWindow()...)
	run.waitForInitSQLWindow(t)
	stopReplicaIOThreads()
	writeInBackground(t, primary, "acknowledged")
	run.waitForRow(t, "acknowledged")
	run.waitForLog(t, "has not committed")
	execOnTablet(t, replica1, "START REPLICA IO_THREAD")
	require.NoError(t, run.wait(t))
	assert.Len(t, listBackups(t), len(backups)+1)
	execOnTablet(t, replica2, "START REPLICA IO_THREAD")

	// A reparent loses the write, so the run fails and uploads nothing.
	backups = listBackups(t)
	run = startVtbackup(t, initSQLWindow()...)
	run.waitForInitSQLWindow(t)
	stopReplicaIOThreads()
	writeInBackground(t, primary, "lost")
	run.waitForRow(t, "lost")
	run.waitForLog(t, "has not committed")
	killTabletHard(t, primary)
	// With durability none, the next subtest's primary commits without an
	// acknowledgement, and the reparent there needs no acknowledging replica.
	setDurabilityPolicy(t, "none")
	require.NoError(t, localCluster.VtctldClientProcess.ExecuteCommand("EmergencyReparentShard", shardKsName,
		"--new-primary", replica1.Alias, "--wait-replicas-timeout", "30s"))
	requireRowAbsent(t, replica1, "lost")

	err := run.wait(t)
	require.ErrorContains(t, err, "the data to back up has errant GTIDs")
	require.ErrorContains(t, err, "during catch-up")
	assert.Len(t, listBackups(t), len(backups))
}

// testReparentDuringUpload makes a reparent lose a committed write while
// vtbackup uploads a backup that contains it. The check before the MANIFEST
// must abort the backup. It continues from testPrimaryBlockedOnSemiSync:
// replica1 is the primary, under durability none.
func testReparentDuringUpload(t *testing.T) {
	backups := listBackups(t)
	holdDir := t.TempDir()
	started, release := path.Join(holdDir, "started"), path.Join(holdDir, "release")
	// The compressor holds the upload until the test releases it.
	compressor := fmt.Sprintf("sh -c 'touch %s; while [ ! -e %s ]; do sleep 0.1; done; exec gzip'", started, release)

	run := startVtbackup(t, append(initSQLWindow(), "--external-compressor", compressor)...)
	// Cleanups run last in, first out, so a failed test releases the compressors
	// before startVtbackup's cleanup stops vtbackup.
	t.Cleanup(func() { _ = os.WriteFile(release, nil, 0o644) })
	run.waitForInitSQLWindow(t)
	execOnTablet(t, replica2, "STOP REPLICA IO_THREAD")
	writeInBackground(t, replica1, "lost-in-upload")
	run.waitForRow(t, "lost-in-upload")
	require.Eventually(t, func() bool {
		_, err := os.Stat(started)
		return err == nil
	}, 4*time.Minute, 250*time.Millisecond, "the upload never started")

	killTabletHard(t, replica1)
	// Both dead tablets are ignored: the original primary's record is still in topo.
	// Two unreachable tablets would fail ERS's revoke check, even under durability
	// none, and repointing a dead replica would fail the reparent.
	require.NoError(t, localCluster.VtctldClientProcess.ExecuteCommand("EmergencyReparentShard", shardKsName,
		"--new-primary", replica2.Alias, "--ignore-replicas", primary.Alias+","+replica1.Alias,
		"--wait-replicas-timeout", "30s"))
	requireRowAbsent(t, replica2, "lost-in-upload")
	require.NoError(t, os.WriteFile(release, nil, 0o644))

	err := run.wait(t)
	require.ErrorContains(t, err, "the new backup has errant GTIDs")
	assert.Len(t, listBackups(t), len(backups))
}

// initSQLWindow returns init SQL flags that restart replication after catch-up,
// hold it open for initSQLSleepSeconds, then stop it again before the backup's
// checks. None of these statements writes a GTID.
func initSQLWindow() []string {
	return []string{
		"--init-backup-sql-queries", "START REPLICA",
		"--init-backup-sql-queries", fmt.Sprintf("SET GLOBAL max_connections = %d", initSQLOpen),
		"--init-backup-sql-queries", fmt.Sprintf("DO SLEEP(%d)", initSQLSleepSeconds),
		"--init-backup-sql-queries", "STOP REPLICA",
		"--init-backup-tablet-types", "backup",
		"--init-backup-sql-timeout", "90s",
		"--init-backup-sql-fail-on-error",
	}
}

// vtbackupRun is a vtbackup run in the background.
type vtbackupRun struct {
	socket string
	stderr string
	done   chan error
}

// startVtbackup starts vtbackup in the background, with the errant GTID checks
// on, and with the given extra flags.
func startVtbackup(t *testing.T, extraArgs ...string) *vtbackupRun {
	t.Helper()
	// A short temp path keeps the socket under the unix socket length limit.
	socketFile, err := os.CreateTemp("", "vtb_errant.sock")
	require.NoError(t, err)
	socket := socketFile.Name()
	require.NoError(t, socketFile.Close())
	require.NoError(t, os.Remove(socket))

	vtb := cluster.VtbackupProcessInstance(
		localCluster.GetAndReserveTabletUID(), localCluster.GetAndReservePort(),
		newInitDBFile, keyspaceName, shardName, cell, hostname,
		localCluster.TmpDirectory, localCluster.TopoPort, false,
	)
	vtb.ExtraArgs = append([]string{"--db-credentials-file", dbCredentialFile, "--mysql-socket", socket, "--enable-errant-gtid-checks"}, extraArgs...)
	vtb.StderrFile = path.Join(t.TempDir(), "vtbackup-stderr.txt")
	run := &vtbackupRun{socket: socket, stderr: vtb.StderrFile, done: make(chan error, 1)}
	go func() { run.done <- vtb.Setup() }()
	t.Cleanup(func() { stopLeakedVtbackup(run.done, socket) })
	return run
}

// wait waits for the run to exit and returns its error.
func (r *vtbackupRun) wait(t *testing.T) error {
	t.Helper()
	select {
	case err := <-r.done:
		// Put it back so the cleanup sees that the run has exited.
		r.done <- err
		return err
	case <-time.After(5 * time.Minute):
		require.FailNow(t, "timed out waiting for vtbackup to exit")
		return nil
	}
}

// waitForInitSQLWindow waits until the run's init SQL has restarted replication.
func (r *vtbackupRun) waitForInitSQLWindow(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool {
		qr, err := vtbackupQuery(t, r.socket, "select @@global.max_connections")
		return err == nil && len(qr.Rows) == 1 && qr.Rows[0][0].ToString() == strconv.Itoa(initSQLOpen)
	}, 4*time.Minute, 500*time.Millisecond, "vtbackup never reached its init SQL window")
}

// waitForRow waits until the run's mysqld has the row with msg.
func (r *vtbackupRun) waitForRow(t *testing.T, msg string) {
	t.Helper()
	require.Eventually(t, func() bool {
		qr, err := vtbackupQuery(t, r.socket, fmt.Sprintf("select count(*) from vt_insert_test where msg = '%s'", msg))
		return err == nil && len(qr.Rows) == 1 && qr.Rows[0][0].ToString() == "1"
	}, time.Minute, 250*time.Millisecond, "vtbackup never applied %q", msg)
}

// waitForLog waits until the run's log contains substr.
func (r *vtbackupRun) waitForLog(t *testing.T, substr string) {
	t.Helper()
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(r.stderr)
		return err == nil && strings.Contains(string(data), substr)
	}, 2*time.Minute, 250*time.Millisecond, "vtbackup never logged %q", substr)
}

func initTablets(t *testing.T) {
	t.Helper()
	for _, tablet := range []*cluster.Vttablet{primary, replica1, replica2} {
		tablet.VttabletProcess.ServingStatus = "NOT_SERVING"
		require.NoError(t, localCluster.InitTablet(tablet, keyspaceName, shardName))
		require.NoError(t, tablet.VttabletProcess.Setup())
	}
	require.NoError(t, localCluster.VtctldClientProcess.InitShardPrimary(keyspaceName, shardName, cell, primary.TabletUID))
}

func setDurabilityPolicy(t *testing.T, policy string) {
	t.Helper()
	_, err := localCluster.VtctldClientProcess.ExecuteCommandWithOutput("SetKeyspaceDurabilityPolicy", keyspaceName, "--durability-policy="+policy)
	require.NoError(t, err)
}

func tabletSocketPath(tablet *cluster.Vttablet) string {
	return path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("vt_%010d", tablet.TabletUID), "mysql.sock")
}

// execOnTablet runs the queries on one connection to the tablet's mysqld, in
// the keyspace's database, and returns the last result.
func execOnTablet(t *testing.T, tablet *cluster.Vttablet, queries ...string) *sqltypes.Result {
	t.Helper()
	conn, err := mysql.Connect(t.Context(), &mysql.ConnParams{
		Uname:      "vt_dba",
		Pass:       dbPassword,
		UnixSocket: tabletSocketPath(tablet),
		DbName:     "vt_" + keyspaceName,
	})
	require.NoError(t, err)
	defer conn.Close()
	var qr *sqltypes.Result
	for _, query := range queries {
		qr, err = conn.ExecuteFetch(query, 10, false)
		require.NoError(t, err, query)
	}
	return qr
}

// writeInBackground inserts a row on the tablet's mysqld in the background. On a
// primary that no replica acknowledges, the insert waits for an acknowledgement
// that may never come; the connection dies with the primary.
func writeInBackground(t *testing.T, tablet *cluster.Vttablet, msg string) {
	t.Helper()
	go func() {
		conn, err := mysql.Connect(t.Context(), &mysql.ConnParams{
			Uname:      "vt_dba",
			Pass:       dbPassword,
			UnixSocket: tabletSocketPath(tablet),
			DbName:     "vt_" + keyspaceName,
		})
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = conn.ExecuteFetch(fmt.Sprintf("insert into vt_insert_test (msg) values ('%s')", msg), 0, false)
	}()
}

// requireRowAbsent checks that the tablet doesn't have the row with msg, so the
// scenario is valid.
func requireRowAbsent(t *testing.T, tablet *cluster.Vttablet, msg string) {
	t.Helper()
	qr := execOnTablet(t, tablet, fmt.Sprintf("select count(*) from vt_insert_test where msg = '%s'", msg))
	require.Equal(t, "0", qr.Rows[0][0].ToString(), "tablet %s has %q, so the scenario is invalid", tablet.Alias, msg)
}

// vtbackupQuery runs one query on the vtbackup mysqld over its socket. A new
// connection per call keeps polls working across mysqld restarts.
func vtbackupQuery(t *testing.T, socketPath, query string) (*sqltypes.Result, error) {
	t.Helper()
	params := cluster.NewConnParams(0, dbPassword, socketPath, keyspaceName)
	conn, err := mysql.Connect(t.Context(), &params)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return conn.ExecuteFetch(query, 10, false)
}

// killTabletHard simulates a sudden tablet failure. It kills mysqld with
// SIGKILL first, so a commit waiting for a semi-sync acknowledgement and the
// binlog dump threads die at once, then tears down the vttablet.
func killTabletHard(t *testing.T, tablet *cluster.Vttablet) {
	t.Helper()
	pidData, err := os.ReadFile(path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("vt_%010d", tablet.TabletUID), "mysql.pid"))
	require.NoError(t, err)
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidData)))
	require.NoError(t, err)
	require.NoError(t, syscall.Kill(pid, syscall.SIGKILL))
	require.NoError(t, tablet.VttabletProcess.TearDownWithTimeout(5*time.Second))
}

func listBackups(t *testing.T) []string {
	t.Helper()
	output, err := localCluster.VtctldClientProcess.ExecuteCommandWithOutput("GetBackups", shardKsName)
	require.NoError(t, err)
	var backups []string
	for line := range strings.SplitSeq(output, "\n") {
		if line != "" {
			backups = append(backups, line)
		}
	}
	return backups
}

// stopLeakedVtbackup stops a background vtbackup that is still running when the
// test ends, because the test failed. VtbackupProcess.TearDown can't stop a
// process that is still inside Setup, so this signals it by its socket flag.
// SIGTERM lets vtbackup's deferred cleanup shut down its mysqld, whose own
// command line doesn't name the socket. SIGKILL is the fallback.
func stopLeakedVtbackup(done chan error, socketPath string) {
	select {
	case <-done:
		return
	default:
	}
	_ = exec.Command("pkill", "-TERM", "-f", socketPath).Run()
	select {
	case <-done:
		return
	case <-time.After(time.Minute):
	}
	_ = exec.Command("pkill", "-9", "-f", socketPath).Run()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
	}
}
