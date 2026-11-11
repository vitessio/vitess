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

package failover

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/reparent/utils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

// formerPrimary stops P's vttablet, keeping its mysqld, which stays a source-side semi-sync
// source, and promotes R1 with ERS. It returns P and R1. Restarting P's vttablet keeps the
// stopped vttablet's log as <log>.gen0.
func formerPrimary(t *testing.T, ci *cluster.LocalProcessCluster) (p, r1 *cluster.Vttablet) {
	tablets := ci.Keyspaces[0].Shards[0].Vttablets
	p, r1 = tablets[0], tablets[1]
	utils.ConfirmReplication(t, p, tablets[1:])
	require.NoError(t, p.VttabletProcess.TearDownWithTimeout(30*time.Second))
	out, err := vtctldclient(t.Context(), ci, "EmergencyReparentShard", utils.KeyspaceName+"/"+utils.ShardName,
		"--new-primary", r1.Alias, "--wait-replicas-timeout", "30s")
	require.NoError(t, err, out)
	require.Equal(t, r1.Alias, utils.GetShardPrimary(t, ci).Alias)
	return p, r1
}

// fenceState returns tab's @@global.super_read_only and source-side semi-sync setting.
func fenceState(t *testing.T, tab *cluster.Vttablet) (superReadOnly, sourceSemiSync string) {
	row := utils.RunSQL(t.Context(), t, "select @@global.super_read_only, @@global.rpl_semi_sync_source_enabled", tab).Rows[0]
	return row[0].ToString(), row[1].ToString()
}

// TestFormerPrimaryFencedWhenDeferred restarts a former primary's vttablet under a held shard
// lock while a commit X on it waits for semi-sync acknowledgements that no replica sends. The
// deferral must fence it first: X is released and stays errant, the tablet rejects writes and
// stays out of rotation. VTOrc must then leave the shard lock available instead of retrying a
// writable-replica repair under it (#21803).
func TestFormerPrimaryFencedWhenDeferred(t *testing.T) {
	ci := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	teardownOnCleanup(t, ci)
	p, r1 := formerPrimary(t, ci)

	before := utils.RunSQL(t.Context(), t, "select @@global.gtid_executed", p).Rows[0][0].ToString()
	conn, err := utils.GetMySQLConn(t.Context(), p)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	const x = 7777
	xDone := make(chan error, 1)
	go func() {
		_, err := conn.ExecuteFetch(utils.GetInsertQuery(x), 0, false)
		xDone <- err
	}()
	require.Eventually(t, func() bool {
		qr := utils.RunSQL(t.Context(), t, "select count(*) from information_schema.processlist where state like '%semi-sync%'", p)
		n, err := qr.Rows[0][0].ToInt()
		return err == nil && n > 0
	}, 30*time.Second, 100*time.Millisecond, "X does not wait for acknowledgements")

	l := holdShardLock(t, ci.TopoProcess.Server, "reparent", 30*time.Second, 10*time.Second)
	restartVttablet(t, p, 0)
	waitForTabletManager(t, ci, p, time.Now().Add(30*time.Second))
	select {
	case err := <-xDone:
		require.NoError(t, err, "X was not released")
	case <-time.After(30 * time.Second):
		require.FailNow(t, "X was not released")
	}
	superReadOnly, sourceSemiSync := fenceState(t, p)
	assert.Equal(t, "1", superReadOnly, "the former primary accepts writes")
	assert.Equal(t, "0", sourceSemiSync)
	xGTID := utils.RunSQL(t.Context(), t, fmt.Sprintf("select gtid_subtract(@@global.gtid_executed, '%s')", before), p).Rows[0][0].ToString()
	require.NotEmpty(t, xGTID)
	assert.False(t, hasGTIDs(t, r1, utils.RunSQL(t.Context(), t, "select @@global.gtid_executed", r1).Rows[0][0].ToString(), xGTID), "X reached the new primary")
	requireOutOfRotation(t, p, 2*time.Second, "the former primary serves under the shard lock")
	l.release(t)

	require.NoError(t, ci.StartVTOrc(ci.Cell, ""))
	// Sample only once VTOrc has analyzed the former primary. Unfenced, it finds a writable
	// replica instead, and retries its repair under the lock.
	errant := strings.Join([]string{"ErrantGTIDDetected", p.Alias, utils.KeyspaceName, utils.ShardName}, ".")
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		problems, _ := ci.VTOrcProcesses[0].GetVars()["DetectedProblems"].(map[string]any)
		assert.NotZero(c, problems[errant], "detected problems: %v", problems)
	}, 60*time.Second, time.Second, "VTOrc did not detect the former primary's errant GTID")
	// Each sample takes the lock for a moment; a repair retried under the lock makes most
	// samples fail.
	failed := 0
	for range 20 {
		if err := tryLockShard(t, ci.TopoProcess.Server); err != nil {
			require.True(t, topo.IsErrType(err, topo.NodeExists), "unexpected lock error: %v", err)
			failed++
		}
		requireOutOfRotation(t, p, time.Second, "VTOrc returned the former primary to rotation")
	}
	assert.LessOrEqual(t, failed, 3, "VTOrc held the shard lock for %d of 20 samples", failed)
}

// TestFormerPrimaryFenceFailureFailsStartup restarts a former primary's vttablet under a held
// shard lock while another connection holds a write lock on a table, so enabling
// super_read_only waits until the fence's deadline. The fence fails, and the vttablet must exit
// instead of deferring, with source-side semi-sync restored and replication unchanged. Once the
// table lock is released, a restart fences and defers.
func TestFormerPrimaryFenceFailureFailsStartup(t *testing.T) {
	ci := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	teardownOnCleanup(t, ci)
	p, _ := formerPrimary(t, ci)

	conn, err := utils.GetMySQLConn(t.Context(), p)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	_, err = conn.ExecuteFetch("lock tables vt_insert_test write", 0, false)
	require.NoError(t, err)
	l := holdShardLock(t, ci.TopoProcess.Server, "reparent", 30*time.Second, 10*time.Second)
	vp := p.VttabletProcess
	vp.ServingStatus = ""
	require.NoError(t, os.Rename(vp.ErrorLog, vp.ErrorLog+".gen0"))
	require.NoError(t, vp.Setup())
	select {
	case <-vp.Exited():
	case <-time.After(90 * time.Second):
		require.FailNow(t, "the vttablet did not exit after its fence failed")
	}
	stderr, err := os.ReadFile(vp.ErrorLog)
	require.NoError(t, err)
	assert.Contains(t, string(stderr), "cannot fence a former primary before deferring replication initialization")
	superReadOnly, sourceSemiSync := fenceState(t, p)
	assert.Equal(t, "0", superReadOnly)
	assert.Equal(t, "1", sourceSemiSync, "a failed fence must leave the former primary to fence again")
	assert.Empty(t, receiverState(t, p), "replication changed")

	_, err = conn.ExecuteFetch("unlock tables", 0, false)
	require.NoError(t, err)
	restartVttablet(t, p, 1)
	waitForTabletManager(t, ci, p, time.Now().Add(30*time.Second))
	superReadOnly, sourceSemiSync = fenceState(t, p)
	assert.Equal(t, "1", superReadOnly)
	assert.Equal(t, "0", sourceSemiSync)
	requireOutOfRotation(t, p, 2*time.Second, "the former primary serves under the shard lock")
	l.release(t)
}

// TestFormerPrimaryFencedBeforeErrantGTIDCheck restarts a former primary's vttablet with the
// shard lock available, an errant GTID already executed, and a commit X waiting for semi-sync
// acknowledgements. The locked initialization must fence it before its errant GTID check fails
// startup: X is released, and the vttablet exits with the tablet rejecting writes and
// replication unchanged.
func TestFormerPrimaryFencedBeforeErrantGTIDCheck(t *testing.T) {
	ci := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	teardownOnCleanup(t, ci)
	p, r1 := formerPrimary(t, ci)

	// Adding to gtid_purged makes a GTID errant without a commit that waits for acknowledgements.
	utils.RunSQL(t.Context(), t, "set global gtid_purged = '+aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1'", p)
	before := utils.RunSQL(t.Context(), t, "select @@global.gtid_executed", p).Rows[0][0].ToString()
	conn, err := utils.GetMySQLConn(t.Context(), p)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	const x = 7778
	xDone := make(chan error, 1)
	go func() {
		_, err := conn.ExecuteFetch(utils.GetInsertQuery(x), 0, false)
		xDone <- err
	}()
	require.Eventually(t, func() bool {
		qr := utils.RunSQL(t.Context(), t, "select count(*) from information_schema.processlist where state like '%semi-sync%'", p)
		n, err := qr.Rows[0][0].ToInt()
		return err == nil && n > 0
	}, 30*time.Second, 100*time.Millisecond, "X does not wait for acknowledgements")

	vp := p.VttabletProcess
	vp.ServingStatus = ""
	require.NoError(t, os.Rename(vp.ErrorLog, vp.ErrorLog+".gen0"))
	require.NoError(t, vp.Setup())
	select {
	case <-vp.Exited():
	case <-time.After(90 * time.Second):
		require.FailNow(t, "the vttablet did not exit on its errant GTID")
	}
	select {
	case err := <-xDone:
		require.NoError(t, err, "X was not released")
	case <-time.After(30 * time.Second):
		require.FailNow(t, "X was not released")
	}
	stderr, err := os.ReadFile(vp.ErrorLog)
	require.NoError(t, err)
	assert.Contains(t, string(stderr), "Errant GTID detected")
	superReadOnly, sourceSemiSync := fenceState(t, p)
	assert.Equal(t, "1", superReadOnly, "the former primary accepts writes")
	assert.Equal(t, "0", sourceSemiSync)
	assert.Empty(t, receiverState(t, p), "replication changed")
	xGTID := utils.RunSQL(t.Context(), t, fmt.Sprintf("select gtid_subtract(@@global.gtid_executed, '%s')", before), p).Rows[0][0].ToString()
	require.NotEmpty(t, xGTID)
	assert.False(t, hasGTIDs(t, r1, utils.RunSQL(t.Context(), t, "select @@global.gtid_executed", r1).Rows[0][0].ToString(), xGTID), "X reached the new primary")
}
