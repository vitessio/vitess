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
	"context"
	"fmt"
	"strconv"
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

// holdApplier blocks tab's replication applier until the returned function runs: a dedicated
// connection holds a read lock on the table that the test writes. Cleanup closes the
// connection, which releases the lock and interrupts a statement that does not return.
func holdApplier(t *testing.T, tab *cluster.Vttablet) func() error {
	conn, err := utils.GetMySQLConn(t.Context(), tab)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	_, err = conn.ExecuteFetch("lock tables vt_insert_test read", 0, false)
	require.NoError(t, err)
	return func() error {
		_, err := conn.ExecuteFetch("unlock tables", 0, false)
		return err
	}
}

// positionWaits counts the connections on tab that wait for its applier to reach a position,
// as the tablet manager's WaitForPosition does.
func positionWaits(t *testing.T, tab *cluster.Vttablet) int {
	qr := utils.RunSQL(t.Context(), t, "select count(*) from information_schema.processlist where info like 'SELECT WAIT_FOR_EXECUTED_GTID_SET%'", tab)
	n, err := qr.Rows[0][0].ToInt()
	require.NoError(t, err)
	return n
}

func hasGTIDs(t *testing.T, tab *cluster.Vttablet, set, gtids string) bool {
	qr := utils.RunSQL(t.Context(), t, fmt.Sprintf("select gtid_subset('%s', '%s')", gtids, strings.ReplaceAll(set, "\n", "")), tab)
	return qr.Rows[0][0].ToString() == "1"
}

// TestERSHeldByApplierBarriers holds a real EmergencyReparentShard after it has stopped the
// replicas and before it promotes, restarts a replica's vttablet during the hold, then
// releases it and verifies the promotion.
//
// Layout: P is the primary, R1 replicates from R4, and R3 and R4 from P. Read locks on R3 and
// R4 block their appliers, so a write T1 on P reaches their relay logs only. R1 never
// receives T1, because R4's binary log lacks it. ERS therefore waits on R3 and R4 to apply T1
// and excludes R1 as strictly behind. ERS holds the shard lock for the whole wait, and
// nothing renews its lease there, so the hold has a budget well within the lease TTL and the
// lease's remaining TTL is checked before the release.
//
// Whether R1's restarted vttablet reconnects its receiver during the hold depends on the
// revision under test; this control asserts neither outcome.
func TestERSHeldByApplierBarriers(t *testing.T) {
	holdERSAndRestartReplica(t, nil)
}

// holdERSAndRestartReplica runs TestERSHeldByApplierBarriers, and calls duringHold with R1
// once R1's restarted vttablet answers tablet manager RPCs, while ERS still holds the shard
// lock. duringHold must return well within the hold budget.
func holdERSAndRestartReplica(t *testing.T, duringHold func(t *testing.T, r1 *cluster.Vttablet)) {
	holdERS(t, nil, func(t *testing.T, ci *cluster.LocalProcessCluster, r1 *cluster.Vttablet, deadline time.Time) {
		restartVttablet(t, r1, 0)
		waitForTabletManager(t, ci, r1, deadline)
		if duringHold != nil {
			duringHold(t, r1)
		}
	})
}

// holdERS holds a real ERS as TestERSHeldByApplierBarriers describes. It calls prepare, if
// not nil, once the cluster replicates and before it sets up the hold, and duringHold once
// ERS has stopped R1's receiver and waits on R3 and R4. duringHold must return before the
// deadline, when the hold budget ends.
func holdERS(t *testing.T, prepare func(t *testing.T, ci *cluster.LocalProcessCluster), duringHold func(t *testing.T, ci *cluster.LocalProcessCluster, r1 *cluster.Vttablet, deadline time.Time)) {
	ci := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	teardownOnCleanup(t, ci)
	tablets := ci.Keyspaces[0].Shards[0].Vttablets
	p, r1, r3, r4 := tablets[0], tablets[1], tablets[2], tablets[3]
	base := utils.ConfirmReplication(t, p, tablets[1:])
	if prepare != nil {
		prepare(t, ci)
	}

	utils.RunSQLs(t.Context(), t, []string{
		"stop replica",
		fmt.Sprintf("change replication source to source_host='%s', source_port=%d", r4.VttabletProcess.TabletHostname, r4.MySQLPort),
		"start replica",
	}, r1)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		st := replicaStatus(t, r1)
		assert.Equal(c, "Yes", st["Replica_IO_Running"])
		assert.Equal(c, strconv.Itoa(r4.MySQLPort), st["Source_Port"])
	}, 30*time.Second, 100*time.Millisecond)
	chained := utils.ConfirmReplication(t, p, tablets[1:])

	releaseR3, releaseR4 := holdApplier(t, r3), holdApplier(t, r4)
	before := utils.RunSQL(t.Context(), t, "select @@global.gtid_executed", p).Rows[0][0].ToString()
	utils.RunSQL(t.Context(), t, utils.GetInsertQuery(1000), p)
	t1 := utils.RunSQL(t.Context(), t, fmt.Sprintf("select gtid_subtract(@@global.gtid_executed, '%s')", before), p).Rows[0][0].ToString()
	for _, tab := range []*cluster.Vttablet{r3, r4} {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.True(c, hasGTIDs(t, tab, replicaStatus(t, tab)["Retrieved_Gtid_Set"], t1), "%s did not receive T1", tab.Alias)
		}, 30*time.Second, 100*time.Millisecond)
	}

	// One ERS attempt. Cleanup kills it if it is still running and waits for it to exit,
	// before the barriers' connections close.
	var ersOut string
	var ersErr error
	ersDone := make(chan struct{})
	ersCtx, killERS := context.WithCancel(t.Context())
	start := time.Now()
	go func() {
		defer close(ersDone)
		ersOut, ersErr = vtctldclient(ersCtx, ci, "--action-timeout", "90s", "EmergencyReparentShard", utils.KeyspaceName+"/"+utils.ShardName,
			"--new-primary", r3.Alias, "--wait-replicas-timeout", "60s")
	}()
	t.Cleanup(func() {
		killERS()
		<-ersDone
		if t.Failed() {
			t.Logf("ERS returned %v:\n%s", ersErr, ersOut)
		}
	})

	// ERS's lease decays from its last check, about a second after it starts. The hold must
	// end within this budget; a longer one makes the attempt invalid, not a failed reparent.
	const holdBudget = 20 * time.Second
	deadline := start.Add(holdBudget)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, "No", replicaStatus(t, r1)["Replica_IO_Running"], "ERS did not stop R1's receiver")
		assert.Positive(c, positionWaits(t, r3), "ERS does not wait on R3")
		assert.Positive(c, positionWaits(t, r4), "ERS does not wait on R4")
	}, time.Until(deadline), 100*time.Millisecond)
	require.Zero(t, positionWaits(t, r1), "ERS waits on R1")
	duringHold(t, ci, r1, deadline)

	select {
	case <-ersDone:
		require.FailNow(t, "ERS finished during the hold", "%v: %s", ersErr, ersOut)
	default:
	}
	err := tryLockShard(t, ci.TopoProcess.Server)
	require.True(t, topo.IsErrType(err, topo.NodeExists), "ERS does not hold the shard lock: %v", err)
	// The lease reads are bounded by the hold budget, so an unresponsive etcd fails the hold.
	leaseCtx, cancelLease := context.WithDeadline(t.Context(), deadline)
	defer cancelLease()
	cli, lease := shardLockLease(leaseCtx, t, ci)
	ttl, err := cli.TimeToLive(leaseCtx, lease)
	require.NoError(t, err)
	held := time.Since(start)
	t.Logf("held ERS for %v; its shard lock lease has %ds left", held, ttl.TTL)
	require.Less(t, held, holdBudget, "invalid control: the hold exceeded its budget")
	require.Greater(t, ttl.TTL, int64(5), "invalid control: ERS's shard lock lease is about to expire")
	require.NoError(t, releaseR3())
	require.NoError(t, releaseR4())

	select {
	case <-ersDone:
		require.NoError(t, ersErr, ersOut)
	case <-time.After(90 * time.Second):
		require.FailNow(t, "ERS did not finish after the release")
	}
	require.Equal(t, r3.Alias, utils.GetShardPrimary(t, ci).Alias)
	for _, n := range []int{base, chained, 1000} {
		require.NoError(t, utils.CheckInsertedValues(t.Context(), t, r3, n))
	}
	utils.ConfirmReplication(t, r3, []*cluster.Vttablet{p, r1, r4})
}
