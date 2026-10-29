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
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/reparent/utils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

// receiverState returns tab's Replica_IO_Running, or "" if it cannot be read.
func receiverState(t *testing.T, tab *cluster.Vttablet) string {
	conn, err := utils.GetMySQLConn(t.Context(), tab)
	if err != nil {
		return ""
	}
	defer conn.Close()
	qr, err := conn.ExecuteFetch("show replica status", 1, true)
	if err != nil || len(qr.Rows) == 0 {
		return ""
	}
	for i, f := range qr.Fields {
		if f.Name == "Replica_IO_Running" {
			return qr.Rows[0][i].ToString()
		}
	}
	return ""
}

// requireOutOfRotation requires that, for d, tab's receiver neither runs nor connects and
// tab does not serve queries.
func requireOutOfRotation(t *testing.T, tab *cluster.Vttablet, d time.Duration, msg string) {
	var receiver, state string
	if !assert.Never(t, func() bool {
		receiver, state = receiverState(t, tab), tab.VttabletProcess.GetTabletStatus()
		return receiver == "Yes" || receiver == "Connecting" || state != "NOT_SERVING"
	}, d, 100*time.Millisecond, msg) {
		require.FailNow(t, msg, "receiver %q, tablet %s", receiver, state)
	}
}

// requireRepaired starts VTOrc and requires that it starts tab's replication from primary, and
// that tab then serves.
func requireRepaired(t *testing.T, ci *cluster.LocalProcessCluster, tab, primary *cluster.Vttablet) {
	require.NoError(t, ci.StartVTOrc(ci.Cell, ""))
	requireReplicating(t, tab, primary, "VTOrc did not repair %s", tab.Alias)
}

// requireReplicating requires that tab replicates from primary and serves.
func requireReplicating(t *testing.T, tab, primary *cluster.Vttablet, msgAndArgs ...any) {
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		st := replicaStatus(t, tab)
		assert.Equal(c, "Yes", st["Replica_IO_Running"])
		assert.Equal(c, "Yes", st["Replica_SQL_Running"])
		assert.Equal(c, strconv.Itoa(primary.MySQLPort), st["Source_Port"])
		assert.Equal(c, "SERVING", tab.VttabletProcess.GetTabletStatus())
	}, 90*time.Second, 250*time.Millisecond, msgAndArgs...)
}

// restoreInBackground runs vtctld's RestoreFromBackup of tab. Cleanup kills it if it still
// runs and waits for it to exit.
func restoreInBackground(t *testing.T, ci *cluster.LocalProcessCluster, tab *cluster.Vttablet) (done chan struct{}, out *string, err *error) {
	done, out, err = make(chan struct{}), new(string), new(error)
	ctx, kill := context.WithCancel(t.Context())
	go func() {
		defer close(done)
		*out, *err = vtctldclient(ctx, ci, "RestoreFromBackup", tab.Alias)
	}()
	t.Cleanup(func() {
		kill()
		<-done
	})
	return done, out, err
}

// requireRestoreFollowupWaits requires that, before deadline, vtctld's follow-up of the restore
// of tab waits for the shard lock, which is held, instead of returning.
func requireRestoreFollowupWaits(t *testing.T, ci *cluster.LocalProcessCluster, tab *cluster.Vttablet, restored chan struct{}, deadline time.Time) {
	require.Eventually(t, func() bool {
		select {
		case <-restored:
			return true
		default:
		}
		return slices.Contains(shardLockActions(t, ci), "SetReplicationSource")
	}, time.Until(deadline), 100*time.Millisecond, "the restore does not finish")
	select {
	case <-restored:
		st := replicaStatus(t, tab)
		require.FailNow(t, "RestoreFromBackup finished while the shard lock was held",
			"%s's receiver %q, source port %s", tab.Alias, st["Replica_IO_Running"], st["Source_Port"])
	default:
	}
}

// TestERSRestartedReplicaStaysStopped restarts R1's vttablet while a real ERS holds the shard
// lock, after ERS stopped R1's receiver and before it promotes (see
// TestERSHeldByApplierBarriers). The restarted vttablet must answer the RPCs that ERS needs
// without reconnecting R1's receiver to the old primary, and stay out of rotation while ERS
// holds the lock. ERS then promotes and repoints R1.
func TestERSRestartedReplicaStaysStopped(t *testing.T) {
	holdERSAndRestartReplica(t, func(t *testing.T, r1 *cluster.Vttablet) {
		requireOutOfRotation(t, r1, 2*time.Second, "R1's receiver was revived during the ERS")
	})
}

// TestStoppedReplicaRestartedUnderShardLock restarts the vttablet of a replica whose
// replication is stopped while another operation holds the shard lock. The replica answers
// RPCs, leaves replication stopped and stays out of rotation, also after the lock is released,
// until VTOrc starts replication; then it serves.
func TestStoppedReplicaRestartedUnderShardLock(t *testing.T) {
	ci := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	teardownOnCleanup(t, ci)
	tablets := ci.Keyspaces[0].Shards[0].Vttablets
	p, r1 := tablets[0], tablets[1]
	base := utils.ConfirmReplication(t, p, tablets[1:])
	out, err := vtctldclient(t.Context(), ci, "StopReplication", r1.Alias)
	require.NoError(t, err, out)

	l := holdShardLock(t, ci.TopoProcess.Server, "reparent", 30*time.Second, 10*time.Second)
	restartVttablet(t, r1, 0)
	waitForTabletManager(t, ci, r1, time.Now().Add(30*time.Second))
	requireOutOfRotation(t, r1, 5*time.Second, "R1 started replication under the shard lock")
	l.release(t)
	requireOutOfRotation(t, r1, 5*time.Second, "R1 started replication after the shard lock was released")

	requireRepaired(t, ci, r1, p)
	n := utils.ConfirmReplication(t, p, tablets[1:])
	for _, i := range []int{base, n} {
		require.NoError(t, utils.CheckInsertedValues(t.Context(), t, r1, i))
	}
}

// TestRestoreUnderShardLock restores a replica from a backup through vtctld while another
// operation holds the shard lock. The replica answers RPCs, leaves replication unconfigured
// and stays out of rotation, and vtctld's follow-up, which sets the replica's source to the
// shard primary, waits for the lock. Once the lock is released, the follow-up starts
// replication from the primary and the replica serves, with the backup's rows and later ones.
func TestRestoreUnderShardLock(t *testing.T) {
	ci := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	teardownOnCleanup(t, ci)
	tablets := ci.Keyspaces[0].Shards[0].Vttablets
	p, r1, r2 := tablets[0], tablets[1], tablets[2]
	base := utils.ConfirmReplication(t, p, tablets[1:])
	out, err := vtctldclient(t.Context(), ci, "Backup", r2.Alias)
	require.NoError(t, err, out)
	after := utils.ConfirmReplication(t, p, tablets[1:])

	l := holdShardLock(t, ci.TopoProcess.Server, "reparent", 30*time.Second, 10*time.Second)
	restored, restoreOut, restoreErr := restoreInBackground(t, ci, r1)
	requireRestoreFollowupWaits(t, ci, r1, restored, time.Now().Add(60*time.Second))
	assert.Equal(t, "replica", r1.VttabletProcess.GetTabletType())
	requireOutOfRotation(t, r1, 5*time.Second, "R1 started replication under the shard lock")
	l.release(t)

	select {
	case <-restored:
		require.NoError(t, *restoreErr, *restoreOut)
	case <-time.After(60 * time.Second):
		require.FailNow(t, "RestoreFromBackup did not finish after the release")
	}
	requireReplicating(t, r1, p, "the restore follow-up did not start R1's replication from the primary")
	n := utils.ConfirmReplication(t, p, tablets[1:])
	for _, i := range []int{base, after, n} {
		require.NoError(t, utils.CheckInsertedValues(t.Context(), t, r1, i))
	}
}

// TestRestoreDuringERS restores R1 from a backup through vtctld while a real ERS holds the
// shard lock (see TestERSHeldByApplierBarriers). While ERS holds it, vtctld's follow-up,
// which sets R1's source to the shard primary, waits for the lock and does not repoint R1.
//
// ERS does not publish the new primary in the shard record; the new primary does, shortly
// after its promotion (#21670). A contender queued before the follow-up therefore takes the
// lock after ERS and keeps it until the shard record names R3, so the follow-up reads R3.
func TestRestoreDuringERS(t *testing.T) {
	var (
		restored              chan struct{}
		restoreOut            *string
		restoreErr            *error
		contenderErr          error
		contenderDone         = make(chan struct{})
		r1, r3                *cluster.Vttablet
		contenderCtx, release = context.WithCancel(t.Context())
	)
	t.Cleanup(func() {
		release()
		<-contenderDone
	})
	holdERS(t, func(t *testing.T, ci *cluster.LocalProcessCluster) {
		out, err := vtctldclient(t.Context(), ci, "Backup", ci.Keyspaces[0].Shards[0].Vttablets[3].Alias)
		require.NoError(t, err, out)
	}, func(t *testing.T, ci *cluster.LocalProcessCluster, tab *cluster.Vttablet, deadline time.Time) {
		r1, r3 = tab, ci.Keyspaces[0].Shards[0].Vttablets[2]
		ts := ci.TopoProcess.Server
		go func() {
			defer close(contenderDone)
			ctx, cancel := context.WithTimeout(contenderCtx, 2*time.Minute)
			defer cancel()
			_, unlock, err := ts.LockShard(ctx, utils.KeyspaceName, utils.ShardName, "contender", topo.WithAcquireTimeout(2*time.Minute))
			if err != nil {
				contenderErr = err
				return
			}
			defer unlock(&contenderErr)
			for {
				si, err := ts.GetShard(ctx, utils.KeyspaceName, utils.ShardName)
				if err == nil && topoproto.TabletAliasString(si.PrimaryAlias) == r3.Alias {
					return
				}
				select {
				case <-ctx.Done():
					contenderErr = fmt.Errorf("the shard record does not name R3: %v, %w", err, ctx.Err())
					return
				case <-time.After(100 * time.Millisecond):
				}
			}
		}()
		require.Eventually(t, func() bool {
			return slices.Contains(shardLockActions(t, ci), "contender")
		}, time.Until(deadline), 100*time.Millisecond, "the contender is not queued")

		restored, restoreOut, restoreErr = restoreInBackground(t, ci, r1)
		requireRestoreFollowupWaits(t, ci, r1, restored, deadline)
		requireOutOfRotation(t, r1, time.Second, "R1 started replication while ERS held the shard lock")
	})

	select {
	case <-restored:
		require.NoError(t, *restoreErr, *restoreOut)
	case <-time.After(2 * time.Minute):
		require.FailNow(t, "RestoreFromBackup did not finish after ERS")
	}
	<-contenderDone
	require.NoError(t, contenderErr)
	requireReplicating(t, r1, r3, "R1 does not replicate from the new primary")
}
