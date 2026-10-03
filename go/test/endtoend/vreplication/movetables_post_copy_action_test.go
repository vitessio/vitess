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

package vreplication

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/throttler"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// TestMoveTablesStopCancelDuringPostCopyAction covers stopping and cancelling
// a MoveTables workflow with deferred secondary keys while its streams are
// executing the post copy action which re-adds the keys: an ALTER TABLE that
// can run for hours on a large table. Both operations must complete promptly
// instead of waiting for the ALTER, which is killed -- and when it cannot be
// killed, because no DBA connection can be obtained to do so, the ALTER is
// left to run on its own and the operations must still complete promptly. A
// stopped workflow must have the keys in place once it is started again,
// whether it re-adds them or finds them added by the ALTER it abandoned.
//
// The ALTER is held up deterministically: the target's copier is throttled so
// that the copy pauses right after the keys have been dropped, a transaction
// which has read the target table is left open on each target primary so
// that the ALTER blocks on its metadata lock once the copy completes, and the
// copier is then unthrottled.
func TestMoveTablesStopCancelDuringPostCopyAction(t *testing.T) {
	ogReplicas := defaultReplicas
	ogRdOnly := defaultRdonly
	defer func() {
		defaultReplicas = ogReplicas
		defaultRdonly = ogRdOnly
	}()
	defaultRdonly = 0
	defaultReplicas = 0

	vc = setupMinimalCluster(t)
	defer vc.TearDown()
	currentWorkflowType = binlogdatapb.VReplicationWorkflowType_MoveTables
	targetTablets := setupMinimalTargetKeyspace(t)
	targetPrimaries := []*cluster.VttabletProcess{targetTab1, targetTab2}
	require.Len(t, targetTablets, len(targetPrimaries))

	// loadtest has a single secondary key, on name, which is deferred.
	const table = "loadtest"
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	for i := 1; i <= 100; i++ {
		execVtgateQuery(vtgateConn, defaultSourceKs, fmt.Sprintf("insert into %s(id, name) values (%d, 'name-%d')", table, i, i))
	}

	// The actions run the re-add ALTER on the target primaries' MySQL. The
	// checks below go over connections opened up front, which keep working
	// while the DBA account is locked further down.
	tabletConns := make(map[*cluster.VttabletProcess]*mysql.Conn, len(targetPrimaries))
	for _, tablet := range targetPrimaries {
		conn, err := tablet.TabletConn(defaultTargetKs, true)
		require.NoError(t, err)
		defer conn.Close()
		tabletConns[tablet] = conn
	}
	countOnTablet := func(t *testing.T, tablet *cluster.VttabletProcess, query string) int64 {
		qr, err := tabletConns[tablet].ExecuteFetch(query, 1, false)
		require.NoError(t, err)
		require.Len(t, qr.Rows, 1)
		n, err := qr.Rows[0][0].ToInt64()
		require.NoError(t, err)
		return n
	}
	alterQuery := "select count(*) from information_schema.processlist where info like 'alter table%" + table + "%add key%'"
	alterRunning := func(t *testing.T, tablet *cluster.VttabletProcess) bool {
		return countOnTablet(t, tablet, alterQuery) > 0
	}
	alterBlocked := func(t *testing.T, tablet *cluster.VttabletProcess) bool {
		return countOnTablet(t, tablet, alterQuery+" and state = 'Waiting for table metadata lock'") > 0
	}
	tableExists := func(t *testing.T, tablet *cluster.VttabletProcess) bool {
		return countOnTablet(t, tablet, "select count(*) from information_schema.tables where table_schema = database() and table_name = '"+table+"'") > 0
	}
	hasSecondaryKey := func(t *testing.T, tablet *cluster.VttabletProcess) bool {
		return countOnTablet(t, tablet, "select count(*) from information_schema.statistics where table_schema = database() and table_name = '"+table+"' and index_name = 'name'") > 0
	}
	forAllTargets := func(t *testing.T, cond func(*testing.T, *cluster.VttabletProcess) bool) func() bool {
		return func() bool {
			for _, tablet := range targetPrimaries {
				if !cond(t, tablet) {
					return false
				}
			}
			return true
		}
	}

	// runBounded runs the workflow command through vtctld with an action
	// timeout: an operation blocked behind the ALTER fails with a deadline
	// error rather than hanging the test.
	runBounded := func(t *testing.T, action string, flags ...string) {
		t.Helper()
		args := append([]string{"MoveTables", "--workflow", defaultWorkflowName, "--target-keyspace", defaultTargetKs, action}, flags...)
		args = append(args, "--action-timeout=2m")
		t.Logf("Executing workflow command: vtctldclient %s", args)
		started := time.Now()
		output, err := vc.VtctldClient.ExecuteCommandWithOutput(args...)
		require.NoError(t, err, "%s did not complete while the post copy ALTER was running: %s", action, output)
		t.Logf("%s completed in %s", action, time.Since(started))
	}

	// startWorkflowBlockedOnAlter creates and starts the workflow and returns
	// once each target primary is executing the re-add ALTER, blocked on the
	// metadata lock held by the returned connections' open transactions.
	// release ends those transactions, letting a still running ALTER
	// complete.
	startWorkflowBlockedOnAlter := func(t *testing.T) (release func(t *testing.T)) {
		require.NoError(t, throttler.ThrottleKeyspaceApp(vc.VtctldClient, defaultTargetKs, throttlerapp.VCopierName))
		for _, tablet := range targetPrimaries {
			require.True(t, waitForTabletThrottlingStatus(t, tablet, throttlerapp.VCopierName, throttlerStatusThrottled))
		}

		err := tstWorkflowExec(t, defaultCellName, defaultWorkflowName, defaultSourceKs, defaultTargetKs,
			table, workflowActionCreate, "", "", "", defaultWorkflowExecOptions)
		require.NoError(t, err)

		// The copy phase starts by dropping the deferred keys, recording
		// the action which re-adds them, and then pauses on the throttler.
		require.Eventually(t, forAllTargets(t, func(t *testing.T, tablet *cluster.VttabletProcess) bool {
			return tableExists(t, tablet) && !hasSecondaryKey(t, tablet)
		}), defaultTimeout, defaultTick, "the deferred secondary key was not dropped on the target")

		// Hold a shared metadata lock on the target table: the copy's
		// inserts are compatible with it, the ALTER's exclusive lock is not.
		conns := make([]*mysql.Conn, 0, len(targetPrimaries))
		for _, tablet := range targetPrimaries {
			conn, err := tablet.TabletConn(defaultTargetKs, true)
			require.NoError(t, err)
			conns = append(conns, conn)
			_, err = conn.ExecuteFetch("begin", 1, false)
			require.NoError(t, err)
			_, err = conn.ExecuteFetch("select * from "+table, 1000, false)
			require.NoError(t, err)
		}
		released := false
		release = func(t *testing.T) {
			if released {
				return
			}
			released = true
			for _, conn := range conns {
				_, err := conn.ExecuteFetch("rollback", 1, false)
				assert.NoError(t, err)
				conn.Close()
			}
		}
		t.Cleanup(func() { release(t) })

		require.NoError(t, throttler.UnthrottleKeyspaceApp(vc.VtctldClient, defaultTargetKs, throttlerapp.VCopierName))
		for _, tablet := range targetPrimaries {
			require.True(t, waitForTabletThrottlingStatus(t, tablet, throttlerapp.VCopierName, throttlerStatusNotThrottled))
		}

		require.Eventually(t, forAllTargets(t, alterBlocked), defaultTimeout, defaultTick, "the post copy ALTER did not start on every target primary")
		return release
	}

	// The KILL of the connection executing the ALTER is issued over a new
	// connection as the tablet's DBA user. Locking that account makes the
	// connection, and so the KILL, fail -- existing connections, such as the
	// ones the checks above use, are unaffected.
	setDbaAccountLocked := func(t *testing.T, locked bool) {
		t.Helper()
		query := "alter user 'vt_dba'@'localhost' account unlock"
		if locked {
			query = "alter user 'vt_dba'@'localhost' account lock"
		}
		for _, tablet := range targetPrimaries {
			_, err := tabletConns[tablet].ExecuteFetch(query, 1, false)
			require.NoError(t, err)
		}
	}

	stopAndStart := func(t *testing.T, killFails bool) {
		release := startWorkflowBlockedOnAlter(t)
		if killFails {
			setDbaAccountLocked(t, true)
			t.Cleanup(func() { setDbaAccountLocked(t, false) })
		}

		// The stop completes promptly rather than waiting for the ALTER.
		runBounded(t, "Stop")
		if killFails {
			// Only the kill during the stop had to fail; the checks below
			// go through the tablet's DBA connections again.
			setDbaAccountLocked(t, false)
		}
		require.NoError(t, waitForWorkflowState(vc, defaultKsWorkflow, binlogdatapb.VReplicationWorkflowState_Stopped.String()))

		if killFails {
			// The ALTER could not be killed and runs on, still blocked...
			require.True(t, forAllTargets(t, alterBlocked)(), "the ALTER should have been left running")
			// ...and completes on its own once unblocked, adding the key.
			release(t)
			require.Eventually(t, forAllTargets(t, func(t *testing.T, tablet *cluster.VttabletProcess) bool {
				return !alterRunning(t, tablet) && hasSecondaryKey(t, tablet)
			}), defaultTimeout, defaultTick, "the abandoned ALTER did not complete")
		} else {
			// The ALTER was killed and did not add the key.
			require.Eventually(t, forAllTargets(t, func(t *testing.T, tablet *cluster.VttabletProcess) bool {
				return !alterRunning(t, tablet)
			}), defaultTimeout, defaultTick, "the post copy ALTER was not killed")
			require.True(t, forAllTargets(t, func(t *testing.T, tablet *cluster.VttabletProcess) bool {
				return !hasSecondaryKey(t, tablet)
			})(), "the killed ALTER should not have re-added the key")
			release(t)
		}

		// Once started again the workflow re-runs the action: it re-adds
		// the key, or finds it already added and moves on.
		runBounded(t, "Start")
		require.NoError(t, waitForWorkflowState(vc, defaultKsWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String()))
		confirmTablesHaveSecondaryKeys(t, targetPrimaries, defaultTargetKs, table)

		runBounded(t, "Cancel")
		confirmNoWorkflows(t, defaultTargetKs)
	}

	t.Run("stop and start", func(t *testing.T) {
		stopAndStart(t, false)
	})

	t.Run("stop and start with failed kill", func(t *testing.T) {
		stopAndStart(t, true)
	})

	t.Run("cancel", func(t *testing.T) {
		release := startWorkflowBlockedOnAlter(t)

		// The cancel completes promptly as well. The target tables are
		// kept as dropping them would wait on the same metadata lock.
		runBounded(t, "Cancel", "--keep-data")
		confirmNoWorkflows(t, defaultTargetKs)
		require.Eventually(t, forAllTargets(t, func(t *testing.T, tablet *cluster.VttabletProcess) bool {
			return !alterRunning(t, tablet)
		}), defaultTimeout, defaultTick, "the post copy ALTER was not killed")
		require.True(t, forAllTargets(t, func(t *testing.T, tablet *cluster.VttabletProcess) bool {
			return tableExists(t, tablet) && !hasSecondaryKey(t, tablet)
		})(), "the killed ALTER should not have re-added the key")

		release(t)
		for _, tablet := range targetPrimaries {
			_, err := tablet.QueryTablet("drop table "+table, defaultTargetKs, true)
			require.NoError(t, err)
		}
	})
}
