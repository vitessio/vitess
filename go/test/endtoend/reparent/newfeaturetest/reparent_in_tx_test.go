/*
Copyright 2025 The Vitess Authors.

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

package newfeaturetest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/reparent/utils"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"
)

var primary int

// This test ensures that we get a VT15001 error when doing COMMIT while having primary being turned off.
// The test then ensures that, if there was no partial commit, all the inserts were rolled back correctly.
func testCommitError(t *testing.T, conn *mysql.Conn, clusterInstance *cluster.LocalProcessCluster, tablets []*cluster.Vttablet) {
	tabletStopped := make(chan bool)
	commitDone := make(chan bool)
	idx := 1
	go func() {
		createTxAndInsertRows(conn, t, idx)
		<-tabletStopped
		time.Sleep(1 * time.Second)
		_, err := conn.ExecuteFetch("commit", 0, false)
		require.ErrorContains(t, err, vterrors.VT15001(0).ID)
		commitDone <- true
	}()

	reparent(t, clusterInstance, tablets, tabletStopped, commitDone)

	// if the unhealthy shard is the first one where we commited, let's assert that the table is empty on all the shards
	r, err := conn.ExecuteFetch("select * from vt_insert_test", 1, false)
	require.NoError(t, err)
	require.Len(t, r.Rows, 0)
}

func testExecuteError(t *testing.T, conn *mysql.Conn, clusterInstance *cluster.LocalProcessCluster, tablets []*cluster.Vttablet) {
	tabletStopped := make(chan bool)
	executeDone := make(chan bool)
	idx := 1
	go func() {
		createTxAndInsertRows(conn, t, idx)

		idx += 5
		<-tabletStopped
		_, err := conn.ExecuteFetch(utils.GetInsertMultipleValuesQuery(idx, idx+1, idx+2, idx+3), 0, false)
		require.ErrorContains(t, err, vterrors.VT15001(0).ID)
		executeDone <- true
	}()

	reparent(t, clusterInstance, tablets, tabletStopped, executeDone)

	// if the unhealthy shard is the first one where we commited, let's assert that the table is empty on all the shards
	r, err := conn.ExecuteFetch("select * from vt_insert_test", 1, false)
	require.NoError(t, err)
	require.Len(t, r.Rows, 0)
}

func createTxAndInsertRows(conn *mysql.Conn, t *testing.T, idx int) {
	_, err := conn.ExecuteFetch("begin", 0, false)
	require.NoError(t, err)

	for i := 0; i < 25; i++ {
		idx += 5
		_, err = conn.ExecuteFetch(utils.GetInsertMultipleValuesQuery(idx, idx+1, idx+2, idx+3), 0, false)
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond)
	}
}

func reparent(t *testing.T, clusterInstance *cluster.LocalProcessCluster, tablets []*cluster.Vttablet, tabletStopped, actionDone chan bool) {
	// Reparent to the other replica
	utils.ShardName = "40-80"
	defer func() {
		utils.ShardName = "0"
	}()

	prsTo := primary - 1
	if primary == 0 {
		prsTo = primary + 1
	}
	output, err := utils.Prs(t, clusterInstance, tablets[prsTo])
	require.NoError(t, err, "error in PlannedReparentShard output - %s", output)

	time.Sleep(5 * time.Second)

	// We now restart the vttablet that became a replica.
	utils.StopTablet(t, tablets[primary], false)
	tabletStopped <- true
	primary = prsTo

	time.Sleep(5 * time.Second)

	// Wait for the action triggering the VT15001 to be done before moving on
	<-actionDone

	tablets[0].VttabletProcess.ServingStatus = "SERVING"
	err = tablets[0].VttabletProcess.Setup()
	require.NoError(t, err)

}

func TestErrorsInTransaction(t *testing.T) {
	clusterInstance := utils.SetupShardedReparentCluster(t, policy.DurabilitySemiSync)
	defer utils.TeardownCluster(clusterInstance)

	keyspace := clusterInstance.Keyspaces[0]
	vtParams := clusterInstance.GetVTParams(keyspace.Name)
	tablets := clusterInstance.Keyspaces[0].Shards[1].Vttablets

	primary = 0

	// Start by reparenting all the shards to the first tablet.
	// Confirm that the replication is setup correctly in the beginning.
	// tablets[0] is the primary tablet in the beginning.
	utils.ConfirmReplication(t, tablets[primary], []*cluster.Vttablet{tablets[1], tablets[2]})

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)

	_, err = conn.ExecuteFetch("delete from vt_insert_test", 0, false)
	require.NoError(t, err)

	t.Run("commit while reparenting", func(t *testing.T) {
		testCommitError(t, conn, clusterInstance, tablets)
	})

	t.Run("execute DML while reparenting", func(t *testing.T) {
		testExecuteError(t, conn, clusterInstance, tablets)
	})
}
