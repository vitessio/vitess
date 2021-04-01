/*
Copyright 2019 The Vitess Authors.

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

package tabletmanager

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

// TestLockAndUnlock tests the lock ability by locking a replica and asserting it does not see changes
func TestLockAndUnlock(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	masterConn, err := mysql.Connect(ctx, &masterTabletParams)
	require.Nil(t, err)
	defer masterConn.Close()

	replicaConn, err := mysql.Connect(ctx, &replicaTabletParams)
	require.Nil(t, err)
	defer replicaConn.Close()

	// first make sure that our writes to the master make it to the replica
	exec(t, masterConn, "delete from t1")
	exec(t, masterConn, "insert into t1(id, value) values(1,'a'), (2,'b')")
	checkDataOnReplica(t, replicaConn, `[[VARCHAR("a")] [VARCHAR("b")]]`)

	// now lock the replica
	err = tmcLockTables(ctx, replicaTablet.GrpcPort)
	require.Nil(t, err)
	// make sure that writing to the master does not show up on the replica while locked
	exec(t, masterConn, "insert into t1(id, value) values(3,'c')")
	checkDataOnReplica(t, replicaConn, `[[VARCHAR("a")] [VARCHAR("b")]]`)

	// finally, make sure that unlocking the replica leads to the previous write showing up
	err = tmcUnlockTables(ctx, replicaTablet.GrpcPort)
	require.Nil(t, err)
	checkDataOnReplica(t, replicaConn, `[[VARCHAR("a")] [VARCHAR("b")] [VARCHAR("c")]]`)

	// Unlocking when we do not have a valid lock should lead to an exception being raised
	err = tmcUnlockTables(ctx, replicaTablet.GrpcPort)
	want := "tables were not locked"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Table unlock: %v, must contain %s", err, want)
	}

	// Clean the table for further testing
	exec(t, masterConn, "delete from t1")
}

// TestStartReplicationUntilAfter tests by writing three rows, noting the gtid after each, and then replaying them one by one
func TestStartReplicationUntilAfter(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	masterConn, err := mysql.Connect(ctx, &masterTabletParams)
	require.Nil(t, err)
	defer masterConn.Close()

	replicaConn, err := mysql.Connect(ctx, &replicaTabletParams)
	require.Nil(t, err)
	defer replicaConn.Close()

	//first we stop replication to the replica, so we can move forward step by step.
	err = tmcStopReplication(ctx, replicaTablet.GrpcPort)
	require.Nil(t, err)

	exec(t, masterConn, "insert into t1(id, value) values(1,'a')")
	pos1, err := tmcMasterPosition(ctx, masterTablet.GrpcPort)
	require.Nil(t, err)

	exec(t, masterConn, "insert into t1(id, value) values(2,'b')")
	pos2, err := tmcMasterPosition(ctx, masterTablet.GrpcPort)
	require.Nil(t, err)

	exec(t, masterConn, "insert into t1(id, value) values(3,'c')")
	pos3, err := tmcMasterPosition(ctx, masterTablet.GrpcPort)
	require.Nil(t, err)

	// Now, we'll resume stepwise position by position and make sure that we see the expected data
	checkDataOnReplica(t, replicaConn, `[]`)

	// starts the mysql replication until
	timeout := 10 * time.Second
	err = tmcStartReplicationUntilAfter(ctx, replicaTablet.GrpcPort, pos1, timeout)
	require.Nil(t, err)
	// first row should be visible
	checkDataOnReplica(t, replicaConn, `[[VARCHAR("a")]]`)

	err = tmcStartReplicationUntilAfter(ctx, replicaTablet.GrpcPort, pos2, timeout)
	require.Nil(t, err)
	checkDataOnReplica(t, replicaConn, `[[VARCHAR("a")] [VARCHAR("b")]]`)

	err = tmcStartReplicationUntilAfter(ctx, replicaTablet.GrpcPort, pos3, timeout)
	require.Nil(t, err)
	checkDataOnReplica(t, replicaConn, `[[VARCHAR("a")] [VARCHAR("b")] [VARCHAR("c")]]`)

	// Strat replication to the replica
	err = tmcStartReplication(ctx, replicaTablet.GrpcPort)
	require.Nil(t, err)
	// Clean the table for further testing
	exec(t, masterConn, "delete from t1")
}

// TestLockAndTimeout tests that the lock times out and updates can be seen after timeout
func TestLockAndTimeout(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	masterConn, err := mysql.Connect(ctx, &masterTabletParams)
	require.Nil(t, err)
	defer masterConn.Close()

	replicaConn, err := mysql.Connect(ctx, &replicaTabletParams)
	require.Nil(t, err)
	defer replicaConn.Close()

	// first make sure that our writes to the master make it to the replica
	exec(t, masterConn, "insert into t1(id, value) values(1,'a')")
	checkDataOnReplica(t, replicaConn, `[[VARCHAR("a")]]`)

	// now lock the replica
	err = tmcLockTables(ctx, replicaTablet.GrpcPort)
	require.Nil(t, err)

	// make sure that writing to the master does not show up on the replica while locked
	exec(t, masterConn, "insert into t1(id, value) values(2,'b')")
	checkDataOnReplica(t, replicaConn, `[[VARCHAR("a")]]`)

	// the tests sets the lock timeout to 5 seconds, so sleeping 8 should be safe
	time.Sleep(8 * time.Second)
	checkDataOnReplica(t, replicaConn, `[[VARCHAR("a")] [VARCHAR("b")]]`)

	// Clean the table for further testing
	exec(t, masterConn, "delete from t1")
}

func checkDataOnReplica(t *testing.T, replicaConn *mysql.Conn, want string) {
	startTime := time.Now()
	for {
		qr := exec(t, replicaConn, "select value from t1")
		got := fmt.Sprintf("%v", qr.Rows)

		if time.Since(startTime) > 3*time.Second /* timeout */ {
			assert.Equal(t, want, got)
			break
		}

		if got == want {
			assert.Equal(t, want, got)
			break
		} else {
			time.Sleep(300 * time.Millisecond /* interval at which to check again */)
		}
	}
}
