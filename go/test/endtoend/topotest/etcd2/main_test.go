/*
Copyright 2021 The Vitess Authors.

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

package ectd2

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	topoutils "vitess.io/vitess/go/test/endtoend/topotest/utils"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/topo"

	"vitess.io/vitess/go/vt/log"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	cell            = "zone1"
	hostname        = "localhost"
	KeyspaceName    = "customer"
	SchemaSQL       = `
CREATE TABLE t1 (
    c1 BIGINT NOT NULL,
    c2 BIGINT NOT NULL,
    c3 BIGINT,
    c4 varchar(100),
    PRIMARY KEY (c1),
    UNIQUE KEY (c2),
    UNIQUE KEY (c3),
    UNIQUE KEY (c4)
) ENGINE=Innodb;`
	VSchema = `
{
    "sharded": false,
    "tables": {
        "t1": {}
    }
}
`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Start keyspace
		Keyspace := &cluster.Keyspace{
			Name:      KeyspaceName,
			SchemaSQL: SchemaSQL,
			VSchema:   VSchema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*Keyspace, 0, false); err != nil {
			log.Fatal(err.Error())
			return 1
		}

		// Start vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
			log.Fatal(err.Error())
			return 1
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestTopoDownServingQuery(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	defer utils.Exec(t, conn, `delete from t1`)

	execMulti(t, conn, `insert into t1(c1, c2, c3, c4) values (300,100,300,'abc'); ;; insert into t1(c1, c2, c3, c4) values (301,101,301,'abcd');;`)
	utils.AssertMatches(t, conn, `select c1,c2,c3 from t1`, `[[INT64(300) INT64(100) INT64(300)] [INT64(301) INT64(101) INT64(301)]]`)
	clusterInstance.TopoProcess.TearDown(clusterInstance.Cell, clusterInstance.OriginalVTDATAROOT, clusterInstance.CurrentVTDATAROOT, true, *clusterInstance.TopoFlavorString())
	defer func() {
		_ = clusterInstance.TopoProcess.SetupEtcd()
	}()
	time.Sleep(3 * time.Second)
	utils.AssertMatches(t, conn, `select c1,c2,c3 from t1`, `[[INT64(300) INT64(100) INT64(300)] [INT64(301) INT64(101) INT64(301)]]`)
}

// TestShardLocking tests that shard locking works as intended.
func TestShardLocking(t *testing.T) {
	// create topo server connection
	ts, err := topo.OpenServer(*clusterInstance.TopoFlavorString(), clusterInstance.VtctlProcess.TopoGlobalAddress, clusterInstance.VtctlProcess.TopoGlobalRoot)
	require.NoError(t, err)

	// Acquire a shard lock.
	ctx, unlock, err := ts.LockShard(context.Background(), KeyspaceName, "0", "TestShardLocking")
	require.NoError(t, err)
	// Check that we can't reacquire it from the same context.
	_, _, err = ts.LockShard(ctx, KeyspaceName, "0", "TestShardLocking")
	require.ErrorContains(t, err, "lock for shard customer/0 is already held")
	// Also check that TryLockShard is non-blocking and returns an error.
	_, _, err = ts.TryLockShard(context.Background(), KeyspaceName, "0", "TestShardLocking")
	require.ErrorContains(t, err, "node already exists: lock already exists at path keyspaces/customer/shards/0")
	// Check that CheckShardLocked doesn't return an error.
	err = topo.CheckShardLocked(ctx, KeyspaceName, "0")
	require.NoError(t, err)

	// We'll now try to acquire the lock from a different thread.
	secondThreadLockAcquired := false
	go func() {
		_, unlock, err := ts.LockShard(context.Background(), KeyspaceName, "0", "TestShardLocking")
		defer unlock(&err)
		require.NoError(t, err)
		secondThreadLockAcquired = true
	}()

	// Wait for some time and ensure that the second acquiring of lock shard is blocked.
	time.Sleep(100 * time.Millisecond)
	require.False(t, secondThreadLockAcquired)

	// Unlock the shard.
	unlock(&err)
	// Check that we no longer have shard lock acquired.
	err = topo.CheckShardLocked(ctx, KeyspaceName, "0")
	require.ErrorContains(t, err, "shard customer/0 is not locked (no lockInfo in map)")

	// Wait to see that the second thread was able to acquire the shard lock.
	topoutils.WaitForBoolValue(t, &secondThreadLockAcquired, true)
}

// TestKeyspaceLocking tests that keyspace locking works as intended.
func TestKeyspaceLocking(t *testing.T) {
	// create topo server connection
	ts, err := topo.OpenServer(*clusterInstance.TopoFlavorString(), clusterInstance.VtctlProcess.TopoGlobalAddress, clusterInstance.VtctlProcess.TopoGlobalRoot)
	require.NoError(t, err)

	// Acquire a keyspace lock.
	ctx, unlock, err := ts.LockKeyspace(context.Background(), KeyspaceName, "TestKeyspaceLocking")
	require.NoError(t, err)
	// Check that we can't reacquire it from the same context.
	_, _, err = ts.LockKeyspace(ctx, KeyspaceName, "TestKeyspaceLocking")
	require.ErrorContains(t, err, "lock for keyspace customer is already held")
	// Check that CheckKeyspaceLocked doesn't return an error.
	err = topo.CheckKeyspaceLocked(ctx, KeyspaceName)
	require.NoError(t, err)

	// We'll now try to acquire the lock from a different thread.
	secondThreadLockAcquired := false
	go func() {
		_, unlock, err := ts.LockKeyspace(context.Background(), KeyspaceName, "TestKeyspaceLocking")
		defer unlock(&err)
		require.NoError(t, err)
		secondThreadLockAcquired = true
	}()

	// Wait for some time and ensure that the second acquiring of lock shard is blocked.
	time.Sleep(100 * time.Millisecond)
	require.False(t, secondThreadLockAcquired)

	// Unlock the keyspace.
	unlock(&err)
	// Check that we no longer have keyspace lock acquired.
	err = topo.CheckKeyspaceLocked(ctx, KeyspaceName)
	require.ErrorContains(t, err, "keyspace customer is not locked (no lockInfo in map)")

	// Wait to see that the second thread was able to acquire the shard lock.
	topoutils.WaitForBoolValue(t, &secondThreadLockAcquired, true)
}

// TestLockingWithTTL tests that locking with the TTL override works as intended.
func TestLockingWithTTL(t *testing.T) {
	// Create the topo server connection.
	ts, err := topo.OpenServer(*clusterInstance.TopoFlavorString(), clusterInstance.VtctlProcess.TopoGlobalAddress, clusterInstance.VtctlProcess.TopoGlobalRoot)
	require.NoError(t, err)

	ctx := context.Background()

	// Acquire a keyspace lock with a short custom TTL.
	ttl := 1 * time.Second
	ctx, unlock, err := ts.LockKeyspace(ctx, KeyspaceName, "TestLockingWithTTL", topo.WithTTL(ttl))
	require.NoError(t, err)
	defer unlock(&err)

	// Check that CheckKeyspaceLocked DOES return an error after waiting more than
	// the specified TTL as we should have lost our lock.
	time.Sleep(ttl * 2)
	err = topo.CheckKeyspaceLocked(ctx, KeyspaceName)
	require.Error(t, err)
}

// TestNamedLocking tests that named locking works as intended.
func TestNamedLocking(t *testing.T) {
	// Create topo server connection.
	ts, err := topo.OpenServer(*clusterInstance.TopoFlavorString(), clusterInstance.VtctlProcess.TopoGlobalAddress, clusterInstance.VtctlProcess.TopoGlobalRoot)
	require.NoError(t, err)

	ctx := context.Background()
	lockName := "TestNamedLocking"
	action := "Testing"

	// Acquire a named lock.
	ctx, unlock, err := ts.LockName(ctx, lockName, action)
	require.NoError(t, err)

	// Check that we can't reacquire it from the same context.
	_, _, err = ts.LockName(ctx, lockName, action)
	require.ErrorContains(t, err, fmt.Sprintf("lock for named %s is already held", lockName))

	// Check that CheckNameLocked doesn't return an error as we should still be
	// holding the lock.
	err = topo.CheckNameLocked(ctx, lockName)
	require.NoError(t, err)

	// We'll now try to acquire the lock from a different goroutine.
	secondCallerAcquired := false
	go func() {
		_, unlock, err := ts.LockName(context.Background(), lockName, action)
		defer unlock(&err)
		require.NoError(t, err)
		secondCallerAcquired = true
	}()

	// Wait for some time and ensure that the second attempt at acquiring the lock
	// is blocked.
	time.Sleep(100 * time.Millisecond)
	require.False(t, secondCallerAcquired)

	// Unlock the name.
	unlock(&err)
	// Check that we no longer have the named lock.
	err = topo.CheckNameLocked(ctx, lockName)
	require.ErrorContains(t, err, fmt.Sprintf("named %s is not locked (no lockInfo in map)", lockName))

	// Wait to see that the second goroutine WAS now able to acquire the named lock.
	topoutils.WaitForBoolValue(t, &secondCallerAcquired, true)
}

func execMulti(t *testing.T, conn *mysql.Conn, query string) []*sqltypes.Result {
	t.Helper()
	var res []*sqltypes.Result
	qr, more, err := conn.ExecuteFetchMulti(query, 1000, true)
	res = append(res, qr)
	require.NoError(t, err)
	for more == true {
		qr, more, _, err = conn.ReadQueryResult(1000, true)
		require.NoError(t, err)
		res = append(res, qr)
	}
	return res
}
