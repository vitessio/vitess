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

package consul

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	topoutils "vitess.io/vitess/go/test/endtoend/topotest/utils"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/topo"

	// Registers the consul topology factory so topo.OpenServer can dial it.
	_ "vitess.io/vitess/go/vt/topo/consultopo"
)

var (
	clusterInstance *vitesst.Cluster
	vtParams        mysql.ConnParams
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

func setupCluster(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithTopo("consul"),
		vitesst.WithKeyspace(KeyspaceName).
			WithShardNames("0").
			WithSchema(SchemaSQL).
			WithVSchema(VSchema),
	)
	require.NoError(t, err)
	cleanup, err := cluster.Start(t, ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if cleanupErr := cleanup(cleanupCtx); cleanupErr != nil {
			t.Logf("cluster teardown: %v", cleanupErr)
		}
	})

	clusterInstance = cluster
	vtParams = cluster.VTParams(ctx, "")
}

func TestTopoRestart(t *testing.T) {
	setupCluster(t)
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	execMulti(t, conn, `insert into t1(c1, c2, c3, c4) values (300,100,300,'abc'); ;; insert into t1(c1, c2, c3, c4) values (301,101,301,'abcd');;`)
	assertMatches(t, conn, `select c1,c2,c3 from t1`, `[[INT64(300) INT64(100) INT64(300)] [INT64(301) INT64(101) INT64(301)]]`)

	defer execute(t, conn, `delete from t1`)

	ch := make(chan any)

	go func() {
		clusterInstance.Topo().StopContainer(ctx, 15*time.Second)

		// Some sleep to server few queries when topo is down.
		time.Sleep(400 * time.Millisecond)

		clusterInstance.Topo().StartContainer(ctx)

		// topo is up now.
		ch <- 1
	}()

	timeOut := time.After(15 * time.Second)

	for {
		select {
		case <-ch:
			return
		case <-timeOut:
			require.Fail(t, "timed out - topo process did not come up")
		case <-time.After(100 * time.Millisecond):
			assertMatches(t, conn, `select c1,c2,c3 from t1`, `[[INT64(300) INT64(100) INT64(300)] [INT64(301) INT64(101) INT64(301)]]`)
		}
	}
}

// TestShardLocking tests that shard locking works as intended.
func TestShardLocking(t *testing.T) {
	setupCluster(t)
	// create topo server connection
	addr, err := clusterInstance.Topo().HTTPAddr(t.Context())
	require.NoError(t, err)
	ts, err := topo.OpenServer("consul", addr, "global")
	require.NoError(t, err)

	// Acquire a shard lock.
	ctx, unlock, err := ts.LockShard(t.Context(), KeyspaceName, "0", "TestShardLocking")
	require.NoError(t, err)
	// Check that we can't reacquire it from the same context.
	_, _, err = ts.LockShard(ctx, KeyspaceName, "0", "TestShardLocking")
	require.ErrorContains(t, err, "lock for shard customer/0 is already held")
	// Also check that TryLockShard is non-blocking and returns an error.
	_, _, err = ts.TryLockShard(t.Context(), KeyspaceName, "0", "TestShardLocking")
	require.ErrorContains(t, err, "node already exists: lock already exists at path keyspaces/customer/shards/0")
	// Check that CheckShardLocked doesn't return an error.
	err = topo.CheckShardLocked(ctx, KeyspaceName, "0")
	require.NoError(t, err)

	// We'll now try to acquire the lock from a different thread.
	secondThreadLockAcquired := false
	go func() {
		_, unlock, err := ts.LockShard(t.Context(), KeyspaceName, "0", "TestShardLocking")
		if !assert.NoError(t, err) {
			return
		}
		defer unlock(&err)
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
	setupCluster(t)
	// create topo server connection
	addr, err := clusterInstance.Topo().HTTPAddr(t.Context())
	require.NoError(t, err)
	ts, err := topo.OpenServer("consul", addr, "global")
	require.NoError(t, err)

	// Acquire a keyspace lock.
	ctx, unlock, err := ts.LockKeyspace(t.Context(), KeyspaceName, "TestKeyspaceLocking")
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
		_, unlock, err := ts.LockKeyspace(t.Context(), KeyspaceName, "TestKeyspaceLocking")
		if !assert.NoError(t, err) {
			return
		}
		defer unlock(&err)
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

// TestNamedLocking tests that named locking works as intended.
func TestNamedLocking(t *testing.T) {
	setupCluster(t)
	// Create topo server connection.
	addr, err := clusterInstance.Topo().HTTPAddr(t.Context())
	require.NoError(t, err)
	ts, err := topo.OpenServer("consul", addr, "global")
	require.NoError(t, err)

	ctx := t.Context()
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
		_, unlock, err := ts.LockName(t.Context(), lockName, action)
		if !assert.NoError(t, err) {
			return
		}
		defer unlock(&err)
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

func execute(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err)
	return qr
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

func assertMatches(t *testing.T, conn *mysql.Conn, query, expected string) {
	t.Helper()
	qr := execute(t, conn, query)
	got := fmt.Sprintf("%v", qr.Rows)
	diff := cmp.Diff(expected, got)
	if diff != "" {
		assert.Failf(t, "query mismatch", "Query: %s (-want +got):\n%s", query, diff)
	}
}
