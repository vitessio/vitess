/*
Copyright 2020 The Vitess Authors.

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

package reservedconn

import (
	"context"
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/sync2"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

func TestLockUnlock(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	assertMatches(t, conn, `select release_lock('lock name')`, `[[NULL]]`)
	assertMatches(t, conn, `select get_lock('lock name', 2)`, `[[INT64(1)]]`)
	assertMatches(t, conn, `select get_lock('lock name', 2)`, `[[INT64(1)]]`)
	assertMatches(t, conn, `select is_free_lock('lock name')`, `[[INT64(0)]]`)
	assert.NotEmpty(t,
		checkedExec(t, conn, `select is_used_lock('lock name')`))
	assertMatches(t, conn, `select release_lock('lock name')`, `[[INT64(1)]]`)
	assertMatches(t, conn, `select release_all_locks()`, `[[UINT64(1)]]`)
	assertMatches(t, conn, `select release_lock('lock name')`, `[[NULL]]`)
}

func TestLocksDontIntersect(t *testing.T) {
	conn1, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn1.Close()
	conn2, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn2.Close()

	assertMatches(t, conn1, `select get_lock('lock1', 2)`, `[[INT64(1)]]`)
	assertMatches(t, conn2, `select get_lock('lock2', 2)`, `[[INT64(1)]]`)
	assertMatches(t, conn1, `select release_lock('lock1')`, `[[INT64(1)]]`)
	assertMatches(t, conn2, `select release_lock('lock2')`, `[[INT64(1)]]`)
}

func TestLocksIntersect(t *testing.T) {
	conn1, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn1.Close()
	conn2, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn2.Close()

	assertMatches(t, conn1, `select get_lock('lock1', 100)`, `[[INT64(1)]]`)
	assertMatches(t, conn2, `select get_lock('lock2', 100)`, `[[INT64(1)]]`)

	// Locks will not succeed.
	assertMatches(t, conn1, `select get_lock('lock2', 1)`, `[[INT64(0)]]`)
	assertMatches(t, conn2, `select get_lock('lock1', 1)`, `[[INT64(0)]]`)
	assertMatches(t, conn1, `select release_lock('lock2')`, `[[INT64(0)]]`)
	assertMatches(t, conn2, `select release_lock('lock1')`, `[[INT64(0)]]`)

	assertMatches(t, conn1, `select release_lock('lock1')`, `[[INT64(1)]]`)
	assertMatches(t, conn2, `select release_lock('lock2')`, `[[INT64(1)]]`)
}

func TestLocksAreExplicitlyReleaseAndRegrab(t *testing.T) {
	conn1, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn1.Close()
	conn2, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn2.Close()

	assertMatches(t, conn1, `select get_lock('lock', 2)`, `[[INT64(1)]]`)
	assertMatches(t, conn1, `select release_lock('lock')`, `[[INT64(1)]]`)
	assertMatches(t, conn2, `select get_lock('lock', 2)`, `[[INT64(1)]]`)
}

func TestLocksAreReleasedWhenConnectionIsClosed(t *testing.T) {
	conn1, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn1.Close()
	conn2, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn2.Close()

	assertMatches(t, conn1, `select get_lock('lock', 2)`, `[[INT64(1)]]`)
	conn1.Close()

	assertMatches(t, conn2, `select get_lock('lock', 2)`, `[[INT64(1)]]`)
}

func TestLocksBlockEachOther(t *testing.T) {
	conn1, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn1.Close()

	// in the first connection, grab a lock
	assertMatches(t, conn1, `select get_lock('lock', 2)`, `[[INT64(1)]]`)

	released := sync2.NewAtomicBool(false)

	go func() {
		conn2, err := mysql.Connect(context.Background(), &vtParams)
		require.NoError(t, err)
		defer conn2.Close()

		// in the second connection, we try to grab a lock, and should get blocked
		assertMatches(t, conn2, `select get_lock('lock', 2)`, `[[INT64(1)]]`)
		assert.True(t, released.Get(), "was not blocked by get_lock")
		assertMatches(t, conn2, `select release_lock('lock')`, `[[INT64(1)]]`)
	}()

	time.Sleep(1 * time.Second)

	released.Set(true)
	assertMatches(t, conn1, `select release_lock('lock')`, `[[INT64(1)]]`)
}

func TestLocksBlocksWithTx(t *testing.T) {
	conn1, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn1.Close()

	// in the first connection, grab a lock
	assertMatches(t, conn1, `select get_lock('lock', 2)`, `[[INT64(1)]]`)
	exec(t, conn1, "begin")
	exec(t, conn1, "insert into test(id, val1) values(1,'1')") // -80
	exec(t, conn1, "commit")

	released := sync2.NewAtomicBool(false)

	go func() {
		conn2, err := mysql.Connect(context.Background(), &vtParams)
		require.NoError(t, err)
		defer conn2.Close()

		// in the second connection, we try to grab a lock, and should get blocked
		assertMatches(t, conn2, `select get_lock('lock', 2)`, `[[INT64(1)]]`)
		assert.True(t, released.Get(), "was not blocked by get_lock")
		assertMatches(t, conn2, `select release_lock('lock')`, `[[INT64(1)]]`)
	}()

	time.Sleep(1 * time.Second)

	released.Set(true)
	assertMatches(t, conn1, `select release_lock('lock')`, `[[INT64(1)]]`)
	exec(t, conn1, "delete from test")
}

func TestLocksWithTxFailure(t *testing.T) {
	conn1, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn1.Close()

	conn2, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn2.Close()

	// in the first connection, grab a lock for infinite time
	assertMatches(t, conn1, `select get_lock('lock', -1)`, `[[INT64(1)]]`)

	exec(t, conn1, "use `ks:80-`")
	exec(t, conn1, "begin")
	qr := checkedExec(t, conn1, "select connection_id()")
	exec(t, conn1, "use ks")
	// kill the mysql connection shard which has transaction open.
	vttablet1 := clusterInstance.Keyspaces[0].Shards[1].MasterTablet() // 80-
	vttablet1.VttabletProcess.QueryTablet(fmt.Sprintf("kill %s", qr.Rows[0][0].ToString()), keyspaceName, false)

	// transaction fails on commit.
	_, err = conn1.ExecuteFetch("commit", 1, true)
	require.Error(t, err)

	// in the second connection, lock acquisition should fail as first connection still hold the lock though the transaction has failed.
	assertMatches(t, conn2, `select get_lock('lock', 2)`, `[[INT64(0)]]`)
	assertMatches(t, conn2, `select release_lock('lock')`, `[[INT64(0)]]`)
}

func TestLocksWithTxOngoingAndReleaseLock(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	assertMatches(t, conn, `select get_lock('lock', -1)`, `[[INT64(1)]]`)
	exec(t, conn, "begin")
	exec(t, conn, "insert into test(id, val1) values(1,'1')")
	assertMatches(t, conn, `select release_lock('lock')`, `[[INT64(1)]]`)
	assertMatches(t, conn, `select id, val1 from test where id = 1`, `[[INT64(1) VARCHAR("1")]]`)
	exec(t, conn, "rollback")
	assertIsEmpty(t, conn, `select id, val1 from test where id = 1`)
}

func TestLocksWithTxOngoingAndLockFails(t *testing.T) {
	conn1, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn1.Close()

	conn2, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn2.Close()

	assertMatches(t, conn2, `select get_lock('lock', -1)`, `[[INT64(1)]]`)

	exec(t, conn1, "begin")
	exec(t, conn1, "insert into test(id, val1) values(1,'1')")
	assertMatches(t, conn1, `select get_lock('lock', 1)`, `[[INT64(0)]]`)
	assertMatches(t, conn1, `select id, val1 from test where id = 1`, `[[INT64(1) VARCHAR("1")]]`)
	exec(t, conn1, "rollback")
	assertIsEmpty(t, conn1, `select id, val1 from test where id = 1`)

	assertMatches(t, conn2, `select get_lock('lock', -1)`, `[[INT64(1)]]`)
}

func TestLocksKeepLockConnectionActive(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	assertMatches(t, conn, `select get_lock('lock', -1)`, `[[INT64(1)]]`)
	time.Sleep(3 * time.Second)                                      // lock heartbeat time is 2 seconds.
	assertMatches(t, conn, `select * from test where id = 42`, `[]`) // this will trigger heartbeat.
	time.Sleep(3 * time.Second)                                      // lock connection will not timeout after 5 seconds.
	assertMatches(t, conn, `select is_free_lock('lock')`, `[[INT64(0)]]`)

}

func TestLocksResetLockOnTimeout(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	assertMatches(t, conn, `select get_lock('lock', -1)`, `[[INT64(1)]]`)
	time.Sleep(6 * time.Second) // lock connection timeout is 5 seconds.
	_, err = exec(t, conn, `select is_free_lock('lock')`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "held locks released")
	assertMatches(t, conn, `select is_free_lock('lock')`, `[[INT64(1)]]`)
}
