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

package kill

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/utils"
)

// TestKillConnection kills its own connection and checks the error message received.
func TestKillOwnConnection(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	_, err = utils.ExecAllowError(t, conn, fmt.Sprintf("kill %d", conn.ConnectionID))
	require.NoError(t, err)

	// the connection should be closed.
	_, err = utils.ExecAllowError(t, conn, "select 1")
	require.ErrorContains(t, err, "EOF (errno 2013) (sqlstate HY000)")
}

// TestKillDifferentConnection kills different connection and check relevant error messages.
func TestKillDifferentConnection(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	killConn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer killConn.Close()

	// connection does not exist
	_, err = utils.ExecAllowError(t, killConn, "kill 12345")
	require.ErrorContains(t, err, "Unknown thread id: 12345 (errno 1094) (sqlstate HY000)")

	// connection exist
	_, err = utils.ExecAllowError(t, killConn, fmt.Sprintf("kill %d", conn.ConnectionID))
	require.NoError(t, err)

	// executing on closed connection
	_, err = utils.ExecAllowError(t, conn, "select 1")
	require.ErrorContains(t, err, "EOF (errno 2013) (sqlstate HY000)")
}

// TestKillOwnQuery kills the kill statement itself
func TestKillOwnQuery(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	_, err = utils.ExecAllowError(t, conn, fmt.Sprintf("kill query %d", conn.ConnectionID))
	// TODO: does not really change anything, but expect to receive Queery Interrupted error
	//  "(errno 1317) (sqlstate 70100)"
	require.NoError(t, err)
}

// TestKillDifferentConnectionQuery kills query on different connection and check relevant error messages.
func TestKillDifferentConnectionQuery(t *testing.T) {
	setupData(t, false)
	defer dropData(t)

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	killConn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer killConn.Close()

	// killing query on non-existent connection
	_, err = utils.ExecAllowError(t, killConn, "kill query 12345")
	require.ErrorContains(t, err, "Unknown thread id: 12345 (errno 1094) (sqlstate HY000)")

	done := make(chan error)
	go func() {
		// 20 seconds sleep. Should be stopped by kill statement.
		_, err := utils.ExecAllowError(t, conn, "select sleep(20) from test")
		done <- err
	}()

	for {
		select {
		case execErr := <-done:
			require.ErrorContains(t, execErr, "context canceled (errno 1317) (sqlstate 70100)")
			return
		case <-time.After(100 * time.Millisecond):
			_, err = utils.ExecAllowError(t, killConn, fmt.Sprintf("kill query %d", conn.ConnectionID))
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("test did not complete in 5 seconds.")
		}
	}
}

// TestKillOnHungQuery test that any hung query should return.
func TestKillOnHungQuery(t *testing.T) {

	t.Run("connection close", func(t *testing.T) {
		testHungQuery(t, func(conn1 *mysql.Conn, conn2 *mysql.Conn) {
			// closing the hung query connection.
			conn2.Close()
		}, "(errno 2013) (sqlstate HY000)")
	})

	t.Run("connection kill", func(t *testing.T) {
		testHungQuery(t, func(conn1 *mysql.Conn, conn2 *mysql.Conn) {
			// kill the hung connection
			utils.Exec(t, conn1, fmt.Sprintf("kill %d", conn2.ConnectionID))
		}, "context canceled")
	})

	t.Run("query kill", func(t *testing.T) {
		testHungQuery(t, func(conn1 *mysql.Conn, conn2 *mysql.Conn) {
			// kill the hung query
			utils.Exec(t, conn1, fmt.Sprintf("kill query %d", conn2.ConnectionID))
		}, "context canceled")
	})
}

func testHungQuery(t *testing.T, killFunc func(*mysql.Conn, *mysql.Conn), errMsg string) {
	conn1, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn1.Close()

	utils.Exec(t, conn1, "begin")
	utils.Exec(t, conn1, "insert into test(id, msg, extra) values (1, 'a', 'e')")

	conn2, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn2.Close()

	ch := make(chan error)
	go func() {
		ch <- nil
		utils.Exec(t, conn2, "begin")
		_, err := utils.ExecAllowError(t, conn2, "insert into test(id, msg, extra) values (1, 'a', 'e')")
		ch <- err
	}()
	<-ch                        // started
	time.Sleep(1 * time.Second) // just little more time

	killFunc(conn1, conn2)

	select {
	case execErr := <-ch:
		require.ErrorContains(t, execErr, errMsg)
	case <-time.After(5 * time.Second):
		t.Fatal("test did not complete in 5 seconds.")
	}
}

// TestKillStmtOnHugeData tests different kill scenario on huge data.
func TestKillStmtOnHugeData(t *testing.T) {
	setupData(t, true)
	defer dropData(t)

	t.Run("oltp - kill conn", func(t *testing.T) {
		testHugeData(t, "oltp", func(conn *mysql.Conn, killConn *mysql.Conn) {
			utils.ExecAllowError(t, killConn, fmt.Sprintf("kill query %d", conn.ConnectionID))
		}, "context canceled (errno 1317) (sqlstate 70100)")
	})

	t.Run("oltp - kill query", func(t *testing.T) {
		testHugeData(t, "oltp", func(conn *mysql.Conn, killConn *mysql.Conn) {
			utils.ExecAllowError(t, killConn, fmt.Sprintf("kill query %d", conn.ConnectionID))
		}, "(errno 1317) (sqlstate 70100)")
	})

	t.Run("olap - kill conn", func(t *testing.T) {
		testHugeData(t, "olap", func(conn *mysql.Conn, killConn *mysql.Conn) {
			utils.ExecAllowError(t, killConn, fmt.Sprintf("kill query %d", conn.ConnectionID))
		}, "context canceled (errno 1317) (sqlstate 70100)", "EOF (errno 2013) (sqlstate HY000)")
	})

	t.Run("olap - kill query", func(t *testing.T) {
		testHugeData(t, "olap", func(conn *mysql.Conn, killConn *mysql.Conn) {
			utils.ExecAllowError(t, killConn, fmt.Sprintf("kill query %d", conn.ConnectionID))
		}, "context canceled (errno 1317) (sqlstate 70100)", "EOF (errno 2013) (sqlstate HY000)")
	})
}

func testHugeData(t *testing.T, workload string, killFunc func(*mysql.Conn, *mysql.Conn), errMsgs ...string) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	utils.Exec(t, conn, fmt.Sprintf("set workload = %s", workload))

	killConn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer killConn.Close()
	utils.Exec(t, killConn, fmt.Sprintf("set workload = %s", workload))

	ch := make(chan error)
	go func() {
		_, err := utils.ExecWithRowCount(t, conn, "select * from test", 640000)
		ch <- err
	}()

	totalTime := time.After(5 * time.Second)
	for {
		select {
		case execErr := <-ch:
			require.Error(t, execErr)
			if execErr == nil {
				return
			}
			for _, errMsg := range errMsgs {
				if strings.Contains(execErr.Error(), errMsg) {
					return
				}
			}
			require.Failf(t, "error message does not match", "%v does not contain any of %v", execErr.Error(), errMsgs)
			return
		case <-time.After(10 * time.Millisecond):
			killFunc(conn, killConn)
		case <-totalTime:
			t.Fatal("test did not complete in 5 seconds.")
		}
	}
}
