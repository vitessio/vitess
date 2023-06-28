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
	// TODO: expected error "errno 1317) (sqlstate 70100)" as the kill query itself will be interrupted,
	//  that error message should be relayed back first before closing the connection.
	//  Currently, the connection is closed first, so the error message is not relayed back to the client.
	require.ErrorContains(t, err, "EOF (errno 2013) (sqlstate HY000)")

	// the connection should be closed.
	_, err = utils.ExecAllowError(t, conn, "select 1")
	require.ErrorContains(t, err, "EOF (errno 2013) (sqlstate HY000)")

	err = conn.Ping()
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
