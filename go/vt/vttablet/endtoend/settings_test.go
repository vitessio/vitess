/*
Copyright 2022 The Vitess Authors.

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

package endtoend

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
)

func TestSelectNoConnectionReservationOnSettings(t *testing.T) {
	client := framework.NewClient()
	defer client.Release()

	query := "select @@sql_mode"
	setting := "set @@sql_mode = ''"

	for _, withTx := range []bool{false, true} {
		if withTx {
			err := client.Begin(false)
			require.NoError(t, err)
		}

		qr, err := client.ReserveExecute(query, []string{setting}, nil)
		require.NoError(t, err)
		assert.Zero(t, client.ReservedID())
		assert.Equal(t, `[[VARCHAR("")]]`, fmt.Sprintf("%v", qr.Rows))

		qr, err = client.ReserveStreamExecute(query, []string{setting}, nil)
		require.NoError(t, err)
		assert.Zero(t, client.ReservedID())
		assert.Equal(t, `[[VARCHAR("")]]`, fmt.Sprintf("%v", qr.Rows))
	}
}

func TestSetttingsReuseConnWithSettings(t *testing.T) {
	resetTxConnPool(t)

	client := framework.NewClient()
	defer client.Release()

	connectionIDQuery := "select connection_id()"
	setting := "set @@sql_mode = ''"

	// Create a connection
	connectionIDRes, err := client.BeginExecute(connectionIDQuery, nil, nil)
	require.NoError(t, err)

	// Add settings to the connection
	res, err := client.ReserveExecute(connectionIDQuery, []string{setting}, nil)
	require.NoError(t, err)
	require.Equal(t, connectionIDRes, res)

	// Release the connection back
	err = client.Rollback()
	require.NoError(t, err)

	// We iterate in a loop and try to get a connection with the same settings as before
	// but only 1 at a time. So we expect the same connection to be reused everytime.
	for i := 0; i < 100; i++ {
		res, err = client.ReserveBeginExecute(connectionIDQuery, []string{setting}, nil, nil)
		require.NoError(t, err)
		require.True(t, connectionIDRes.Equal(res))
		err = client.Rollback()
		require.NoError(t, err)
	}

	// Create a new client
	client2 := framework.NewClient()
	defer client2.Release()
	// Ask for a connection with the same settings. This too should be reused.
	res, err = client.ReserveBeginExecute(connectionIDQuery, []string{setting}, nil, nil)
	require.NoError(t, err)
	require.True(t, connectionIDRes.Equal(res))

	// Ask for a connection with the same settings, but the previous connection hasn't been released yet. So this will be a new connection.
	connectionIDRes2, err := client2.ReserveBeginExecute(connectionIDQuery, []string{setting}, nil, nil)
	require.NoError(t, err)
	require.False(t, connectionIDRes.Equal(connectionIDRes2))

	// Release both the connections
	err = client.Rollback()
	require.NoError(t, err)
	err = client2.Rollback()
	require.NoError(t, err)

	// We iterate in a loop and try to get a connection with the same settings as before
	// but only 1 at a time. So we expect the two connections to be reused, and we should be seeing both of them.
	reusedConnection1 := false
	reusedConnection2 := false
	for i := 0; i < 100; i++ {
		res, err = client.ReserveBeginExecute(connectionIDQuery, []string{setting}, nil, nil)
		require.NoError(t, err)
		if connectionIDRes.Equal(res) {
			reusedConnection1 = true
		} else if connectionIDRes2.Equal(res) {
			reusedConnection2 = true
		} else {
			t.Fatalf("The connection should be either of the already created connections")
		}

		err = client.Rollback()
		require.NoError(t, err)
		if reusedConnection2 && reusedConnection1 {
			break
		}
	}
	require.True(t, reusedConnection1)
	require.True(t, reusedConnection2)
}

// resetTxConnPool resets the settings pool by fetching all the connections from the pool with no settings.
// this will make sure that the settings pool connections if any will be taken and settings are reset.
func resetTxConnPool(t *testing.T) {
	txPoolSize := framework.Server.Config().TxPool.Size
	clients := make([]*framework.QueryClient, txPoolSize)
	for i := 0; i < txPoolSize; i++ {
		client := framework.NewClient()
		_, err := client.BeginExecute("select 1", nil, nil)
		require.NoError(t, err)
		clients[i] = client
	}
	for _, client := range clients {
		require.NoError(t,
			client.Release())
	}
}

func TestDDLNoConnectionReservationOnSettings(t *testing.T) {
	client := framework.NewClient()
	defer client.Release()

	query := "create table temp(c_date datetime default '0000-00-00')"
	setting := "set sql_mode='TRADITIONAL'"

	for _, withTx := range []bool{false, true} {
		if withTx {
			err := client.Begin(false)
			require.NoError(t, err)
		}
		_, err := client.ReserveExecute(query, []string{setting}, nil)
		require.Error(t, err, "create table should have failed with TRADITIONAL mode")
		require.Contains(t, err.Error(), "Invalid default value")
		assert.Zero(t, client.ReservedID())
	}
}

func TestDMLNoConnectionReservationOnSettings(t *testing.T) {
	client := framework.NewClient()
	defer client.Release()

	_, err := client.Execute("create table temp(c_date datetime)", nil)
	require.NoError(t, err)
	defer client.Execute("drop table temp", nil)

	_, err = client.Execute("insert into temp values ('2022-08-25')", nil)
	require.NoError(t, err)

	setting := "set sql_mode='TRADITIONAL'"
	queries := []string{
		"insert into temp values('0000-00-00')",
		"update temp set c_date = '0000-00-00'",
	}

	for _, withTx := range []bool{false, true} {
		if withTx {
			err := client.Begin(false)
			require.NoError(t, err)
		}
		for _, query := range queries {
			t.Run(query, func(t *testing.T) {
				_, err = client.ReserveExecute(query, []string{setting}, nil)
				require.Error(t, err, "query should have failed with TRADITIONAL mode")
				require.Contains(t, err.Error(), "Incorrect datetime value")
				assert.Zero(t, client.ReservedID())
			})
		}
	}
}

func TestSelectNoConnectionReservationOnSettingsWithTx(t *testing.T) {
	client := framework.NewClient()

	query := "select @@sql_mode"
	setting := "set @@sql_mode = ''"

	qr, err := client.ReserveBeginExecute(query, []string{setting}, nil, nil)
	require.NoError(t, err)
	assert.Zero(t, client.ReservedID())
	assert.Equal(t, `[[VARCHAR("")]]`, fmt.Sprintf("%v", qr.Rows))
	require.NoError(t,
		client.Release())

	qr, err = client.ReserveBeginStreamExecute(query, []string{setting}, nil, nil)
	require.NoError(t, err)
	assert.Zero(t, client.ReservedID())
	assert.Equal(t, `[[VARCHAR("")]]`, fmt.Sprintf("%v", qr.Rows))
	require.NoError(t,
		client.Release())
}

func TestDDLNoConnectionReservationOnSettingsWithTx(t *testing.T) {
	client := framework.NewClient()
	defer client.Release()

	query := "create table temp(c_date datetime default '0000-00-00')"
	setting := "set sql_mode='TRADITIONAL'"

	_, err := client.ReserveBeginExecute(query, []string{setting}, nil, nil)
	require.Error(t, err, "create table should have failed with TRADITIONAL mode")
	require.Contains(t, err.Error(), "Invalid default value")
	assert.Zero(t, client.ReservedID())
}

func TestDMLNoConnectionReservationOnSettingsWithTx(t *testing.T) {
	client := framework.NewClient()

	_, err := client.Execute("create table temp(c_date datetime)", nil)
	require.NoError(t, err)
	defer client.Execute("drop table temp", nil)

	_, err = client.Execute("insert into temp values ('2022-08-25')", nil)
	require.NoError(t, err)

	setting := "set sql_mode='TRADITIONAL'"
	queries := []string{
		"insert into temp values('0000-00-00')",
		"update temp set c_date = '0000-00-00'",
	}

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			_, err = client.ReserveBeginExecute(query, []string{setting}, nil, nil)
			require.Error(t, err, "query should have failed with TRADITIONAL mode")
			require.Contains(t, err.Error(), "Incorrect datetime value")
			assert.Zero(t, client.ReservedID())
			require.NoError(t,
				client.Release())
		})
	}
}

func TestSetQueryOnReserveApis(t *testing.T) {
	client := framework.NewClient()
	defer client.Release()

	setting := "set @@sql_mode = ''"

	_, err := client.ReserveExecute(setting, []string{setting}, nil)
	require.NoError(t, err)
	assert.Zero(t, client.ReservedID())

	_, err = client.ReserveBeginExecute(setting, []string{setting}, nil, nil)
	require.NoError(t, err)
	assert.Zero(t, client.ReservedID())
}

func TestGetLockQueryOnReserveExecute(t *testing.T) {
	client := framework.NewClient()
	defer client.Release()

	lockQuery := "select get_lock('test', 1)"

	// without settings
	_, err := client.ReserveExecute(lockQuery, nil, nil)
	require.NoError(t, err)
	assert.NotZero(t, client.ReservedID())
	require.NoError(t,
		client.Release())

	// with settings
	_, err = client.ReserveExecute(lockQuery, []string{"set @@sql_mode = ''"}, nil)
	require.NoError(t, err)
	assert.NotZero(t, client.ReservedID())
	require.NoError(t,
		client.Release())
}

func TestTempTableOnReserveExecute(t *testing.T) {
	client := framework.NewClient()
	defer client.Release()
	defer client.Execute("drop table if exists temp", nil)

	tempTblQuery := "create temporary table if not exists temp(id bigint primary key)"

	_, err := client.Execute(tempTblQuery, nil)
	require.Error(t, err)

	_, err = client.ReserveExecute(tempTblQuery, nil, nil)
	require.NoError(t, err)
	assert.NotZero(t, client.ReservedID())
	require.NoError(t,
		client.Release())

	_, err = client.ReserveBeginExecute(tempTblQuery, nil, nil, nil)
	require.NoError(t, err)
	assert.NotZero(t, client.ReservedID())
	require.NoError(t,
		client.Release())

	// drop the table
	_, err = client.Execute("drop table if exists temp", nil)
	require.NoError(t, err)

	// with settings

	tempTblQuery = "create temporary table if not exists temp(c_date datetime default '0000-00-00')"
	setting := "set sql_mode='TRADITIONAL'"

	_, err = client.ReserveExecute(tempTblQuery, []string{setting}, nil)
	require.Error(t, err, "create table should have failed with TRADITIONAL mode")
	require.Contains(t, err.Error(), "Invalid default value")
	assert.NotZero(t, client.ReservedID(), "as this goes through fallback path of reserving a connection due to temporary tables")
	require.NoError(t,
		client.Release())

	_, err = client.ReserveBeginExecute(tempTblQuery, []string{setting}, nil, nil)
	require.Error(t, err, "create table should have failed with TRADITIONAL mode")
	require.Contains(t, err.Error(), "Invalid default value")
	assert.NotZero(t, client.ReservedID(), "as this goes through fallback path of reserving a connection due to temporary tables")
	require.NoError(t,
		client.Release())
}

func TestInfiniteSessions(t *testing.T) {
	client := framework.NewClient()
	qr, err := client.Execute("select @@max_connections", nil)
	require.NoError(t, err)
	maxConn, err := qr.Rows[0][0].ToInt64()
	require.NoError(t, err)

	// twice the number of sessions than the pool size.
	numOfSessions := int(maxConn * 2)

	// read session
	for i := 0; i < numOfSessions; i++ {
		client := framework.NewClient()
		_, err := client.ReserveExecute("select 1", []string{"set sql_mode = ''"}, nil)
		require.NoError(t, err)
	}

	// write session
	for i := 0; i < numOfSessions; i++ {
		client := framework.NewClient()
		_, err := client.ReserveBeginExecute("select 1", []string{"set sql_mode = ''"}, nil, nil)
		require.NoError(t, err)
		require.NoError(t,
			client.Commit())
	}
}

func TestSetQueriesMultipleWays(t *testing.T) {
	framework.Server.Config().EnableSettingsPool = false
	client := framework.NewClient()
	defer client.Release()
	_, err := client.ReserveExecute("select 1", []string{"set sql_safe_updates = 1"}, nil)
	require.NoError(t, err)

	_, err = client.Execute("set sql_safe_updates = 1", nil)
	require.NoError(t, err)

	framework.Server.Config().EnableSettingsPool = true
	client2 := framework.NewClient()
	_, err = client2.ReserveExecute("select 1", []string{"set sql_safe_updates = 1"}, nil)
	require.NoError(t, err)

	// this should not panic.
	_, err = client.Execute("set sql_safe_updates = 1", nil)
	require.NoError(t, err)
}
